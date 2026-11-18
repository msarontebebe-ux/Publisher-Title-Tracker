"""
Gold layer computation — runs after every silver upsert.

Computes the aggregation tables in the gold schema:
  gold.series_stats    — per-series totals and averages
  gold.yearly_trends   — publication counts per year per series
  gold.top_authors     — most published authors per series
  gold.dim_publisher   — publisher dimension (publisher > imprint)
  gold.publisher_map   — (stream, raw publisher name) -> publisher_key
"""

from datetime import datetime
from sqlalchemy import text
from pipeline.db import DatabaseConfig
from pipeline.models import GoldSeriesStat, GoldYearlyTrend, GoldTopAuthor


def compute_series_stats(session, series_filter: str = None):
    """
    Recompute gold.series_stats.
    If series_filter is given, only recomputes that series (faster after incremental scrape).
    """
    where = f"WHERE p.series = :series" if series_filter else "WHERE p.series IS NOT NULL"
    params = {'series': series_filter} if series_filter else {}

    sql = text(f"""
        SELECT
            p.series,
            COUNT(DISTINCT p.id)          AS total_publications,
            COUNT(DISTINCT pa.author_id)  AS total_unique_authors,
            MIN(p.year)                   AS year_min,
            MAX(p.year)                   AS year_max,
            ROUND(
                CAST(COUNT(pa.author_id) AS NUMERIC) /
                NULLIF(COUNT(DISTINCT p.id), 0), 2
            )                             AS avg_authors_per_paper
        FROM publications p
        LEFT JOIN publication_authors pa ON p.id = pa.publication_id
        {where}
        GROUP BY p.series
    """)

    rows = session.execute(sql, params).fetchall()
    now  = datetime.utcnow()

    for row in rows:
        stat = session.query(GoldSeriesStat).filter_by(series=row.series).first()
        if stat:
            stat.total_publications    = row.total_publications
            stat.total_unique_authors  = row.total_unique_authors
            stat.year_min              = row.year_min
            stat.year_max              = row.year_max
            stat.avg_authors_per_paper = row.avg_authors_per_paper
            stat.computed_at           = now
        else:
            session.add(GoldSeriesStat(
                series=row.series,
                total_publications=row.total_publications,
                total_unique_authors=row.total_unique_authors,
                year_min=row.year_min,
                year_max=row.year_max,
                avg_authors_per_paper=row.avg_authors_per_paper,
                computed_at=now,
            ))


def compute_yearly_trends(session, series_filter: str = None):
    """
    Recompute gold.yearly_trends.
    """
    where = "WHERE p.series = :series AND p.year IS NOT NULL" if series_filter \
            else "WHERE p.series IS NOT NULL AND p.year IS NOT NULL"
    params = {'series': series_filter} if series_filter else {}

    sql = text(f"""
        SELECT series, year, COUNT(*) AS publication_count
        FROM publications p
        {where}
        GROUP BY series, year
        ORDER BY series, year
    """)

    rows = session.execute(sql, params).fetchall()
    now  = datetime.utcnow()

    for row in rows:
        trend = session.query(GoldYearlyTrend).filter_by(
            series=row.series, year=row.year
        ).first()
        if trend:
            trend.publication_count = row.publication_count
            trend.computed_at       = now
        else:
            session.add(GoldYearlyTrend(
                series=row.series,
                year=row.year,
                publication_count=row.publication_count,
                computed_at=now,
            ))


def compute_top_authors(session, series_filter: str = None, top_n: int = 50):
    """
    Recompute gold.top_authors (top N per series).
    Deletes existing rows for the series before reinserting.
    """
    where = "WHERE p.series = :series" if series_filter else "WHERE p.series IS NOT NULL"
    params = {'series': series_filter} if series_filter else {}

    # Delete stale rows
    if series_filter:
        session.query(GoldTopAuthor).filter_by(series=series_filter).delete()
    else:
        session.query(GoldTopAuthor).delete()

    sql = text(f"""
        SELECT
            p.series,
            a.name AS author_name,
            COUNT(DISTINCT p.id) AS publication_count
        FROM publications p
        JOIN publication_authors pa ON p.id = pa.publication_id
        JOIN authors a              ON a.id  = pa.author_id
        {where}
        GROUP BY p.series, a.name
        ORDER BY p.series, publication_count DESC
    """)

    rows    = session.execute(sql, params).fetchall()
    now     = datetime.utcnow()
    seen: dict[str, int] = {}   # series → count of authors already inserted

    for row in rows:
        seen[row.series] = seen.get(row.series, 0)
        if seen[row.series] >= top_n:
            continue
        session.add(GoldTopAuthor(
            series=row.series,
            author_name=row.author_name,
            publication_count=row.publication_count,
            computed_at=now,
        ))
        seen[row.series] += 1


def compute_dim_publisher(session) -> int:
    """
    Keep the publisher dimension complete: every (stream, raw publisher name) pair found
    in publications or events gets a clean publisher (from the stream, see
    pipeline.config.publisher_of) and an imprint (the raw name). Only adds; never changes
    or removes, so it is safe to run after every load. Returns how many pairs were new.
    """
    from pipeline.config import load_publisher_streams, publisher_of

    streams = load_publisher_streams()
    pairs = session.execute(text("""
        SELECT series, publisher FROM publications WHERE series IS NOT NULL AND publisher IS NOT NULL
        UNION
        SELECT series, publisher FROM events WHERE series IS NOT NULL AND publisher IS NOT NULL
    """)).fetchall()

    added = 0
    for series, raw in pairs:
        key = session.execute(text("""
            INSERT INTO gold.dim_publisher (publisher, imprint) VALUES (:p, :i)
            ON CONFLICT (publisher, imprint) DO UPDATE SET imprint = EXCLUDED.imprint
            RETURNING publisher_key
        """), {'p': publisher_of(series, streams), 'i': raw}).scalar()
        added += session.execute(text("""
            INSERT INTO gold.publisher_map (series, raw_publisher, publisher_key)
            VALUES (:s, :r, :k) ON CONFLICT (series, raw_publisher) DO NOTHING
        """), {'s': series, 'r': raw, 'k': key}).rowcount
    return added


def compute_quality_metrics(session, series_filter: str = None):
    """
    Compute per-series data quality percentages and write to quality_metrics.

    Tracks four dimensions:
      doi_coverage_pct        — should always be 100% (DOI required for insert)
      year_coverage_pct       — % of records that have a publication year
      event_date_coverage_pct — % of records that have a conference start date
      author_coverage_pct     — % of records linked to at least one author
    """
    from pipeline.models import QualityMetric

    where  = "WHERE p.series = :series" if series_filter else "WHERE p.series IS NOT NULL"
    params = {'series': series_filter} if series_filter else {}

    sql = text(f"""
        SELECT
            p.series,
            COUNT(*) AS total_records,
            ROUND(100.0 * SUM(CASE WHEN p.doi              IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS doi_pct,
            ROUND(100.0 * SUM(CASE WHEN p.event_date_start IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS event_date_pct,
            ROUND(100.0 * SUM(CASE WHEN p.event_year       IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS event_year_pct,
            ROUND(100.0 * SUM(CASE WHEN ac.auth_count > 0  THEN 1 ELSE 0 END) / COUNT(*), 2) AS author_pct
        FROM publications p
        LEFT JOIN (
            SELECT publication_id, COUNT(*) AS auth_count
            FROM publication_authors
            GROUP BY publication_id
        ) ac ON p.id = ac.publication_id
        {where}
        GROUP BY p.series
    """)

    rows = session.execute(sql, params).fetchall()
    now  = datetime.utcnow()

    for row in rows:
        existing = session.query(QualityMetric).filter_by(series=row.series).first()
        if existing:
            existing.total_records           = row.total_records
            existing.doi_coverage_pct        = row.doi_pct
            existing.event_date_coverage_pct = row.event_date_pct
            existing.event_year_coverage_pct = row.event_year_pct
            existing.author_coverage_pct     = row.author_pct
            existing.validated_at            = now
        else:
            session.add(QualityMetric(
                series=row.series,
                total_records=row.total_records,
                doi_coverage_pct=row.doi_pct,
                event_date_coverage_pct=row.event_date_pct,
                event_year_coverage_pct=row.event_year_pct,
                author_coverage_pct=row.author_pct,
                validated_at=now,
            ))


# Gold tables are derived from `publications`, but every compute_* function above
# only adds or updates rows. A series that stops existing in `publications`
# (renamed, merged into another, or no longer collected) would keep its old gold
# rows forever and the API would keep listing a series with no titles. Each rule
# says when a row has lost all the data it was computed from.
_NO_SERIES = 'NOT EXISTS (SELECT 1 FROM publications p WHERE p.series = t.series)'
_ORPHAN_RULES = (
    ('gold.series_stats', _NO_SERIES),
    ('gold.top_authors', _NO_SERIES),
    ('quality_metrics', _NO_SERIES),
    # per (series, year): also drops a year that no longer has any title
    ('gold.yearly_trends', 'NOT EXISTS (SELECT 1 FROM publications p '
                           'WHERE p.series = t.series AND p.year = t.year)'),
)


def prune_orphans(session) -> int:
    """
    Delete gold/quality rows that no publication backs any more; returns how many.

    Safe to run on every refresh (the scheduled jobs refresh one series at a time,
    so this cannot wait for a full rebuild): a row is only removed when nothing in
    `publications` could have produced it. Rows with a NULL series are left alone.
    The table names are constants above, never user input.
    """
    removed = 0
    for table, rule in _ORPHAN_RULES:
        result = session.execute(text(
            f'DELETE FROM {table} AS t WHERE t.series IS NOT NULL AND {rule}'))
        removed += result.rowcount or 0
    return removed


def refresh_gold(series_filter: str = None):
    """
    Recompute all gold tables + quality metrics.
    Call this after every silver upsert. Also removes rows left behind by series
    that no longer have any publications (see prune_orphans), whatever the filter.

    Args:
        series_filter: if given, only recomputes that series (e.g. 'Lecture Notes in Computer Science')
    """
    label = series_filter or 'ALL series'
    print(f"  Gold: computing aggregations for {label}...")

    session = DatabaseConfig.get_session()
    try:
        compute_series_stats(session,     series_filter)
        compute_yearly_trends(session,    series_filter)
        compute_top_authors(session,      series_filter)
        compute_quality_metrics(session,  series_filter)
        compute_dim_publisher(session)
        pruned = prune_orphans(session)
        session.commit()
        if pruned:
            print(f"  Gold: removed {pruned} stale row(s) with no publications behind them")
        print(f"  Gold: done")
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == '__main__':
    # Run manually to rebuild all gold tables from scratch
    import os
    os.environ.setdefault('PYTHONIOENCODING', 'utf-8')
    print("Rebuilding gold layer...")
    refresh_gold()
    print("Done.")
