"""
Star schema (multidimensional model) - fills the dimensions and rebuilds the fact tables.
Tables: pipeline/models.py (gold schema).

  * Dimensions keep stable keys: members are only inserted, never removed. Key -1 is the
    "Unknown" member of a dimension.
  * The two fact tables are rebuilt from `events`, `publications` and `pipeline_runs` on every
    call (TRUNCATE + INSERT ... SELECT in the caller's transaction), so they always equal the
    source tables and a repeated call changes nothing. Their surrogate keys are not stable.

Called at the end of pipeline.gold.refresh_gold. Raw tables are only read, never changed.
"""

import re
from typing import Dict, Iterable, Optional, Tuple

from sqlalchemy import text

BACKLOG_DAYS = 14               # a title first reported later than this after registration is "Backlog"
DATE_FIRST, DATE_LAST = '1970-01-01', '2030-12-31'

TYPE_GROUPS = {
    'proceedings': 'Proceedings',
    'book': 'Book', 'edited-book': 'Book', 'monograph': 'Book',
    'journal-issue': 'Journal issue', 'journal': 'Journal issue',
}

LOAD_TYPES = {
    1: 'Daily arrival',
    2: 'Backlog / first load',
    3: 'Registration unknown',
    4: 'Baseline (existing title)',
    5: 'Change to existing title',
}

# Spellings of the same country seen in CrossRef / IEEE data (keys are lower case).
COUNTRY_ALIASES = {
    'usa': 'USA', 'us': 'USA', 'u.s.a.': 'USA', 'united states': 'USA',
    'united states of america': 'USA',
    'uk': 'United Kingdom', 'u.k.': 'United Kingdom', 'england': 'United Kingdom',
    'great britain': 'United Kingdom',
    'turkiye': 'Turkey', 'türkiye': 'Turkey',
    'russian federation': 'Russia',
    'korea': 'South Korea', 'republic of korea': 'South Korea', 'korea, republic of': 'South Korea',
    'iran, islamic republic of': 'Iran',
    'viet nam': 'Vietnam', 'czechia': 'Czech Republic', 'uae': 'United Arab Emirates',
    'the netherlands': 'Netherlands',
}
_SUFFIXES = ('republic of', 'islamic republic of')


# ------------------------------------------------------------------ location parsing
def _country(name: str) -> str:
    name = name.strip()
    return COUNTRY_ALIASES.get(name.lower(), name)


def _tidy(city: str) -> str:
    """'CHENNAI' and 'chennai' become 'Chennai'; mixed case is left as it is."""
    return city.title() if city.isupper() or city.islower() else city


def parse_location(raw: Optional[str], known_countries: Iterable[str] = ()) -> Optional[Tuple[str, str]]:
    """
    'Mainz, Germany' -> ('Mainz', 'Germany'); 'Santa Barbara, CA, USA' -> ('Santa Barbara', 'USA');
    'Seoul, Korea, Republic of' -> ('Seoul', 'South Korea'). A single part is a country when it is
    one that appears elsewhere in the data ('Japan' -> ('Unknown', 'Japan')), a virtual event
    ('Virtual Event' -> ('Virtual Event', 'Virtual')), otherwise a city ('Nuremberg' ->
    ('Nuremberg', 'Unknown')). None when there is nothing to parse.
    """
    parts = [p.strip() for p in (raw or '').split(',') if p.strip()]
    if not parts:
        return None
    if len(parts) >= 2:
        if parts[-1].lower() in _SUFFIXES and len(parts) >= 2:
            country = _country(f'{parts[-2]}, {parts[-1]}')
            city = parts[0] if len(parts) >= 3 else 'Unknown'
        else:
            country, city = _country(parts[-1]), parts[0]
        return (_tidy(city), country)
    only = parts[0]
    if _country(only) in set(known_countries):
        return ('Unknown', _country(only))
    if 'virtual' in only.lower():
        return (only, 'Virtual')
    return (_tidy(only), 'Unknown')


# ------------------------------------------------------------------------ dimensions
def _ensure_dates(session) -> None:
    """dim_date: one row per day 1970-2030 (generated once, plain date arithmetic so the session
    time zone and daylight saving cannot drop or repeat a day) plus the Unknown member."""
    session.execute(text("""
        INSERT INTO gold.dim_date (date_key, full_date) VALUES (-1, NULL) ON CONFLICT DO NOTHING"""))
    done = session.execute(text('SELECT 1 FROM gold.dim_date WHERE date_key = :k'),
                           {'k': int(DATE_LAST.replace('-', ''))}).fetchone()
    if done:
        return
    session.execute(text("""
        INSERT INTO gold.dim_date (date_key, full_date, year, quarter, month, month_name,
                                   iso_week, day_of_week, is_weekend)
        SELECT to_char(d, 'YYYYMMDD')::int, d, EXTRACT(year FROM d)::int,
               EXTRACT(quarter FROM d)::int, EXTRACT(month FROM d)::int, trim(to_char(d, 'Month')),
               EXTRACT(week FROM d)::int, EXTRACT(isodow FROM d)::int, EXTRACT(isodow FROM d) >= 6
        FROM (SELECT CAST(:a AS date) + g.i AS d
              FROM generate_series(0, CAST(:b AS date) - CAST(:a AS date)) AS g(i)) AS days
        ON CONFLICT DO NOTHING"""), {'a': DATE_FIRST, 'b': DATE_LAST})


def _fill_publisher_unknown(session) -> None:
    session.execute(text("""
        INSERT INTO gold.dim_publisher (publisher_key, publisher, imprint)
        VALUES (-1, 'Unknown', 'Unknown') ON CONFLICT DO NOTHING"""))


def _fill_series(session) -> None:
    from pipeline.config import load_config, publisher_of, load_publisher_streams
    config = load_config()
    streams = load_publisher_streams()
    rows = {}
    for s in config.get('springer_series', []):
        rows[s['name']] = (s['abbreviation'], 'Springer', 'Springer series')
    for p in config.get('publishers', []):
        rows[p['name']] = (p['abbreviation'], p['name'], 'Publisher stream')
    seen = session.execute(text("""
        SELECT series FROM events WHERE series IS NOT NULL
        UNION SELECT series FROM publications WHERE series IS NOT NULL
        UNION SELECT series FROM pipeline_runs WHERE series IS NOT NULL""")).fetchall()
    for (name,) in seen:
        rows.setdefault(name, (None, publisher_of(name, streams), 'Other stream'))
    for name, (abbr, publisher, kind) in rows.items():
        session.execute(text("""
            INSERT INTO gold.dim_series (series, abbreviation, publisher, stream_kind)
            VALUES (:s, :a, :p, :k)
            ON CONFLICT (series) DO UPDATE SET abbreviation = EXCLUDED.abbreviation,
                publisher = EXCLUDED.publisher, stream_kind = EXCLUDED.stream_kind"""),
            {'s': name, 'a': abbr, 'p': publisher, 'k': kind})
    session.execute(text("""
        INSERT INTO gold.dim_series (series_key, series, publisher, stream_kind)
        VALUES (-1, 'Unknown', 'Unknown', 'Unknown') ON CONFLICT DO NOTHING"""))


def _fill_work_types(session) -> None:
    types = {r[0] for r in session.execute(text(
        'SELECT DISTINCT work_type FROM publications WHERE work_type IS NOT NULL'))}
    for t in types | set(TYPE_GROUPS):
        session.execute(text("""
            INSERT INTO gold.dim_work_type (crossref_type, type_group) VALUES (:t, :g)
            ON CONFLICT (crossref_type) DO UPDATE SET type_group = EXCLUDED.type_group"""),
            {'t': t, 'g': TYPE_GROUPS.get(t, 'Other')})
    session.execute(text("""
        INSERT INTO gold.dim_work_type (work_type_key, crossref_type, type_group)
        VALUES (-1, 'Unknown', 'Other') ON CONFLICT DO NOTHING"""))


def _fill_sources(session) -> None:
    sources = {r[0] for r in session.execute(text(
        'SELECT DISTINCT source FROM events WHERE source IS NOT NULL'))}
    for s in sources | {'crossref'}:
        session.execute(text("""
            INSERT INTO gold.dim_source (source, kind) VALUES (:s, :k)
            ON CONFLICT (source) DO UPDATE SET kind = EXCLUDED.kind"""),
            {'s': s, 'k': 'Registration agency' if s == 'crossref' else 'Publisher API'})
    session.execute(text("""
        INSERT INTO gold.dim_source (source_key, source, kind) VALUES (-1, 'Unknown', 'Unknown')
        ON CONFLICT DO NOTHING"""))


def _fill_event_types(session) -> None:
    kinds = {r[0] for r in session.execute(text(
        'SELECT DISTINCT event_type FROM events WHERE event_type IS NOT NULL'))}
    for k in kinds | {'title.added', 'title.updated', 'title.baseline'}:
        session.execute(text("""
            INSERT INTO gold.dim_event_type (event_type, counts_as_new) VALUES (:k, :n)
            ON CONFLICT (event_type) DO UPDATE SET counts_as_new = EXCLUDED.counts_as_new"""),
            {'k': k, 'n': k == 'title.added'})
    session.execute(text("""
        INSERT INTO gold.dim_event_type (event_type_key, event_type, counts_as_new)
        VALUES (-1, 'Unknown', false) ON CONFLICT DO NOTHING"""))


def _fill_load_types(session) -> None:
    for key, name in LOAD_TYPES.items():
        session.execute(text("""
            INSERT INTO gold.dim_load_type (load_type_key, load_type) VALUES (:k, :n)
            ON CONFLICT (load_type_key) DO UPDATE SET load_type = EXCLUDED.load_type"""),
            {'k': key, 'n': name})


def _fill_locations(session) -> int:
    """dim_location + location_map. Only raw strings that are not mapped yet are processed."""
    session.execute(text("""
        INSERT INTO gold.dim_location (location_key, city, country, raw_location)
        VALUES (-1, 'Unknown', 'Unknown', NULL) ON CONFLICT DO NOTHING"""))
    raws = [r[0] for r in session.execute(text(
        'SELECT DISTINCT event_location FROM publications WHERE event_location IS NOT NULL'))]
    # Countries seen with a city in front of them: a one-part value such as 'Japan' is one of these.
    known = {parse_location(r)[1] for r in raws if len([p for p in r.split(',') if p.strip()]) >= 2
             and parse_location(r)}
    mapped = {r[0] for r in session.execute(text('SELECT raw_location FROM gold.location_map'))}
    new = [r for r in raws if r not in mapped]
    for raw in new:
        parsed = parse_location(raw, known)
        if parsed is None:
            continue
        city, country = parsed
        key = session.execute(text("""
            INSERT INTO gold.dim_location (city, country, raw_location) VALUES (:c, :k, :r)
            ON CONFLICT (city, country) DO UPDATE SET city = EXCLUDED.city
            RETURNING location_key"""), {'c': city, 'k': country, 'r': raw}).scalar()
        session.execute(text("""
            INSERT INTO gold.location_map (raw_location, location_key) VALUES (:r, :k)
            ON CONFLICT (raw_location) DO NOTHING"""), {'r': raw, 'k': key})
    return len(new)


# ------------------------------------------------------------------------------ facts
FACT_TITLE_EVENT_SQL = """
WITH base AS (
    SELECT e.event_id, e.source_record_id AS doi, e.event_type, e.source, e.series,
           e.publisher, e.detected_at, p.work_type, p.event_location,
           p.event_date_start AS conference_date,
           COALESCE(e.registered_at, p.registered_at) AS registered_at,
           (p.event_date_start IS NOT NULL)::int AS has_conference_date,
           (EXISTS (SELECT 1 FROM publication_authors pa WHERE pa.publication_id = p.id))::int AS has_editors
    FROM events e LEFT JOIN publications p ON p.doi = e.source_record_id
), calc AS (
    SELECT base.*, CASE WHEN registered_at IS NULL THEN NULL
                        ELSE EXTRACT(EPOCH FROM (detected_at - registered_at)) / 3600 END AS latency
    FROM base
)
INSERT INTO gold.fact_title_event
    (event_id, doi, detected_date_key, registered_date_key, conference_date_key, publisher_key,
     series_key, work_type_key, source_key, event_type_key, location_key, load_type_key,
     event_count, latency_hours, has_conference_date, has_editors)
SELECT c.event_id, c.doi,
       COALESCE(dd.date_key, -1), COALESCE(dr.date_key, -1), COALESCE(dc.date_key, -1),
       COALESCE(pm.publisher_key, -1), COALESCE(ds.series_key, -1), COALESCE(wt.work_type_key, -1),
       COALESCE(src.source_key, -1), COALESCE(et.event_type_key, -1), COALESCE(lm.location_key, -1),
       CASE WHEN c.event_type = 'title.baseline' THEN 4
            WHEN c.event_type = 'title.updated' THEN 5
            WHEN c.latency IS NULL THEN 3
            WHEN c.latency <= :backlog_hours THEN 1
            ELSE 2 END,
       1, ROUND(CAST(c.latency AS numeric), 2), c.has_conference_date, c.has_editors
FROM calc c
LEFT JOIN gold.dim_date dd ON dd.date_key = to_char(c.detected_at, 'YYYYMMDD')::int
LEFT JOIN gold.dim_date dr ON dr.date_key = to_char(c.registered_at, 'YYYYMMDD')::int
LEFT JOIN gold.dim_date dc ON dc.date_key = to_char(c.conference_date, 'YYYYMMDD')::int
LEFT JOIN gold.publisher_map pm ON pm.series = c.series AND pm.raw_publisher = c.publisher
LEFT JOIN gold.dim_series ds ON ds.series = c.series
LEFT JOIN gold.dim_work_type wt ON wt.crossref_type = c.work_type
LEFT JOIN gold.dim_source src ON src.source = c.source
LEFT JOIN gold.dim_event_type et ON et.event_type = c.event_type
LEFT JOIN gold.location_map lm ON lm.raw_location = c.event_location
"""

FACT_PIPELINE_RUN_SQL = """
INSERT INTO gold.fact_pipeline_run
    (run_date_key, series_key, run_type, runs, records_fetched, records_new,
     duration_seconds, is_success, is_partial)
SELECT COALESCE(dd.date_key, -1), COALESCE(ds.series_key, -1), pr.run_type, 1,
       pr.records_fetched, pr.records_new,
       CASE WHEN pr.started_at IS NULL OR pr.completed_at IS NULL THEN NULL
            ELSE ROUND(EXTRACT(EPOCH FROM (pr.completed_at - pr.started_at)))::int END,
       (pr.status = 'success')::int, (pr.status = 'partial')::int
FROM pipeline_runs pr
LEFT JOIN gold.dim_date dd ON dd.date_key = to_char(pr.started_at, 'YYYYMMDD')::int
LEFT JOIN gold.dim_series ds ON ds.series = pr.series
"""


def refresh_star(session) -> Dict[str, int]:
    """Fill the dimensions, rebuild both fact tables. The caller commits. Returns row counts."""
    _ensure_dates(session)
    _fill_publisher_unknown(session)
    _fill_series(session)
    _fill_work_types(session)
    _fill_sources(session)
    _fill_event_types(session)
    _fill_load_types(session)
    new_locations = _fill_locations(session)

    session.execute(text('TRUNCATE gold.fact_title_event, gold.fact_pipeline_run RESTART IDENTITY'))
    session.execute(text(FACT_TITLE_EVENT_SQL), {'backlog_hours': BACKLOG_DAYS * 24})
    session.execute(text(FACT_PIPELINE_RUN_SQL))
    return {
        'fact_title_event': session.execute(text('SELECT COUNT(*) FROM gold.fact_title_event')).scalar(),
        'fact_pipeline_run': session.execute(text('SELECT COUNT(*) FROM gold.fact_pipeline_run')).scalar(),
        'new_locations': new_locations,
    }
