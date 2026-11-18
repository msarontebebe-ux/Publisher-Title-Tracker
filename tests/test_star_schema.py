"""
Star schema: grain, keys, unknown members, load types, latency, idempotent rebuild.
Uses the throwaway test database.
"""

from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine, text

from pipeline.models import ScrapedAuthor, ScrapedPublication
from pipeline.runner import _log_pipeline_run, _write_events, _write_silver
from pipeline.star import parse_location

LNCS = 'Lecture Notes in Computer Science'


def rows(url, sql):
    engine = create_engine(url)
    with engine.connect() as conn:
        out = conn.execute(text(sql)).fetchall()
    engine.dispose()
    return out


def stamp(hours_ago):
    return (datetime.utcnow() - timedelta(hours=hours_ago)).strftime('%Y-%m-%dT%H:%M:%SZ')


def pub(doi, series, publisher, work_type='book', created=None, location=None,
        conf_date=None, authors=('Ada Editor',), title=None, source='crossref'):
    return ScrapedPublication(
        title=title or f'T {doi}', authors=[ScrapedAuthor(name=a) for a in authors], year=2026,
        series=series, publisher=publisher, doi=doi, work_type=work_type, created=created,
        event_location=location, event_date_start=conf_date, source=source)


def refresh():
    from pipeline.db import DatabaseConfig
    from pipeline.gold import compute_dim_publisher
    from pipeline.star import refresh_star
    session = DatabaseConfig.get_session()
    try:
        compute_dim_publisher(session)
        counts = refresh_star(session)
        session.commit()
        return counts
    finally:
        session.close()


@pytest.fixture()
def seeded(db_env):
    daily = pub('10.1007/daily', LNCS, 'Springer Nature Switzerland', 'proceedings', created=stamp(5),
                location='Mainz, Germany', conf_date='2026-10-05')
    backlog = pub('10.1109/old', 'IEEE', 'IEEE', 'proceedings', created=stamp(60 * 24),
                  location='CHENNAI, India', authors=())
    unknown_reg = pub('10.1002/w', 'Wiley', 'Wiley', 'journal-issue', created=None)
    no_type = pub('10.1016/e', 'Elsevier', 'Elsevier', work_type=None, location='Chennai, India')
    japan = pub('10.1145/a', 'ACM', 'ACM', 'proceedings', created=stamp(2), location='Japan')
    tokyo = pub('10.1145/b', 'ACM', 'ACM', 'proceedings', created=stamp(2), location='Tokyo, Japan')
    base = pub('10.1007/base', LNCS, 'Springer Nature Switzerland', created=stamp(900))
    upd = pub('10.1007/upd', LNCS, 'Springer Nature Switzerland', created=stamp(3))
    records = [daily, backlog, unknown_reg, no_type, japan, tokyo, base, upd]
    _write_silver(records, 'mixed')
    for r in records:
        _write_events([r], r.series, stored_before={base.doi} if r is base else None)
    _write_events([pub('10.1007/upd', LNCS, 'Springer Nature Switzerland', created=stamp(3),
                       title='Changed title')], LNCS, stored_before={upd.doi})
    _log_pipeline_run(LNCS, 100, 5, 'success', started_at=datetime.utcnow() - timedelta(seconds=30), run_type='incremental')
    _log_pipeline_run('Wiley', 10, 0, 'partial', error='x', started_at=datetime.utcnow(), run_type='incremental')
    refresh()
    return db_env


# ------------------------------------------------------------------ location parsing
@pytest.mark.parametrize('raw,known,expected', [
    ('Mainz, Germany', (), ('Mainz', 'Germany')),
    ('Santa Barbara, CA, USA', (), ('Santa Barbara', 'USA')),
    ('CHENNAI, India', (), ('Chennai', 'India')),
    ('Montreal, QC, Canada', (), ('Montreal', 'Canada')),
    ('London, UK', (), ('London', 'United Kingdom')),
    ('Istanbul, Türkiye', (), ('Istanbul', 'Turkey')),
    ('Seoul, Korea, Republic of', (), ('Seoul', 'South Korea')),
    ('Tehran, Iran, Islamic Republic of', (), ('Tehran', 'Iran')),
    ('Amsterdam, The Netherlands', (), ('Amsterdam', 'Netherlands')),
    ('Japan', ('Japan',), ('Unknown', 'Japan')),                 # a country seen elsewhere
    ('UK', ('United Kingdom',), ('Unknown', 'United Kingdom')),
    ('Virtual Event', (), ('Virtual Event', 'Virtual')),
    ('Nuremberg', ('Germany',), ('Nuremberg', 'Unknown')),        # one part, not a country: a city
    ('', (), None), (None, (), None), (' , ', (), None),
])
def test_parse_location(raw, known, expected):
    assert parse_location(raw, known) == expected


# --------------------------------------------------------------------- grain and keys
def test_one_fact_row_per_event(seeded):
    assert rows(seeded, 'SELECT COUNT(*) FROM gold.fact_title_event') == rows(seeded, 'SELECT COUNT(*) FROM events')
    assert rows(seeded, 'SELECT COUNT(*) FROM gold.fact_title_event') == [(9,)]    # 8 added/baseline + 1 update
    assert rows(seeded, 'SELECT COUNT(DISTINCT event_id), COUNT(*) FROM gold.fact_title_event')[0][0] == 9


def test_every_key_points_at_a_dimension_member(seeded):
    for dim, pk, col in (('dim_date', 'date_key', 'detected_date_key'), ('dim_date', 'date_key', 'registered_date_key'),
                         ('dim_date', 'date_key', 'conference_date_key'), ('dim_publisher', 'publisher_key', 'publisher_key'),
                         ('dim_series', 'series_key', 'series_key'), ('dim_work_type', 'work_type_key', 'work_type_key'),
                         ('dim_source', 'source_key', 'source_key'), ('dim_event_type', 'event_type_key', 'event_type_key'),
                         ('dim_location', 'location_key', 'location_key'), ('dim_load_type', 'load_type_key', 'load_type_key')):
        orphans = rows(seeded, f'SELECT COUNT(*) FROM gold.fact_title_event f LEFT JOIN gold.{dim} d '
                               f'ON d.{pk} = f.{col} WHERE d.{pk} IS NULL')
        assert orphans == [(0,)], col


def test_missing_values_use_the_unknown_member_not_a_dropped_row(seeded):
    sql = ("SELECT registered_date_key, conference_date_key, location_key, work_type_key, latency_hours "
           "FROM gold.fact_title_event WHERE doi = '{}' AND event_type_key = (SELECT event_type_key "
           "FROM gold.dim_event_type WHERE event_type = 'title.added')")
    assert rows(seeded, sql.format('10.1002/w')) == [(-1, -1, -1, rows(seeded, "SELECT work_type_key FROM gold.dim_work_type WHERE crossref_type='journal-issue'")[0][0], None)]
    assert rows(seeded, sql.format('10.1016/e'))[0][3] == -1                      # work type missing


# ---------------------------------------------------------------- measures and flags
def test_latency_and_flags(seeded):
    (lat, conf, ed), = rows(seeded, "SELECT latency_hours, has_conference_date, has_editors "
                                    "FROM gold.fact_title_event WHERE doi = '10.1007/daily'")
    assert 4.9 <= float(lat) <= 5.2 and (conf, ed) == (1, 1)
    assert rows(seeded, "SELECT has_conference_date, has_editors FROM gold.fact_title_event WHERE doi = '10.1109/old'") == [(0, 0)]


def test_coverage_can_be_computed_by_summing_the_flags(seeded):
    (pct,), = rows(seeded, "SELECT ROUND(100.0 * SUM(has_conference_date) / SUM(event_count), 1) FROM gold.fact_title_event")
    assert float(pct) == round(100.0 * 1 / 9, 1)                                           # only the 'daily' title has a date


# -------------------------------------------------------------------------- load type
def test_load_types(seeded):
    got = dict(rows(seeded, "SELECT f.doi || ':' || et.event_type, lt.load_type FROM gold.fact_title_event f "
                            "JOIN gold.dim_load_type lt USING (load_type_key) JOIN gold.dim_event_type et USING (event_type_key)"))
    assert got['10.1007/daily:title.added'] == 'Daily arrival'
    assert got['10.1109/old:title.added'] == 'Backlog / first load'
    assert got['10.1002/w:title.added'] == 'Registration unknown'
    assert got['10.1007/base:title.baseline'] == 'Baseline (existing title)'
    assert got['10.1007/upd:title.updated'] == 'Change to existing title'


# --------------------------------------------------------------------------- dimensions
def test_dimension_members(seeded):
    assert rows(seeded, "SELECT type_group FROM gold.dim_work_type WHERE crossref_type = 'edited-book'") == [('Book',)]
    assert rows(seeded, "SELECT type_group FROM gold.dim_work_type WHERE crossref_type = 'journal-issue'") == [('Journal issue',)]
    assert {r[0] for r in rows(seeded, "SELECT event_type FROM gold.dim_event_type WHERE counts_as_new")} == {'title.added'}
    assert dict(rows(seeded, 'SELECT source, kind FROM gold.dim_source'))['crossref'] == 'Registration agency'
    series = dict(rows(seeded, 'SELECT series, stream_kind FROM gold.dim_series'))
    assert series[LNCS] == 'Springer series' and series['Wiley'] == 'Publisher stream' and series['Unknown'] == 'Unknown'
    assert rows(seeded, 'SELECT COUNT(*) FROM gold.dim_series WHERE series_key > 0') == [(12,)]   # 8 Springer + 4 publisher streams
    assert rows(seeded, 'SELECT COUNT(*) FROM gold.dim_load_type') == [(5,)]


def test_date_dimension_is_complete_and_correct(seeded):
    assert rows(seeded, 'SELECT COUNT(*) FROM gold.dim_date WHERE date_key > 0') == [(22280,)]   # 1970-01-01 .. 2030-12-31
    assert rows(seeded, 'SELECT full_date, year, quarter, month, month_name, iso_week, day_of_week, is_weekend '
                        'FROM gold.dim_date WHERE date_key = 20261017') == [
        (datetime(2026, 10, 17).date(), 2026, 4, 10, 'October', 42, 6, True)]                    # a Saturday
    assert rows(seeded, 'SELECT full_date FROM gold.dim_date WHERE date_key = -1') == [(None,)]


def test_locations_are_normalised_and_shared(seeded):
    key = lambda doi: rows(seeded, f"SELECT location_key FROM gold.fact_title_event WHERE doi = '{doi}' AND event_count = 1 LIMIT 1")[0][0]
    assert key('10.1109/old') == key('10.1016/e')                                   # 'CHENNAI, India' and 'Chennai, India'
    assert rows(seeded, f"SELECT city, country FROM gold.dim_location WHERE location_key = {key('10.1145/a')}") == [('Unknown', 'Japan')]
    assert rows(seeded, f"SELECT city, country FROM gold.dim_location WHERE location_key = {key('10.1145/b')}") == [('Tokyo', 'Japan')]
    assert rows(seeded, f"SELECT city, country FROM gold.dim_location WHERE location_key = {key('10.1007/daily')}") == [('Mainz', 'Germany')]


# ------------------------------------------------------------------------------ run fact
def test_pipeline_run_fact(seeded):
    assert rows(seeded, 'SELECT COUNT(*) FROM gold.fact_pipeline_run') == [(2,)]
    got = rows(seeded, 'SELECT s.series, f.is_success, f.is_partial, f.records_fetched, f.run_type '
                       'FROM gold.fact_pipeline_run f JOIN gold.dim_series s USING (series_key) ORDER BY 1')
    assert got == [(LNCS, 1, 0, 100, 'incremental'), ('Wiley', 0, 1, 10, 'incremental')]


# --------------------------------------------------------------------------- idempotence
def test_rebuilding_changes_nothing_and_keeps_dimension_keys(seeded):
    snap = lambda: (rows(seeded, 'SELECT COUNT(*) FROM gold.fact_title_event'),
                    rows(seeded, 'SELECT COUNT(*) FROM gold.fact_pipeline_run'),
                    rows(seeded, 'SELECT series, series_key FROM gold.dim_series ORDER BY 1'),
                    rows(seeded, 'SELECT raw_location, location_key FROM gold.location_map ORDER BY 1'),
                    rows(seeded, 'SELECT doi, latency_hours, load_type_key FROM gold.fact_title_event ORDER BY doi, event_type_key'))
    before = snap()
    assert refresh()['new_locations'] == 0
    assert snap() == before


def test_new_events_appear_after_the_next_refresh_and_old_keys_stay(seeded):
    lncs_key = rows(seeded, f"SELECT series_key FROM gold.dim_series WHERE series = '{LNCS}'")
    new = pub('10.1007/new', LNCS, 'Springer Nature Singapore', created=stamp(1), location='Kyoto, Japan')
    _write_silver([new], LNCS)
    _write_events([new], LNCS)
    refresh()
    assert rows(seeded, 'SELECT COUNT(*) FROM gold.fact_title_event') == [(10,)]
    assert rows(seeded, f"SELECT series_key FROM gold.dim_series WHERE series = '{LNCS}'") == lncs_key
    assert ('Springer', 'Springer Nature Singapore') in rows(seeded, 'SELECT publisher, imprint FROM gold.dim_publisher')


def test_refresh_gold_rebuilds_the_star(db_env):
    from pipeline.gold import refresh_gold
    r = pub('10.1007/x', LNCS, 'Springer Nature Switzerland', created=stamp(4))
    _write_silver([r], LNCS)
    _write_events([r], LNCS)
    refresh_gold(series_filter=LNCS)
    assert rows(db_env, 'SELECT COUNT(*) FROM gold.fact_title_event') == [(1,)]


def test_raw_tables_are_never_changed(seeded):
    before = rows(seeded, 'SELECT COUNT(*), MAX(id) FROM events'), rows(seeded, 'SELECT COUNT(*) FROM publications')
    refresh()
    assert (rows(seeded, 'SELECT COUNT(*), MAX(id) FROM events'), rows(seeded, 'SELECT COUNT(*) FROM publications')) == before
