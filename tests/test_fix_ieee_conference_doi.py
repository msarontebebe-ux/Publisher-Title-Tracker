"""The one-off correction of the IEEE conference stored under the DOI '10'."""

from sqlalchemy import create_engine, text

from pipeline.events import payload_hash
from pipeline.models import ScrapedAuthor, ScrapedPublication
from pipeline.runner import _write_events, _write_silver

URL = 'https://ieeexplore.ieee.org/xpl/conhome/11732832/proceeding'


def rows(url, sql):
    engine = create_engine(url)
    with engine.connect() as conn:
        out = conn.execute(text(sql)).fetchall()
    engine.dispose()
    return out


def seed(doi='10'):
    pub = ScrapedPublication(title='PCIM Conference 2026', authors=[ScrapedAuthor(name='A')],
                             year=2026, series='IEEE', publisher='IEEE', doi=doi, ee=URL,
                             work_type='proceedings', source='ieee-xplore')
    _write_silver([pub], 'IEEE')
    _write_events([pub], 'IEEE')


def test_dry_run_changes_nothing(db_env):
    from fix_ieee_conference_doi import run
    seed()
    r = run(dry_run=True)
    assert (r['found'], r['new_id'], r['events'], r['applied']) == (True, 'ieee:conf:11732832', 1, False)
    assert rows(db_env, 'SELECT doi FROM publications') == [('10',)]
    assert rows(db_env, 'SELECT source_record_id FROM events') == [('10',)]


def test_title_and_event_are_renamed_together_and_the_fingerprint_still_matches(db_env):
    from fix_ieee_conference_doi import run
    seed()
    assert run()['applied'] is True
    assert rows(db_env, 'SELECT doi FROM publications') == [('ieee:conf:11732832',)]
    (rid, etype, payload, phash), = rows(
        db_env, 'SELECT source_record_id, event_type, payload, payload_hash FROM events')
    assert (rid, etype, payload['doi']) == ('ieee:conf:11732832', 'title.added', 'ieee:conf:11732832')
    assert phash == payload_hash(payload)


def test_the_same_record_later_under_the_new_id_is_not_a_change(db_env):
    from fix_ieee_conference_doi import run
    seed()
    run()
    pub = ScrapedPublication(title='PCIM Conference 2026', authors=[ScrapedAuthor(name='A')],
                             year=2026, series='IEEE', publisher='IEEE', doi='ieee:conf:11732832',
                             ee=URL, work_type='proceedings', source='ieee-xplore')
    r = _write_events([pub], 'IEEE', stored_before={'ieee:conf:11732832'})
    assert (r.added, r.updated, r.unchanged) == (0, 0, 1)


def test_running_it_twice_is_harmless_and_a_missing_title_is_fine(db_env):
    from fix_ieee_conference_doi import run
    assert run()['found'] is False
    seed()
    run()
    assert run()['found'] is False
    assert rows(db_env, 'SELECT COUNT(*) FROM events') == [(1,)]


def test_it_refuses_to_overwrite_an_existing_identifier(db_env):
    from fix_ieee_conference_doi import run
    seed()
    seed('ieee:conf:11732832')
    r = run()
    assert r['applied'] is False and 'already exists' in r['error']
    assert rows(db_env, "SELECT COUNT(*) FROM publications WHERE doi = '10'") == [(1,)]
