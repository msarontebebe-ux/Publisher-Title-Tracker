"""
CrossRef registration time (registered_at): parsed, stored on titles and events, kept
out of the fingerprint, never overwritten, and backfilled for old rows.
"""

from datetime import datetime

import pytest
from sqlalchemy import create_engine, text

from pipeline.events import payload_hash
from pipeline.models import ScrapedAuthor, ScrapedPublication
from pipeline.runner import (
    _fill_registered_at, _registered_at, _update_silver, _write_events, _write_silver,
)

SERIES = 'Lecture Notes in Computer Science'


def pub(doi='10.1007/978-3-000-00000-1', created='2026-10-17T22:11:05Z', **extra):
    fields = dict(title='Volume', authors=[ScrapedAuthor(name='Ada Editor')], year=2026,
                  series=SERIES, publisher='Springer', doi=doi, created=created)
    fields.update(extra)
    return ScrapedPublication(**fields)


def rows(url, sql):
    engine = create_engine(url)
    with engine.connect() as conn:
        out = conn.execute(text(sql)).fetchall()
    engine.dispose()
    return out


# ------------------------------------------------------------------- parsing
@pytest.mark.parametrize('raw,expected', [
    ('2026-10-17T22:11:05Z', datetime(2026, 10, 17, 22, 11, 5)),
    ('2026-10-17T22:11:05+00:00', datetime(2026, 10, 17, 22, 11, 5)),
    ('2026-10-17T22:11:05+02:00', datetime(2026, 10, 17, 20, 11, 5)),   # converted to UTC
    ('2026-10-17T22:11:05', datetime(2026, 10, 17, 22, 11, 5)),          # already naive: kept
    (None, None), ('', None), ('not a date', None),
])
def test_registered_at_is_naive_utc(raw, expected):
    assert _registered_at({'created': raw}) == expected


def test_registered_at_does_not_change_the_fingerprint():
    """Storing the registration time must never look like a change to a title."""
    with_date = pub().to_dict()
    without = pub(created=None).to_dict()
    assert payload_hash(with_date) == payload_hash(without)


# -------------------------------------------------------------- stored in the db
class TestStored:

    def test_silver_and_event_both_get_the_time(self, db_env):
        records = [pub()]
        _write_silver(records, SERIES)
        _write_events(records, SERIES)
        assert rows(db_env, 'SELECT registered_at FROM publications') == [(datetime(2026, 10, 17, 22, 11, 5),)]
        assert rows(db_env, 'SELECT registered_at FROM events') == [(datetime(2026, 10, 17, 22, 11, 5),)]

    def test_records_without_a_time_store_null(self, db_env):
        records = [pub(created=None, source='springer-api')]
        _write_silver(records, SERIES)
        _write_events(records, SERIES)
        assert rows(db_env, 'SELECT registered_at FROM publications') == [(None,)]
        assert rows(db_env, 'SELECT registered_at FROM events') == [(None,)]

    def test_a_volume_found_first_by_the_api_gets_its_time_when_crossref_arrives(self, db_env):
        _write_silver([pub(created=None, source='springer-api')], SERIES)
        assert rows(db_env, 'SELECT registered_at FROM publications') == [(None,)]
        assert _fill_registered_at([pub()]) == 1
        assert rows(db_env, 'SELECT registered_at FROM publications') == [(datetime(2026, 10, 17, 22, 11, 5),)]

    def test_an_existing_time_is_never_overwritten(self, db_env):
        _write_silver([pub()], SERIES)
        assert _fill_registered_at([pub(created='2030-01-01T00:00:00Z')]) == 0
        _update_silver([pub(created=None, title='Changed title')])      # no time: keeps the old one
        assert rows(db_env, 'SELECT title, registered_at FROM publications') == [
            ('Changed title', datetime(2026, 10, 17, 22, 11, 5))]

    def test_rerunning_the_same_record_adds_no_event(self, db_env):
        records = [pub()]
        _write_silver(records, SERIES)
        _write_events(records, SERIES)
        again = _write_events([pub()], SERIES, stored_before={records[0].doi})
        assert (again.added, again.updated) == (0, 0)
        assert rows(db_env, 'SELECT COUNT(*) FROM events') == [(1,)]


# ------------------------------------------------------------------- backfill
class TestBackfill:

    def _seed(self, db_env):
        old = [pub('10.1007/old-1', created=None), pub('10.1007/old-2', created=None),
               pub('ieee:issue:1:2', created=None), pub('10', created=None)]
        _write_silver(old, SERIES)
        _write_events(old, SERIES)

    def fake(self, calls=None):
        known = {'10.1007/old-1': datetime(2026, 9, 1, 8, 0, 0),
                 '10.1007/old-2': datetime(2026, 9, 2, 9, 0, 0)}

        def fetch(dois):
            if calls is not None:
                calls.append(list(dois))
            return {d.lower(): known[d] for d in dois if d in known}
        return fetch

    def test_fills_titles_and_events_and_leaves_unknown_dois_empty(self, db_env):
        from backfill_registered_at import run
        self._seed(db_env)
        result = run(fetch_created=self.fake(), pause=0)
        assert (result['missing'], result['found'], result['not_found']) == (4, 2, 2)
        assert result['not_a_doi'] == 2
        assert (result['publications'], result['events']) == (2, 2)
        assert rows(db_env, 'SELECT doi, registered_at FROM publications ORDER BY doi') == [
            ('10', None),
            ('10.1007/old-1', datetime(2026, 9, 1, 8, 0, 0)),
            ('10.1007/old-2', datetime(2026, 9, 2, 9, 0, 0)),
            ('ieee:issue:1:2', None)]

    def test_made_up_identifiers_are_never_sent_to_crossref(self, db_env):
        from backfill_registered_at import run
        self._seed(db_env)
        calls = []
        run(fetch_created=self.fake(calls), pause=0)
        sent = [d for chunk in calls for d in chunk]
        assert sorted(sent) == ['10.1007/old-1', '10.1007/old-2']

    def test_dry_run_writes_nothing(self, db_env):
        from backfill_registered_at import run
        self._seed(db_env)
        result = run(dry_run=True, fetch_created=self.fake(), pause=0)
        assert result['found'] == 2 and result['publications'] == 0
        assert rows(db_env, 'SELECT COUNT(*) FROM publications WHERE registered_at IS NOT NULL') == [(0,)]

    def test_second_run_only_retries_the_empty_rows(self, db_env):
        from backfill_registered_at import run
        self._seed(db_env)
        run(fetch_created=self.fake(), pause=0)
        calls = []
        again = run(fetch_created=self.fake(calls), pause=0)
        assert calls == [] and again['found'] == 0 and again['publications'] == 0

    def test_it_creates_no_update_events(self, db_env):
        from backfill_registered_at import run
        self._seed(db_env)
        run(fetch_created=self.fake(), pause=0)
        assert rows(db_env, "SELECT COUNT(*) FROM events WHERE event_type = 'title.updated'") == [(0,)]


class TestCrossrefLookup:
    """crossref_created against a fake HTTP layer: a rejected batch falls back to single lookups."""

    @staticmethod
    def response(status, payload=None):
        from unittest.mock import MagicMock
        resp = MagicMock(status_code=status)
        resp.json.return_value = payload
        import requests
        resp.raise_for_status.side_effect = None if status < 400 else requests.HTTPError(f'HTTP {status}')
        return resp

    def test_a_rejected_batch_is_retried_one_doi_at_a_time(self, monkeypatch):
        import backfill_registered_at as bf
        item = lambda doi: {'DOI': doi, 'created': {'date-time': '2026-09-01T08:00:00Z'}}
        urls = []

        def fake_get(url, params=None, headers=None, timeout=None):
            urls.append(url)
            if url.endswith('/works'):
                return self.response(400)                                  # the whole batch is rejected
            if url.endswith('bad'):
                return self.response(404)                                  # unknown DOI
            return self.response(200, {'message': item(url.rsplit('/works/', 1)[1])})

        monkeypatch.setattr(bf.requests, 'get', fake_get)
        monkeypatch.setattr(bf.time, 'sleep', lambda s: None)
        out = bf.crossref_created(['10.1007/a', '10.1007/bad', '10.1007/b'], 'x@example.com')
        assert out == {'10.1007/a': datetime(2026, 9, 1, 8, 0, 0), '10.1007/b': datetime(2026, 9, 1, 8, 0, 0)}
        assert len(urls) == 4                                              # 1 batch + 3 singles

    def test_server_errors_are_retried_and_then_raised(self, monkeypatch):
        import backfill_registered_at as bf
        calls = []
        monkeypatch.setattr(bf.requests, 'get', lambda *a, **k: calls.append(1) or self.response(503))
        monkeypatch.setattr(bf.time, 'sleep', lambda s: None)
        with pytest.raises(Exception):
            bf.crossref_created(['10.1007/a'], 'x@example.com')
        assert len(calls) == 3
