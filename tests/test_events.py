"""
Tests for the event layer (Phase 2) — the append-only change log.

Two kinds of tests:
  * Pure/unit  — the fingerprint and classification logic in pipeline.events
                 (no database, no network).
  * Integration — _write_events against a real Postgres, proving:
                     - first run emits title.added for every record
                     - an unchanged re-run emits ZERO events (idempotency)
                     - a changed record emits exactly one title.updated
"""

import pytest

from pipeline.events import (
    payload_hash, canonical_record, classify_event_type,
    TITLE_ADDED, TITLE_UPDATED,
)


# ---------------------------------------------------------------------------
# Pure unit tests — fingerprint + classification (no DB)
# ---------------------------------------------------------------------------

class TestPayloadHash:

    def _pub(self, **overrides):
        base = {
            'doi': '10.1007/978-3-031-00000-1',
            'title': 'Some Proceedings Volume',
            'year': 2024,
            'series': 'Lecture Notes in Computer Science',
            'authors': [{'name': 'Alice'}, {'name': 'Bob'}],
        }
        base.update(overrides)
        return base

    def test_same_record_same_hash(self):
        assert payload_hash(self._pub()) == payload_hash(self._pub())

    def test_author_order_does_not_change_hash(self):
        a = self._pub(authors=[{'name': 'Alice'}, {'name': 'Bob'}])
        b = self._pub(authors=[{'name': 'Bob'}, {'name': 'Alice'}])
        assert payload_hash(a) == payload_hash(b)

    def test_changed_title_changes_hash(self):
        a = self._pub(title='Original Title')
        b = self._pub(title='Corrected Title')
        assert payload_hash(a) != payload_hash(b)

    def test_volatile_fetched_at_ignored(self):
        # fetched_at changes every scrape; it must NOT affect the fingerprint,
        # otherwise every run would look like an update.
        a = self._pub(fetched_at='2026-01-01T00:00:00')
        b = self._pub(fetched_at='2026-10-11T12:00:00')
        assert payload_hash(a) == payload_hash(b)

    def test_added_isbn_changes_hash(self):
        a = self._pub(isbn=None)
        b = self._pub(isbn='9783031000001')
        assert payload_hash(a) != payload_hash(b)


class TestClassify:

    def test_new_doi_is_added(self):
        pub = {'doi': '10.1/x', 'title': 'T'}
        assert classify_event_type(pub, None) == TITLE_ADDED

    def test_unchanged_is_none(self):
        pub = {'doi': '10.1/x', 'title': 'T'}
        h = payload_hash(pub)
        assert classify_event_type(pub, h) is None

    def test_changed_is_updated(self):
        old = {'doi': '10.1/x', 'title': 'Old'}
        new = {'doi': '10.1/x', 'title': 'New'}
        assert classify_event_type(new, payload_hash(old)) == TITLE_UPDATED


# ---------------------------------------------------------------------------
# Integration tests — _write_events against real Postgres
# ---------------------------------------------------------------------------

def _scraped(doi, title='T', **extra):
    """Build a ScrapedPublication-like object with a .to_dict()."""
    from pipeline.models import ScrapedPublication, ScrapedAuthor
    fields = dict(title=title, doi=doi, year=2024,
                  series='Lecture Notes in Computer Science',
                  authors=[ScrapedAuthor(name='Alice')])
    fields.update(extra)
    return ScrapedPublication(**fields)


class TestWriteEvents:

    def _event_counts(self, url):
        from sqlalchemy import create_engine, text
        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(text(
                'SELECT event_type, COUNT(*) FROM events GROUP BY event_type'
            )).fetchall()
        engine.dispose()
        return {r[0]: r[1] for r in rows}

    def test_first_run_emits_added_for_each(self, db_env):
        from pipeline.runner import _write_events
        pubs = [_scraped('10.1/a'), _scraped('10.1/b'), _scraped('10.1/c')]
        result = _write_events(pubs, 'Lecture Notes in Computer Science')
        assert result.failed == 0
        assert result.added == 3
        assert result.updated == 0
        counts = self._event_counts(db_env)
        assert counts.get('title.added') == 3

    def test_rerun_unchanged_emits_nothing(self, db_env):
        from pipeline.runner import _write_events
        pubs = [_scraped('10.1/a'), _scraped('10.1/b')]
        _write_events(pubs, 'Lecture Notes in Computer Science')
        # Second identical run must add ZERO new events (idempotency).
        result = _write_events(pubs, 'Lecture Notes in Computer Science')
        assert result.failed == 0
        assert result.added == 0
        assert result.updated == 0
        assert result.unchanged == 2
        counts = self._event_counts(db_env)
        assert counts == {'title.added': 2}   # unchanged — no duplicates

    def test_changed_record_emits_one_updated(self, db_env):
        from pipeline.runner import _write_events
        # First run: two titles.
        _write_events([_scraped('10.1/a'), _scraped('10.1/b')],
                      'Lecture Notes in Computer Science')
        # Second run: same DOIs, but 10.1/a's title changed.
        changed = [_scraped('10.1/a', title='Corrected Title'),
                   _scraped('10.1/b')]
        result = _write_events(changed, 'Lecture Notes in Computer Science')
        assert result.failed == 0
        assert result.updated == 1     # only 10.1/a changed
        assert result.added == 0
        assert result.unchanged == 1  # 10.1/b untouched
        counts = self._event_counts(db_env)
        assert counts == {'title.added': 2, 'title.updated': 1}
