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
    TITLE_ADDED, TITLE_UPDATED, TITLE_BASELINE,
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


# ---------------------------------------------------------------------------
# Baselines: titles stored before the event log must not be announced as new
# ---------------------------------------------------------------------------

class TestBaseline:

    def test_stored_title_without_event_is_baseline_not_added(self):
        pub = {'doi': '10.1/x', 'title': 'T'}
        assert classify_event_type(pub, None, baseline=True) == TITLE_BASELINE

    def test_unknown_title_is_still_added(self):
        assert classify_event_type({'doi': '10.1/x'}, None, baseline=False) == TITLE_ADDED

    def test_existing_event_wins_over_the_baseline_flag(self):
        pub = {'doi': '10.1/x', 'title': 'T'}
        assert classify_event_type(pub, payload_hash(pub), baseline=True) is None
        assert classify_event_type(dict(pub, title='New'), payload_hash(pub),
                                   baseline=True) == TITLE_UPDATED

    def test_created_date_is_not_part_of_the_fingerprint(self):
        a = {'doi': '10.1/x', 'title': 'T'}
        assert payload_hash(a) == payload_hash(dict(a, created='2026-10-01T00:00:00Z'))


class TestWriteEventsBaseline:

    def _types(self, url):
        from sqlalchemy import create_engine, text
        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(text(
                'SELECT source_record_id, event_type FROM events ORDER BY id')).fetchall()
        engine.dispose()
        return [tuple(r) for r in rows]

    def test_legacy_title_gets_a_quiet_baseline(self, db_env):
        from pipeline.runner import _write_events
        result = _write_events([_scraped('10.1/old'), _scraped('10.1/new')],
                               'Lecture Notes in Computer Science',
                               stored_before={'10.1/old'})
        assert (result.added, result.baseline, result.updated) == (1, 1, 0)
        assert self._types(db_env) == [('10.1/old', 'title.baseline'),
                                       ('10.1/new', 'title.added')]

    def test_change_after_a_baseline_is_a_real_update(self, db_env):
        from pipeline.runner import _write_events
        _write_events([_scraped('10.1/old')], 'Lecture Notes in Computer Science',
                      stored_before={'10.1/old'})
        result = _write_events([_scraped('10.1/old', title='Corrected')],
                               'Lecture Notes in Computer Science',
                               stored_before={'10.1/old'})
        assert (result.updated, result.added, result.baseline) == (1, 0, 0)
        assert [t for _, t in self._types(db_env)] == ['title.baseline', 'title.updated']

    def test_rerun_after_a_baseline_adds_nothing(self, db_env):
        from pipeline.runner import _write_events
        pubs = [_scraped('10.1/old')]
        _write_events(pubs, 'Lecture Notes in Computer Science', stored_before={'10.1/old'})
        result = _write_events(pubs, 'Lecture Notes in Computer Science',
                               stored_before={'10.1/old'})
        assert (result.added, result.baseline, result.updated, result.unchanged) == (0, 0, 0, 1)


class TestOldTitleFilter:
    """from-update-date returns old titles that were merely touched; skip untracked ones."""

    def _pub(self, doi, created):
        return _scraped(doi, created=created)

    def test_old_untracked_title_is_dropped(self):
        from pipeline.runner import _drop_old_untracked
        pubs = [self._pub('10.1/old', '2019-03-01T00:00:00Z'),
                self._pub('10.1/fresh', '2026-10-14T09:00:00Z')]
        kept, dropped = _drop_old_untracked(pubs, '2026-10-13', stored=set())
        assert [p.doi for p in kept] == ['10.1/fresh'] and dropped == 1

    def test_old_but_stored_title_is_kept_for_update_detection(self):
        from pipeline.runner import _drop_old_untracked
        kept, dropped = _drop_old_untracked([self._pub('10.1/old', '2019-03-01T00:00:00Z')],
                                            '2026-10-13', stored={'10.1/old'})
        assert len(kept) == 1 and dropped == 0

    def test_grace_period_keeps_a_title_created_just_before_the_window(self):
        from pipeline.runner import _drop_old_untracked, NEW_TITLE_GRACE_DAYS
        just_before = '2026-10-12T23:50:00Z'      # window starts 2026-10-13
        assert NEW_TITLE_GRACE_DAYS >= 1
        kept, dropped = _drop_old_untracked([self._pub('10.1/edge', just_before)],
                                            '2026-10-13', stored=set())
        assert len(kept) == 1 and dropped == 0

    def test_record_without_created_date_is_kept(self):
        from pipeline.runner import _drop_old_untracked
        kept, dropped = _drop_old_untracked([self._pub('ieee:issue:1:2', None)],
                                            '2026-10-13', stored=set())
        assert len(kept) == 1 and dropped == 0


# ---------------------------------------------------------------------------
# HTML entities: CrossRef sometimes sends 'A &amp; B'. Store plain text, and make
# sure old events hashed on the encoded text do not turn into false updates.
# ---------------------------------------------------------------------------

class TestEntityDecoding:

    @pytest.mark.parametrize('raw,expected', [
        ('Pharmacology &amp; Therapeutics', 'Pharmacology & Therapeutics'),
        ('&lt; &lt;UML&gt; &gt; 2004', '< <UML> > 2004'),
        ('Caf&#233;', 'Caf\u00e9'),
        ('R&D and AT&T', 'R&D and AT&T'),          # no closing ';' -> left alone
        ('Plain title', 'Plain title'),
        ('', ''),
    ])
    def test_decode_entities(self, raw, expected):
        from pipeline.events import decode_entities
        assert decode_entities(raw) == expected

    def test_non_strings_pass_through(self):
        from pipeline.events import decode_entities
        assert decode_entities(None) is None
        assert decode_entities(2026) == 2026

    def test_decoding_twice_changes_nothing(self):
        from pipeline.events import decode_entities
        once = decode_entities('A &amp; B')
        assert decode_entities(once) == once == 'A & B'

    def test_encoded_and_plain_titles_have_the_same_fingerprint(self):
        encoded = {'doi': '10.1/x', 'title': 'Pharmacology &amp; Therapeutics'}
        plain = {'doi': '10.1/x', 'title': 'Pharmacology & Therapeutics'}
        assert payload_hash(encoded) == payload_hash(plain)

    def test_canonical_payload_is_stored_decoded(self):
        rec = canonical_record({'doi': '10.1/x', 'title': 'A &amp; B'})
        assert rec['title'] == 'A & B'

    def test_a_real_title_change_is_still_detected(self):
        old = {'doi': '10.1/x', 'title': 'A &amp; B'}
        new = {'doi': '10.1/x', 'title': 'A & C'}
        assert classify_event_type(new, payload_hash(old)) == TITLE_UPDATED

    def test_scraper_decodes_the_title(self):
        from pipeline.scraper import CrossRefScraper
        item = {'DOI': '10.1/x', 'title': ['Pharmacology &amp; Therapeutics'],
                'type': 'journal-issue', 'publisher': 'Wiley'}
        pub = CrossRefScraper(email='t@example.com', delay=0)._parse_publication(item)
        assert pub.title == 'Pharmacology & Therapeutics'

    def test_scraper_decodes_a_built_issue_title(self):
        from pipeline.scraper import CrossRefScraper
        item = {'DOI': '10.1/y', 'type': 'journal-issue', 'publisher': 'Wiley',
                'container-title': ['Gynecology &amp; Obstetrics'], 'volume': '175', 'issue': '2'}
        pub = CrossRefScraper(email='t@example.com', delay=0)._parse_publication(item)
        assert pub.title == 'Gynecology & Obstetrics Vol. 175 No. 2'


class TestOldEncodedEvents:

    def test_stored_hash_is_recomputed_under_current_rules(self):
        from pipeline.events import fingerprint_of_stored
        legacy_payload = {'doi': '10.1/x', 'title': 'A &amp; B'}
        assert fingerprint_of_stored(legacy_payload, 'stale-old-hash') == \
            payload_hash({'doi': '10.1/x', 'title': 'A & B'})

    def test_falls_back_to_the_stored_hash_without_a_payload(self):
        from pipeline.events import fingerprint_of_stored
        assert fingerprint_of_stored(None, 'abc') == 'abc'
        assert fingerprint_of_stored({}, 'abc') == 'abc'

    def test_old_encoded_event_does_not_become_a_false_update(self, db_env):
        """The scenario that motivated the fix: an event written while the title was
        still encoded must compare as unchanged once the record arrives decoded."""
        import json
        from sqlalchemy import create_engine, text
        from pipeline.models import ScrapedPublication, ScrapedAuthor
        from pipeline.runner import _write_events

        legacy = {'doi': '10.1/enc', 'title': 'A &amp; B', 'year': 2024,
                  'series': 'Wiley', 'publisher': 'Wiley'}
        old_payload = {f: None for f in __import__('pipeline.events', fromlist=['x']).CANONICAL_FIELDS}
        old_payload.update(legacy, authors=[])
        engine = create_engine(db_env)
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO events (event_id, event_type, source, source_record_id, series, "
                "publisher, payload_hash, payload, detected_at) VALUES "
                "('e-enc', 'title.added', 'crossref', '10.1/enc', 'Wiley', 'Wiley', "
                "'hash-from-the-encoded-era', CAST(:p AS json), now())"),
                {'p': json.dumps(old_payload)})
        engine.dispose()

        fresh = ScrapedPublication(title='A & B', doi='10.1/enc', year=2024, series='Wiley',
                                   publisher='Wiley', authors=[])
        result = _write_events([fresh], 'Wiley', stored_before={'10.1/enc'})
        assert (result.updated, result.added, result.baseline) == (0, 0, 0)
        assert result.unchanged == 1


class TestFetchedAtIsUtc:
    """ScrapedPublication.fetched_at becomes publications.fetched_at and
    events.observed_at. Every other timestamp in the system is naive UTC, so this
    one must be too. The bug only shows when the machine's local time zone is not
    UTC, so the test sets the process time zone itself (CI runs on UTC)."""

    @pytest.mark.parametrize('tz', ['Europe/Bratislava', 'America/New_York', 'Asia/Tokyo'])
    def test_default_fetched_at_is_naive_utc_in_any_local_timezone(self, tz, monkeypatch):
        import time
        from datetime import datetime, timezone
        from pipeline.models import ScrapedPublication
        if not hasattr(time, 'tzset'):
            pytest.skip('time.tzset is not available on this platform')
        monkeypatch.setenv('TZ', tz)
        time.tzset()
        try:
            stamp = ScrapedPublication(title='T', authors=[]).fetched_at
        finally:
            monkeypatch.undo()
            time.tzset()
        parsed = datetime.fromisoformat(stamp)
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        assert parsed.tzinfo is None
        assert abs((now_utc - parsed).total_seconds()) < 5, (tz, stamp)
