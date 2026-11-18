"""
The one-off repair of the Springer start-up backlog: missing volumes are added as quiet
baselines, and book-chapter rows are deleted only when that is safe.
"""

import pytest
from sqlalchemy import create_engine, text

from pipeline.models import ScrapedAuthor, ScrapedPublication
from pipeline.runner import _write_events, _write_silver

LNNS = 'Lecture Notes in Networks and Systems'
VOL1 = '10.1007/978-3-000-00001-1'      # missing here, registered before the system started
VOL2 = '10.1007/978-3-000-00002-2'      # already stored
OLD = '10.1007/978-3-000-00009-9'       # registered before --since: out of scope
RECENT = '10.1007/978-3-000-00008-8'    # registered after the first loading day: the nightly job's


def pub(doi, work_type='book', created='2026-09-01T10:00:00Z', title=None):
    return ScrapedPublication(title=title or f'T {doi}', authors=[ScrapedAuthor(name='Ada Editor')],
                              year=2027, series=LNNS, publisher='Springer Nature Switzerland',
                              doi=doi, work_type=work_type, created=created)


def rows(url, sql):
    engine = create_engine(url)
    with engine.connect() as conn:
        out = conn.execute(text(sql)).fetchall()
    engine.dispose()
    return out


@pytest.fixture()
def seeded(db_env):
    """Stored: volume 2, and four chapters (parents: missing / stored / unknown / missing but updated)."""
    _write_silver([pub(VOL2)], LNNS)
    chapters = [pub('10.1007/978-3-000-00001-1_1', 'book-chapter'),      # parent VOL1: will be added
                pub(VOL2 + '_1', 'book-chapter'),                         # parent VOL2: stored
                pub('10.1007/978-3-000-00003-3_1', 'book-chapter'),      # parent unknown
                pub('10.1007/978-3-000-00004-4_1', 'book-chapter')]      # has a title.updated
    _write_silver(chapters, LNNS)
    _write_events(chapters, LNNS)
    _write_events([pub('10.1007/978-3-000-00004-4_1', 'book-chapter', title='Changed')], LNNS,
                  stored_before={c.doi for c in chapters})
    return db_env


def fake_fetch(series):
    if series['abbreviation'] != 'LNNS':
        return []
    return [pub(VOL1), pub(VOL2), pub(OLD, created='2026-07-01T00:00:00Z'),
            pub(RECENT, created='2026-11-18T09:00:00Z')]


def test_dry_run_reports_and_writes_nothing(seeded):
    from repair_springer_backlog import run
    before = rows(seeded, 'SELECT COUNT(*) FROM publications')[0][0]
    r = run(dry_run=True, fetch=fake_fetch, pause=0)
    assert dict(r['volumes_missing']) == {'LNNS': 1}              # VOL2 stored; OLD and RECENT out of scope
    assert (r['chapters'], r['chapters_deletable'],
            r['chapters_kept_no_parent'], r['chapters_kept_unsafe']) == (4, 2, 1, 1)
    assert rows(seeded, 'SELECT COUNT(*) FROM publications')[0][0] == before
    assert rows(seeded, "SELECT COUNT(*) FROM events WHERE event_type = 'title.baseline'") == [(0,)]


def test_apply_adds_the_volume_quietly_and_deletes_only_the_safe_chapters(seeded):
    from repair_springer_backlog import run
    r = run(fetch=fake_fetch, pause=0)
    assert (r['volumes_added'], r['baseline_events']) == (1, 1)
    assert (r['chapters_deleted'], r['events_deleted']) == (2, 2)
    assert rows(seeded, f"SELECT event_type, source FROM events WHERE source_record_id = '{VOL1}'") == [
        ('title.baseline', 'crossref')]                                  # stored, not announced
    left = {d for (d,) in rows(seeded, "SELECT doi FROM publications WHERE work_type = 'book-chapter'")}
    assert left == {'10.1007/978-3-000-00003-3_1', '10.1007/978-3-000-00004-4_1'}
    assert rows(seeded, f"SELECT registered_at IS NOT NULL FROM publications WHERE doi = '{VOL1}'") == [(True,)]
    assert rows(seeded, f"SELECT COUNT(*) FROM publications WHERE doi IN ('{OLD}', '{RECENT}')") == [(0,)]


def test_the_repair_creates_no_added_or_updated_events(seeded):
    from repair_springer_backlog import run
    before = dict(rows(seeded, "SELECT event_type, COUNT(*) FROM events GROUP BY 1"))
    run(fetch=fake_fetch, pause=0)
    after = dict(rows(seeded, "SELECT event_type, COUNT(*) FROM events GROUP BY 1"))
    assert after.get('title.updated', 0) == before.get('title.updated', 0)
    assert after.get('title.added', 0) == before['title.added'] - 2      # only the 2 deleted chapters
    assert after['title.baseline'] == 1


def test_a_second_run_finds_nothing_to_do(seeded):
    from repair_springer_backlog import run
    run(fetch=fake_fetch, pause=0)
    again = run(fetch=fake_fetch, pause=0)
    assert sum(again['volumes_missing'].values()) == 0
    assert (again['chapters_deleted'], again['volumes_added']) == (0, 0)
    # 3 events of the 2 kept chapters (added; added + updated) + 1 baseline for the added volume
    assert rows(seeded, 'SELECT COUNT(*) FROM events') == [(4,)]


def test_chapter_authors_links_are_removed_with_the_chapters(seeded):
    from repair_springer_backlog import run
    run(fetch=fake_fetch, pause=0)
    assert rows(seeded, 'SELECT COUNT(*) FROM publication_authors pa LEFT JOIN publications p '
                        'ON p.id = pa.publication_id WHERE p.id IS NULL') == [(0,)]


class TestCrossrefRateLimit:
    """A 429 from CrossRef is waited out and retried, not fatal."""

    @staticmethod
    def response(status, items=None, retry_after=None):
        from unittest.mock import MagicMock
        import requests
        resp = MagicMock(status_code=status, headers={'Retry-After': retry_after} if retry_after else {})
        resp.json.return_value = {'message': {'items': items or [], 'next-cursor': 'c'}}
        resp.raise_for_status.side_effect = None if status < 400 else requests.HTTPError(f'HTTP {status}')
        return resp

    def scraper(self, responses):
        from unittest.mock import MagicMock
        scraper = MagicMock(base_url='https://api.crossref.org/works')
        scraper.session.get.side_effect = list(responses)
        scraper._parse_publication.side_effect = lambda item: pub(item['DOI'])
        return scraper

    def test_it_waits_for_retry_after_and_then_continues(self, monkeypatch):
        import repair_springer_backlog as rb
        waits = []
        monkeypatch.setattr(rb.time, 'sleep', lambda s: waits.append(s))
        scraper = self.scraper([self.response(429, retry_after='7'),
                                self.response(200, [{'DOI': VOL1}]),
                                self.response(200, [])])
        found = rb.fetch_created_since(scraper, '1234-5678', '2026-08-01', pause=0)
        assert [p.doi for p in found] == [VOL1]
        assert 7.0 in waits                                              # waited as CrossRef asked

    def test_without_retry_after_it_backs_off_and_gives_up_after_the_last_attempt(self, monkeypatch):
        import requests
        import repair_springer_backlog as rb
        waits = []
        monkeypatch.setattr(rb.time, 'sleep', lambda s: waits.append(s))
        scraper = self.scraper([self.response(429)] * 6)
        with pytest.raises(requests.HTTPError):
            rb.fetch_created_since(scraper, '1234-5678', '2026-08-01', pause=0)
        assert waits == [10, 20, 40, 80, 160]
