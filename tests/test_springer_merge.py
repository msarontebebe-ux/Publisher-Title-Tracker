"""
Springer API + CrossRef working together: field-level merge, early detection that
CrossRef later completes, no false updates, and silver following title.updated.
Database tests use the throwaway test database (skipped when it is unreachable).
"""

import pytest
from sqlalchemy import create_engine, text

from pipeline.models import ScrapedAuthor, ScrapedPublication
from pipeline.runner import (
    _keep_enrichment, _update_silver, _write_events, _write_silver, merge_api_volumes,
)

SERIES = 'Lecture Notes in Computer Science'
DOI = '10.1007/978-3-032-41086-3'


def crossref(doi=DOI, **extra):
    fields = dict(title='Discovery Science: 29th International Conference, DS 2026, Proceedings',
                  authors=[ScrapedAuthor(name='Ada Editor')], year=2026, series=SERIES,
                  isbn='978-3-032-41086-3', publisher='Springer Nature Switzerland',
                  work_type='book', doi=doi, ee=f'https://doi.org/{doi}')
    fields.update(extra)
    return ScrapedPublication(**fields)


def api(doi=DOI, **extra):
    fields = dict(title='Discovery Science', authors=[ScrapedAuthor(name='Ada Editor')],
                  year=2026, series=SERIES, isbn='978-3-032-41085-6',
                  publisher='Springer Nature Switzerland', work_type='proceedings',
                  source='springer-api', doi=doi, ee=f'https://doi.org/{doi}',
                  event_name='DS 2026', event_location='Mainz, Germany',
                  event_date_start='2026-10-05', event_date_end='2026-10-09',
                  event_year=2026, event_month=10, event_date_confidence='high')
    fields.update(extra)
    return ScrapedPublication(**fields)


def rows(url, sql):
    engine = create_engine(url)
    with engine.connect() as conn:
        out = conn.execute(text(sql)).fetchall()
    engine.dispose()
    return out


# ------------------------------------------------------------------ pure merge
class TestMerge:

    def test_volume_in_both_keeps_crossref_content_and_gains_api_fields(self):
        merged = merge_api_volumes([crossref()], [api()])
        assert len(merged) == 1
        pub = merged[0]
        assert pub.title.startswith('Discovery Science: 29th')      # CrossRef title wins
        assert pub.isbn == '978-3-032-41086-3' and pub.source == 'crossref'
        assert pub.work_type == 'proceedings'                       # API label
        assert (pub.event_name, pub.event_location) == ('DS 2026', 'Mainz, Germany')
        assert pub.event_date_start == '2026-10-05' and pub.event_date_confidence == 'high'

    def test_volume_only_in_the_api_is_added_as_an_api_record(self):
        merged = merge_api_volumes([], [api()])
        assert [(p.doi, p.source) for p in merged] == [(DOI, 'springer-api')]

    def test_doi_match_ignores_case(self):
        merged = merge_api_volumes([crossref(doi=DOI.upper())], [api()])
        assert len(merged) == 1 and merged[0].work_type == 'proceedings'

    def test_api_without_a_conference_date_leaves_crossref_event_fields_alone(self):
        base = crossref(event_name='Regex name', event_date_start='2026-01-02',
                        event_date_confidence='medium')
        no_event = api(event_date_start=None, event_name=None)
        pub = merge_api_volumes([base], [no_event])[0]
        assert (pub.event_name, pub.event_date_start) == ('Regex name', '2026-01-02')

    def test_old_volumes_with_a_late_chapter_are_not_new_titles(self):
        old = api(doi='10.1007/978-3-000-00000-1', year=2019)
        assert merge_api_volumes([], [old], since='2026-10-10') == []
        assert len(merge_api_volumes([], [api(year=2025)], since='2026-10-10')) == 1


# ---------------------------------------------------------------- with database
class TestEarlyDetectionLifecycle:

    def test_api_first_then_crossref_gives_one_update_and_silver_follows(self, db_env):
        # run 1: the API sees the volume before CrossRef has it
        early = [api()]
        _write_silver(early, SERIES)
        r1 = _write_events(early, SERIES, stored_before=set())
        assert (r1.added, r1.updated) == (1, 0)
        assert rows(db_env, 'SELECT source, event_type FROM events') == [
            ('springer-api', 'title.added')]

        # run 2: CrossRef registers it with the full title; no API data this time
        full = [crossref()]
        _keep_enrichment(full)
        assert full[0].work_type == 'proceedings'                   # carried from silver
        assert full[0].event_date_start == '2026-10-05'
        r2 = _write_events(full, SERIES, stored_before={DOI})
        assert (r2.added, r2.updated, r2.updated_dois) == (0, 1, (DOI,))
        assert _update_silver(full) == 1
        title, wtype, ev, isbn = rows(
            db_env, 'SELECT title, work_type, event_name, isbn FROM publications')[0]
        assert title.startswith('Discovery Science: 29th') and wtype == 'proceedings'
        assert (ev, isbn) == ('DS 2026', '978-3-032-41086-3')

        # run 3: the same CrossRef record again changes nothing
        again = [crossref()]
        _keep_enrichment(again)
        r3 = _write_events(again, SERIES, stored_before={DOI})
        assert (r3.added, r3.updated, r3.unchanged) == (0, 0, 1)
        assert rows(db_env, 'SELECT COUNT(*) FROM events')[0][0] == 2

    def test_crossref_only_title_without_api_data_is_never_downgraded(self, db_env):
        enriched = [crossref(work_type='proceedings', event_name='DS 2026',
                             event_date_start='2026-10-05', event_date_end='2026-10-09',
                             event_year=2026, event_month=10, event_date_confidence='high')]
        _write_silver(enriched, SERIES)
        plain = [crossref()]                                         # CrossRef: 'book', no dates
        _keep_enrichment(plain)
        assert plain[0].work_type == 'proceedings'
        assert plain[0].event_date_start == '2026-10-05' and plain[0].event_name == 'DS 2026'

    def test_crossref_event_data_of_its_own_wins_over_stored_values(self, db_env):
        _write_silver([api()], SERIES)
        own = [crossref(event_date_start='2026-11-01', event_date_end='2026-11-02',
                        event_name='Own', event_date_confidence='high')]
        _keep_enrichment(own)
        assert own[0].event_date_start == '2026-11-01' and own[0].event_name == 'Own'

    def test_silver_update_replaces_changed_editors(self, db_env):
        _write_silver([crossref()], SERIES)
        changed = crossref(authors=[ScrapedAuthor(name='Ada Editor'),
                                    ScrapedAuthor(name='Bo Second')])
        _update_silver([changed])
        names = rows(db_env, 'SELECT a.name FROM authors a JOIN publication_authors pa '
                             'ON pa.author_id = a.id ORDER BY pa.author_id')
        assert sorted(n[0] for n in names) == ['Ada Editor', 'Bo Second']


class TestApiOnlyRecordsForStoredTitles:

    def test_stored_volume_reported_again_by_the_api_is_dropped(self, db_env, monkeypatch):
        """A later chapter of a volume we already have must not become an event."""
        from pipeline.runner import _persist_and_log
        _write_silver([crossref()], SERIES)
        _persist_and_log([api()], [], SERIES, 'LNCS', None, 'incremental', storage=None)
        assert rows(db_env, 'SELECT COUNT(*) FROM events')[0][0] == 0
        assert rows(db_env, 'SELECT title FROM publications')[0][0].startswith('Discovery Science: 29th')


class TestScrapeSeriesWithApi:

    def _scraper(self, pubs=()):
        from unittest.mock import MagicMock
        scraper = MagicMock(fetch_ok=True)
        scraper.search_by_issn.return_value = (list(pubs), [])
        return scraper

    def test_incremental_run_adds_volumes_only_the_api_knows(self, db_env, monkeypatch):
        from unittest.mock import MagicMock
        from pipeline.config import load_config
        from pipeline.runner import scrape_single_series
        from tests.test_springer_api_collector import FakeSession, SERIES as IDS, make

        monkeypatch.setattr('pipeline.runner._get_api_collector', lambda: make(FakeSession()))
        lncs = next(s for s in load_config()['springer_series'] if s['abbreviation'] == 'LNCS')
        result = scrape_single_series(lncs, self._scraper(), MagicMock(), run_type='incremental')
        assert result and all(p.source == 'springer-api' for p in result)
        sources = {r[0] for r in rows(db_env, 'SELECT DISTINCT source FROM events')}
        assert sources == {'springer-api'}
        assert {p.series for p in result} == {IDS['558']}          # only the series being scraped
        assert rows(db_env, "SELECT COUNT(*) FROM publications WHERE work_type = 'proceedings'")[0][0] == len(result)

    def test_full_run_never_calls_the_api(self, db_env, monkeypatch):
        from unittest.mock import MagicMock
        from pipeline.config import load_config
        from pipeline.runner import scrape_single_series
        called = []
        monkeypatch.setattr('pipeline.runner._get_api_collector', lambda: called.append(1))
        lncs = next(s for s in load_config()['springer_series'] if s['abbreviation'] == 'LNCS')
        scrape_single_series(lncs, self._scraper(), MagicMock(), run_type='full')
        assert called == []
