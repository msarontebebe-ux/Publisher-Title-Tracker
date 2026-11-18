"""
Tests for the IEEE Xplore collector. Uses saved real API responses
(tests/fixtures/ieee_sample.json) - no network, no API key, no database.
"""

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline.api_quota import DailyQuota, MemoryUsage
from pipeline.collectors import build_collector, CrossRefPublisherCollector
from pipeline.collectors.ieee import (
    IeeeXploreCollector, parse_conference_dates, proceedings_doi,
)

FIXTURE = json.loads(
    (Path(__file__).parent / 'fixtures' / 'ieee_sample.json').read_text(encoding='utf-8'))
KEY = 'SECRETKEY1234567890ABCD'
CONFIG = {'name': 'IEEE', 'abbreviation': 'IEEE', 'source': 'ieee_api',
          'crossref_prefixes': ['10.1109'], 'types': ['proceedings', 'journal'],
          'ieee_daily_limit': 200, 'ieee_reserve': 20}


# ---------------------------------------------------------------- pure helpers
@pytest.mark.parametrize('text,expected', [
    ('5-9 July 2026', ('2026-07-05', '2026-07-09')),
    ('2-4 Sept. 2026', ('2026-09-02', '2026-09-04')),
    ('28 Aug.-2 Sept. 2026', ('2026-08-28', '2026-09-02')),
    ('28 Dec. 2026-1 Jan. 2027', ('2026-12-28', '2027-01-01')),
    ('1 Sept. 2026', ('2026-09-01', '2026-09-01')),
    ('5\u20139 July 2026', ('2026-07-05', '2026-07-09')),   # en dash
])
def test_conference_dates_are_parsed(text, expected):
    assert parse_conference_dates(text) == expected


@pytest.mark.parametrize('text', [None, '', 'July 2026', 'sometime in 2026', '5-9 Smarch 2026'])
def test_unparseable_dates_return_none(text):
    assert parse_conference_dates(text) is None


@pytest.mark.parametrize('papers', [
    ['10.30420/456789012'],                       # one dot: the stem would be just '10'
    ['10.30420/456789012', '10.30420/456789013'],
    ['not a doi.1'], [], [None, ''],
])
def test_proceedings_doi_rejects_stems_that_are_not_dois(papers):
    assert proceedings_doi(papers) is None


def test_proceedings_doi_keeps_other_publishers_prefixes():
    assert proceedings_doi(['10.23919/eumc69230.2026.11746901']) == '10.23919/eumc69230.2026'
    assert proceedings_doi(['10.1049/icp.2026.1234']) == '10.1049/icp.2026'


def test_proceedings_doi_matches_crossref_style():
    assert proceedings_doi(['10.1109/ICMEW69720.2026.11720727',
                            '10.1109/ICMEW69720.2026.11720696']) == '10.1109/icmew69720.2026'
    assert proceedings_doi([]) is None


# ----------------------------------------------------------------- fake HTTP
class FakeSession:
    """Serves fixture pages; records every request."""

    def __init__(self, status=200, error=None):
        self.calls, self.status, self.error = [], status, error

    def get(self, url, params=None, timeout=None):
        self.calls.append(dict(params))
        if self.error:
            raise self.error
        resp = MagicMock(status_code=self.status)
        key = 'conferences' if params['content_type'] == 'Conferences' else 'journals'
        resp.json.return_value = FIXTURE[key]
        return resp


def make(session=None, limit=200, reserve=0, usage=None, **cfg):
    quota = DailyQuota('ieee-xplore', limit, reserve, usage=usage or MemoryUsage())
    scraper = MagicMock()
    return IeeeXploreCollector(dict(CONFIG, **cfg), scraper, KEY, quota=quota,
                               session=session or FakeSession(), delay=0)


# ------------------------------------------------------------------ grouping
class TestGrouping:

    def test_papers_become_one_title_per_proceedings(self):
        pubs, raw = make().fetch(from_created_date='2026-10-13')
        procs = [p for p in pubs if p.work_type == 'proceedings']
        assert len(procs) == 3                       # 6 papers -> 3 volumes
        assert len({p.doi for p in procs}) == 3
        assert len(raw) == 10                        # raw keeps every paper

    def test_proceedings_doi_and_metadata(self):
        pubs, _ = make().fetch()
        icmew = next(p for p in pubs if p.doi == '10.1109/icmew69720.2026')
        assert icmew.publisher == 'IEEE'
        assert icmew.event_location == 'Bangkok, Thailand'
        assert icmew.event_date_start == '2026-07-05'
        assert icmew.event_date_end == '2026-07-09'
        assert (icmew.event_year, icmew.event_month) == (2026, 7)
        assert icmew.event_date_confidence == 'high'
        assert 'Multimedia and Expo' in icmew.title
        assert icmew.ee.endswith('/11720687/proceeding')

    def test_journal_papers_become_one_title_per_issue(self):
        pubs, _ = make().fetch()
        issues = [p for p in pubs if p.work_type == 'journal-issue']
        assert len(issues) == 2                      # 4 papers -> 2 issues
        assert all(p.doi.startswith('ieee:issue:') for p in issues)
        assert all(' Vol. ' in p.title for p in issues)

    def test_to_dict_is_event_ready(self):
        pubs, _ = make().fetch()
        d = pubs[0].to_dict()
        assert d['doi'] and d['title'] and d['work_type']


# ----------------------------------------------------------- request + quota
class TestRequestsAndQuota:

    def test_date_window_and_paging_params(self):
        session = FakeSession()
        make(session).fetch(from_created_date='2026-10-13')
        first = session.calls[0]
        assert first['content_type'] == 'Conferences'
        assert first['start_date'] == '20261013'
        assert first['max_records'] == 200 and first['start_record'] == 1
        assert first['apikey'] == KEY

    def test_each_request_is_counted(self):
        usage = MemoryUsage()
        c = make(usage=usage)
        c.fetch()
        assert c.calls_made == 2                     # conferences + journals
        assert c.quota.used() == 2                   # the shared counter saw both calls

    def test_budget_exhaustion_stops_calls_and_marks_incomplete(self):
        session = FakeSession()
        c = make(session, limit=1)                   # only one call allowed
        pubs, _ = c.fetch()
        assert len(session.calls) == 1               # never exceeded the budget
        assert c.fetch_ok is False

    def test_reserve_is_kept_back(self):
        c = make(limit=200, reserve=20)
        assert c.quota.budget == 180

    def test_zero_budget_makes_no_calls(self):
        session = FakeSession()
        c = make(session, limit=5, reserve=5)
        pubs, _ = c.fetch()
        assert session.calls == [] and pubs == [] and c.fetch_ok is False

    def test_paging_requests_following_pages_until_total(self):
        pages = [
            {'total_records': 3, 'articles': FIXTURE['conferences']['articles'][:2]},
            {'total_records': 3, 'articles': FIXTURE['conferences']['articles'][2:3]},
        ]
        session = MagicMock()
        replies = []
        for p in pages:
            replies.append(MagicMock(status_code=200, json=MagicMock(return_value=p)))
        empty = MagicMock(status_code=200,
                          json=MagicMock(return_value={'total_records': 0, 'articles': []}))
        session.get.side_effect = replies + [empty]
        c = IeeeXploreCollector(dict(CONFIG, types=['proceedings']), MagicMock(), KEY,
                                quota=DailyQuota('x', 200, 0, usage=MemoryUsage()),
                                session=session, delay=0)
        c.fetch()
        starts = [call.kwargs['params']['start_record'] for call in session.get.call_args_list]
        assert starts[:2] == [1, 3]


# --------------------------------------------------------------- failure modes
class TestFailures:

    def test_http_error_marks_incomplete(self):
        c = make(FakeSession(status=403))
        pubs, _ = c.fetch()
        assert pubs == [] and c.fetch_ok is False

    def test_network_error_never_leaks_the_key(self, capsys):
        err = RuntimeError(f'connection failed for ...?apikey={KEY}&format=json')
        c = make(FakeSession(error=err))
        c.fetch()
        out = capsys.readouterr().out
        assert KEY not in out and '<KEY>' in out
        assert c.fetch_ok is False


# ------------------------------------------------------------------ books
class TestBooksStayOnCrossRef:

    def test_book_types_are_fetched_from_crossref_and_deduplicated(self):
        scraper = MagicMock()
        book = MagicMock(doi='10.1109/book-1', work_type='edited-book')
        dup = MagicMock(doi='10.1109/icmew69720.2026', work_type='proceedings')
        scraper.search_by_filter.return_value = ([book, dup], [{'DOI': 'x'}])
        scraper.fetch_ok = True
        c = IeeeXploreCollector(dict(CONFIG, types=['proceedings', 'edited-book']), scraper, KEY,
                                quota=DailyQuota('x', 200, 0, usage=MemoryUsage()),
                                session=FakeSession(), delay=0)
        pubs, _ = c.fetch()
        flt = scraper.search_by_filter.call_args.args[0]
        assert 'type:edited-book' in flt and 'type:proceedings' not in flt
        assert [p.doi for p in pubs].count('10.1109/icmew69720.2026') == 1   # no duplicate
        assert any(p.doi == '10.1109/book-1' for p in pubs)


# ------------------------------------------------------------------ selection
class TestBuildCollector:

    def test_api_collector_when_key_present(self, monkeypatch):
        monkeypatch.setenv('IEEE_API_KEY', KEY)
        assert isinstance(build_collector(CONFIG, MagicMock()), IeeeXploreCollector)

    def test_falls_back_to_crossref_without_key(self, monkeypatch):
        monkeypatch.delenv('IEEE_API_KEY', raising=False)
        monkeypatch.setattr('pipeline.config.ensure_dotenv', lambda: None)
        assert isinstance(build_collector(CONFIG, MagicMock()), CrossRefPublisherCollector)

    def test_other_publishers_always_use_crossref(self, monkeypatch):
        monkeypatch.setenv('IEEE_API_KEY', KEY)
        wiley = {'name': 'Wiley', 'abbreviation': 'WILEY', 'crossref_prefixes': ['10.1002'],
                 'types': ['journal-issue']}
        assert isinstance(build_collector(wiley, MagicMock()), CrossRefPublisherCollector)


class TestDbQuota:
    """The counter must survive between separate CI jobs, so it lives in Postgres."""

    def test_counts_persist_across_instances(self, db_env):
        from pipeline.api_quota import DbUsage
        DbUsage().increment('ieee-xplore', date.today())
        DbUsage().increment('ieee-xplore', date.today())
        assert DbUsage().get('ieee-xplore', date.today()) == 2

    def test_quota_budget_is_shared_between_jobs(self, db_env):
        from pipeline.api_quota import DbUsage
        first = DailyQuota('ieee-xplore', limit=3, usage=DbUsage())
        assert [first.consume() for _ in range(2)] == [True, True]
        second = DailyQuota('ieee-xplore', limit=3, usage=DbUsage())   # a later CI job
        assert second.remaining() == 1
        assert second.consume() is True and second.consume() is False

    def test_days_are_independent(self, db_env):
        from pipeline.api_quota import DbUsage
        DbUsage().increment('ieee-xplore', date(2026, 1, 1))
        assert DbUsage().get('ieee-xplore', date(2026, 1, 2)) == 0


def test_conferences_without_a_usable_doi_get_their_own_identifier_and_never_collide():
    """The PCIM case: one-dot paper DOIs used to give the stem '10' for every conference."""
    def paper(pn, doi, title):
        return {'publication_number': pn, 'doi': doi, 'publication_title': title,
                'publication_year': '2026', 'conference_dates': '5-9 July 2026'}

    pubs = make()._proceedings([
        paper(11732832, '10.30420/456789012', 'PCIM Conference 2026'),
        paper(11732832, '10.30420/456789013', 'PCIM Conference 2026'),
        paper(11999999, '10.30420/999999991', 'Another Conference 2026'),
        paper(11000001, '10.1109/ICMEW69720.2026.11720727', 'A normal IEEE conference'),
    ])
    ids = {p.title: p.doi for p in pubs}
    assert ids == {'PCIM Conference 2026': 'ieee:conf:11732832',
                   'Another Conference 2026': 'ieee:conf:11999999',
                   'A normal IEEE conference': '10.1109/icmew69720.2026'}
    assert len({p.doi for p in pubs}) == 3
    assert all(p.doi != '10' for p in pubs)
