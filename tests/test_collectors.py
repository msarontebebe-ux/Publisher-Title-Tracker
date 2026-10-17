"""
Tests for the publisher collector layer: filter building, journal-issue title
fallback, work_type capture, and fetch_ok (incomplete-fetch) tracking.
Zero network, zero database — CrossRef responses are faked.
"""

from unittest.mock import MagicMock

import pytest
import requests

from pipeline.collectors import Collector, CrossRefPublisherCollector
from pipeline.scraper import CrossRefScraper

WILEY = {
    'name': 'Wiley', 'abbreviation': 'WILEY',
    'crossref_prefixes': ['10.1002'], 'types': ['journal-issue', 'monograph'],
}

ISSUE_ITEM = {
    'DOI': '10.1002/advs.v13.56', 'type': 'journal-issue', 'publisher': 'Wiley',
    'created': {'date-time': '2026-10-05T08:30:00Z'},
    'container-title': ['Advanced Science'], 'volume': '13', 'issue': '56',
    'published-print': {'date-parts': [[2026, 10]]},
}
BOOK_ITEM = {
    'DOI': '10.1002/9781119999999', 'type': 'monograph', 'publisher': 'Wiley',
    'title': ['A Monograph'], 'author': [{'given': 'Ada', 'family': 'Lovelace'}],
    'published-print': {'date-parts': [[2026]]},
}


def _scraper_with(*pages, error=None):
    """CrossRefScraper whose HTTP session returns the given item pages."""
    scraper = CrossRefScraper(email='t@example.com', delay=0)
    session = MagicMock()
    if error:
        session.get.side_effect = error
    else:
        responses = []
        for i, items in enumerate(pages):
            resp = MagicMock()
            resp.json.return_value = {'message': {
                'items': items,
                'next-cursor': f'c{i + 1}',
                'total-results': sum(len(p) for p in pages),
            }}
            responses.append(resp)
        empty = MagicMock()
        empty.json.return_value = {'message': {'items': [], 'total-results': 0}}
        responses.append(empty)
        session.get.side_effect = responses
    scraper.session = session
    return scraper


class TestFilterBuilding:

    def test_prefix_and_types_are_joined(self):
        c = CrossRefPublisherCollector(WILEY, scraper=MagicMock())
        assert c.build_filter() == 'prefix:10.1002,type:journal-issue,type:monograph'

    def test_multiple_prefixes(self):
        cfg = dict(WILEY, crossref_prefixes=['10.1002', '10.1111'])
        c = CrossRefPublisherCollector(cfg, scraper=MagicMock())
        assert c.build_filter().startswith('prefix:10.1002,prefix:10.1111,')

    @pytest.mark.parametrize('missing', ['crossref_prefixes', 'types'])
    def test_missing_config_is_rejected(self, missing):
        cfg = {k: v for k, v in WILEY.items() if k != missing}
        with pytest.raises(ValueError):
            CrossRefPublisherCollector(cfg, scraper=MagicMock()).build_filter()

    def test_is_a_collector(self):
        assert issubclass(CrossRefPublisherCollector, Collector)


class TestFetch:

    def test_filter_and_watermark_reach_crossref(self):
        scraper = _scraper_with([ISSUE_ITEM])
        CrossRefPublisherCollector(WILEY, scraper).fetch(from_created_date='2026-10-01')
        params = scraper.session.get.call_args_list[0].kwargs['params']
        assert params['filter'] == ('prefix:10.1002,type:journal-issue,type:monograph,'
                                    'from-update-date:2026-10-01')

    def test_journal_issue_gets_readable_title_and_work_type(self):
        scraper = _scraper_with([ISSUE_ITEM, BOOK_ITEM])
        pubs, raw = CrossRefPublisherCollector(WILEY, scraper).fetch()
        by_doi = {p.doi: p for p in pubs}
        issue = by_doi['10.1002/advs.v13.56']
        assert issue.title == 'Advanced Science Vol. 13 No. 56'
        assert issue.work_type == 'journal-issue'
        assert issue.publisher == 'Wiley'
        assert by_doi['10.1002/9781119999999'].work_type == 'monograph'
        assert len(raw) == 2

    def test_work_type_is_in_to_dict(self):
        scraper = _scraper_with([BOOK_ITEM])
        pubs, _ = CrossRefPublisherCollector(WILEY, scraper).fetch()
        assert pubs[0].to_dict()['work_type'] == 'monograph'


class TestFetchOk:

    def test_complete_fetch_is_ok(self):
        scraper = _scraper_with([ISSUE_ITEM])
        CrossRefPublisherCollector(WILEY, scraper).fetch()
        assert scraper.fetch_ok is True

    def test_network_error_marks_fetch_incomplete(self):
        scraper = _scraper_with(error=requests.ConnectionError('boom'))
        pubs, _ = CrossRefPublisherCollector(WILEY, scraper).fetch()
        assert pubs == []
        assert scraper.fetch_ok is False

    def test_hitting_max_results_marks_fetch_incomplete(self):
        scraper = _scraper_with([ISSUE_ITEM, BOOK_ITEM], [dict(BOOK_ITEM, DOI='10.1002/x')])
        CrossRefPublisherCollector(WILEY, scraper).fetch(max_results=2)
        assert scraper.fetch_ok is False

    def test_flag_resets_between_searches(self):
        scraper = _scraper_with(error=requests.ConnectionError('boom'))
        CrossRefPublisherCollector(WILEY, scraper).fetch()
        assert scraper.fetch_ok is False
        scraper.session = _scraper_with([ISSUE_ITEM]).session
        CrossRefPublisherCollector(WILEY, scraper).fetch()
        assert scraper.fetch_ok is True


class TestCreatedDate:

    def test_created_date_is_captured_from_crossref(self):
        scraper = _scraper_with([ISSUE_ITEM])
        pubs, _ = CrossRefPublisherCollector(WILEY, scraper).fetch()
        assert pubs[0].created == '2026-10-05T08:30:00Z'

    def test_created_is_requested_from_crossref(self):
        scraper = _scraper_with([ISSUE_ITEM])
        CrossRefPublisherCollector(WILEY, scraper).fetch()
        select = scraper.session.get.call_args_list[0].kwargs['params']['select']
        assert 'created' in select.split(',')

    def test_missing_created_is_none(self):
        scraper = _scraper_with([BOOK_ITEM])
        pubs, _ = CrossRefPublisherCollector(WILEY, scraper).fetch()
        assert pubs[0].created is None
