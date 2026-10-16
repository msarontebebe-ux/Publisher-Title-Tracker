"""
Regression guard: an empty ISSN search must NOT trigger the fuzzy title search.
That fallback ignored the watermark and paged through thousands of unrelated
records (log full of 'Fetched 0/5000 publications...') before finding nothing.
Zero network, zero database.
"""

from unittest.mock import MagicMock

import pytest
import requests

from pipeline import runner
from pipeline.scraper import CrossRefScraper


@pytest.fixture(autouse=True)
def no_db(monkeypatch):
    monkeypatch.setattr(runner, '_db_configured', lambda: False)


def _scraper():
    scraper = MagicMock()
    scraper.search_by_issn.return_value = ([], [])
    scraper.search_by_series_title.return_value = []
    scraper.fetch_ok = True
    return scraper


WITH_ISSN = {'name': 'Lecture Notes in Electrical Engineering', 'abbreviation': 'LNEE',
             'issn': '1876-1100', 'issn_electronic': '1876-1119'}
NO_ISSN = {'name': 'Some Series Without ISSN', 'abbreviation': 'SSW'}


def test_empty_issn_result_does_not_fall_back_to_title_search():
    scraper = _scraper()
    result = runner.scrape_single_series(WITH_ISSN, scraper, MagicMock(),
                                         run_type='incremental')
    assert result == []
    scraper.search_by_series_title.assert_not_called()


def test_both_issns_are_still_tried_before_giving_up():
    scraper = _scraper()
    runner.scrape_single_series(WITH_ISSN, scraper, MagicMock(), run_type='incremental')
    issns = [c.args[0] for c in scraper.search_by_issn.call_args_list]
    assert issns == ['1876-1100', '1876-1119']


def test_series_without_issn_still_uses_title_search():
    scraper = _scraper()
    runner.scrape_single_series(NO_ISSN, scraper, MagicMock(), run_type='full')
    scraper.search_by_series_title.assert_called_once()
    scraper.search_by_issn.assert_not_called()


def test_title_search_failure_marks_fetch_incomplete():
    scraper = CrossRefScraper(email='t@example.com', delay=0)
    session = MagicMock()
    session.get.side_effect = requests.ConnectionError('boom')
    scraper.session = session
    assert scraper.search_by_series_title('Some Series') == []
    assert scraper.fetch_ok is False
