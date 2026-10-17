"""
Tests for the Springer Meta API collector. Uses one saved real API page
(tests/fixtures/springer_sample.json) - no network, no API key, no database.
"""

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline.api_quota import DailyQuota, MemoryUsage
from pipeline.collectors import build_springer_api_collector
from pipeline.collectors.springer_api import (
    SpringerApiCollector, _given_family, volume_doi,
)

PAGE = json.loads(
    (Path(__file__).parent / 'fixtures' / 'springer_sample.json').read_text(encoding='utf-8'))
KEY = 'SECRETKEY1234567890ABCD'
SERIES = {'558': 'Lecture Notes in Computer Science',
          '7899': 'Communications in Computer and Information Science'}
CONFIG = {'name': 'Springer API', 'series_by_id': SERIES, 'daily_limit': 500,
          'reserve': 25, 'page_size': 25, 'initial_days': 7}


def one_page(total=None):
    """The fixture page, claiming that it is the whole result (so paging stops)."""
    page = json.loads(json.dumps(PAGE))
    page['result'][0]['total'] = str(total or len(page['records']))
    return page


class FakeSession:
    def __init__(self, status=200, pages=None, error=None):
        self.calls, self.status, self.error = [], status, error
        self.pages = list(pages) if pages else [one_page()]

    def get(self, url, params=None, timeout=None):
        self.calls.append(dict(params))
        if self.error:
            raise self.error
        resp = MagicMock(status_code=self.status)
        resp.json.return_value = self.pages.pop(0) if len(self.pages) > 1 else self.pages[0]
        return resp


def make(session=None, limit=500, reserve=0, series=SERIES, **cfg):
    quota = DailyQuota('springer-api', limit, reserve, usage=MemoryUsage())
    config = dict(CONFIG, series_by_id=series, **cfg)
    return SpringerApiCollector(config, api_key=KEY, quota=quota,
                                session=session or FakeSession(), delay=0)


# ---------------------------------------------------------------- pure helpers
@pytest.mark.parametrize('chapter,book', [
    ('10.1007/978-3-032-41086-3_52', '10.1007/978-3-032-41086-3'),
    ('10.1007/978-3-031-00000-0_1', '10.1007/978-3-031-00000-0'),
    ('10.1007/s12345-020-00001-1', None),     # an article: no _<n> suffix
    ('', None), (None, None),
])
def test_volume_doi(chapter, book):
    assert volume_doi(chapter) == book


def test_editor_names_use_the_crossref_order():
    assert _given_family('Maisch, Bettina') == 'Bettina Maisch'
    assert _given_family('Plato') == 'Plato'


# ------------------------------------------------------------------- fetching
def test_chapters_are_grouped_into_volumes_of_tracked_series():
    pubs, chapters = make().fetch(from_created_date='2026-10-10')
    assert len(chapters) == 25
    assert len(pubs) == 9                              # 22 series chapters -> 9 volumes
    assert len({p.doi for p in pubs}) == 9
    assert {p.series for p in pubs} <= set(SERIES.values())
    assert all(p.source == 'springer-api' and p.publisher for p in pubs)
    assert all(not p.doi.rsplit('_', 1)[-1].isdigit() or '-' in p.doi for p in pubs)


def test_untracked_series_are_ignored():
    pubs, _ = make(series={'7899': SERIES['7899']}).fetch(from_created_date='2026-10-10')
    assert [p.series for p in pubs] == [SERIES['7899']]


def test_proceedings_label_and_conference_data_come_from_the_api():
    pubs, _ = make().fetch(from_created_date='2026-10-10')
    vol = next(p for p in pubs if p.doi == '10.1007/978-3-032-41086-3')
    assert vol.work_type == 'proceedings'
    assert vol.event_date_confidence == 'high'
    assert vol.event_date_start == '2026-10-05' and vol.event_date_end == '2026-10-09'
    assert (vol.event_year, vol.event_month) == (2026, 10)
    assert vol.event_name == 'DS 2026'
    assert vol.event_location == 'Mainz, Germany'
    assert vol.title == 'Discovery Science'            # the API only has the short name


def test_request_uses_the_watermark_date_and_the_basic_page_size():
    session = FakeSession()
    make(session).fetch(from_created_date='2026-10-10')
    call = session.calls[0]
    assert call['q'] == 'type:Book onlinedatefrom:2026-10-10'
    assert call['p'] == 25 and call['s'] == 1


def test_without_watermark_it_looks_back_initial_days():
    session = FakeSession()
    make(session).fetch()
    since = session.calls[0]['q'].split('onlinedatefrom:')[1]
    assert (date.today() - date.fromisoformat(since)).days in (7, 8)


def test_paging_continues_until_every_chapter_is_read():
    session = FakeSession(pages=[one_page(total=50), one_page(total=50)])
    collector = make(session)
    _, chapters = collector.fetch(from_created_date='2026-10-10')
    assert len(chapters) == 50 and [c['s'] for c in session.calls] == [1, 26]
    assert collector.fetch_ok is True


def test_nothing_found_is_a_valid_empty_answer():
    collector = make(FakeSession(status=404))
    pubs, chapters = collector.fetch(from_created_date='2026-10-10')
    assert pubs == [] and chapters == [] and collector.fetch_ok is True


@pytest.mark.parametrize('status', [403, 500])
def test_http_errors_stop_the_fetch(status):
    collector = make(FakeSession(status=status))
    collector.fetch(from_created_date='2026-10-10')
    assert collector.fetch_ok is False


def test_daily_budget_stops_the_fetch():
    session = FakeSession()
    collector = make(session, limit=0)
    pubs, _ = collector.fetch(from_created_date='2026-10-10')
    assert session.calls == [] and pubs == [] and collector.fetch_ok is False


def test_chapter_limit_marks_the_fetch_incomplete():
    session = FakeSession(pages=[one_page(total=500)])
    collector = make(session)
    collector.fetch(from_created_date='2026-10-10', max_results=25)
    assert collector.fetch_ok is False and len(session.calls) == 1


def test_the_key_is_never_printed(capsys):
    collector = make(FakeSession(error=RuntimeError(f'GET ...api_key={KEY} failed')))
    collector.fetch(from_created_date='2026-10-10')
    out = capsys.readouterr().out
    assert KEY not in out and '<KEY>' in out and collector.fetch_ok is False


# --------------------------------------------------------------------- builder
def test_builder_needs_key_and_switch(monkeypatch):
    monkeypatch.setattr('pipeline.config.ensure_dotenv', lambda: None)
    config = {'springer_api': {'enabled': True},
              'springer_series': [
                  {'name': 'LNCS full', 'springer_series_id': '558'},
                  {'name': 'LNAI full', 'springer_series_id': '1244', 'scrape_via_issn': False}]}
    monkeypatch.delenv('SPRINGER_API_KEY', raising=False)
    assert build_springer_api_collector(config) is None            # no key
    monkeypatch.setenv('SPRINGER_API_KEY', KEY)
    assert build_springer_api_collector({'springer_api': {'enabled': False}}) is None
    assert build_springer_api_collector({}) is None
    collector = build_springer_api_collector(config)
    assert collector.series_by_id == {'558': 'LNCS full'}          # LNAI stays with CrossRef
