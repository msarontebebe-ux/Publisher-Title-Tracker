"""
Read-only endpoints for the web page (/api/dashboard/...), against the throwaway test database.
Data: the star-schema seed from tests/test_star_schema.py. By the page's definition it holds
6 new titles (Springer 2, Wiley 1, Elsevier 1, ACM 2), 1 update, 1 backlog title (IEEE) and
1 baseline, which is neither new nor an update.
"""

import importlib

import pytest
from sqlalchemy import create_engine, text

from tests.test_star_schema import rows, seeded      # noqa: F401  (fixture)


@pytest.fixture()
def client(seeded):
    import api_server
    importlib.reload(api_server)
    return api_server.app.test_client()


def get(client, url):
    resp = client.get(url)
    return resp.status_code, resp.get_json()


# ----------------------------------------------------------------------------- summary
def test_summary_counts_follow_the_page_definition(client):
    status, d = get(client, '/api/dashboard/summary?days=7')
    assert status == 200
    assert d['new_titles'] == 6                      # backlog and baseline are not new
    assert d['updated_titles'] == 1
    assert d['new_titles_previous'] is None          # the previous 7 days start before the data does
    assert d['titles_stored'] == 8
    assert d['latency']['titles'] == 4               # Springer 2 + ACM 2 (daily arrivals with a registration time)
    assert 1.5 <= d['latency']['median_hours'] <= 5.5
    assert d['runs'] == {'total': 2, 'successful': 1}
    assert d['data_since'] is not None and d['last_success_at'] is None


def test_all_time_has_no_previous_period(client):
    status, d = get(client, '/api/dashboard/summary?days=0')
    assert status == 200 and d['new_titles'] == 6 and d['new_titles_previous'] is None


def test_days_is_clamped(client):
    assert get(client, '/api/dashboard/summary?days=999999')[1]['days'] == 3650
    assert get(client, '/api/dashboard/summary?days=-5')[1]['days'] == 0


# ---------------------------------------------------------------------------- arrivals
def test_arrivals_are_daily_for_short_periods_with_empty_days_filled(client):
    status, d = get(client, '/api/dashboard/arrivals?days=7')
    assert status == 200 and d['granularity'] == 'day' and len(d['buckets']) == 7
    assert sum(b['total'] for b in d['buckets']) == 6
    today = d['buckets'][-1]
    assert today['values'] == {'Springer': 2, 'Wiley': 1, 'Elsevier': 1, 'ACM': 2}
    assert all(b['total'] == 0 for b in d['buckets'][:-1])
    assert 'IEEE' not in d['publishers']              # its only title is a backlog title


def test_arrivals_are_weekly_for_long_periods(client):
    from datetime import date
    status, d = get(client, '/api/dashboard/arrivals?days=90')
    assert d['granularity'] == 'week'
    assert all(date.fromisoformat(b['start']).weekday() == 0 for b in d['buckets'])     # weeks start on Monday
    assert sum(b['total'] for b in d['buckets']) == 6
    assert d['buckets'][-1]['end'] == d['to']


# ----------------------------------------------------------------- latency and countries
def test_latency_per_publisher(client):
    _, d = get(client, '/api/dashboard/latency?days=7')
    by = {r['publisher']: r for r in d['publishers']}
    assert set(by) == {'Springer', 'ACM'}              # Wiley/Elsevier unknown, IEEE is backlog
    assert by['Springer']['titles'] == 2 and by['ACM']['titles'] == 2
    assert by['ACM']['median_hours'] == pytest.approx(2.0, abs=0.2)
    assert by['ACM']['p90_hours'] >= by['ACM']['median_hours']


def test_countries_skip_unknown_and_count_new_titles_only(client):
    _, d = get(client, '/api/dashboard/countries?days=7')
    got = {c['country']: c['titles'] for c in d['countries']}
    assert got == {'Japan': 2, 'Germany': 1, 'India': 1}   # the IEEE 'Chennai' title is backlog: not counted
    assert [c['country'] for c in d['countries']][0] == 'Japan'
    assert len(get(client, '/api/dashboard/countries?limit=1')[1]['countries']) == 1


# -------------------------------------------------------------------------------- titles
def test_titles_default_lists_new_titles_newest_first_with_links_and_conference(client):
    status, d = get(client, '/api/dashboard/titles?days=7')
    assert status == 200 and d['total'] == 6 and d['count'] == 6
    daily = next(r for r in d['results'] if r['doi'] == '10.1007/daily')
    assert daily['event'] == 'title.added' and daily['publisher'] == 'Springer'
    assert daily['url'] == 'https://doi.org/10.1007/daily'
    assert daily['conference'] == {'name': None, 'place': 'Mainz, Germany', 'start': '2026-10-05', 'end': None}
    stamps = [r['detected_at'] for r in d['results']]
    assert stamps == sorted(stamps, reverse=True)


@pytest.mark.parametrize('query,expected', [
    ('kind=updated', 1), ('kind=all', 7), ('kind=new&publisher=acm', 2),
    ('kind=new&publisher=ACM&type_group=Proceedings', 2), ('kind=new&type_group=Journal+issue', 1),
    ('kind=new&q=daily', 1), ('kind=new&q=DAILY', 1), ('kind=new&q=nothing-matches', 0),
])
def test_titles_filters(client, query, expected):
    assert get(client, f'/api/dashboard/titles?days=7&{query}')[1]['total'] == expected


def test_titles_pagination_and_limits(client):
    _, page1 = get(client, '/api/dashboard/titles?limit=4&offset=0')
    _, page2 = get(client, '/api/dashboard/titles?limit=4&offset=4')
    assert (page1['total'], page1['count'], page2['count']) == (6, 4, 2)
    assert not {r['doi'] for r in page1['results']} & {r['doi'] for r in page2['results']}
    assert get(client, '/api/dashboard/titles?limit=100000')[1]['count'] == 6      # clamped to 100


def test_titles_rejects_an_unknown_kind(client):
    assert get(client, '/api/dashboard/titles?kind=bogus')[0] == 400


def test_a_search_term_is_matched_literally(client):
    assert get(client, '/api/dashboard/titles?q=%25')[1]['total'] == 0          # a lone % matches nothing
    assert get(client, '/api/dashboard/search?q=_')[1]['total'] == 0            # nor does a lone _


def test_reading_the_page_never_writes(client, seeded):
    for url in ('summary', 'arrivals', 'latency', 'countries', 'titles?kind=all', 'search?q=T'):
        assert get(client, f'/api/dashboard/{url}')[0] == 200
    assert rows(seeded, 'SELECT COUNT(*) FROM events WHERE delivered_at IS NOT NULL') == [(0,)]


# --------------------------------------------------------------------------------- search
def test_search_covers_all_stored_titles(client):
    _, d = get(client, '/api/dashboard/search')
    assert d['total'] == 8 and d['count'] == 8                   # includes the backlog and the baseline title
    _, d = get(client, '/api/dashboard/search?publisher=IEEE')
    assert d['total'] == 1 and d['results'][0]['publisher'] == 'IEEE'
    assert get(client, '/api/dashboard/search?year=2026')[1]['total'] == 8
    assert get(client, '/api/dashboard/search?year=1999')[1]['total'] == 0
    _, d = get(client, '/api/dashboard/search?q=old&limit=5')
    assert [r['doi'] for r in d['results']] == ['10.1109/old']


# ---------------------------------------------------------------------------- robustness
def test_answers_can_be_cached_for_a_minute(client):
    assert client.get('/api/dashboard/summary').headers['Cache-Control'] == 'public, max-age=60'


def test_a_missing_star_schema_is_a_clear_503(client, seeded):
    from pipeline.models import Base
    engine = create_engine(seeded)
    try:
        with engine.begin() as conn:
            conn.execute(text('DROP TABLE gold.fact_title_event CASCADE'))
        status, d = get(client, '/api/dashboard/summary')
        assert status == 503 and 'python -m pipeline.gold' in d['error']
    finally:
        Base.metadata.create_all(engine)                          # put the table back for the other tests
        engine.dispose()


def test_without_a_database_the_endpoints_say_so(seeded, monkeypatch):
    import api_server
    importlib.reload(api_server)
    for name in ('DATABASE_URL', 'AWS_RDS_CONNECTION_STRING', 'AZURE_SQL_CONNECTION_STRING', 'DB_TYPE'):
        monkeypatch.delenv(name, raising=False)
    status, d = get(api_server.app.test_client(), '/api/dashboard/summary')
    assert status == 503 and d['error'] == 'Database not configured'


# ------------------------------------------------------------------------------- the page
def test_the_page_is_self_contained_and_only_reads(client):
    import re
    html = client.get('/').get_data(as_text=True)
    assert '<title>Publisher Title Tracker</title>' in html
    for anchor in ('id="overview"', 'id="charts"', 'id="titles"', 'id="about"', 'id="c-arrivals"',
                   'id="c-latency"', 'id="c-countries"'):
        assert anchor in html
    assert not re.search(r'(src|href)="https?://', html)           # no external script, style or font
    assert 'innerHTML' not in html                                  # text from the data is never parsed as markup
    assert "API = '/api/dashboard/'" in html
    assert '/api/events' not in html                                # that endpoint marks events as delivered


def test_the_page_offers_dark_mode_and_a_table_twin_for_every_chart(client):
    html = client.get('/').get_data(as_text=True)
    assert 'prefers-color-scheme: dark' in html and 'data-theme="dark"' in html
    assert "'Table view'" in html and 'prefers-reduced-motion' in html


def test_the_old_endpoints_still_work(client):
    assert client.get('/api').status_code == 200
    assert 'dashboard' in str(client.get('/api').get_json()['endpoints'])
    assert client.get('/api/publishers').status_code == 200
