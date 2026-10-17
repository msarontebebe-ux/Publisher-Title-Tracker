"""
API tests for publisher/work-type filtering, using Flask's test client against
the throwaway test database (never production).
"""

import importlib
from datetime import datetime, timedelta

import pytest

from pipeline.config import load_publisher_streams, resolve_publisher


def test_streams_include_springer_and_all_publishers():
    streams = load_publisher_streams('config.json')
    assert [s['key'] for s in streams] == ['SPRINGER', 'ELSEVIER', 'WILEY', 'IEEE', 'ACM']
    springer = streams[0]
    assert 'Lecture Notes in Computer Science' in springer['series']
    assert [s['series'] for s in streams[1:]] == [['Elsevier'], ['Wiley'], ['IEEE'], ['ACM']]


def test_resolve_publisher_by_key_or_name():
    streams = load_publisher_streams('config.json')
    assert resolve_publisher('wiley', streams)['key'] == 'WILEY'
    assert resolve_publisher('Springer', streams)['key'] == 'SPRINGER'
    assert resolve_publisher('nope', streams) is None
    assert resolve_publisher('', streams) is None


@pytest.fixture()
def client(db_env, monkeypatch):
    import api_server
    importlib.reload(api_server)
    from pipeline.db import DatabaseConfig
    from pipeline.models import Publication, TitleEvent, PipelineState

    now = datetime.utcnow()
    session = DatabaseConfig.get_session()
    rows = [
        ('10.1/a', 'LNCS Vol', 'Lecture Notes in Computer Science', 'proceedings', now),
        ('10.1002/b', 'Advanced Science Vol. 13 No. 1', 'Wiley', 'journal-issue', now),
        ('10.1002/c', 'A Wiley Monograph', 'Wiley', 'monograph', now - timedelta(days=30)),
        ('10.1109/d', '2026 IEEE Conf', 'IEEE', 'proceedings', now),
    ]
    for doi, title, series, wt, fetched in rows:
        session.add(Publication(doi=doi, title=title, series=series, work_type=wt,
                                year=2026, publisher=series, fetched_at=fetched))
        session.add(TitleEvent(event_id='e-' + doi, event_type='title.added',
                               source='crossref', source_record_id=doi, series=series,
                               publisher=series, payload_hash='h-' + doi,
                               payload={'title': title}, detected_at=fetched))
    session.add(PipelineState(series='Wiley', last_success_at=now))
    session.commit()
    session.close()
    api_server.app.config['TESTING'] = True
    return api_server.app.test_client()


def _dois(resp):
    return {r['doi'] for r in resp.get_json()['results']}


class TestPublicationFilters:

    def test_filter_by_publisher(self, client):
        assert _dois(client.get('/api/publications?publisher=wiley')) == {'10.1002/b', '10.1002/c'}

    def test_springer_means_its_series(self, client):
        assert _dois(client.get('/api/publications?publisher=SPRINGER')) == {'10.1/a'}

    def test_filter_by_type(self, client):
        assert _dois(client.get('/api/publications?type=proceedings')) == {'10.1/a', '10.1109/d'}

    def test_publisher_and_type_combine(self, client):
        r = client.get('/api/publications?publisher=WILEY&type=monograph')
        assert _dois(r) == {'10.1002/c'}

    def test_work_type_is_returned(self, client):
        r = client.get('/api/publications?publisher=IEEE').get_json()['results'][0]
        assert r['work_type'] == 'proceedings'

    def test_unknown_publisher_is_400(self, client):
        assert client.get('/api/publications?publisher=nope').status_code == 400


class TestNewAndEvents:

    def test_new_since_filters_by_publisher(self, client):
        since = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')
        r = client.get(f'/api/publications/new?since={since}&publisher=WILEY')
        assert _dois(r) == {'10.1002/b'}   # the 30-day-old monograph is excluded

    def test_events_filter_by_publisher_and_carry_work_type(self, client):
        r = client.get('/api/events?since=2020-01-01&publisher=WILEY').get_json()
        assert {e['doi'] for e in r['results']} == {'10.1002/b', '10.1002/c'}
        assert {e['work_type'] for e in r['results']} == {'journal-issue', 'monograph'}

    def test_events_filter_by_work_type(self, client):
        r = client.get('/api/events?since=2020-01-01&work_type=proceedings').get_json()
        assert {e['doi'] for e in r['results']} == {'10.1/a', '10.1109/d'}

    def test_events_unknown_publisher_is_400(self, client):
        assert client.get('/api/events?publisher=nope').status_code == 400

    def test_atom_feed_filters_by_publisher(self, client):
        body = client.get('/feed/events.atom?publisher=IEEE').get_data(as_text=True)
        assert '2026 IEEE Conf' in body and 'Advanced Science' not in body

    def test_rss_filters_by_publisher_and_type(self, client):
        body = client.get('/feed.rss?publisher=WILEY&type=journal-issue').get_data(as_text=True)
        assert 'Advanced Science' in body and 'Monograph' not in body


class TestPublishersEndpoint:

    def test_overview(self, client):
        data = client.get('/api/publishers?days=7').get_json()
        by_key = {p['key']: p for p in data['publishers']}
        assert set(by_key) == {'SPRINGER', 'ELSEVIER', 'WILEY', 'IEEE', 'ACM'}
        wiley = by_key['WILEY']
        assert wiley['total'] == 2
        assert wiley['new_in_window'] == 1
        assert wiley['by_type'] == {'journal-issue': 1, 'monograph': 1}
        assert wiley['last_success_at'] is not None
        assert by_key['ACM']['total'] == 0 and by_key['ACM']['last_success_at'] is None


def _fake_first_hop(monkeypatch, location):
    """Make doi.org answer with a single 302 to `location` (no network)."""
    import urllib.error
    import urllib.request
    from email.message import Message

    class FakeOpener:
        def open(self, req, timeout=8):
            headers = Message()
            if location:
                headers['Location'] = location
            raise urllib.error.HTTPError(req.full_url, 302 if location else 500,
                                         'redirect', headers, None)

    monkeypatch.setattr(urllib.request, 'build_opener', lambda *a, **k: FakeOpener())


def test_doi_redirect_uses_first_hop_target(client, monkeypatch):
    _fake_first_hop(monkeypatch, 'https://dl.acm.org/doi/proceedings/10.1145/3833907')
    r = client.get('/api/doi/10.1145/3833907')
    assert r.status_code == 302
    assert r.headers['Location'] == 'https://dl.acm.org/doi/proceedings/10.1145/3833907'


def test_doi_redirect_never_follows_to_the_publisher(client, monkeypatch):
    """Wiley/ACM return 403 to server-side requests - we must not depend on them."""
    import urllib.request
    _fake_first_hop(monkeypatch, 'https://sid.onlinelibrary.wiley.com/toc/19383657/34/10')
    monkeypatch.setattr(urllib.request, 'urlopen',
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError('followed')))
    r = client.get('/api/doi/10.1002/jsid.v34.10')
    assert r.headers['Location'] == 'https://sid.onlinelibrary.wiley.com/toc/19383657/34/10'


def test_doi_redirect_reroutes_broken_ieee_landing_pages(client, monkeypatch):
    _fake_first_hop(
        monkeypatch,
        'https://ieeexplore.ieee.org/xpl/mostRecentIssue.jsp?punumber=11720042/')
    r = client.get('/api/doi/10.1109/d')
    loc = r.headers['Location']
    assert 'ieeexplore.ieee.org' in loc and '10.1109/d' in loc
    assert 'mostRecentIssue' not in loc


def test_doi_redirect_falls_back_to_scholar_only_when_doi_org_fails(client, monkeypatch):
    _fake_first_hop(monkeypatch, None)
    r = client.get('/api/doi/10.1/a')
    assert r.headers['Location'].startswith('https://scholar.google.com/scholar?q=')


class TestBaselineHiddenFromApi:

    @pytest.fixture()
    def with_baseline(self, client):
        from pipeline.db import DatabaseConfig
        from pipeline.models import TitleEvent
        session = DatabaseConfig.get_session()
        session.add(TitleEvent(event_id='e-base', event_type='title.baseline',
                               source='crossref', source_record_id='10.1109/d',
                               series='IEEE', publisher='IEEE', payload_hash='h-base',
                               payload={'title': 'Quiet baseline'},
                               detected_at=datetime.utcnow()))
        session.commit()
        session.close()
        return client

    def test_events_hide_baselines_by_default(self, with_baseline):
        r = with_baseline.get('/api/events?since=2020-01-01&publisher=IEEE').get_json()
        assert 'title.baseline' not in {e['event_type'] for e in r['results']}

    def test_baselines_can_be_requested_explicitly(self, with_baseline):
        r = with_baseline.get('/api/events?since=2020-01-01&type=title.baseline').get_json()
        assert [e['event_type'] for e in r['results']] == ['title.baseline']

    def test_atom_feed_hides_baselines(self, with_baseline):
        body = with_baseline.get('/feed/events.atom?publisher=IEEE').get_data(as_text=True)
        assert 'Quiet baseline' not in body and '2026 IEEE Conf' in body
