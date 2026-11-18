"""
Safeguards for the public site: read-only mode, switched-off endpoints, rate limit, security
headers and the deployment file. Uses the throwaway test database.
"""

import importlib
from pathlib import Path

import pytest

from tests.test_star_schema import rows, seeded      # noqa: F401  (fixture)


def make_client(monkeypatch, **env):
    for name in ('READ_ONLY', 'RATE_LIMIT_PER_MINUTE', 'RENDER', 'TRUST_PROXY'):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    import api_server
    importlib.reload(api_server)
    return api_server.app.test_client()


# ----------------------------------------------------------------------------- headers
def test_security_headers_are_sent_on_the_page_and_on_the_api(seeded, monkeypatch):
    client = make_client(monkeypatch)
    for url in ('/', '/api/dashboard/summary'):
        h = client.get(url).headers
        assert h['X-Content-Type-Options'] == 'nosniff' and h['X-Frame-Options'] == 'DENY'
        assert "default-src 'self'" in h['Content-Security-Policy']
        assert "frame-ancestors 'none'" in h['Content-Security-Policy']
        assert 'Strict-Transport-Security' not in h                      # plain http: no HSTS


def test_https_adds_hsts(seeded, monkeypatch):
    client = make_client(monkeypatch)
    assert client.get('/', base_url='https://example.org').headers['Strict-Transport-Security']


def test_the_policy_blocks_everything_external(seeded, monkeypatch):
    csp = make_client(monkeypatch).get('/').headers['Content-Security-Policy']
    assert 'http' not in csp and "connect-src 'self'" in csp and "img-src 'self' data:" in csp


# ------------------------------------------------------------------------- read-only mode
def test_read_only_switches_off_the_endpoints_that_call_other_websites(seeded, monkeypatch):
    client = make_client(monkeypatch, READ_ONLY='1')
    for url in ('/api/doi/10.1007/978-3-032-41086-3', '/api/cover/9783032410863'):
        resp = client.get(url)
        assert resp.status_code == 404 and 'switched off' in resp.get_json()['error']
    assert client.get('/api/dashboard/summary').status_code == 200        # the page keeps working


def test_read_only_never_marks_events_as_delivered(seeded, monkeypatch):
    client = make_client(monkeypatch, READ_ONLY='1')
    resp = client.get('/api/events?since=2020-01-01&limit=50')
    assert resp.status_code == 200 and resp.get_json()['count'] > 0
    assert rows(seeded, 'SELECT COUNT(*) FROM events WHERE delivered_at IS NOT NULL') == [(0,)]


def test_without_read_only_the_events_endpoint_still_records_delivery(seeded, monkeypatch):
    client = make_client(monkeypatch)
    assert client.get('/api/events?since=2020-01-01&limit=50').status_code == 200
    assert rows(seeded, 'SELECT COUNT(*) FROM events WHERE delivered_at IS NOT NULL')[0][0] > 0


@pytest.mark.parametrize('value,expected', [('1', True), ('true', True), ('YES', True), ('0', False), ('', False)])
def test_read_only_flag_parsing(monkeypatch, value, expected):
    import public_mode
    monkeypatch.setenv('READ_ONLY', value)
    assert public_mode.read_only() is expected


# ---------------------------------------------------------------------------- rate limit
def test_a_visitor_over_the_limit_gets_429_with_retry_after(seeded, monkeypatch):
    client = make_client(monkeypatch, RATE_LIMIT_PER_MINUTE='3')
    codes = [client.get('/api/dashboard/latency').status_code for _ in range(5)]
    assert codes == [200, 200, 200, 429, 429]
    resp = client.get('/api/dashboard/latency')
    assert 1 <= int(resp.headers['Retry-After']) <= 60 and 'Too many' in resp.get_json()['error']


def test_the_page_itself_is_never_rate_limited(seeded, monkeypatch):
    client = make_client(monkeypatch, RATE_LIMIT_PER_MINUTE='1')
    assert all(client.get('/').status_code == 200 for _ in range(5))


def test_a_limit_of_zero_switches_it_off(seeded, monkeypatch):
    client = make_client(monkeypatch, RATE_LIMIT_PER_MINUTE='0')
    assert all(client.get('/api/dashboard/latency').status_code == 200 for _ in range(6))


def test_each_visitor_has_their_own_budget_behind_the_proxy(seeded, monkeypatch):
    client = make_client(monkeypatch, RATE_LIMIT_PER_MINUTE='2', RENDER='1')
    def hit(ip):
        return client.get('/api/dashboard/latency', headers={'X-Forwarded-For': ip}).status_code
    assert [hit('1.1.1.1'), hit('1.1.1.1'), hit('1.1.1.1')] == [200, 200, 429]
    assert hit('2.2.2.2') == 200                                          # another visitor is not affected


def test_the_forwarded_address_is_ignored_when_not_behind_a_proxy(seeded, monkeypatch):
    client = make_client(monkeypatch, RATE_LIMIT_PER_MINUTE='2')
    codes = [client.get('/api/dashboard/latency', headers={'X-Forwarded-For': f'9.9.9.{i}'}).status_code for i in range(3)]
    assert codes == [200, 200, 429]                                       # cannot be dodged by sending a header


# ------------------------------------------------------------------------ deployment file
def test_render_file_is_safe():
    text = (Path(__file__).resolve().parent.parent / 'render.yaml').read_text(encoding='utf-8')
    assert 'postgresql' not in text and 'password' not in text.lower()
    assert 'key: DATABASE_URL' in text and 'sync: false' in text         # the secret is typed in the dashboard
    assert 'key: READ_ONLY' in text and 'value: "1"' in text
    assert 'healthCheckPath: /health' in text and 'gunicorn api_server:app' in text
