"""
Safeguards for running the API on the public internet (Render). Everything is driven by
environment variables, so the same code runs unchanged on a laptop.

  READ_ONLY=1               nothing is written to the database and the two endpoints that call other
                            websites (/api/doi/..., /api/cover/...) are switched off
  RATE_LIMIT_PER_MINUTE=N   at most N /api/ requests per minute per visitor (default 300, 0 = off)
  RENDER / TRUST_PROXY      behind Render's proxy: use the visitor's address, not the proxy's

Security headers are always sent. The page uses inline scripts and styles, so the policy allows
those but blocks every external script, font, image and connection.
"""

import os
import threading
import time
from collections import defaultdict, deque

from flask import jsonify, request

SWITCHED_OFF_WHEN_READ_ONLY = ('/api/doi/', '/api/cover/')
CSP = ("default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "
       "img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'none'; "
       "frame-ancestors 'none'")


def read_only() -> bool:
    return os.getenv('READ_ONLY', '').strip().lower() in ('1', 'true', 'yes', 'on')


def install(app) -> None:
    """Add the guard, the rate limit and the security headers to a Flask app."""
    if os.getenv('RENDER') or os.getenv('TRUST_PROXY'):
        from werkzeug.middleware.proxy_fix import ProxyFix
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)

    hits, lock = defaultdict(deque), threading.Lock()

    @app.before_request
    def guard():
        path = request.path
        if read_only() and path.startswith(SWITCHED_OFF_WHEN_READ_ONLY):
            return jsonify({'error': 'This endpoint is switched off on the public site.'}), 404
        try:
            limit = int(os.getenv('RATE_LIMIT_PER_MINUTE', '300'))
        except ValueError:
            limit = 300
        if limit > 0 and path.startswith('/api/'):
            now, visitor = time.monotonic(), request.remote_addr or 'unknown'
            with lock:
                window = hits[visitor]
                while window and now - window[0] > 60:
                    window.popleft()
                if len(window) >= limit:
                    retry = max(1, int(60 - (now - window[0])))
                    resp = jsonify({'error': 'Too many requests. Please wait a moment and try again.'})
                    resp.status_code = 429
                    resp.headers['Retry-After'] = str(retry)
                    return resp
                window.append(now)
                if len(hits) > 5000:                       # forget visitors that have gone quiet
                    for v in [v for v, w in hits.items() if not w or now - w[-1] > 60]:
                        del hits[v]

    @app.after_request
    def security_headers(resp):
        resp.headers.setdefault('X-Content-Type-Options', 'nosniff')
        resp.headers.setdefault('X-Frame-Options', 'DENY')
        resp.headers.setdefault('Referrer-Policy', 'strict-origin-when-cross-origin')
        resp.headers.setdefault('Content-Security-Policy', CSP)
        if request.is_secure:
            resp.headers.setdefault('Strict-Transport-Security', 'max-age=31536000')
        return resp
