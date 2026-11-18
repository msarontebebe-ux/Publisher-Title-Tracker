"""
Read-only JSON endpoints for the web page (/api/dashboard/...).

They read the star schema (gold.fact_title_event, gold.fact_pipeline_run and the dimensions,
filled by pipeline.star) and the titles table. Nothing here writes to the database, unlike
/api/events, which marks events as delivered; the page only uses these.

Definition used everywhere on the page, so every number agrees:
  new title      = a `title.added` event that is not "Backlog / first load" (titles loaded when
                   the system started are stored but not counted as new)
  updated title  = a `title.updated` event
  period         = the last `days` calendar days including today (UTC); days=0 means all time

Params shared by the endpoints: days (default 7; 0 = all time; max 3650).
"""

import re
from datetime import date, datetime, timedelta, timezone

from flask import Blueprint, current_app, jsonify, request
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError
from werkzeug.exceptions import HTTPException

from pipeline.events import decode_entities

dashboard = Blueprint('dashboard', __name__, url_prefix='/api/dashboard')

BACKLOG = 'Backlog / first load'
MAX_DAYS = 3650
NEW = "et.counts_as_new AND lt.load_type <> 'Backlog / first load'"
STAR_MISSING = ('The star schema is not available yet. '
                'Run `python init_database.py` and then `python -m pipeline.gold`.')


# ------------------------------------------------------------------------------ helpers
def _session():
    from pipeline.db import DatabaseConfig
    return DatabaseConfig.get_session()


def _db_ok() -> bool:
    from pipeline.db import DatabaseConfig
    return DatabaseConfig.is_cloud_configured()


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _days() -> int:
    days = request.args.get('days', default=7, type=int)
    return max(0, min(days if days is not None else 7, MAX_DAYS))


def _window(session, days: int):
    """(first_day, last_day, previous_first_day or None) of the period."""
    today = _today()
    if days == 0:
        first = session.execute(text(
            'SELECT MIN(d.full_date) FROM gold.fact_title_event f '
            'JOIN gold.dim_date d ON d.date_key = f.detected_date_key')).scalar() or today
        return first, today, None
    first = today - timedelta(days=days - 1)
    return first, today, first - timedelta(days=days)


def _like(value: str) -> str:
    """Escape % and _ so a search term is matched literally."""
    return '%' + re.sub(r'([\\%_])', r'\\\1', value.strip()) + '%'


def _num(value):
    return None if value is None else float(value)


@dashboard.errorhandler(Exception)
def _handle(exc):
    # A missing star schema (new database, init not run yet) is a clear 503, not a stack trace.
    if isinstance(exc, HTTPException):
        return exc
    if isinstance(exc, ProgrammingError) and 'does not exist' in str(exc.orig):
        return jsonify({'error': STAR_MISSING}), 503
    current_app.logger.exception('dashboard endpoint failed')
    return jsonify({'error': 'Something went wrong while reading the data.'}), 500


@dashboard.before_request
def _need_database():
    if not _db_ok():
        return jsonify({'error': 'Database not configured'}), 503


@dashboard.after_request
def _cache(response):
    # The data changes at most once a night: let browsers and proxies reuse an answer for a minute.
    if response.status_code == 200:
        response.headers['Cache-Control'] = 'public, max-age=60'
    return response


# ------------------------------------------------------------------------------ endpoints
@dashboard.route('/summary')
def summary():
    """Headline numbers for the period, with the previous period for comparison."""
    days = _days()
    session = _session()
    try:
        first, last, prev_first = _window(session, days)
        q = lambda sql, **p: session.execute(text(sql), p)
        facts = ('FROM gold.fact_title_event f '
                 'JOIN gold.dim_date d ON d.date_key = f.detected_date_key '
                 'JOIN gold.dim_event_type et ON et.event_type_key = f.event_type_key '
                 'JOIN gold.dim_load_type lt ON lt.load_type_key = f.load_type_key ')

        def count_new(a, b):
            return q(f'SELECT COALESCE(SUM(f.event_count), 0) {facts} '
                     f'WHERE {NEW} AND d.full_date BETWEEN :a AND :b', a=a, b=b).scalar()

        new_now = count_new(first, last)
        new_before = count_new(prev_first, first - timedelta(days=1)) if prev_first else None
        updated = q(f"SELECT COALESCE(SUM(f.event_count), 0) {facts} "
                    f"WHERE et.event_type = 'title.updated' AND d.full_date BETWEEN :a AND :b",
                    a=first, b=last).scalar()
        lat = q(f"""SELECT COUNT(*),
                           percentile_cont(0.5) WITHIN GROUP (ORDER BY f.latency_hours),
                           percentile_cont(0.9) WITHIN GROUP (ORDER BY f.latency_hours)
                    {facts} WHERE et.counts_as_new AND lt.load_type = 'Daily arrival'
                      AND f.latency_hours IS NOT NULL AND d.full_date BETWEEN :a AND :b""",
                a=first, b=last).one()
        runs = q("""SELECT COALESCE(SUM(r.runs), 0), COALESCE(SUM(r.is_success), 0)
                    FROM gold.fact_pipeline_run r JOIN gold.dim_date d ON d.date_key = r.run_date_key
                    WHERE d.full_date BETWEEN :a AND :b""", a=first, b=last).one()
        stored = q('SELECT COUNT(*) FROM publications').scalar()
        last_run = q('SELECT MAX(last_success_at) FROM pipeline_state').scalar()
        since = q('SELECT MIN(d.full_date) FROM gold.fact_title_event f '
                  'JOIN gold.dim_date d ON d.date_key = f.detected_date_key').scalar()
    finally:
        session.close()
    if since and prev_first and prev_first < since:
        new_before = None            # the previous period starts before the system did: no fair comparison
    return jsonify({
        'days': days, 'from': first.isoformat(), 'to': last.isoformat(),
        'titles_stored': stored,
        'new_titles': int(new_now), 'new_titles_previous': None if new_before is None else int(new_before),
        'updated_titles': int(updated),
        'latency': {'titles': lat[0], 'median_hours': _num(lat[1]), 'p90_hours': _num(lat[2])},
        'runs': {'total': int(runs[0]), 'successful': int(runs[1])},
        'last_success_at': last_run.isoformat() if last_run else None,
        'data_since': since.isoformat() if since else None,
        'generated_at': datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
    })


@dashboard.route('/arrivals')
def arrivals():
    """New titles per day (periods up to 31 days) or per week, split by publisher."""
    days = _days()
    session = _session()
    try:
        first, last, _ = _window(session, days)
        weekly = (last - first).days + 1 > 31
        bucket = "(d.full_date - (EXTRACT(isodow FROM d.full_date)::int - 1))" if weekly else 'd.full_date'
        rows = session.execute(text(f"""
            SELECT {bucket} AS b, p.publisher, SUM(f.event_count)
            FROM gold.fact_title_event f
            JOIN gold.dim_date d ON d.date_key = f.detected_date_key
            JOIN gold.dim_publisher p ON p.publisher_key = f.publisher_key
            JOIN gold.dim_event_type et ON et.event_type_key = f.event_type_key
            JOIN gold.dim_load_type lt ON lt.load_type_key = f.load_type_key
            WHERE {NEW} AND d.full_date BETWEEN :a AND :b GROUP BY 1, 2"""), {'a': first, 'b': last}).fetchall()
    finally:
        session.close()

    step = timedelta(days=7 if weekly else 1)
    start = first - timedelta(days=first.weekday()) if weekly else first
    buckets, cur = {}, start
    while cur <= last:                                  # every bucket exists, empty ones are 0
        buckets[cur] = {}
        cur += step
    publishers = []
    for b, publisher, n in rows:
        buckets.setdefault(b, {})[publisher] = int(n)
        if publisher not in publishers:
            publishers.append(publisher)
    return jsonify({
        'granularity': 'week' if weekly else 'day', 'from': first.isoformat(), 'to': last.isoformat(),
        'publishers': publishers,
        'buckets': [{'start': b.isoformat(),
                     'end': (min(b + timedelta(days=6), last) if weekly else b).isoformat(),
                     'values': v, 'total': sum(v.values())} for b, v in sorted(buckets.items())],
    })


@dashboard.route('/latency')
def latency():
    """Hours from CrossRef registration to the first report, per publisher (daily arrivals)."""
    days = _days()
    session = _session()
    try:
        first, last, _ = _window(session, days)
        rows = session.execute(text("""
            SELECT p.publisher, COUNT(*),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY f.latency_hours),
                   percentile_cont(0.9) WITHIN GROUP (ORDER BY f.latency_hours), MAX(f.latency_hours)
            FROM gold.fact_title_event f
            JOIN gold.dim_date d ON d.date_key = f.detected_date_key
            JOIN gold.dim_publisher p ON p.publisher_key = f.publisher_key
            JOIN gold.dim_event_type et ON et.event_type_key = f.event_type_key
            JOIN gold.dim_load_type lt ON lt.load_type_key = f.load_type_key
            WHERE et.counts_as_new AND lt.load_type = 'Daily arrival' AND f.latency_hours IS NOT NULL
              AND d.full_date BETWEEN :a AND :b GROUP BY 1 ORDER BY 2 DESC, 1"""),
            {'a': first, 'b': last}).fetchall()
    finally:
        session.close()
    return jsonify({'from': first.isoformat(), 'to': last.isoformat(), 'publishers': [
        {'publisher': r[0], 'titles': r[1], 'median_hours': _num(r[2]), 'p90_hours': _num(r[3]),
         'max_hours': _num(r[4])} for r in rows]})


@dashboard.route('/countries')
def countries():
    """Countries with the most new titles that have a known conference location."""
    days = _days()
    limit = max(1, min(request.args.get('limit', default=10, type=int), 50))
    session = _session()
    try:
        first, last, _ = _window(session, days)
        rows = session.execute(text(f"""
            SELECT l.country, SUM(f.event_count)
            FROM gold.fact_title_event f
            JOIN gold.dim_date d ON d.date_key = f.detected_date_key
            JOIN gold.dim_location l ON l.location_key = f.location_key
            JOIN gold.dim_event_type et ON et.event_type_key = f.event_type_key
            JOIN gold.dim_load_type lt ON lt.load_type_key = f.load_type_key
            WHERE {NEW} AND l.location_key <> -1 AND l.country NOT IN ('Unknown', 'Virtual')
              AND d.full_date BETWEEN :a AND :b
            GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT :n"""), {'a': first, 'b': last, 'n': limit}).fetchall()
    finally:
        session.close()
    return jsonify({'from': first.isoformat(), 'to': last.isoformat(),
                    'countries': [{'country': r[0], 'titles': int(r[1])} for r in rows]})


def _title_item(r):
    doi = r['doi'] or ''
    link = r['url'] or (f'https://doi.org/{doi}' if doi.startswith('10.') else None)
    conference = None
    if r['event_name'] or r['event_location'] or r['event_date_start']:
        conference = {'name': decode_entities(r['event_name']), 'place': decode_entities(r['event_location']),
                      'start': r['event_date_start'].isoformat() if r['event_date_start'] else None,
                      'end': r['event_date_end'].isoformat() if r['event_date_end'] else None}
    item = {'title': decode_entities(r['title']) or '(untitled)', 'doi': doi, 'url': link,
            'publisher': r['publisher'], 'series': r['series'], 'type_group': r['type_group'],
            'work_type': r['work_type'], 'conference': conference}
    return item


@dashboard.route('/titles')
def titles():
    """
    New and updated titles, newest first (read-only: nothing is marked as delivered).
    Params: days, kind (new | updated | all, default new), publisher, type_group, q (title
    contains), limit (default 25, max 100), offset.
    """
    days, kind = _days(), request.args.get('kind', 'new')
    if kind not in ('new', 'updated', 'all'):
        return jsonify({'error': 'kind must be new, updated or all'}), 400
    limit = max(1, min(request.args.get('limit', default=25, type=int), 100))
    offset = max(0, request.args.get('offset', default=0, type=int))
    cond = {'new': f'({NEW})', 'updated': "et.event_type = 'title.updated'",
            'all': f"(({NEW}) OR et.event_type = 'title.updated')"}[kind]
    params = {'n': limit, 'o': offset}
    where = [cond, 'd.full_date BETWEEN :a AND :b']
    if request.args.get('publisher'):
        where.append('lower(pu.publisher) = lower(:publisher)')
        params['publisher'] = request.args['publisher']
    if request.args.get('type_group'):
        where.append('lower(w.type_group) = lower(:type_group)')
        params['type_group'] = request.args['type_group']
    if request.args.get('q', '').strip():
        where.append("p.title ILIKE :q ESCAPE '\\'")
        params['q'] = _like(request.args['q'])
    session = _session()
    try:
        params['a'], params['b'], _ = _window(session, days)
        base = f"""
            FROM gold.fact_title_event f
            JOIN events e ON e.event_id = f.event_id
            JOIN gold.dim_date d ON d.date_key = f.detected_date_key
            JOIN gold.dim_event_type et ON et.event_type_key = f.event_type_key
            JOIN gold.dim_load_type lt ON lt.load_type_key = f.load_type_key
            JOIN gold.dim_publisher pu ON pu.publisher_key = f.publisher_key
            JOIN gold.dim_series s ON s.series_key = f.series_key
            JOIN gold.dim_work_type w ON w.work_type_key = f.work_type_key
            LEFT JOIN publications p ON p.doi = f.doi
            WHERE {' AND '.join(where)}"""
        total = session.execute(text('SELECT COUNT(*) ' + base),
                                {k: v for k, v in params.items() if k not in ('n', 'o')}).scalar()
        rows = session.execute(text(f"""
            SELECT e.detected_at, et.event_type, pu.publisher, COALESCE(s.abbreviation, s.series) AS series,
                   w.type_group, w.crossref_type AS work_type, f.doi, p.title, p.url,
                   p.event_name, p.event_location, p.event_date_start, p.event_date_end
            {base} ORDER BY e.detected_at DESC, f.doi LIMIT :n OFFSET :o"""), params).mappings().all()
    finally:
        session.close()
    results = []
    for r in rows:
        item = _title_item(r)
        item.update({'detected_at': r['detected_at'].isoformat(), 'event': r['event_type']})
        results.append(item)
    return jsonify({'total': total, 'count': len(results), 'offset': offset, 'results': results})


@dashboard.route('/search')
def search():
    """
    Search all stored titles. Params: q (title contains), publisher, type_group, year,
    limit (default 25, max 100), offset.
    """
    limit = max(1, min(request.args.get('limit', default=25, type=int), 100))
    offset = max(0, request.args.get('offset', default=0, type=int))
    params, where = {'n': limit, 'o': offset}, ['TRUE']
    if request.args.get('publisher'):
        where.append('lower(pu.publisher) = lower(:publisher)')
        params['publisher'] = request.args['publisher']
    if request.args.get('type_group'):
        where.append('lower(w.type_group) = lower(:type_group)')
        params['type_group'] = request.args['type_group']
    if request.args.get('year', type=int):
        where.append('p.year = :year')
        params['year'] = request.args.get('year', type=int)
    if request.args.get('q', '').strip():
        where.append("p.title ILIKE :q ESCAPE '\\'")
        params['q'] = _like(request.args['q'])
    base = f"""
        FROM publications p
        LEFT JOIN gold.publisher_map pm ON pm.series = p.series AND pm.raw_publisher = p.publisher
        LEFT JOIN gold.dim_publisher pu ON pu.publisher_key = pm.publisher_key
        LEFT JOIN gold.dim_series s ON s.series = p.series
        LEFT JOIN gold.dim_work_type w ON w.crossref_type = p.work_type
        WHERE {' AND '.join(where)}"""
    session = _session()
    try:
        total = session.execute(text('SELECT COUNT(*) ' + base),
                                {k: v for k, v in params.items() if k not in ('n', 'o')}).scalar()
        rows = session.execute(text(f"""
            SELECT p.year, COALESCE(pu.publisher, p.publisher) AS publisher,
                   COALESCE(s.abbreviation, p.series) AS series, w.type_group, p.work_type,
                   p.doi, p.title, p.url, p.event_name, p.event_location, p.event_date_start, p.event_date_end
            {base} ORDER BY p.year DESC NULLS LAST, p.fetched_at DESC, p.id DESC LIMIT :n OFFSET :o"""),
            params).mappings().all()
    finally:
        session.close()
    results = []
    for r in rows:
        item = _title_item(r)
        item['year'] = r['year']
        results.append(item)
    return jsonify({'total': total, 'count': len(results), 'offset': offset, 'results': results})
