"""
REST API for the publisher title tracker (Springer, Elsevier, Wiley, IEEE, ACM),
serving data from Neon PostgreSQL.
Falls back to local JSON files if DATABASE_URL is not configured.
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timezone, timedelta
from xml.sax.saxutils import escape as xml_escape
from email.utils import format_datetime as fmt_rfc2822

# Load .env first so DATABASE_URL is visible before anything else.
# Importing pipeline.config loads .env and centralises the helpers below.
from pipeline.config import (
    load_series_map, resolve_series_name, load_publisher_streams, resolve_publisher,
)

from flask import Flask, jsonify, request, send_from_directory, redirect
from flask_cors import CORS
from sqlalchemy import text

app = Flask(__name__)
CORS(app)

DATA_DIR = Path("data")

# ---------------------------------------------------------------------------
# Series abbreviation → full name mapping (loaded from config.json)
# ---------------------------------------------------------------------------

def _load_series_map() -> Dict[str, str]:
    """Return {abbreviation: full_name} from config.json."""
    try:
        return load_series_map()
    except Exception:
        return {}

SERIES_MAP = _load_series_map()   # e.g. {"LNCS": "Lecture Notes in Computer Science", ...}


def _load_publisher_streams() -> list:
    try:
        return load_publisher_streams()
    except Exception:
        return []

PUBLISHER_STREAMS = _load_publisher_streams()


def _publisher_series(publisher: str):
    """Series/stream names belonging to a publisher, or None if it is unknown."""
    entry = resolve_publisher(publisher, PUBLISHER_STREAMS)
    return entry['series'] if entry else None


def _resolve_series(abbr_or_name: str) -> str:
    """
    Accept either an abbreviation (LNCS) or a full name and always
    return the full name as stored in the DB.
    """
    return resolve_series_name(abbr_or_name, SERIES_MAP)


# ---------------------------------------------------------------------------
# Database availability check
# ---------------------------------------------------------------------------

def _db_available() -> bool:
    """True when a cloud DB connection string is present in the environment.

    Delegates to DatabaseConfig.is_cloud_configured() — the single source of
    truth for the env-var check shared with the pipeline (runner._db_configured).
    """
    from pipeline.db import DatabaseConfig
    return DatabaseConfig.is_cloud_configured()


# ---------------------------------------------------------------------------
# Database-backed query helpers
# ---------------------------------------------------------------------------

def _get_session():
    from pipeline.db import DatabaseConfig
    return DatabaseConfig.get_session()


def _search_db(series: str = None, year: int = None,
               query: str = None, limit: int = 100,
               publisher_series: List[str] = None, work_type: str = None) -> List[Dict]:
    from pipeline.models import Publication
    session = _get_session()
    try:
        q = session.query(Publication)
        if series:
            full_name = _resolve_series(series)
            q = q.filter(Publication.series.ilike(f'%{full_name}%'))
        if publisher_series:
            q = q.filter(Publication.series.in_(publisher_series))
        if work_type:
            q = q.filter(Publication.work_type == work_type)
        if year:
            q = q.filter(Publication.year == year)
        if query:
            q = q.filter(Publication.title.ilike(f'%{query}%'))
        q = q.order_by(Publication.year.desc(), Publication.fetched_at.desc()).limit(limit)
        return [pub.to_dict() for pub in q.all()]
    finally:
        session.close()


def _stats_db() -> Dict:
    session = _get_session()
    try:
        total        = session.execute(text('SELECT COUNT(*) FROM publications')).scalar()
        series_count = session.execute(text('SELECT COUNT(DISTINCT series) FROM publications')).scalar()
        year_min     = session.execute(text('SELECT MIN(year) FROM publications')).scalar()
        year_max     = session.execute(text('SELECT MAX(year) FROM publications')).scalar()
        authors      = session.execute(text('SELECT COUNT(*) FROM authors')).scalar()

        year_rows = session.execute(text("""
            SELECT year, COUNT(*) FROM publications
            WHERE year IS NOT NULL
            GROUP BY year ORDER BY year
        """)).fetchall()
        by_year = {str(y): c for y, c in year_rows}

        return {
            'total_publications':    total,
            'total_authors':         authors,
            'total_series':          series_count,
            'year_range':            {'earliest': year_min, 'latest': year_max},
            'publications_by_year':  by_year,
            'source':                'database',
            'last_updated':          datetime.utcnow().isoformat(),
        }
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Gold layer helpers
# ---------------------------------------------------------------------------

def _gold_series_stats() -> List[Dict]:
    session = _get_session()
    try:
        rows = session.execute(text("""
            SELECT series, total_publications, total_unique_authors,
                   year_min, year_max, avg_authors_per_paper, computed_at
            FROM gold.series_stats
            ORDER BY total_publications DESC
        """)).fetchall()
        return [
            {
                # 'series' is the canonical key used by the /api/gold/* endpoints;
                # 'name'/'abbreviation' mirror it for the /api/series consumer.
                'series':                row[0],
                'name':                  row[0],
                'abbreviation':          row[0],
                'total_publications':    row[1],
                'total_unique_authors':  row[2],
                'year_min':              row[3],
                'year_max':              row[4],
                'avg_authors_per_paper': float(row[5]) if row[5] else 0,
                'computed_at':           row[6].isoformat() if row[6] else None,
            }
            for row in rows
        ]
    finally:
        session.close()


def _gold_yearly_trends(series: str = None) -> List[Dict]:
    session = _get_session()
    try:
        if series:
            full_name = _resolve_series(series)
            rows = session.execute(text("""
                SELECT series, year, publication_count
                FROM gold.yearly_trends
                WHERE series ILIKE :s
                ORDER BY series, year
            """), {'s': f'%{full_name}%'}).fetchall()
        else:
            rows = session.execute(text("""
                SELECT series, year, publication_count
                FROM gold.yearly_trends
                ORDER BY series, year
            """)).fetchall()
        return [
            {'series': row[0], 'year': row[1], 'publication_count': row[2]}
            for row in rows
        ]
    finally:
        session.close()


def _gold_top_authors(series: str = None, limit: int = 20) -> List[Dict]:
    session = _get_session()
    try:
        if series:
            full_name = _resolve_series(series)
            rows = session.execute(text("""
                SELECT series, author_name, publication_count
                FROM gold.top_authors
                WHERE series ILIKE :s
                ORDER BY publication_count DESC
                LIMIT :lim
            """), {'s': f'%{full_name}%', 'lim': limit}).fetchall()
        else:
            rows = session.execute(text("""
                SELECT series, author_name, publication_count
                FROM gold.top_authors
                ORDER BY series, publication_count DESC
            """)).fetchall()
        return [
            {'series': row[0], 'author_name': row[1], 'publication_count': row[2]}
            for row in rows
        ]
    finally:
        session.close()


# ---------------------------------------------------------------------------
# File-based fallback (local development without DATABASE_URL)
# ---------------------------------------------------------------------------

def _load_live_files() -> Dict[str, List[Dict]]:
    """Load per-series publications from the silver JSON files on disk.

    These are the files the pipeline writes to data/silver/{ABBR}_cleaned.json
    (the Silver layer). Used only as a local fallback when DATABASE_URL is not
    configured, so the API still serves something without a database.
    """
    silver_dir = DATA_DIR / "silver"
    series_data = {}
    if not silver_dir.exists():
        return {}
    for file in silver_dir.glob("*_cleaned.json"):
        series_abbr = file.stem.replace('_cleaned', '')
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            series_data[series_abbr] = data.get('publications', [])
        except Exception as e:
            print(f"Error loading {file}: {e}")
    return series_data


def _search_files(series: str = None, year: int = None,
                  query: str = None, limit: int = 100) -> List[Dict]:
    all_data = _load_live_files()
    results = []
    if series:
        results = all_data.get(series.upper(), [])
    else:
        for pubs in all_data.values():
            results.extend(pubs)
    if year:
        results = [p for p in results if p.get('year') == year]
    if query:
        q_lower = query.lower()
        results = [p for p in results if q_lower in p.get('title', '').lower()]
    results.sort(key=lambda p: p.get('year', 0), reverse=True)
    return results[:limit]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route('/')
def home():
    return send_from_directory('.', 'web_interface.html')


@app.route('/api')
def api_index():
    return jsonify({
        'name':    'Publisher Title Tracker API',
        'version': '2.0',
        'backend': 'database' if _db_available() else 'local-files',
        'endpoints': {
            '/api/publishers':            'Tracked publishers with counts, work types and last successful run',
            '/api/series':                'All series with publication counts',
            '/api/publications':          'Search publications (params: publisher, series, type, year, query, limit)',
            '/api/publications/recent':   'Most recent publications',
            '/api/publications/new':      'PUSH — new additions since a date (params: since=YYYY-MM-DD, publisher, series, type)',
            '/feed.rss':                  'PUSH — RSS feed for subscription (params: publisher, series, type, limit)',
            '/api/events':                'Change events title.added / title.updated (params: since, type, publisher, series, work_type, limit)',
            '/feed/events.atom':          'Atom feed of change events (params: publisher, series, limit)',
            '/api/stats':                 'Overall database statistics',
            '/api/gold/stats':            'Gold layer: per-series aggregations',
            '/api/gold/trends':           'Gold layer: yearly publication trends (param: series)',
            '/api/gold/authors':          'Gold layer: top authors (params: series, limit)',
            '/health':                    'Health check',
        },
    })


@app.route('/api/doi/<path:doi>')
def doi_redirect(doi):
    """
    Smart link for a title. Raw https://doi.org/... links break for some
    proceedings (e.g. IEEE, whose DOIs resolve to a dead mostRecentIssue page),
    so resolve the landing page here and fall back to a search engine:
      1. ask doi.org where the DOI points (first redirect hop only) and use it;
      2. for known-broken IEEE mostRecentIssue links, go to IEEE Xplore's own
         search for that DOI instead;
      3. anything else falls back to a Google Scholar title search.
    """
    import urllib.request

    title = None
    target = None
    try:
        from pipeline.models import Publication
        session = _get_session()
        try:
            pub = session.query(Publication).filter(Publication.doi == doi).first()
            if pub:
                title = pub.title
        finally:
            session.close()
    except Exception:
        title = None

    if doi and doi.startswith('ieee:issue:'):
        # Journal issues from the IEEE API have no DOI; their stored URL is the
        # issue's table of contents on IEEE Xplore.
        try:
            from pipeline.models import Publication
            session = _get_session()
            try:
                pub = session.query(Publication).filter(Publication.doi == doi).first()
                url = pub.url if pub else None
            finally:
                session.close()
        except Exception:
            url = None
        if url:
            return redirect(url, code=302)
        return redirect('https://ieeexplore.ieee.org/', code=302)

    if doi and '/' in doi:
        # Read ONLY the first redirect hop from doi.org. Following it onward
        # would contact the publisher, and Wiley/ACM answer server-side
        # requests with 403 (bot protection) even though the link is fine.
        class _NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                return None
        try:
            opener = urllib.request.build_opener(_NoRedirect)
            req = urllib.request.Request(
                f'https://doi.org/{doi}',
                headers={'User-Agent': 'PublisherTitleTracker/1.0'})
            opener.open(req, timeout=8)
        except urllib.error.HTTPError as exc:
            if exc.code in (301, 302, 303, 307, 308):
                target = exc.headers.get('Location')
        except Exception:
            target = None

    if target and 'ieeexplore.ieee.org/xpl/mostRecentIssue.jsp' in target:
        fallback = (f'https://ieeexplore.ieee.org/search/searchresult.jsp'
                    f'?queryText={doi}')
        return redirect(fallback, code=302)
    if target and target != f'https://doi.org/{doi}':
        return redirect(target, code=302)
    if title:
        from urllib.parse import quote_plus
        return redirect(
            f'https://scholar.google.com/scholar?q={quote_plus(title)}', code=302)
    return redirect(f'https://doi.org/{doi}', code=302)


@app.route('/api/cover/<path:isbn>')
def get_cover(isbn):
    """
    Proxy Springer book cover images.
    Springer's CDN blocks direct browser requests (hotlinking), so the
    browser calls this endpoint instead and the server fetches it server-side.
    """
    import requests as req_lib
    from flask import Response
    url = f'https://media.springernature.com/w306/springer-static/cover/book/{isbn}.jpg'
    try:
        r = req_lib.get(url, timeout=5, headers={
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://link.springer.com'
        })
        if r.status_code == 200 and 'image' in r.headers.get('content-type', ''):
            return Response(r.content, mimetype=r.headers['content-type'])
    except Exception:
        pass
    return '', 404


@app.route('/health')
def health():
    if _db_available():
        try:
            session = _get_session()
            session.execute(text('SELECT 1'))
            session.close()
            return jsonify({'status': 'healthy', 'database': 'connected'})
        except Exception as e:
            return jsonify({'status': 'unhealthy', 'database': str(e)}), 503
    return jsonify({'status': 'healthy', 'database': 'not configured (using local files)'})


@app.route('/api/publishers')
def get_publishers():
    """
    Tracked publishers with totals, a per-work-type breakdown, how many titles
    arrived in the last `days` days, and the last successful run time.

    Params:
      days — window for the "new" count (default 7, max 365)
    """
    days = max(1, min(request.args.get('days', default=7, type=int), 365))
    base = [{'key': p['key'], 'name': p['name'], 'series': p['series']}
            for p in PUBLISHER_STREAMS]
    if not _db_available():
        return jsonify({'publishers': base, 'days': days, 'source': 'config-only'})

    try:
        since = datetime.utcnow() - timedelta(days=days)
        session = _get_session()
        try:
            totals = {r[0]: r[1] for r in session.execute(text(
                'SELECT series, COUNT(*) FROM publications GROUP BY series'))}
            recent = {r[0]: r[1] for r in session.execute(text(
                'SELECT series, COUNT(*) FROM publications '
                'WHERE fetched_at >= :since GROUP BY series'), {'since': since})}
            types = {}
            for ser, wt, n in session.execute(text(
                    'SELECT series, work_type, COUNT(*) FROM publications '
                    'GROUP BY series, work_type')):
                types.setdefault(ser, {})[wt or 'unknown'] = n
            try:
                last_run = {r[0]: r[1] for r in session.execute(text(
                    'SELECT series, last_success_at FROM pipeline_state'))}
            except Exception:
                session.rollback()
                last_run = {}
        finally:
            session.close()

        out = []
        for p in base:
            by_type = {}
            for ser in p['series']:
                for wt, n in types.get(ser, {}).items():
                    by_type[wt] = by_type.get(wt, 0) + n
            last = [last_run[s] for s in p['series'] if s in last_run]
            out.append({
                'key':             p['key'],
                'name':            p['name'],
                'total':           sum(totals.get(s, 0) for s in p['series']),
                'new_in_window':   sum(recent.get(s, 0) for s in p['series']),
                'by_type':         by_type,
                'last_success_at': max(last).isoformat() if last else None,
            })
        return jsonify({'publishers': out, 'days': days, 'source': 'database'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/series')
def get_series():
    if _db_available():
        try:
            return jsonify({
                'series':       _gold_series_stats(),
                'source':       'database',
            })
        except Exception as e:
            print(f"DB error: {e}")

    data = _load_live_files()
    return jsonify({
        'series': [
            {
                'name':               abbr,
                'abbreviation':       abbr,
                'total_publications': len(pubs),
                'years':              sorted({p.get('year') for p in pubs if p.get('year')}),
            }
            for abbr, pubs in data.items()
        ],
        'source': 'local-files',
    })


@app.route('/api/publications')
def search_publications():
    series = request.args.get('series')
    year   = request.args.get('year', type=int)
    query  = request.args.get('query')
    publisher = request.args.get('publisher')
    work_type = request.args.get('type')
    limit  = min(request.args.get('limit', default=100, type=int), 1000)

    publisher_series = None
    if publisher:
        publisher_series = _publisher_series(publisher)
        if publisher_series is None:
            return jsonify({'error': f'Unknown publisher: {publisher}'}), 400

    if _db_available():
        try:
            results = _search_db(series=series, year=year, query=query, limit=limit,
                                 publisher_series=publisher_series, work_type=work_type)
            return jsonify({
                'results': results,
                'count':   len(results),
                'filters': {'series': series, 'publisher': publisher, 'type': work_type,
                            'year': year, 'query': query},
                'source':  'database',
            })
        except Exception as e:
            print(f"DB error: {e}")

    results = _search_files(series=series, year=year, query=query, limit=limit)
    return jsonify({
        'results': results,
        'count':   len(results),
        'filters': {'series': series, 'year': year, 'query': query},
        'source':  'local-files',
    })


@app.route('/api/publications/recent')
def get_recent_publications():
    limit = min(request.args.get('limit', default=50, type=int), 1000)
    if _db_available():
        try:
            results = _search_db(limit=limit)
            return jsonify({'results': results, 'count': len(results), 'source': 'database'})
        except Exception as e:
            print(f"DB error: {e}")

    results = _search_files(limit=limit)
    return jsonify({'results': results, 'count': len(results), 'source': 'local-files'})


@app.route('/api/stats')
def get_stats():
    if _db_available():
        try:
            return jsonify(_stats_db())
        except Exception as e:
            print(f"DB error: {e}")

    data    = _load_live_files()
    total   = sum(len(pubs) for pubs in data.values())
    by_year: Dict[str, int] = {}
    for pubs in data.values():
        for pub in pubs:
            y = pub.get('year')
            if y:
                by_year[str(y)] = by_year.get(str(y), 0) + 1

    return jsonify({
        'total_publications': total,
        'total_series':       len(data),
        'year_range': {
            'earliest': min(int(y) for y in by_year) if by_year else None,
            'latest':   max(int(y) for y in by_year) if by_year else None,
        },
        'publications_by_year': dict(sorted(by_year.items())),
        'source':       'local-files',
        'last_updated': datetime.utcnow().isoformat(),
    })


# ---------------------------------------------------------------------------
# Gold layer endpoints
# ---------------------------------------------------------------------------

@app.route('/api/gold/stats')
def gold_stats():
    """Per-series aggregations from gold.series_stats."""
    if not _db_available():
        return jsonify({'error': 'Database not configured'}), 503
    try:
        return jsonify({'stats': _gold_series_stats(), 'source': 'gold'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/gold/trends')
def gold_trends():
    """
    Yearly publication counts from gold.yearly_trends.
    Optional param: series (abbreviation or full name)
    """
    if not _db_available():
        return jsonify({'error': 'Database not configured'}), 503
    series = request.args.get('series')
    try:
        trends = _gold_yearly_trends(series=series)
        return jsonify({'trends': trends, 'count': len(trends), 'source': 'gold'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/quality')
def quality_metrics():
    """Per-series data quality metrics from quality_metrics table."""
    if not _db_available():
        return jsonify({'error': 'Database not configured'}), 503
    try:
        session = _get_session()
        rows = session.execute(text("""
            SELECT series, total_records,
                   doi_coverage_pct, event_date_coverage_pct,
                   event_year_coverage_pct, author_coverage_pct,
                   validated_at
            FROM quality_metrics
            ORDER BY total_records DESC
        """)).fetchall()
        session.close()
        return jsonify({
            'metrics': [
                {
                    'series':                   row[0],
                    'total_records':            row[1],
                    'doi_coverage_pct':         float(row[2]) if row[2] else 0,
                    'event_date_coverage_pct':  float(row[3]) if row[3] else 0,
                    'event_year_coverage_pct':  float(row[4]) if row[4] else 0,
                    'author_coverage_pct':      float(row[5]) if row[5] else 0,
                    'validated_at':             row[6].isoformat() if row[6] else None,
                }
                for row in rows
            ],
            'source': 'quality_metrics',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/gold/authors')
def gold_authors():
    """
    Top authors from gold.top_authors.
    Optional params: series (abbreviation or full name), limit (default 20)
    """
    if not _db_available():
        return jsonify({'error': 'Database not configured'}), 503
    series = request.args.get('series')
    limit  = min(request.args.get('limit', default=20, type=int), 200)
    try:
        authors = _gold_top_authors(series=series, limit=limit)
        return jsonify({'authors': authors, 'count': len(authors), 'source': 'gold'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ---------------------------------------------------------------------------
# Push endpoints
# ---------------------------------------------------------------------------

@app.route('/api/publications/new')
def get_new_publications():
    """
    Return publications added since a given date — the pull side of push.
    Clients poll this on a schedule to get only what is new.

    Params:
      since  — ISO date (YYYY-MM-DD). Defaults to 7 days ago.
      series — optional series filter (abbreviation or full name)
      publisher — optional publisher filter (SPRINGER, ELSEVIER, WILEY, IEEE, ACM)
      type   — optional work type (proceedings, journal-issue, book, ...)
      limit  — max results (default 100, max 1000)
    """
    since_str = request.args.get('since')
    series    = request.args.get('series')
    publisher = request.args.get('publisher')
    work_type = request.args.get('type')
    limit     = min(request.args.get('limit', default=100, type=int), 1000)
    publisher_series = None
    if publisher:
        publisher_series = _publisher_series(publisher)
        if publisher_series is None:
            return jsonify({'error': f'Unknown publisher: {publisher}'}), 400

    if since_str:
        try:
            since = datetime.fromisoformat(since_str)
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    else:
        since = datetime.utcnow() - timedelta(days=7)

    if not _db_available():
        return jsonify({'error': 'Database not configured'}), 503

    try:
        from pipeline.models import Publication
        session = _get_session()
        try:
            q = session.query(Publication).filter(Publication.fetched_at >= since)
            if series:
                full_name = _resolve_series(series)
                q = q.filter(Publication.series.ilike(f'%{full_name}%'))
            if publisher_series:
                q = q.filter(Publication.series.in_(publisher_series))
            if work_type:
                q = q.filter(Publication.work_type == work_type)
            q = q.order_by(Publication.fetched_at.desc()).limit(limit)
            results = [pub.to_dict() for pub in q.all()]
        finally:
            session.close()
        return jsonify({
            'results': results,
            'count':   len(results),
            'since':   since.isoformat(),
            'source':  'database',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/feed.rss')
def rss_feed():
    """
    RSS 2.0 feed of the most recently scraped publications.
    Users subscribe with any RSS reader and get notified automatically
    when new publications are added — this is the push mechanism.

    Params:
      series — optional filter (abbreviation or full name)
      publisher — optional publisher filter (e.g. WILEY)
      type   — optional work type (proceedings, journal-issue, book, ...)
      limit  — number of items (default 50, max 200)
    """
    series = request.args.get('series')
    publisher = request.args.get('publisher')
    work_type = request.args.get('type')
    limit  = min(request.args.get('limit', default=50, type=int), 200)
    publisher_series = None
    if publisher:
        publisher_series = _publisher_series(publisher)
        if publisher_series is None:
            return f'Unknown publisher: {publisher}', 400

    if not _db_available():
        return 'Database not configured', 503

    try:
        from pipeline.models import Publication
        session = _get_session()
        try:
            q = session.query(Publication)
            if series:
                full_name = _resolve_series(series)
                q = q.filter(Publication.series.ilike(f'%{full_name}%'))
            if publisher_series:
                q = q.filter(Publication.series.in_(publisher_series))
            if work_type:
                q = q.filter(Publication.work_type == work_type)
            q = q.order_by(Publication.fetched_at.desc()).limit(limit)

            # Build RSS items while session is still open so lazy-loaded
            # relationships (authors) are accessible.
            items_xml = []
            for pub in q.all():
                title = xml_escape(pub.title or 'Untitled')
                link  = xml_escape(pub.url or (f'https://doi.org/{pub.doi}' if pub.doi else ''))
                guid  = xml_escape(f'https://doi.org/{pub.doi}' if pub.doi else pub.title or '')

                desc_parts = []
                if pub.work_type:        desc_parts.append(f'Type: {pub.work_type}')
                if pub.series:           desc_parts.append(f'Series: {pub.series}')
                if pub.year:             desc_parts.append(f'Year: {pub.year}')
                if pub.event_name:       desc_parts.append(f'Conference: {pub.event_name}')
                if pub.event_location:   desc_parts.append(f'Location: {pub.event_location}')
                if pub.event_date_start: desc_parts.append(f'Event date: {pub.event_date_start}')
                author_names = [a.name for a in pub.authors[:5]]
                if author_names:         desc_parts.append(f'Editors: {"; ".join(author_names)}')
                description = xml_escape(' | '.join(desc_parts))

                pub_date = ''
                if pub.fetched_at:
                    dt = pub.fetched_at.replace(tzinfo=timezone.utc)
                    pub_date = f'<pubDate>{fmt_rfc2822(dt)}</pubDate>'

                items_xml.append(f"""    <item>
      <title>{title}</title>
      <link>{link}</link>
      <description>{description}</description>
      <guid isPermaLink="false">{guid}</guid>
      {pub_date}
    </item>""")
        finally:
            session.close()
    except Exception as e:
        return str(e), 500

    feed_label = (series or publisher or '').upper()
    feed_title = f'New Publications — {feed_label}' if feed_label else 'New Publications'
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>{xml_escape(feed_title)}</title>
    <link>{xml_escape(request.host_url)}</link>
    <description>New titles from tracked academic publishers</description>
    <language>en</language>
{chr(10).join(items_xml)}
  </channel>
</rss>"""

    return app.response_class(xml, mimetype='application/rss+xml')


# ---------------------------------------------------------------------------
# Event stream (Phase 2) — the change log delivered to users
# ---------------------------------------------------------------------------

@app.route('/api/events')
def get_events():
    """
    Return change events (title.added / title.updated) as JSON — the pull side
    of the event stream. Clients poll this to see what changed since a date.

    Params:
      since   — ISO datetime/date. Defaults to 7 days ago.
      type    — optional filter: 'title.added' or 'title.updated'
      series  — optional series filter (abbreviation or full name)
      publisher — optional publisher filter (SPRINGER, ELSEVIER, WILEY, IEEE, ACM)
      work_type — optional work type (proceedings, journal-issue, book, ...)
      limit   — max results (default 100, max 1000)

    Marks returned events as delivered (delivered_at) so end-to-end latency
    (detected_at -> delivered_at) can be measured later.
    """
    since_str = request.args.get('since')
    ev_type   = request.args.get('type')
    series    = request.args.get('series')
    publisher = request.args.get('publisher')
    work_type = request.args.get('work_type') or request.args.get('type_of_work')
    limit     = min(request.args.get('limit', default=100, type=int), 1000)
    publisher_series = None
    if publisher:
        publisher_series = _publisher_series(publisher)
        if publisher_series is None:
            return jsonify({'error': f'Unknown publisher: {publisher}'}), 400

    if since_str:
        try:
            since = datetime.fromisoformat(since_str)
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    else:
        since = datetime.utcnow() - timedelta(days=7)

    if not _db_available():
        return jsonify({'error': 'Database not configured'}), 503

    try:
        from pipeline.models import TitleEvent, Publication
        session = _get_session()
        try:
            # work_type lives on the publication, not in the (hash-stable) event
            # payload, so join on DOI (publications.doi is unique -> no fan-out).
            q = (session.query(TitleEvent, Publication.work_type)
                 .outerjoin(Publication, Publication.doi == TitleEvent.source_record_id)
                 .filter(TitleEvent.detected_at >= since))
            if ev_type:
                q = q.filter(TitleEvent.event_type == ev_type)
            if series:
                full_name = _resolve_series(series)
                q = q.filter(TitleEvent.series.ilike(f'%{full_name}%'))
            if publisher_series:
                q = q.filter(TitleEvent.series.in_(publisher_series))
            if work_type:
                q = q.filter(Publication.work_type == work_type)
            q = q.order_by(TitleEvent.detected_at.desc()).limit(limit)
            rows = q.all()
            events = [r[0] for r in rows]
            results = []
            for ev, wt in rows:
                d = ev.to_dict()
                d['work_type'] = wt
                results.append(d)

            # Mark as delivered (best-effort; never fails the request).
            now = datetime.utcnow()
            for ev in events:
                if ev.delivered_at is None:
                    ev.delivered_at = now
            session.commit()
        finally:
            session.close()
        return jsonify({
            'results': results,
            'count':   len(results),
            'since':   since.isoformat(),
            'source':  'database',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/feed/events.atom')
def events_atom_feed():
    """
    Atom feed of recent change events. Subscribe in any feed reader to be
    notified when a title is added or updated — the push mechanism for the
    event stream.

    Params:
      series — optional filter (abbreviation or full name)
      publisher — optional publisher filter (e.g. IEEE)
      limit  — number of entries (default 50, max 200)
    """
    series = request.args.get('series')
    publisher = request.args.get('publisher')
    limit  = min(request.args.get('limit', default=50, type=int), 200)
    publisher_series = None
    if publisher:
        publisher_series = _publisher_series(publisher)
        if publisher_series is None:
            return f'Unknown publisher: {publisher}', 400

    if not _db_available():
        return 'Database not configured', 503

    try:
        from pipeline.models import TitleEvent
        session = _get_session()
        try:
            q = session.query(TitleEvent)
            if series:
                full_name = _resolve_series(series)
                q = q.filter(TitleEvent.series.ilike(f'%{full_name}%'))
            if publisher_series:
                q = q.filter(TitleEvent.series.in_(publisher_series))
            q = q.order_by(TitleEvent.detected_at.desc()).limit(limit)
            events = q.all()

            entries_xml = []
            for ev in events:
                doi = ev.source_record_id or ''
                payload = ev.payload or {}
                title_text = payload.get('title') or doi or 'Untitled'
                entry_title = xml_escape(f"[{ev.event_type}] {title_text}")
                link = xml_escape(f'https://doi.org/{doi}' if doi else request.host_url)
                updated = fmt_rfc2822(
                    (ev.detected_at or datetime.utcnow()).replace(tzinfo=timezone.utc)
                )
                summary_bits = [ev.event_type, ev.series or '', ev.publisher or '']
                summary = xml_escape(' · '.join(b for b in summary_bits if b))
                entries_xml.append(f"""  <entry>
    <title>{entry_title}</title>
    <link href="{link}"/>
    <id>urn:uuid:{xml_escape(ev.event_id or '')}</id>
    <updated>{updated}</updated>
    <summary>{summary}</summary>
  </entry>""")
        finally:
            session.close()
    except Exception as e:
        return str(e), 500

    feed_label = (series or publisher or '').upper()
    feed_title = f'Title Events — {feed_label}' if feed_label else 'Title Events'
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>{xml_escape(feed_title)}</title>
  <link href="{xml_escape(request.base_url)}"/>
  <updated>{fmt_rfc2822(datetime.utcnow().replace(tzinfo=timezone.utc))}</updated>
{chr(10).join(entries_xml)}
</feed>"""

    return app.response_class(xml, mimetype='application/atom+xml')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    backend = 'Neon (DATABASE_URL)' if _db_available() else 'local JSON files'
    print("=" * 60)
    print("Publisher Title Tracker API")
    print(f"Backend: {backend}")
    print("Series map loaded:", list(SERIES_MAP.keys()))
    print("=" * 60)
    print("Endpoints:")
    print("  GET /health")
    print("  GET /api/publishers")
    print("  GET /api/series")
    print("  GET /api/publications?series=LNCS&year=2024&query=machine")
    print("  GET /api/publications/recent")
    print("  GET /api/publications/new?since=2024-01-01&series=LNCS  [PUSH]")
    print("  GET /feed.rss?series=LNCS                               [PUSH/RSS]")
    print("  GET /api/events?since=2024-01-01&type=title.added       [EVENTS]")
    print("  GET /feed/events.atom?series=LNCS                       [EVENTS/ATOM]")
    print("  GET /api/stats")
    print("  GET /api/gold/stats")
    print("  GET /api/gold/trends?series=LNCS")
    print("  GET /api/gold/authors?series=LNCS&limit=10")
    print("=" * 60 + "\n")
    port = int(os.getenv('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
