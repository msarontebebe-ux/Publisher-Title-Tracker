"""
One-off backfill: set registered_at (when CrossRef first registered the DOI) for the
titles and events stored before that column existed.

CrossRef is asked for each DOI (50 per request). A title CrossRef does not know
(IEEE journal issues have a made-up identifier) is left empty rather than guessed.

Safe by design:
  * only fills rows whose registered_at is still NULL; never overwrites a value
  * commits chunk by chunk, so an interruption loses nothing - just re-run
  * never changes the event fingerprint (registered_at is not a canonical field),
    so it creates no title.updated events
  * idempotent - a second run only retries rows that are still empty

Usage:
    python backfill_registered_at.py --dry-run   # look up dates, report, write nothing
    python backfill_registered_at.py             # look up dates and write them
"""

import argparse
import re
import sys
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List
from urllib.parse import quote

import requests

CHUNK = 50
# Only real DOIs can be looked up. The tracker also stores made-up identifiers
# (ieee:issue:...) and CrossRef rejects a whole batch that contains one.
REAL_DOI = re.compile(r'^10\.\d{4,9}/[^\s,]+$')


def _parse(item) -> tuple:
    stamp = (item.get('created') or {}).get('date-time')
    if not stamp:
        return None
    when = datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    return item['DOI'].lower(), when.astimezone(timezone.utc).replace(tzinfo=None)


def _get(url, params, headers):
    """GET with retries for network trouble, rate limits and server errors; 4xx is returned."""
    last_error = None
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=60)
            if resp.status_code == 429 or resp.status_code >= 500:
                resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(2 * (attempt + 1))
    raise last_error


def crossref_created(dois: List[str], email: str) -> Dict[str, datetime]:
    """
    Return {doi_lower: registration time (naive UTC)} for the DOIs CrossRef knows.

    One request per chunk. If CrossRef rejects the chunk (HTTP 400, e.g. one odd DOI),
    each DOI of that chunk is asked for on its own, so one bad DOI costs only itself.
    """
    headers = {'User-Agent': f'PublisherTracker/1.0 (mailto:{email})'}
    resp = _get('https://api.crossref.org/works',
                {'filter': ','.join(f'doi:{d}' for d in dois), 'rows': len(dois),
                 'select': 'DOI,created'}, headers)
    out = {}
    if resp.status_code == 200:
        for item in resp.json()['message']['items']:
            parsed = _parse(item)
            if parsed:
                out[parsed[0]] = parsed[1]
        return out
    if resp.status_code != 400:
        resp.raise_for_status()
    for doi in dois:
        single = _get(f'https://api.crossref.org/works/{quote(doi, safe="/")}', None, headers)
        if single.status_code == 200:
            parsed = _parse(single.json()['message'])
            if parsed:
                out[parsed[0]] = parsed[1]
        elif single.status_code not in (400, 404):
            single.raise_for_status()
        time.sleep(0.2)
    return out


def run(dry_run: bool = False,
        fetch_created: Callable[[List[str]], Dict[str, datetime]] = None,
        pause: float = 0.2, progress: bool = False) -> dict:
    from sqlalchemy import text
    from pipeline.config import load_config
    from pipeline.db import DatabaseConfig

    config = load_config()
    email = config.get('crossref_api', {}).get('email', 'research@example.com')
    fetch_created = fetch_created or (lambda dois: crossref_created(dois, email))

    # Neon closes idle connections and the lookups take minutes, so the DOI list is
    # read on one short connection and every chunk's UPDATE uses its own fresh one.
    session = DatabaseConfig.get_session()
    try:
        dois = [r[0] for r in session.execute(text(
            'SELECT doi FROM publications WHERE registered_at IS NULL ORDER BY doi'))]
    finally:
        session.close()

    # Made-up identifiers (ieee:issue:...) and malformed values are never sent to CrossRef.
    valid = [d for d in dois if REAL_DOI.match(d)]
    result = {'missing': len(dois), 'found': 0, 'not_found': len(dois) - len(valid),
              'not_a_doi': len(dois) - len(valid),
              'publications': 0, 'events': 0, 'dry_run': dry_run}
    dois = valid

    for start in range(0, len(dois), CHUNK):
        chunk = dois[start:start + CHUNK]
        known = fetch_created(chunk)
        found = [(d, known[d.lower()]) for d in chunk if d.lower() in known]
        result['found'] += len(found)
        result['not_found'] += len(chunk) - len(found)
        if not dry_run and found:
            session = DatabaseConfig.get_session()
            try:
                for doi, when in found:
                    result['publications'] += session.execute(text(
                        'UPDATE publications SET registered_at = :at '
                        'WHERE doi = :doi AND registered_at IS NULL'),
                        {'at': when, 'doi': doi}).rowcount
                    result['events'] += session.execute(text(
                        'UPDATE events SET registered_at = :at '
                        'WHERE source_record_id = :doi AND registered_at IS NULL'),
                        {'at': when, 'doi': doi}).rowcount
                session.commit()
            finally:
                session.close()
        if progress:
            print(f"  {min(start + CHUNK, len(dois))}/{len(dois)} looked up", end='\r', flush=True)
        time.sleep(pause)
    if progress and dois:
        print()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--dry-run', action='store_true', help='look up dates, write nothing')
    args = parser.parse_args()

    r = run(dry_run=args.dry_run, progress=True)
    print(f"Titles without registration time : {r['missing']}")
    print(f"Found in CrossRef                : {r['found']}")
    print(f"Not found (left empty)           : {r['not_found']}"
          f"  (of which not real DOIs: {r['not_a_doi']})")
    print(f"Updated titles / events          : {r['publications']} / {r['events']}"
          + ('  (dry run - nothing written)' if r['dry_run'] else ''))
    return 0


if __name__ == '__main__':
    sys.exit(main())
