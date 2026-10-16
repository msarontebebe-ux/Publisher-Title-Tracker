"""
One-off backfill: set publications.work_type for the legacy Springer rows that
were stored before that column existed.

Every row's real type is looked up in CrossRef (50 DOIs per request). A blanket
'book' would be wrong: a sample showed some legacy rows are 'book-chapter'
(they entered through the title-search fallback, which has no type filter).
Rows CrossRef cannot find are left empty rather than guessed.

Safe by design:
  * only touches Springer-series rows whose work_type is still NULL
  * commits chunk by chunk, so an interruption loses nothing - just re-run
  * never changes the event fingerprint (work_type is not a canonical field),
    so it creates no title.updated events
  * idempotent - a second run only retries rows that are still empty

Usage:
    python backfill_work_type.py --dry-run   # look up types, report, write nothing
    python backfill_work_type.py             # look up types and write them
"""

import argparse
import sys
import time
from collections import Counter
from typing import Callable, Dict, List

import requests

CHUNK = 50


def crossref_types(dois: List[str], email: str) -> Dict[str, str]:
    """Return {doi_lower: CrossRef type} for the DOIs CrossRef knows."""
    headers = {'User-Agent': f'PublisherTracker/1.0 (mailto:{email})'}
    last_error = None
    for attempt in range(3):
        try:
            resp = requests.get(
                'https://api.crossref.org/works',
                params={'filter': ','.join(f'doi:{d}' for d in dois),
                        'rows': len(dois), 'select': 'DOI,type'},
                headers=headers, timeout=60)
            resp.raise_for_status()
            return {i['DOI'].lower(): i['type'] for i in resp.json()['message']['items']}
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(2 * (attempt + 1))
    raise last_error


def run(dry_run: bool = False,
        fetch_types: Callable[[List[str]], Dict[str, str]] = None,
        pause: float = 0.2, progress: bool = False) -> dict:
    from sqlalchemy import text
    from pipeline.config import load_config, load_publisher_streams
    from pipeline.db import DatabaseConfig

    config = load_config()
    email = config.get('crossref_api', {}).get('email', 'research@example.com')
    fetch_types = fetch_types or (lambda dois: crossref_types(dois, email))
    springer = next(s for s in load_publisher_streams() if s['key'] == 'SPRINGER')['series']

    # Neon closes idle connections, and the CrossRef lookups take minutes. So the
    # DOI list is read on one short connection and every chunk's UPDATE uses its
    # own fresh one - no connection is ever held open while waiting on the network.
    session = DatabaseConfig.get_session()
    try:
        dois = [r[0] for r in session.execute(text(
            'SELECT doi FROM publications '
            'WHERE work_type IS NULL AND series = ANY(:series) ORDER BY doi'),
            {'series': springer})]
    finally:
        session.close()

    result = {'missing': len(dois), 'types': Counter(), 'not_found': 0,
              'updated': 0, 'dry_run': dry_run}

    for start in range(0, len(dois), CHUNK):
        chunk = dois[start:start + CHUNK]
        known = fetch_types(chunk)
        by_type: Dict[str, List[str]] = {}
        for doi in chunk:
            kind = known.get(doi.lower())
            if kind is None:
                result['not_found'] += 1
            else:
                result['types'][kind] += 1
                by_type.setdefault(kind, []).append(doi)
        if not dry_run and by_type:
            session = DatabaseConfig.get_session()
            try:
                for kind, group in by_type.items():
                    res = session.execute(text(
                        'UPDATE publications SET work_type = :t '
                        'WHERE work_type IS NULL AND doi = ANY(:dois)'),
                        {'t': kind, 'dois': group})
                    result['updated'] += res.rowcount
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
    parser.add_argument('--dry-run', action='store_true', help='look up types, write nothing')
    args = parser.parse_args()

    r = run(dry_run=args.dry_run, progress=True)
    print(f"Springer rows without work_type : {r['missing']}")
    print(f"Types found in CrossRef         : {dict(r['types']) or 'none'}")
    print(f"Not found in CrossRef (left empty): {r['not_found']}")
    print(f"Updated rows                    : {r['updated']}"
          + ('  (dry run - nothing written)' if r['dry_run'] else ''))
    return 0


if __name__ == '__main__':
    sys.exit(main())
