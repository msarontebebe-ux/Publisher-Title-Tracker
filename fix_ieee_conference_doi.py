"""
One-off correction: the IEEE conference stored under the DOI '10' (PCIM Conference 2026).

An old bug in the IEEE collector cut the last segment off a one-dot paper DOI and
left just '10' (fixed in pipeline/collectors/ieee.py: such a conference now gets the
identifier 'ieee:conf:<publication_number>'). This renames the stored title and its
event(s) to that identifier, taken from the title's own IEEE page (.../conhome/<n>/...).

The event log is append-only, so this is a deliberate one-time correction: the event
keeps its id, type and times; its identifier, payload DOI and fingerprint are updated
together, so the fingerprint still matches the payload and no title.updated appears.

Usage:
    python fix_ieee_conference_doi.py --dry-run   # show what would change
    python fix_ieee_conference_doi.py             # apply it
"""

import argparse
import json
import re
import sys

BAD_DOI = '10'


def run(dry_run: bool = False) -> dict:
    from sqlalchemy import text
    from pipeline.db import DatabaseConfig
    from pipeline.events import payload_hash

    result = {'found': False, 'new_id': None, 'events': 0, 'applied': False, 'dry_run': dry_run}
    session = DatabaseConfig.get_session()
    try:
        row = session.execute(text('SELECT id, url FROM publications WHERE doi = :d'),
                              {'d': BAD_DOI}).fetchone()
        if row is None:
            return result
        result['found'] = True
        m = re.search(r'/conhome/(\d+)/', row.url or '')
        if not m:
            result['error'] = 'no IEEE conference number in the url'
            return result
        new_id = f'ieee:conf:{m.group(1)}'
        result['new_id'] = new_id
        taken = session.execute(text(
            'SELECT 1 FROM publications WHERE doi = :n UNION ALL '
            'SELECT 1 FROM events WHERE source_record_id = :n'), {'n': new_id}).fetchone()
        if taken:
            result['error'] = f'{new_id} already exists - nothing changed'
            return result

        events = session.execute(text(
            'SELECT id, payload FROM events WHERE source_record_id = :d ORDER BY id'),
            {'d': BAD_DOI}).fetchall()
        result['events'] = len(events)
        if dry_run:
            return result

        session.execute(text('UPDATE publications SET doi = :n WHERE doi = :d'),
                        {'n': new_id, 'd': BAD_DOI})
        for ev in events:
            payload = dict(ev.payload or {})
            payload['doi'] = new_id
            session.execute(text(
                'UPDATE events SET source_record_id = :n, payload = CAST(:p AS json), '
                'payload_hash = :h WHERE id = :i'),
                {'n': new_id, 'p': json.dumps(payload), 'h': payload_hash(payload), 'i': ev.id})
        session.commit()
        result['applied'] = True
        return result
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--dry-run', action='store_true', help='show what would change, write nothing')
    r = run(dry_run=parser.parse_args().dry_run)
    if not r['found']:
        print(f"No title with the DOI '{BAD_DOI}' - nothing to do.")
        return 0
    print(f"Title with DOI '{BAD_DOI}' found; new identifier: {r['new_id']}")
    print(f"Events to correct: {r['events']}")
    if r.get('error'):
        print('Problem:', r['error'])
        return 1
    print('Applied.' if r['applied'] else '(dry run - nothing written)')
    return 0


if __name__ == '__main__':
    sys.exit(main())
