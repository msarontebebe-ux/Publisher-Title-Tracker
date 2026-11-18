"""
One-off repair of the Springer start-up backlog (found by the completeness check).

Two problems from the first loading days (11-12 October 2026):

  1. Volumes registered in CrossRef BEFORE the system started (--until, 11 October) were never stored when their
     print year was 2027: the first loads asked for 2026 only and the nightly runs only
     look at what changed since the last run. This adds them to `publications` with a
     quiet `title.baseline` event, exactly like any title that existed before the event
     log, so they are stored but not announced as new.
  2. 137 book CHAPTERS were stored by the old title-search fallback (no type filter) and
     got a `title.added` event each. The tracker follows volumes, not chapters. They are
     deleted together with their events, but ONLY when it is safe:
       * the parent volume is stored (or is added by step 1), and
       * their only events are `title.added` from CrossRef (nothing was updated).

Safe by design: a dry run writes nothing; every step is idempotent (a second run finds
nothing to do); volumes are only added when missing; the baseline events are the ones the
system already uses and never create a title.updated.

Usage:
    python repair_springer_backlog.py --dry-run   # fetch from CrossRef, report, write nothing
    python repair_springer_backlog.py             # apply
    python repair_springer_backlog.py --since 2026-08-01 --until 2026-10-11   # the window (defaults)
"""

import argparse
import re
import sys
import time
from collections import Counter

SINCE_DEFAULT = '2026-08-01'
# The system's first loading day. Volumes registered from then on belong to the nightly job,
# which announces them as new; the repair must leave them alone.
UNTIL_DEFAULT = '2026-10-11'
# Same fields the scraper asks for (pipeline/scraper.py), so records are parsed identically.
SELECT = ('DOI,title,subtitle,author,editor,published-print,published-online,container-title,'
          'volume,issue,page,ISBN,ISSN,publisher,type,event,created')


def _get_with_retry(scraper, params, attempts: int = 6):
    """
    GET one page from CrossRef. On 429 (rate limit) or a server error it waits and tries
    again, using the Retry-After header when CrossRef sends one, else 10, 20, 40 ... s.
    """
    for attempt in range(attempts):
        resp = scraper.session.get(scraper.base_url, params=params, timeout=60)
        if resp.status_code != 429 and resp.status_code < 500:
            resp.raise_for_status()
            return resp
        if attempt == attempts - 1:
            resp.raise_for_status()
        try:
            wait = float(resp.headers.get('Retry-After', ''))
        except (TypeError, ValueError):
            wait = 10 * 2 ** attempt
        print(f"  CrossRef says slow down (HTTP {resp.status_code}) - waiting {wait:.0f} s")
        time.sleep(min(wait, 300))


def fetch_created_since(scraper, issn: str, since: str, pause: float = 2.0):
    """Volumes (type:book) of one ISSN that CrossRef registered on or after `since`, parsed."""
    pubs, cursor = [], '*'
    while True:
        params = {'filter': f'issn:{issn},type:book,from-created-date:{since}',
                  'rows': 500, 'cursor': cursor, 'select': SELECT}
        message = _get_with_retry(scraper, params).json()['message']
        if not message['items']:
            return pubs
        for item in message['items']:
            pub = scraper._parse_publication(item)
            if pub and pub.doi:
                pubs.append(pub)
        cursor = message['next-cursor']
        time.sleep(pause)


def run(dry_run: bool = False, since: str = SINCE_DEFAULT, until: str = UNTIL_DEFAULT,
        fetch=None, pause: float = 2.0) -> dict:
    from sqlalchemy import text
    from pipeline.config import load_config
    from pipeline.db import DatabaseConfig
    from pipeline.runner import (_log_pipeline_run, _refresh_gold, _utcnow,
                                 _write_events, _write_silver)
    from pipeline.scraper import CrossRefScraper

    config = load_config()
    if fetch is None:
        crossref = config.get('crossref_api', {})
        scraper = CrossRefScraper(email=crossref.get('email', 'research@example.com'),
                                  delay=crossref.get('delay', 1.0))
        fetch = lambda series: [p for issn in (series.get('issn'), series.get('issn_electronic'))
                                if issn for p in fetch_created_since(scraper, issn, since, pause)]

    started = _utcnow()
    result = {'dry_run': dry_run, 'since': since, 'until': until, 'volumes_missing': Counter(), 'volumes_added': 0,
              'baseline_events': 0, 'chapters': 0, 'chapters_deleted': 0,
              'chapters_kept_unsafe': 0, 'chapters_kept_no_parent': 0, 'events_deleted': 0}

    # ---- 1. volumes registered before the system started and never stored
    session = DatabaseConfig.get_session()
    try:
        stored = {r[0].lower() for r in session.execute(text('SELECT doi FROM publications'))}
    finally:
        session.close()

    to_add = {}                                   # series name -> [pubs]
    for series in config['springer_series']:
        if not series.get('scrape_via_issn', True):
            continue                               # LNAI/LNBI are filed under LNCS by CrossRef
        found = {}
        for pub in fetch(series):
            created = (pub.created or '')[:10]
            if pub.doi.lower() not in stored and since <= created < until:
                pub.series = series['name']
                found[pub.doi.lower()] = pub
        if found:
            to_add[series['name']] = list(found.values())
            result['volumes_missing'][series['abbreviation']] = len(found)
    new_dois = {p.doi.lower() for pubs in to_add.values() for p in pubs}

    # ---- 2. chapters that must not be tracked
    session = DatabaseConfig.get_session()
    try:
        chapters = [r[0] for r in session.execute(text(
            "SELECT doi FROM publications WHERE work_type = 'book-chapter' AND doi ~ '_[0-9]+$' "
            "ORDER BY doi"))]
        unsafe = {r[0] for r in session.execute(text(
            "SELECT DISTINCT source_record_id FROM events WHERE source_record_id = ANY(:d) "
            "AND (event_type <> 'title.added' OR source <> 'crossref')"), {'d': chapters})} if chapters else set()
    finally:
        session.close()
    result['chapters'] = len(chapters)

    def parent(doi):
        return re.sub(r'_[0-9]+$', '', doi).lower()

    deletable = []
    for doi in chapters:
        if doi in unsafe:
            result['chapters_kept_unsafe'] += 1
        elif parent(doi) not in stored and parent(doi) not in new_dois:
            result['chapters_kept_no_parent'] += 1
        else:
            deletable.append(doi)
    result['chapters_deletable'] = len(deletable)

    if dry_run:
        result['volumes_added'] = len(new_dois)
        return result

    # ---- apply 1: silver rows + quiet baseline events (never title.added / title.updated)
    for name, pubs in to_add.items():
        silver = _write_silver(pubs, name)
        events = _write_events(pubs, name, stored_before={p.doi for p in pubs})
        result['volumes_added'] += silver.inserted
        result['baseline_events'] += events.baseline
        _refresh_gold(name)
        _log_pipeline_run(name, len(pubs), silver.inserted,
                          'success' if not silver.failed and not events.failed else 'partial',
                          error=None if not silver.failed and not events.failed else 'repair: batch failed',
                          started_at=started, run_type='manual')

    # ---- apply 2: delete the chapters (publications, their authors links and their events)
    if deletable:
        session = DatabaseConfig.get_session()
        try:
            rows = session.execute(text(
                'SELECT id, series FROM publications WHERE doi = ANY(:d)'), {'d': deletable}).fetchall()
            session.execute(text('DELETE FROM publication_authors WHERE publication_id = ANY(:i)'),
                            {'i': [r[0] for r in rows]})
            result['events_deleted'] = session.execute(text(
                'DELETE FROM events WHERE source_record_id = ANY(:d)'), {'d': deletable}).rowcount
            result['chapters_deleted'] = session.execute(text(
                'DELETE FROM publications WHERE doi = ANY(:d)'), {'d': deletable}).rowcount
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        for name in {r[1] for r in rows if r[1]}:
            _refresh_gold(name)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--dry-run', action='store_true', help='report only, write nothing')
    parser.add_argument('--since', default=SINCE_DEFAULT, help='registered on/after (YYYY-MM-DD)')
    parser.add_argument('--until', default=UNTIL_DEFAULT, help='registered before (YYYY-MM-DD); default = first loading day')
    args = parser.parse_args()

    r = run(dry_run=args.dry_run, since=args.since, until=args.until)
    print(f"\nVolumes registered {r['since']} to {r['until']} and missing here : {sum(r['volumes_missing'].values())}"
          f"  {dict(r['volumes_missing']) or ''}")
    print(f"Chapter rows found                                  : {r['chapters']}")
    print(f"  safe to delete (parent volume present, only title.added): {r['chapters_deletable']}")
    print(f"  kept, parent volume unknown                       : {r['chapters_kept_no_parent']}")
    print(f"  kept, they have other events                      : {r['chapters_kept_unsafe']}")
    if r['dry_run']:
        print('\n(dry run - nothing written)')
    else:
        print(f"\nVolumes added: {r['volumes_added']}  | baseline events: {r['baseline_events']}")
        print(f"Chapters deleted: {r['chapters_deleted']}  | their events deleted: {r['events_deleted']}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
