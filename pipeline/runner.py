"""Main script for scraping Springer publications using CrossRef API."""

import argparse
import json
import os
from collections import namedtuple
from datetime import datetime, timezone
from pathlib import Path
from pipeline.scraper import CrossRefScraper
from pipeline.storage import DataLakeStorageHandler
# load_config / find_series_by_abbr live in pipeline.config (single source of
# truth); importing it also loads .env so DATABASE_URL is visible below.
from pipeline.config import load_config, find_series_by_abbr

def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)



def _db_configured() -> bool:
    """Return True when cloud DB credentials are present."""
    from pipeline.db import DatabaseConfig
    return DatabaseConfig.is_cloud_configured()


def _prune_bronze(series_abbr: str, keep: int = 3):
    """Delete old bronze batches for this series, keeping the most recent `keep`."""
    try:
        from pipeline.db import DatabaseConfig
        from sqlalchemy import text as sa_text
        session = DatabaseConfig.get_session()
        try:
            result = session.execute(sa_text("""
                DELETE FROM bronze.raw_responses
                WHERE series_abbr = :abbr
                AND id NOT IN (
                    SELECT id FROM bronze.raw_responses
                    WHERE series_abbr = :abbr
                    ORDER BY fetched_at DESC
                    LIMIT :keep
                )
            """), {'abbr': series_abbr, 'keep': keep})
            deleted = result.rowcount
            session.commit()
            if deleted:
                print(f"   Bronze: pruned {deleted} old batch(es) for {series_abbr} (keeping {keep})")
        finally:
            session.close()
    except Exception as exc:
        print(f"   Bronze prune failed: {exc}")


def _write_bronze(raw_data: list, series_abbr: str, series_name: str):
    """Store raw API response in bronze.raw_responses, then prune old batches."""
    if not raw_data:
        return
    try:
        from pipeline.db import DatabaseConfig
        from pipeline.models import BronzeRawResponse
        session = DatabaseConfig.get_session()
        # Store only a sample (first 10 records) — full raw data lives in data/bronze/ files.
        # Storing the entire batch in one DB cell hits Neon's 64 MB response limit.
        session.add(BronzeRawResponse(
            series_abbr=series_abbr,
            series_name=series_name,
            fetched_at=_utcnow(),
            record_count=len(raw_data),
            raw_data=raw_data[:10],
        ))
        session.commit()
        session.close()
        print(f"   Bronze: {len(raw_data)} raw records saved to Neon")
    except Exception as exc:
        print(f"   Bronze write failed: {exc}")
        return
    _prune_bronze(series_abbr, keep=3)


SilverResult = namedtuple('SilverResult',
                          ['inserted', 'skipped', 'failed', 'errors'])


EventResult = namedtuple('EventResult',
                         ['added', 'updated', 'unchanged', 'failed'])


def _write_events(publications, series_name: str) -> "EventResult":
    """
    Append change events (title.added / title.updated) to the events log.

    Unlike _write_silver — which only inserts brand-new DOIs — this pass looks
    at EVERY scraped record, including ones already present in `publications`,
    so that a *changed* record (corrected title, added ISBN, new event date…)
    is detected as title.updated rather than silently ignored.

    Idempotency is enforced by the UNIQUE(source, source_record_id,
    payload_hash) constraint via INSERT ... ON CONFLICT DO NOTHING, so
    re-running over unchanged data adds zero events. See pipeline/events.py.

    Returns an EventResult(added, updated, unchanged, failed) so the caller can
    log truthfully and the evaluation chapter can measure add-vs-update rates.
    """
    from pipeline.db import DatabaseConfig
    from pipeline.models import TitleEvent
    from pipeline.events import (
        payload_hash, canonical_record, classify_event_type, new_event_id,
    )
    from sqlalchemy import text as sa_text

    pub_dicts = [p.to_dict() for p in publications]
    if not pub_dicts:
        return EventResult(added=0, updated=0, unchanged=0, failed=0)

    added = updated = unchanged = 0
    failed = 0

    # Load the last-known fingerprint for every DOI we're about to process, so
    # we can tell "new" from "changed" from "same" in plain Python.
    session = DatabaseConfig.get_session()
    try:
        dois = [d.get('doi') for d in pub_dicts if d.get('doi')]
        last_hash: dict = {}
        if dois:
            # Postgres: DISTINCT ON gives the most recent event per DOI.
            rows = session.execute(sa_text("""
                SELECT DISTINCT ON (source_record_id) source_record_id, payload_hash
                FROM events
                WHERE source = 'crossref' AND source_record_id = ANY(:dois)
                ORDER BY source_record_id, id DESC
            """), {'dois': dois}).fetchall()
            last_hash = {row[0]: row[1] for row in rows}
    finally:
        session.close()

    # One transaction for the whole batch — event rows are tiny and independent,
    # so a single commit is safe and far cheaper than per-record commits.
    session = DatabaseConfig.get_session()
    try:
        for pub_data in pub_dicts:
            doi = pub_data.get('doi')
            if not doi:
                unchanged += 1
                continue

            existing_hash = last_hash.get(doi)
            event_type = classify_event_type(pub_data, existing_hash)
            if event_type is None:
                unchanged += 1
                continue

            current_hash = payload_hash(pub_data)

            # published_at: prefer a real event/publication date if present.
            published_at = None
            for key in ('event_date_start', 'event_date_end'):
                raw = pub_data.get(key)
                if raw:
                    try:
                        published_at = datetime.fromisoformat(raw).date()
                        break
                    except (ValueError, TypeError):
                        pass

            observed_at = None
            if pub_data.get('fetched_at'):
                try:
                    observed_at = datetime.fromisoformat(pub_data['fetched_at'])
                except (ValueError, TypeError):
                    observed_at = None

            session.execute(sa_text("""
                INSERT INTO events
                    (event_id, event_type, source, source_record_id, series,
                     publisher, payload_hash, payload, published_at,
                     observed_at, detected_at)
                VALUES
                    (:event_id, :event_type, :source, :doi, :series,
                     :publisher, :payload_hash, CAST(:payload AS json), :published_at,
                     :observed_at, :detected_at)
                ON CONFLICT (source, source_record_id, payload_hash) DO NOTHING
            """), {
                'event_id':     new_event_id(),
                'event_type':   event_type,
                'source':       'crossref',
                'doi':          doi,
                'series':       pub_data.get('series') or series_name,
                'publisher':    pub_data.get('publisher'),
                'payload_hash': current_hash,
                'payload':      json.dumps(canonical_record(pub_data)),
                'published_at': published_at,
                'observed_at':  observed_at,
                'detected_at':  _utcnow(),
            })

            if event_type == 'title.added':
                added += 1
            else:
                updated += 1
            # Update our in-memory view so multiple records for the same DOI
            # within one run don't each look "new".
            last_hash[doi] = current_hash

        session.commit()
    except Exception as exc:
        session.rollback()
        failed += 1
        print(f"   Events: write failed: {exc}")
    finally:
        session.close()

    print(f"   Events: added={added}  updated={updated}  unchanged={unchanged}"
          + (f"  FAILED" if failed else ""))
    return EventResult(added=added, updated=updated, unchanged=unchanged, failed=failed)


def _write_silver(publications, series_name: str) -> "SilverResult":
    """
    Upsert cleaned publications into silver tables in batches of 100.
    Each batch is its own committed transaction to avoid Neon serverless timeouts.
    Author cache stores IDs (not ORM objects) to avoid detached-instance errors
    across sessions.

    Returns a SilverResult so the caller can log a TRUTHFUL status:
      - inserted : number of rows newly written
      - skipped  : number of rows dropped (no DOI, or DOI already present)
      - failed   : number of batches that rolled back (could not be written)
      - errors   : one message per failed batch

    A non-zero `failed` means the run was only partially successful; the
    caller must NOT record it as a clean 'success'.
    """
    from pipeline.db import DatabaseConfig
    from pipeline.models import Publication, Author
    from sqlalchemy import text as sa_text

    BATCH_SIZE = 100
    pub_dicts = [p.to_dict() for p in publications]
    total_inserted = 0
    total_skipped = 0
    failed_batches = 0
    errors = []

    # Pre-load existing DOIs and author name→id into plain Python dicts/sets.
    # Plain Python types are safe to use across sessions — no detached ORM objects.
    session = DatabaseConfig.get_session()
    try:
        existing_dois = {
            row[0] for row in session.execute(sa_text('SELECT doi FROM publications'))
        }
        # Cache: author name → integer ID (not ORM object)
        author_name_to_id = {
            row[0]: row[1]
            for row in session.execute(sa_text('SELECT name, id FROM authors'))
        }
    finally:
        session.close()

    # Process in committed batches of BATCH_SIZE.
    # No session.flush() inside the loop — SQLAlchemy assigns IDs at commit time.
    # Authors created in this batch are tracked by name until the batch commits,
    # then their real IDs are written into author_name_to_id for future batches.
    for batch_start in range(0, len(pub_dicts), BATCH_SIZE):
        batch = pub_dicts[batch_start: batch_start + BATCH_SIZE]
        session = DatabaseConfig.get_session()
        batch_inserted = 0
        # name → Author object for authors created within this batch (no ID yet)
        new_authors_this_batch: dict = {}
        try:
            for pub_data in batch:
                doi = pub_data.get('doi')
                if not doi or doi in existing_dois:
                    total_skipped += 1
                    continue

                pub = Publication(
                    doi=doi,
                    title=pub_data.get('title', ''),
                    year=pub_data.get('year'),
                    series=pub_data.get('series'),
                    volume=pub_data.get('volume'),
                    pages=pub_data.get('pages'),
                    isbn=pub_data.get('isbn'),
                    publisher=pub_data.get('publisher', 'Springer'),
                    work_type=pub_data.get('work_type'),
                    url=pub_data.get('ee') or pub_data.get('url'),
                    event_date_start=pub_data.get('event_date_start'),
                    event_date_end=pub_data.get('event_date_end'),
                    event_year=pub_data.get('event_year'),
                    event_month=pub_data.get('event_month'),
                    event_date_confidence=pub_data.get('event_date_confidence'),
                    event_name=pub_data.get('event_name'),
                    event_location=pub_data.get('event_location'),
                    fetched_at=(
                        datetime.fromisoformat(pub_data['fetched_at'])
                        if pub_data.get('fetched_at') else _utcnow()
                    ),
                )
                session.add(pub)

                # Deduplicate authors per publication by name to avoid
                # duplicate (publication_id, author_id) constraint violations
                seen_author_names: set = set()

                for author_data in pub_data.get('authors', []):
                    name = (
                        author_data.get('name', '')
                        if isinstance(author_data, dict) else str(author_data)
                    ).strip()
                    if not name or name in seen_author_names:
                        continue
                    seen_author_names.add(name)

                    if name in author_name_to_id:
                        # Already in DB — load by ID into this session
                        author = session.get(Author, author_name_to_id[name])
                    elif name in new_authors_this_batch:
                        # Created earlier in this same batch — reuse the object
                        author = new_authors_this_batch[name]
                    else:
                        # Brand new author — create and track for this batch
                        author = Author(name=name)
                        session.add(author)
                        new_authors_this_batch[name] = author

                    pub.authors.append(author)

                existing_dois.add(doi)
                batch_inserted += 1

            # Single commit per batch — SQLAlchemy assigns all IDs here
            session.commit()
            total_inserted += batch_inserted

            # Now IDs are assigned — persist new authors into the cross-batch cache
            for name, author in new_authors_this_batch.items():
                author_name_to_id[name] = author.id
            # NOTE: skips were already counted inside the record loop above —
            # do not count batch misses again here (would double-count them).

        except Exception as exc:
            session.rollback()
            failed_batches += 1
            errors.append(f"batch {batch_start}-{batch_start+BATCH_SIZE}: {exc}")
            print(f"   Silver: batch {batch_start}-{batch_start+BATCH_SIZE} failed: {exc}")
        finally:
            session.close()

        # Progress for large series
        done = batch_start + len(batch)
        if len(pub_dicts) > BATCH_SIZE:
            print(f"   Silver: {done}/{len(pub_dicts)} processed, {total_inserted} inserted...", end='\r')

    if len(pub_dicts) > BATCH_SIZE:
        print()  # newline after progress line
    print(f"   Silver: inserted={total_inserted}  skipped={total_skipped}"
          + (f"  FAILED_BATCHES={failed_batches}" if failed_batches else ""))
    return SilverResult(inserted=total_inserted, skipped=total_skipped,
                        failed=failed_batches, errors=errors)


def _refresh_gold(series_name: str):
    """Recompute gold aggregations for this series."""
    try:
        from pipeline.gold import refresh_gold
        refresh_gold(series_filter=series_name)
    except Exception as exc:
        print(f"   Gold compute failed: {exc}")


def _log_pipeline_run(series: str, records_fetched: int, records_new: int,
                      status: str, error: str = None,
                      started_at: datetime = None, run_type: str = 'full'):
    """Write a row to pipeline_runs for observability.

    Args:
        run_type: 'full', 'incremental', or 'manual' — reflects how the run
                  was invoked, so the table can be trusted as evaluation data.
    """
    try:
        from pipeline.db import DatabaseConfig
        from pipeline.models import PipelineRun
        session = DatabaseConfig.get_session()
        session.add(PipelineRun(
            run_type=run_type,
            series=series,
            records_fetched=records_fetched,
            records_new=records_new,
            status=status,
            error_message=error,
            started_at=started_at or _utcnow(),
            completed_at=_utcnow(),
        ))
        session.commit()
        session.close()
    except Exception as exc:
        print(f"   Pipeline log failed: {exc}")


def _persist_and_log(publications, raw_data, name: str, abbr: str, started,
                     run_type: str, storage, set_series: bool = True,
                     fetch_ok: bool = True):
    """
    Shared write path for every tracked stream (Springer series or publisher):
    Bronze -> Silver -> Events -> Gold -> run log -> watermark.

    `name` is the stream name used as pipeline_runs.series, the watermark key
    and (when set_series) the publications.series value. fetch_ok=False means
    the fetch was incomplete: the run is logged 'partial' and the watermark
    stays put so the same window is retried.
    """
    if publications and set_series:
        for pub in publications:
            pub.series = name

    if publications:
        records_new = 0

        if _db_configured():
            # --- Cloud pipeline: Bronze → Silver → Events → Gold → Log ---
            print(f"\n  Writing to Neon (3 layers):")
            _write_bronze(raw_data, abbr, name)
            silver = _write_silver(publications, name)
            records_new = silver.inserted
            # Append change events (title.added / title.updated). This is an
            # additive projection — it never affects the silver write above.
            events = _write_events(publications, name)
            _refresh_gold(name)

            # Truthful status: a batch that failed to commit means the run was
            # only partially successful — record 'partial' (with the errors),
            # never a clean 'success'. This is the audit trail NFR3 relies on.
            if silver.failed:
                status = 'partial'
                error_msg = f"{silver.failed} batch(es) failed: " + "; ".join(silver.errors)
            elif not fetch_ok:
                status = 'partial'
                error_msg = 'incomplete fetch (error or max_results reached) - watermark not advanced'
            else:
                status = 'success'
                error_msg = None

            _log_pipeline_run(name, len(publications), records_new, status,
                              error=error_msg,
                              started_at=started, run_type=run_type)
            # Advance the watermark ONLY on a fully successful incremental run,
            # so the next run fetches the delta after this one. A partial/failed
            # run leaves the bookmark put and re-tries the same window.
            if status == 'success' and run_type == 'incremental':
                from pipeline.watermark import advance_watermark
                new_wm = advance_watermark(name, when=_utcnow())
                if new_wm:
                    print(f"  Watermark advanced to {new_wm.isoformat()}")
            print(f"Done: {abbr}: {len(publications)} fetched, {records_new} new, "
                  f"{silver.skipped} skipped, status={status}")
        else:
            # --- Local fallback: write files only ---
            print(f"\n  No DB configured — saving to local files only")
            storage.save_by_series(publications, raw_data, abbr)
            print(f"Done: {abbr}: {len(publications)} fetched, saved to local files")
    else:
        print(f"  No publications found for {abbr}")
        # An empty result is still a completed run — log it (0 fetched, 0 new)
        # so 'ran and found nothing' is distinguishable from 'never ran'.
        if _db_configured():
            _log_pipeline_run(name, 0, 0, 'success' if fetch_ok else 'partial',
                              error=None if fetch_ok else 'incomplete fetch - watermark not advanced',
                              started_at=started, run_type=run_type)
            # Nothing new, but we still successfully checked up to now — advance
            # the bookmark so the next incremental run starts from here.
            if run_type == 'incremental' and fetch_ok:
                from pipeline.watermark import advance_watermark
                new_wm = advance_watermark(name, when=_utcnow())
                if new_wm:
                    print(f"  Watermark advanced to {new_wm.isoformat()}")


def scrape_single_series(series_config: dict, scraper: CrossRefScraper,
                        storage: DataLakeStorageHandler, year_start=None, year_end=None,
                        max_results=50000, run_type: str = 'full'):
    """
    Scrape a single Springer series using CrossRef.
    
    Args:
        series_config: Series configuration dictionary
        scraper: CrossRefScraper instance
        storage: DataLakeStorageHandler instance
        year_start: Optional start year filter
        year_end: Optional end year filter
        max_results: Maximum results to fetch
        run_type: How this run was invoked ('full' / 'incremental' / 'manual');
                  recorded in pipeline_runs so the table is trustworthy
    """
    name = series_config['name']
    abbr = series_config['abbreviation']
    issn = series_config.get('issn')

    print(f"\n{'='*80}")
    print(f"Scraping: {name} ({abbr})")
    print(f"{'='*80}")

    # Skip series whose ISSN is shared with a parent series — querying it would
    # return duplicate records already captured by the parent scrape, and
    # CrossRef provides no field to distinguish them (confirmed: container-title,
    # ISSN, and short-container-title are identical for LNCS/LNAI/LNBI).
    if not series_config.get('scrape_via_issn', True) and not issn:
        parent = series_config.get('issn_shared_with', 'parent series')
        print(f"  Skipped: ISSN shared with {parent} — records captured in that scrape.")
        print(f"  Note: {series_config.get('note', '')}")
        return []

    started = _utcnow()
    publications = []
    raw_data = []

    # Watermark (high-water mark) for incremental runs: ask CrossRef only for
    # what changed since the last SUCCESSFUL run, instead of re-scanning the
    # whole current year. Full/manual runs keep their explicit year window.
    from_created_date = None
    if run_type == 'incremental' and _db_configured():
        from pipeline.watermark import get_watermark, watermark_to_created_date
        wm = get_watermark(name)
        if wm is not None:
            from_created_date = watermark_to_created_date(wm)
            print(f"  Watermark: last successful run {wm.isoformat()} "
                  f"→ fetching changes since {from_created_date}")
        else:
            # First incremental run: no bookmark yet. Fall back to the current
            # year so we still capture recent titles; the watermark is set below.
            print(f"  Watermark: none yet — first incremental run uses the year window")

    try:
        # Try ISSN-based search first (more reliable)
        if issn:
            print(f"Using ISSN: {issn}")
            publications, raw_data = scraper.search_by_issn(
                issn, year_start, year_end, max_results,
                from_created_date=from_created_date)

            # Also try electronic ISSN if available
            if not publications and 'issn_electronic' in series_config:
                e_issn = series_config['issn_electronic']
                print(f"Trying electronic ISSN: {e_issn}")
                publications, raw_data = scraper.search_by_issn(
                    e_issn, year_start, year_end, max_results,
                    from_created_date=from_created_date)
        
        # Fallback to title-based search
        if not publications:
            print(f"Falling back to series title search")
            # Title search doesn't return raw data yet (can be added if needed)
            publications = scraper.search_by_series_title(
                name, "Springer", year_start, year_end, max_results
            )
            raw_data = []  # No raw data from title search
    except Exception as exc:
        # Record the failed run before propagating, so pipeline_runs
        # tells the truth even when a scrape dies mid-flight.
        if _db_configured():
            _log_pipeline_run(name, 0, 0, 'failed', error=str(exc),
                              started_at=started, run_type=run_type)
        raise

    _persist_and_log(publications, raw_data, name, abbr, started, run_type, storage,
                     fetch_ok=getattr(scraper, 'fetch_ok', True) is not False)

    return publications


def scrape_publisher(pub_config: dict, scraper: CrossRefScraper,
                     storage: DataLakeStorageHandler, year_start=None, year_end=None,
                     max_results=50000, run_type: str = 'full'):
    """
    Track one publisher (DOI prefix + work types from config.json `publishers`)
    through the same Bronze/Silver/Events/Gold path as the Springer series.

    Incremental runs use the watermark; the very first one (no bookmark yet)
    looks back `initial_days` (default 7) instead of a whole year, because
    publishers such as Wiley/Elsevier register thousands of records.
    """
    from datetime import timedelta
    from pipeline.collectors import build_collector

    name = pub_config['name']
    abbr = pub_config['abbreviation']

    print(f"\n{'='*80}")
    print(f"Scraping publisher: {name} ({abbr})")
    print(f"{'='*80}")

    started = _utcnow()
    from_created_date = None
    if run_type == 'incremental':
        wm = None
        if _db_configured():
            from pipeline.watermark import get_watermark
            wm = get_watermark(name)
        if wm is not None:
            from pipeline.watermark import watermark_to_created_date
            from_created_date = watermark_to_created_date(wm)
            print(f"  Watermark: {wm.isoformat()} -> fetching changes since {from_created_date}")
        else:
            days = int(pub_config.get('initial_days', 7))
            from_created_date = (_utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')
            print(f"  Watermark: none yet - looking back {days} day(s) to {from_created_date}")

    collector = build_collector(pub_config, scraper)
    try:
        publications, raw_data = collector.fetch(
            from_created_date=from_created_date, year_start=year_start,
            year_end=year_end, max_results=max_results)
    except Exception as exc:
        if _db_configured():
            _log_pipeline_run(name, 0, 0, 'failed', error=str(exc),
                              started_at=started, run_type=run_type)
        raise

    _persist_and_log(publications, raw_data, name, abbr, started, run_type, storage,
                     fetch_ok=collector.fetch_ok)
    return publications


def scrape_all_series(config: dict, year_start=None, year_end=None, max_results=50000,
                      run_type: str = 'manual'):
    """
    Scrape all configured Springer series.
    
    Args:
        config: Configuration dictionary
        year_start: Optional start year filter
        year_end: Optional end year filter
        max_results: Maximum results per series
        run_type: How this run was invoked; recorded in pipeline_runs
    """
    crossref_config = config['crossref_api']
    output_config = config['output']
    
    scraper = CrossRefScraper(
        email=crossref_config['email'],
        delay=crossref_config['delay']
    )
    storage = DataLakeStorageHandler(base_dir=output_config['directory'])
    
    all_publications = []
    series_summary = []
    
    for series_config in config['springer_series']:
        publications = scrape_single_series(
            series_config, scraper, storage, year_start, year_end, max_results,
            run_type=run_type
        )
        all_publications.extend(publications)
        series_summary.append({
            'series': series_config['abbreviation'],
            'name': series_config['name'],
            'count': len(publications)
        })
    
    # Save combined data to silver layer
    if all_publications:
        storage.save_silver_json(all_publications, "all_springer_publications_crossref")
        storage.save_silver_csv(all_publications, "all_springer_publications_crossref")
    
    # Print summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    for item in series_summary:
        print(f"  {item['series']:10} - {item['name']:55} : {item['count']:5} publications")
    print(f"\n  TOTAL: {len(all_publications)} publications")
    print(f"{'='*80}\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Scrape Springer publication metadata from CrossRef API'
    )
    parser.add_argument(
        '--series',
        help='Scrape specific series by abbreviation (e.g., LNCS, CCIS)',
        type=str
    )
    parser.add_argument(
        '--year-start',
        help='Start year for filtering (inclusive)',
        type=int
    )
    parser.add_argument(
        '--year-end',
        help='End year for filtering (inclusive)',
        type=int
    )
    parser.add_argument(
        '--max-results',
        help='Maximum results per series (default: 50000)',
        type=int,
        default=50000
    )
    parser.add_argument(
        '--config',
        help='Path to configuration file (default: config.json)',
        type=str,
        default='config.json'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    crossref_config = config['crossref_api']
    output_config = config['output']
    
    scraper = CrossRefScraper(
        email=crossref_config['email'],
        delay=crossref_config['delay']
    )
    storage = DataLakeStorageHandler(base_dir=output_config['directory'])
    
    if args.series:
        # Scrape specific series
        series_config = find_series_by_abbr(args.series, config)
        
        if not series_config:
            print(f"Error: Series '{args.series}' not found in configuration")
            print(f"Available series: {', '.join([s['abbreviation'] for s in config['springer_series']])}")
            return
        
        scrape_single_series(
            series_config, scraper, storage, 
            args.year_start, args.year_end, args.max_results,
            run_type='manual'   # direct CLI invocation is a manual run
        )
    else:
        # Scrape all series
        scrape_all_series(config, args.year_start, args.year_end, args.max_results,
                          run_type='manual')


if __name__ == '__main__':
    main()
