"""
Two-step enrichment for conference event data:

Step 1 — CrossRef subtitles:
  Re-queries CrossRef by ISSN fetching DOI + subtitle + event fields,
  then updates records missing event_date_start.

Step 2 — Springer Meta API:
  For records still missing event_name or event_location after Step 1,
  queries the Springer Nature Meta API by DOI to get structured
  conferenceInfo (confEventName, confEvtDate, confEvtLocation).
  Respects the free-tier limit of 100 hits/min.
"""

import json
import os
import time
import requests
from pathlib import Path
from sqlalchemy import text
from database_config import DatabaseConfig
from event_date_extractor import ConferenceDateExtractor

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

CROSSREF_URL = "https://api.crossref.org/works"
DELAY = 0.5  # seconds between pages (polite pool)
ROWS = 100


def fetch_subtitles_for_issn(session_http, issn: str, series_name: str) -> dict:
    """
    Fetch DOI -> subtitle mapping from CrossRef for a given ISSN.
    Returns {doi: subtitle_string} for records that have a subtitle.
    """
    cursor = "*"
    doi_to_subtitle = {}
    page = 0

    while True:
        params = {
            "filter": f"issn:{issn},type:book",
            "rows": ROWS,
            "cursor": cursor,
            "select": "DOI,subtitle,event",
        }
        try:
            r = session_http.get(CROSSREF_URL, params=params, timeout=30)
            r.raise_for_status()
            message = r.json().get("message", {})
            items = message.get("items", [])
            next_cursor = message.get("next-cursor")

            for item in items:
                doi = item.get("DOI")
                subtitle_list = item.get("subtitle", [])
                subtitle = subtitle_list[0] if subtitle_list else None
                if doi and subtitle:
                    doi_to_subtitle[doi] = subtitle

            page += 1
            fetched = page * ROWS
            print(f"    page {page}: {fetched} records scanned, "
                  f"{len(doi_to_subtitle)} with subtitle so far...", end="\r")

            if not items or not next_cursor:
                break
            cursor = next_cursor
            time.sleep(DELAY)

        except Exception as exc:
            print(f"\n    Warning: API error on page {page}: {exc}")
            break

    print()  # newline after progress
    return doi_to_subtitle


def update_event_dates(db_engine, doi_to_subtitle: dict) -> int:
    """
    Extract dates from subtitles and batch-update the DB.
    Commits every 500 rows to avoid long-running transactions on Neon.
    """
    # Build the list of update dicts first (pure Python — fast)
    updates = []
    for doi, subtitle in doi_to_subtitle.items():
        info = ConferenceDateExtractor.extract_from_title(subtitle)
        if not info or info.get("confidence") == "low":
            continue
        if not info.get("event_date_start") and not info.get("event_year"):
            continue
        updates.append({
            "doi":      doi,
            "start":    info.get("event_date_start"),
            "end":      info.get("event_date_end"),
            "year":     info.get("event_year"),
            "month":    info.get("event_month"),
            "conf":     info.get("confidence"),
            "name":     info.get("event_name"),
            "location": info.get("event_location"),
        })

    if not updates:
        print("  No extractable dates found in subtitles.")
        return 0

    print(f"  {len(updates)} records with extractable dates — writing to DB in batches...")

    BATCH = 500
    updated = 0
    with db_engine.connect() as conn:
        for i in range(0, len(updates), BATCH):
            batch = updates[i: i + BATCH]
            for row in batch:
                result = conn.execute(text("""
                    UPDATE publications
                    SET event_date_start      = :start,
                        event_date_end        = :end,
                        event_year            = COALESCE(event_year, :year),
                        event_month           = COALESCE(event_month, :month),
                        event_date_confidence = :conf,
                        event_name            = COALESCE(event_name, :name),
                        event_location        = COALESCE(event_location, :location)
                    WHERE doi = :doi
                      AND event_date_start IS NULL
                """), row)
                updated += result.rowcount
            conn.commit()
            done = min(i + BATCH, len(updates))
            print(f"  Progress: {done}/{len(updates)} processed, {updated} updated...", end="\r")

    print()  # newline after progress line
    return updated


def enrich_from_springer(db_engine, batch_size: int = 50) -> int:
    """
    Step 2: For records where event_name or event_location is still NULL,
    query the Springer Nature Meta API by DOI to get structured conference info.

    Free tier: 500 hits/day, 100 hits/min → 0.6 s delay between requests.
    We only query records that are missing data, so usage stays low.
    """
    api_key = os.getenv("SPRINGER_META_API_KEY")
    if not api_key:
        print("\n[Springer] SPRINGER_META_API_KEY not set — skipping Step 2.")
        return 0

    SPRINGER_URL = "https://api.springernature.com/meta/v2/books"
    DELAY = 0.65  # 100 hits/min = 0.6 s minimum; 0.65 s for safety

    # Fetch DOIs that still need event_name or event_location
    with db_engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT doi FROM publications
            WHERE doi IS NOT NULL
              AND (event_name IS NULL OR event_location IS NULL)
            ORDER BY fetched_at DESC
            LIMIT :n
        """), {"n": batch_size}).fetchall()

    dois = [r[0] for r in rows]
    if not dois:
        print("\n[Springer] All records already have event_name + event_location.")
        return 0

    print(f"\n[Springer] Querying Meta API for {len(dois)} records missing event info...")

    session_http = requests.Session()
    session_http.headers["Accept"] = "application/json"
    updated = 0

    for i, doi in enumerate(dois, 1):
        try:
            # Try DOI query first, fall back to ISBN extracted from DOI
            isbn = doi.replace("10.1007/", "") if doi.startswith("10.1007/") else None
            queries = [f"doi:{doi}"]
            if isbn and isbn.startswith("978"):
                queries.append(f"isbn:{isbn}")

            records = []
            for q in queries:
                r = session_http.get(SPRINGER_URL, params={
                    "q": q, "api_key": api_key, "p": 1,
                }, timeout=10)
                if r.status_code == 404:
                    continue  # book not in Springer API — normal, skip silently
                r.raise_for_status()
                records = r.json().get("records", [])
                if records:
                    break
                time.sleep(DELAY)

            if not records:
                continue

            rec  = records[0]
            info = rec.get("conferenceInfo", {})

            name     = (info.get("confEventName")
                        or info.get("confSeriesName")
                        or info.get("confEventAbbreviation"))
            location = (info.get("confEvtLocation")
                        or info.get("confEventLocation")
                        or info.get("confLocation"))

            if not name and not location:
                continue

            with db_engine.connect() as conn:
                conn.execute(text("""
                    UPDATE publications
                    SET event_name     = COALESCE(event_name,     :name),
                        event_location = COALESCE(event_location, :location)
                    WHERE doi = :doi
                """), {"name": name, "location": location, "doi": doi})
                conn.commit()
            updated += 1
            print(f"  [{i}/{len(dois)}] {doi[:45]} → {name or '—'} | {location or '—'}", end="\r")

        except Exception as exc:
            print(f"\n  Warning: Springer API error for {doi}: {exc}")

        time.sleep(DELAY)

    print(f"\n[Springer] Done — {updated} records updated with Springer event info.")
    return updated


def main():
    config = json.load(open("config.json", encoding="utf-8"))

    session_http = requests.Session()
    email = config.get("crossref_api", {}).get("email", "research@example.com")
    session_http.headers["User-Agent"] = f"SpringerEnricher/1.0 (mailto:{email})"

    db_engine = DatabaseConfig.create_engine_from_env()

    print("=" * 60)
    print("Event Date Enrichment from CrossRef Subtitles")
    print("=" * 60)

    total_updated = 0

    for series in config.get("springer_series", []):
        name = series["name"]
        abbr = series["abbreviation"]
        issn = series.get("issn")

        if not series.get("scrape_via_issn", True):
            parent = series.get("issn_shared_with", "parent series")
            print(f"\n[{abbr}] Skipped — ISSN shared with {parent}, already enriched in that pass.")
            continue

        if not issn:
            print(f"\n[{abbr}] No ISSN configured — skipping")
            continue

        print(f"\n[{abbr}] Fetching subtitles from CrossRef (ISSN {issn})...")
        doi_to_subtitle = fetch_subtitles_for_issn(session_http, issn, name)
        print(f"  {len(doi_to_subtitle)} records have subtitle")

        if not doi_to_subtitle:
            continue

        updated = update_event_dates(db_engine, doi_to_subtitle)
        total_updated += updated
        print(f"  {updated} DB records updated with event dates")

    print(f"\n{'='*60}")
    print(f"Enrichment complete: {total_updated} records updated")
    print(f"{'='*60}")

    # Step 2: fill remaining gaps using Springer Meta API
    enrich_from_springer(db_engine, batch_size=200)

    # Recompute gold quality metrics
    print("\nRecomputing quality metrics...")
    from gold_layer import refresh_gold
    refresh_gold()
    print("Done.")


if __name__ == "__main__":
    main()
