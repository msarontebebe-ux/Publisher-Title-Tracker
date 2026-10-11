# Springer Conference Proceedings Monitor

An autonomous data pipeline that **monitors new and updated conference
proceedings** from Springer series (LNCS, CCIS, LNAI, …) via the free
CrossRef API, stores them in a cloud PostgreSQL database using a
medallion (Bronze → Silver → Gold) architecture, detects changes as an
append-only **event log**, and serves the increments to users
through a REST API, a web interface, and RSS/Atom feeds.

It runs on a schedule (GitLab CI) — every night it fetches only what
changed since the last successful run and records what is new or updated.

---

## What it does

1. **Extract** — query CrossRef for proceedings volumes in the configured
   Springer series, using cursor pagination (no 10k limit).
2. **Load (medallion)** — store raw responses (Bronze), cleaned records
   (Silver), and analytics aggregates (Gold) in PostgreSQL.
3. **Detect change** — append a `title.added` or `title.updated` event for
   every observed change (fingerprint-based, idempotent).
4. **Serve increments** — expose new/updated titles via the API, web UI, and
   subscribable feeds, so users are notified instead of checking by hand.

## Architecture

```
CrossRef API
    │  (incremental: only changes since the last watermark)
    ▼
Bronze   raw API responses (one row per batch)
    ▼
Silver   cleaned, deduplicated publications + authors
    ▼
Gold     per-series stats, yearly trends, top authors
    ▼
Events   append-only change log: title.added / title.updated
    ▼
REST API  ·  Web UI  ·  /feed.rss  ·  /feed/events.atom
```

### Key concepts

- **Medallion layers** — Bronze (raw) → Silver (clean) → Gold (aggregates),
  implemented as PostgreSQL schemas/tables in Neon.
- **Watermark (high-water mark)** — each series keeps a `last_success_at`
  bookmark. Incremental runs ask CrossRef only for records created/changed
  after it (`from-created-date`), instead of re-scanning the whole year. The
  bookmark advances only after a successful run.
- **Event log** — every observed change is an append-only row with a stable
  `payload_hash`. A `UNIQUE(source, doi, payload_hash)` constraint makes
  re-runs idempotent by construction. This is what distinguishes a *new*
  title (`title.added`) from a *changed* one (`title.updated`).

## Quick start

```bash
# 1. Install dependencies
python -m pip install -r requirements.txt

# 2. Point at a database (create a .env with your Neon/RDS connection string)
#    DATABASE_URL=postgresql://user:pass@host/dbname?sslmode=require
#    Without it, the pipeline falls back to local JSON files.

# 3. Create the schemas and tables (idempotent, safe to re-run)
python init_database.py

# 4. Run the pipeline (incremental — only what changed since last run)
python orchestrator.py --incremental

# 5. Serve the API + web UI
python api_server.py          # then open http://localhost:5000
```

### Common commands

```bash
python orchestrator.py --incremental                  # nightly delta run
python orchestrator.py --series LNCS                  # one series, current year
python orchestrator.py --series LNCS --year-start 2020   # backfill a range
python orchestrator.py --dry-run                      # validate config only
python main_crossref.py --series CCIS --year-start 2024   # scraper entry point
pytest                                                 # run the test suite
```

## Project structure

```
├── orchestrator.py            # pipeline entry point (scrape→load→events→gold→log)
├── main_crossref.py           # scraper CLI (thin shim over pipeline.runner)
├── api_server.py              # REST API + web interface
├── init_database.py           # create schemas/tables, seed series (idempotent)
├── enrich_event_dates.py      # utility: backfill event dates from CrossRef/Springer Meta
├── migrate_to_database.py     # utility: backfill existing local JSON into the DB
├── config.json                # series definitions (name, ISSN) + API settings
├── .gitlab-ci.yml             # CI: tests on every push + scheduled nightly run
├── pipeline/                  # the reusable pipeline package
│   ├── config.py              # config loading + series lookup helpers
│   ├── db.py                  # database connection (from environment)
│   ├── models.py              # SQLAlchemy models (bronze/silver/gold/events/state)
│   ├── scraper.py             # CrossRef API client (cursor pagination)
│   ├── event_dates.py         # conference date/venue extraction from titles
│   ├── events.py              # change detection: fingerprint + add/update classify
│   ├── watermark.py           # high-water mark helpers
│   ├── storage.py             # Bronze/Silver/Gold file layers (fallback)
│   ├── gold.py                # Gold aggregates + data-quality metrics
│   └── runner.py              # scrape orchestration + DB writes
├── tests/                     # pytest suite
└── data/                      # local fallback output (bronze/silver/gold JSON)
```

## Series monitored

LNCS, LNAI, LNBI, CCIS, IFIP AICT, LNEE, LNNS, AISC — each defined in
`config.json` with its ISSN. Records are **proceedings volumes** (whole
books) with metadata: DOI, ISBN, title, editors/authors, year, pages, and
extracted conference date/venue when present.

## API endpoints

| Endpoint | Purpose |
|---|---|
| `GET /api/publications?series=&year=&query=` | Search publications |
| `GET /api/publications/new?since=` | Increments added since a date |
| `GET /api/publications/recent` | Most recent publications |
| `GET /api/events?since=&type=` | Change events (`title.added` / `title.updated`) |
| `GET /api/series`, `/api/stats` | Series list and overall stats |
| `GET /api/gold/stats`, `/api/gold/trends`, `/api/gold/authors` | Gold aggregates |
| `GET /feed.rss?series=` | RSS feed of new publications |
| `GET /feed/events.atom?series=` | Atom feed of change events |
| `GET /health` | Health / DB connectivity check |

## Scheduling & CI

`.gitlab-ci.yml` defines two jobs:
- **test** — runs the pytest suite (with a disposable Postgres) on every push.
- **scheduled-pipeline** — runs `orchestrator.py --incremental` on a GitLab
  pipeline schedule (e.g. nightly), writing to the cloud DB and keeping each
  run's `logs/` as an artifact.

## Configuration

`config.json` holds the series list (name + ISSN) and CrossRef settings
(email for the polite pool, request delay). Database credentials come from
environment variables (`DATABASE_URL`), never from files — see `pipeline/db.py`.

## Notes

- Uses the **CrossRef REST API** (free; the polite pool just needs an email).
- Deduplication is by DOI; the event log is idempotent by a DB uniqueness
  constraint, so re-runs never create duplicates.
- Free-tier friendly: incremental runs fetch only the delta, not full re-scans.

## License

For academic / research use.

