# Publisher Title Tracker

Keeps track of new titles (proceedings volumes, journal issues, books) from
Springer, Elsevier, Wiley, IEEE and ACM. Every day it fetches what was registered
since the last run, stores it in PostgreSQL and records each new or changed title
as an event. Results are available in a web page, a JSON API and RSS/Atom feeds.

Built as part of a thesis. Individual journal articles are not tracked.

## How it works

1. A collector asks CrossRef (or the IEEE Xplore API for IEEE) for records created
   since the last successful run of that stream.
2. Records are stored raw (bronze), cleaned and de-duplicated by DOI (silver), and
   a few aggregates are recomputed (gold).
3. Each record is fingerprinted. A new DOI gives a `title.added` event, a known
   DOI with changed data gives `title.updated`, unchanged data gives nothing.

Each stream has a watermark (`pipeline_state.last_success_at`) that only moves
forward after a complete run, so a failed run is just retried. The `events` table
has a unique constraint on `(source, source_record_id, payload_hash)`, so running
twice cannot create duplicates.

## What is tracked

- **Springer:** LNCS, CCIS, IFIP AICT, LNEE, LNNS, AISC, via CrossRef by ISSN.
  LNAI and LNBI are skipped because CrossRef files them under LNCS.
- **Elsevier, Wiley, ACM:** via CrossRef by DOI prefix and work type.
- **IEEE:** conferences and journals from the IEEE Xplore API (papers are grouped
  into volumes/issues), books from CrossRef. Falls back to CrossRef without a key.

Publishers and types are set in `config.json`.

## Setup

Python 3.12 and a PostgreSQL database (I use Neon).

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env`:

```
DATABASE_URL=postgresql://user:password@host/dbname?sslmode=require
IEEE_API_KEY=your-key        # optional, free tier is ~200 calls/day
```

Then create the tables (safe to re-run):

```bash
python init_database.py
```

## Usage

```bash
python orchestrator.py --incremental                       # everything, only what is new
python orchestrator.py --incremental --publisher WILEY IEEE
python orchestrator.py --incremental --no-publishers       # Springer series only
python orchestrator.py --dry-run                           # check the config

python api_server.py                                       # http://localhost:5000
```

The first incremental run of a publisher looks back 7 days; after that the
watermark decides. Without `--incremental` it fetches the whole current year,
which is a lot for Wiley, so try ACM first.

The API lives under `/api` (`/api/publishers`, `/api/events`, `/api/publications`,
...; the index at `/api` lists them), with feeds at `/feed.rss` and
`/feed/events.atom`. Most endpoints accept `publisher=` (SPRINGER, ELSEVIER, WILEY,
IEEE, ACM).

## Scheduling and tests

`.gitlab-ci.yml` runs the tests on every push. On a pipeline schedule it runs
`init-database`, `springer` and one `publisher` job per publisher; this needs a
masked `DATABASE_URL` variable. To add a publisher, add it to `config.json` and to
the job matrix in the CI file (a test fails if they differ).

```bash
pip install -r requirements-dev.txt
pytest
```

Database tests use `TEST_DATABASE_URL` and are skipped if it is unreachable; they
never touch the database from `.env`.

## Limitations

- "New" means newly registered in CrossRef, which can be later than the
  publisher's announcement. Metadata is uneven (Elsevier has almost no journal
  issues there).
- Events written before the `source` column was labelled are all `crossref`,
  including some that came from the IEEE API.
- Old Springer rows have no `work_type` until you run `python backfill_work_type.py`.
- The API has no authentication or rate limiting, and reading `/api/events` sets
  `delivered_at`, so it is not read-only.

Data sources: [CrossRef](https://api.crossref.org) and
[IEEE Xplore](https://developer.ieee.org). For academic / research use.
