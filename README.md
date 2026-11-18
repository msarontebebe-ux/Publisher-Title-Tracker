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
   A title that was stored before the event log existed gets a quiet
   `title.baseline` the first time it is seen again, so a later change to it is a
   real update and the old title is not announced as new. Baselines are hidden
   from the API and feeds unless you ask for `type=title.baseline`.

Each stream has a watermark (`pipeline_state.last_success_at`) that only moves
forward after a complete run, so a failed run is just retried. The `events` table
has a unique constraint on `(source, source_record_id, payload_hash)`, so running
twice cannot create duplicates.

## What is tracked

- **Springer:** LNCS, CCIS, IFIP AICT, LNEE, LNNS, AISC, via CrossRef by ISSN.
  LNAI and LNBI are skipped because CrossRef files them under LNCS.
  With a `SPRINGER_API_KEY`, Springer's own Meta API is asked as well on incremental
  runs (see below).
- **Elsevier, Wiley, ACM:** via CrossRef by DOI prefix and work type.
- **IEEE:** conferences and journals from the IEEE Xplore API (papers are grouped
  into volumes/issues), books from CrossRef. Falls back to CrossRef without a key.

Publishers and types are set in `config.json`.

### Springer Meta API (early detection)

The Meta API has one record per chapter and no series filter, so the collector asks
for every book chapter that went online since the watermark, keeps the chapters of the
tracked series (`seriesId`) and groups them into volumes (volume DOI = chapter DOI
without the trailing `_<n>`). A volume shows up as soon as its first chapter is online,
usually before CrossRef has the volume DOI.

CrossRef stays the source for title, editors and ISBN (the API only has the short book
name). For a volume both know, the API adds the proceedings label (`genre`) and the
conference data (name, place, dates). A volume only the API knows is stored with the
API content; when CrossRef registers it, the fuller record produces one `title.updated`
and the stored row is updated to match. Data the API added is never wiped by a later
CrossRef-only record. Without the key, or when the quota is used up, only CrossRef runs.

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
SPRINGER_API_KEY=your-key    # optional, Meta API Basic plan is 500 calls/day
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

The web page (`/`) shows the figures, three charts and the list of new titles; it reads only the
read-only `/api/dashboard/...` endpoints, which are built on the star schema, so opening the page never
changes the database. Light and dark mode follow the system setting.

The API lives under `/api` (`/api/publishers`, `/api/events`, `/api/publications`,
...; the index at `/api` lists them), with feeds at `/feed.rss` and
`/feed/events.atom`. Most endpoints accept `publisher=` (SPRINGER, ELSEVIER, WILEY,
IEEE, ACM).

## Putting the web page online (Render)

`render.yaml` describes the site. Create a **read-only** database user in Neon (SELECT only) and give
Render its connection string as `DATABASE_URL`; never the owner's. With `READ_ONLY=1` (set in the
file) the site writes nothing, the two endpoints that call other websites are off, every visitor is
limited to `RATE_LIMIT_PER_MINUTE` API requests (default 300) and security headers are sent. The
nightly job keeps running on GitLab with the owner's connection string. Check the deployment at
`/health`.

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
- Incremental runs ask CrossRef for records deposited or updated since the last run
  (`from-update-date`) and use each record's `created` date to skip old titles that
  were never tracked here. Real metadata changes in the fingerprinted fields turned
  out to be rare (no `title.updated` among the first 1,643 events). With the Springer
  API, each volume it detects before CrossRef gets one `title.updated` when CrossRef
  completes it.
- Events written before the `source` column was labelled are all `crossref`,
  including some that came from the IEEE API.
- Old Springer rows have no `work_type` until you run `python backfill_work_type.py`.
- The API has no authentication or rate limiting, and reading `/api/events` sets
  `delivered_at`, so it is not read-only.

Data sources: [CrossRef](https://api.crossref.org) and
[IEEE Xplore](https://developer.ieee.org). For academic / research use.
