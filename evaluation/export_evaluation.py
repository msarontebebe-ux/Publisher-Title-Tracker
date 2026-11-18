"""
Export the evaluation numbers as CSV and LaTeX tables (read-only).

    python evaluation/export_evaluation.py              # whole event log
    python evaluation/export_evaluation.py --since 2026-10-17

Writes evaluation/out/<name>.csv and evaluation/out/<name>.tex for every table.

The numbers come from the star schema (gold.fact_title_event, gold.fact_pipeline_run and their
dimensions), so the thesis and the model cannot disagree. Two
tables use the raw tables because the model does not hold that information: updates_by_cause
(what changed in an update) and api_calls_per_day. Only SELECT queries are run, and no network
requests are made. If the star schema is out of date (fewer fact rows than events), the script
says so: run `python -m pipeline.gold` first.
"""

import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import text                      # noqa: E402
from pipeline.db import DatabaseConfig          # noqa: E402

OUT = Path(__file__).resolve().parent / 'out'

# Events with all their dimensions. detected_date_key is never -1 (detected_at is always set).
FROM_EVENTS = """
FROM gold.fact_title_event f
JOIN gold.dim_date d ON d.date_key = f.detected_date_key
JOIN gold.dim_publisher p ON p.publisher_key = f.publisher_key
JOIN gold.dim_series s ON s.series_key = f.series_key
JOIN gold.dim_work_type w ON w.work_type_key = f.work_type_key
JOIN gold.dim_source src ON src.source_key = f.source_key
JOIN gold.dim_event_type et ON et.event_type_key = f.event_type_key
JOIN gold.dim_load_type lt ON lt.load_type_key = f.load_type_key
JOIN gold.dim_location l ON l.location_key = f.location_key
WHERE d.full_date >= CAST(:since AS date)"""

DAILY = "lt.load_type = 'Daily arrival'"


def tex_escape(value) -> str:
    s = '' if value is None else str(value)
    for old, new in (('\\', r'\textbackslash{}'), ('&', r'\&'), ('%', r'\%'), ('_', r'\_'),
                     ('#', r'\#'), ('$', r'\$')):
        s = s.replace(old, new)
    return s


def write_table(name: str, header: list, rows: list, caption: str):
    """One CSV (for plots/Excel) and one ready-to-include LaTeX table."""
    OUT.mkdir(exist_ok=True)
    with open(OUT / f'{name}.csv', 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    cols = 'l' + 'r' * (len(header) - 1)
    lines = [r'\begin{table}[htbp]', r'  \centering', f'  \\caption{{{tex_escape(caption)}}}',
             f'  \\label{{tab:{name}}}', f'  \\begin{{tabular}}{{{cols}}}', r'    \hline',
             '    ' + ' & '.join(tex_escape(h) for h in header) + r' \\', r'    \hline']
    lines += ['    ' + ' & '.join(tex_escape(c) for c in row) + r' \\' for row in rows]
    lines += [r'    \hline', r'  \end{tabular}', r'\end{table}']
    (OUT / f'{name}.tex').write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print(f'  {name}: {len(rows)} row(s)')


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[1])
    ap.add_argument('--since', default='1970-01-01', help='only events detected on/after this date')
    args = ap.parse_args()

    session = DatabaseConfig.get_session()
    q = lambda sql, **p: session.execute(text(sql), dict(p, since=args.since)).fetchall()
    print(f'Exporting to {OUT} (events since {args.since}, from the star schema)')

    events, facts = q('SELECT COUNT(*) FROM events')[0][0], q('SELECT COUNT(*) FROM gold.fact_title_event')[0][0]
    if events != facts:
        print(f'  WARNING: the star schema is out of date ({facts} fact rows, {events} events). '
              f'Run `python -m pipeline.gold` first.')

    write_table('events_by_source_and_type', ['Source', 'Event type', 'Events'],
                q(f"SELECT src.source, et.event_type, SUM(f.event_count) {FROM_EVENTS} GROUP BY 1, 2 ORDER BY 1, 2"),
                'Events in the event log by source and type')

    write_table('new_titles_by_publisher',
                ['Publisher', 'New titles', 'Daily arrivals', 'Backlog / first load', 'Registration unknown'],
                q(f"""SELECT p.publisher, SUM(f.event_count),
                             COALESCE(SUM(f.event_count) FILTER (WHERE lt.load_type = 'Daily arrival'), 0),
                             COALESCE(SUM(f.event_count) FILTER (WHERE lt.load_type = 'Backlog / first load'), 0),
                             COALESCE(SUM(f.event_count) FILTER (WHERE lt.load_type = 'Registration unknown'), 0)
                      {FROM_EVENTS} AND et.counts_as_new GROUP BY 1 ORDER BY 2 DESC, 1"""),
                'New titles found, by publisher and how they were found (daily arrival or first load)')

    write_table('new_titles_by_stream', ['Stream', 'New titles', 'Daily arrivals'],
                q(f"""SELECT s.series, SUM(f.event_count),
                             COALESCE(SUM(f.event_count) FILTER (WHERE {DAILY}), 0)
                      {FROM_EVENTS} AND et.counts_as_new GROUP BY 1 ORDER BY 2 DESC, 1"""),
                'New titles found, by tracked stream (Springer series or publisher)')

    write_table('new_titles_per_day', ['Date', 'Publisher', 'Load type', 'New titles'],
                q(f"""SELECT d.full_date, p.publisher, lt.load_type, SUM(f.event_count)
                      {FROM_EVENTS} AND et.counts_as_new GROUP BY 1, 2, 3 ORDER BY 1, 2, 3"""),
                'New titles per day, publisher and load type (use "Daily arrival" for the daily rate)')

    write_table('new_titles_by_type', ['Publisher', 'Proceedings', 'Book', 'Journal issue', 'Other'],
                q(f"""SELECT p.publisher,
                             COALESCE(SUM(f.event_count) FILTER (WHERE w.type_group = 'Proceedings'), 0),
                             COALESCE(SUM(f.event_count) FILTER (WHERE w.type_group = 'Book'), 0),
                             COALESCE(SUM(f.event_count) FILTER (WHERE w.type_group = 'Journal issue'), 0),
                             COALESCE(SUM(f.event_count) FILTER (WHERE w.type_group NOT IN
                                      ('Proceedings', 'Book', 'Journal issue')), 0)
                      {FROM_EVENTS} AND et.counts_as_new GROUP BY 1 ORDER BY 1"""),
                'New titles by publisher and type group (CrossRef calls most Springer volumes "book")')

    # What changed in each title.updated event (compared with the event before it). The star
    # schema counts updates but does not keep the payload, so this one reads the raw table.
    rows = q("SELECT e.payload, (SELECT o.payload FROM events o WHERE o.source_record_id = "
             "e.source_record_id AND o.id < e.id ORDER BY o.id DESC LIMIT 1) FROM events e "
             "WHERE e.event_type = 'title.updated' AND e.detected_at >= :since")
    causes = {}
    for new, old in rows:
        changed = [k for k in new if old and old.get(k) != new[k]]
        kind = ('conference data' if any(k.startswith('event_') for k in changed)
                else ', '.join(changed) or 'other')
        causes[kind] = causes.get(kind, 0) + 1
    write_table('updates_by_cause', ['What changed', 'Updates'],
                sorted(causes.items(), key=lambda x: -x[1]),
                'Updated titles by the kind of change')

    write_table('pipeline_runs', ['Run type', 'Runs', 'Successful', 'Partial', 'Success rate %'],
                q("""SELECT f.run_type, SUM(f.runs), SUM(f.is_success), SUM(f.is_partial),
                            ROUND(100.0 * SUM(f.is_success) / SUM(f.runs), 1)
                     FROM gold.fact_pipeline_run f JOIN gold.dim_date d ON d.date_key = f.run_date_key
                     WHERE d.full_date >= CAST(:since AS date) GROUP BY 1 ORDER BY 1"""),
                'Pipeline runs by type and outcome')

    write_table('data_quality', ['Stream', 'Titles', 'DOI %', 'Event date %', 'Year %', 'Authors %'],
                [[r[0], r[1]] + [f'{float(x):.1f}' for x in r[2:]] for r in q(
                    "SELECT series, total_records, doi_coverage_pct, event_date_coverage_pct, "
                    "event_year_coverage_pct, author_coverage_pct FROM quality_metrics "
                    "ORDER BY total_records DESC")],
                'Share of all stored titles that have each field, by stream')

    write_table('coverage_of_new_titles',
                ['Stream', 'New titles', 'With conference date %', 'With editors %', 'With location %'],
                q(f"""SELECT COALESCE(s.abbreviation, s.series), SUM(f.event_count),
                             ROUND(100.0 * SUM(f.has_conference_date) / SUM(f.event_count), 1),
                             ROUND(100.0 * SUM(f.has_editors) / SUM(f.event_count), 1),
                             ROUND(100.0 * SUM((f.location_key <> -1)::int) / SUM(f.event_count), 1)
                      {FROM_EVENTS} AND et.counts_as_new GROUP BY 1 ORDER BY 2 DESC, 1"""),
                'Share of new titles that have a conference date, editors and a location, by stream')

    write_table('top_countries', ['Country', 'New titles'],
                q(f"""SELECT l.country, SUM(f.event_count)
                      {FROM_EVENTS} AND et.counts_as_new AND l.country NOT IN ('Unknown', 'Virtual')
                      GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 15"""),
                'Countries with the most new titles that have a known conference location')

    write_table('api_calls_per_day', ['Day', 'Provider', 'Calls'],
                q("SELECT day, provider, calls FROM api_usage WHERE day >= CAST(:since AS date) "
                  "ORDER BY day, provider"),
                'Calls to rate-limited APIs per day')

    # Detection latency: hours from CrossRef registering a title to the system first reporting it.
    # Only daily arrivals (reported within 14 days of registration) with a known registration time;
    # the first loads of old titles would otherwise distort it. A negative value means the system
    # reported the title before CrossRef registered it (Springer API early detection).
    write_table('detection_latency', ['Publisher', 'Titles', 'Median (h)', '90th percentile (h)', 'Max (h)'],
                q(f"""SELECT p.publisher, COUNT(*),
                             ROUND(CAST(percentile_cont(0.5) WITHIN GROUP (ORDER BY f.latency_hours) AS numeric), 1),
                             ROUND(CAST(percentile_cont(0.9) WITHIN GROUP (ORDER BY f.latency_hours) AS numeric), 1),
                             ROUND(CAST(MAX(f.latency_hours) AS numeric), 1)
                      {FROM_EVENTS} AND et.counts_as_new AND {DAILY} AND f.latency_hours IS NOT NULL
                      GROUP BY 1 ORDER BY 2 DESC, 1"""),
                'Hours from CrossRef registration to first report by the system '
                '(titles first reported within 14 days of registration)')

    # Springer API volumes. The registration time comes from the stored data (no request to
    # CrossRef): lead = registration - first report = -latency.
    #   * early detection: reported within 14 days of registration (or not registered yet). A
    #     positive lead means the API reported it before CrossRef registered it.
    #   * late finds: volumes CrossRef registered long ago that the system had missed (the
    #     start-up backlog, see docs) and that the API reported when a late chapter appeared.
    #     They are not early detections and must not be averaged with them.
    api = q(f"""SELECT f.doi, e.detected_at, COALESCE(e.registered_at, pub.registered_at), f.latency_hours,
                       lt.load_type
                {FROM_EVENTS} AND src.source = 'springer-api' AND et.counts_as_new
                ORDER BY e.detected_at, f.doi""".replace(
                    'FROM gold.fact_title_event f',
                    'FROM gold.fact_title_event f JOIN events e ON e.event_id = f.event_id '
                    'LEFT JOIN publications pub ON pub.doi = f.doi'))
    early, late, leads = [], [], []
    for doi, seen, registered, latency, load in api:
        if load == 'Backlog / first load':
            late.append([doi, f'{seen:%Y-%m-%d %H:%M}', f'{registered:%Y-%m-%d %H:%M}', f'{float(latency) / 24:.0f}'])
        elif registered is None:
            early.append([doi, f'{seen:%Y-%m-%d %H:%M}', 'not registered yet', ''])
        else:
            lead = -float(latency)
            leads.append(lead)
            early.append([doi, f'{seen:%Y-%m-%d %H:%M}', f'{registered:%Y-%m-%d %H:%M}', f'{lead:.1f}'])
    write_table('early_detection', ['Volume DOI', 'Seen by Springer API (UTC)',
                                    'Registered in CrossRef (UTC)', 'API earlier by (h)'],
                early, 'Volumes found by the Springer API within 14 days of CrossRef registering them')
    if leads:
        leads.sort()
        write_table('early_detection_summary', ['Volumes', 'API earlier', 'Median (h)', 'Min (h)', 'Max (h)'],
                    [[len(leads), sum(x > 0 for x in leads), f'{leads[len(leads) // 2]:.1f}',
                      f'{leads[0]:.1f}', f'{leads[-1]:.1f}']],
                    'Lead time of the Springer API over CrossRef (daily arrivals only)')
    write_table('springer_api_late_finds', ['Volume DOI', 'Seen by Springer API (UTC)',
                                            'Registered in CrossRef (UTC)', 'Days after registration'],
                late, 'Volumes registered long before the system started that the Springer API reported late')
    session.close()


if __name__ == '__main__':
    main()
