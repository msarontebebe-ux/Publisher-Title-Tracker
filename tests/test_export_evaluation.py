"""
The evaluation export reads from the star schema: tables, numbers, the split between early
detection and late finds, a warning when the model is stale, and valid LaTeX escaping.
"""

import csv
import importlib.util
import sys
from pathlib import Path

import pytest

from pipeline.runner import _write_events, _write_silver
from tests.test_star_schema import LNCS, pub, refresh, rows, seeded, stamp   # noqa: F401  (fixtures)

SCRIPT = Path(__file__).resolve().parent.parent / 'evaluation' / 'export_evaluation.py'


def run_export(tmp_path, monkeypatch, *argv):
    spec = importlib.util.spec_from_file_location('export_evaluation', SCRIPT)
    ex = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ex)
    ex.OUT = tmp_path
    monkeypatch.setattr(sys, 'argv', ['export_evaluation.py', *argv])
    ex.main()
    return {p.stem: list(csv.reader(open(p, encoding='utf-8'))) for p in tmp_path.glob('*.csv')}


@pytest.fixture()
def exported(seeded, tmp_path, monkeypatch):
    """The star-schema seed plus two Springer API volumes: one early, one found long after registration."""
    early = pub('10.1007/api-early', LNCS, 'Springer Nature Switzerland', 'proceedings',
                created=stamp(-3), source='springer-api')              # registered 3 h AFTER we saw it
    late = pub('10.1007/api-late', LNCS, 'Springer Nature Switzerland', 'proceedings',
               created=stamp(60 * 24), source='springer-api')          # registered 60 days earlier
    unregistered = pub('10.1007/api-new', LNCS, 'Springer Nature Switzerland', 'proceedings',
                       created=None, source='springer-api')
    _write_silver([early, late, unregistered], LNCS)
    for r in (early, late, unregistered):
        _write_events([r], LNCS)
    refresh()
    return run_export(tmp_path, monkeypatch)


def test_every_table_is_written(exported, tmp_path):
    expected = {'events_by_source_and_type', 'new_titles_by_publisher', 'new_titles_by_stream',
                'new_titles_per_day', 'new_titles_by_type', 'updates_by_cause', 'pipeline_runs',
                'data_quality', 'coverage_of_new_titles', 'top_countries', 'api_calls_per_day',
                'detection_latency', 'early_detection', 'early_detection_summary',
                'springer_api_late_finds'}
    assert expected <= set(exported)
    assert all((tmp_path / f'{name}.tex').exists() for name in expected)


def test_new_titles_by_publisher_split_by_how_they_were_found(exported):
    got = {r[0]: [int(x) for x in r[1:]] for r in exported['new_titles_by_publisher'][1:]}
    # columns: new titles, daily arrivals, backlog, registration unknown
    assert got['Springer'] == [5, 3, 1, 1]       # daily, upd, api-early | api-late backlog | api-new unknown
    assert got['IEEE'] == [1, 0, 1, 0]           # registered 60 days before it was seen
    assert got['Wiley'] == [1, 0, 0, 1] and got['Elsevier'] == [1, 0, 0, 1] and got['ACM'] == [2, 2, 0, 0]


def test_totals_agree_with_the_event_log(exported, seeded):
    total_new = sum(int(r[1]) for r in exported['new_titles_by_publisher'][1:])
    assert total_new == rows(seeded, "SELECT COUNT(*) FROM events WHERE event_type = 'title.added'")[0][0]
    assert sum(int(r[2]) for r in exported['events_by_source_and_type'][1:]) == \
        rows(seeded, 'SELECT COUNT(*) FROM events')[0][0]


def test_latency_uses_daily_arrivals_with_a_known_registration_only(exported):
    got = {r[0]: r for r in exported['detection_latency'][1:]}
    assert 'Wiley' not in got and 'Elsevier' not in got        # registration unknown
    assert 'IEEE' not in got                                    # a backlog title, not a daily arrival
    assert got['Springer'][1] == '3'                            # daily, upd, api-early (negative latency counts)
    assert float(got['ACM'][2]) == pytest.approx(2.0, abs=0.2)


def test_early_detection_and_late_finds_are_kept_apart(exported):
    early = {r[0]: r for r in exported['early_detection'][1:]}
    assert set(early) == {'10.1007/api-early', '10.1007/api-new'}
    assert float(early['10.1007/api-early'][3]) == pytest.approx(3.0, abs=0.2)     # API 3 h ahead
    assert early['10.1007/api-new'][2] == 'not registered yet'
    summary = exported['early_detection_summary'][1]
    assert summary[:2] == ['1', '1']                                                # only the one with a lead
    late = exported['springer_api_late_finds'][1:]
    assert [r[0] for r in late] == ['10.1007/api-late'] and late[0][3] == '60'


def test_runs_table_and_coverage_from_the_model(exported):
    runs = {r[0]: r for r in exported['pipeline_runs'][1:]}
    assert runs['incremental'][1:4] == ['2', '1', '1'] and runs['incremental'][4] == '50.0'
    cov = {r[0]: r for r in exported['coverage_of_new_titles'][1:]}
    assert cov['LNCS'][1] == '5' and cov['LNCS'][2] == '20.0'              # 1 of the 5 new LNCS titles has a conference date
    assert cov['LNCS'][4] == '20.0'                                         # ... and a location
    assert exported['top_countries'][1][0] in ('Germany', 'India', 'Japan')        # known locations only


def test_since_filters_by_detection_date(seeded, tmp_path, monkeypatch):
    out = run_export(tmp_path, monkeypatch, '--since', '2999-01-01')
    assert out['new_titles_by_publisher'] == [['Publisher', 'New titles', 'Daily arrivals',
                                               'Backlog / first load', 'Registration unknown']]


def test_a_stale_star_schema_is_reported(seeded, tmp_path, monkeypatch, capsys):
    from sqlalchemy import create_engine, text
    engine = create_engine(seeded)
    with engine.begin() as conn:
        conn.execute(text('TRUNCATE gold.fact_title_event'))
    engine.dispose()
    run_export(tmp_path, monkeypatch)
    assert 'WARNING: the star schema is out of date' in capsys.readouterr().out


def test_latex_tables_escape_special_characters(exported, tmp_path):
    tex = (tmp_path / 'coverage_of_new_titles.tex').read_text(encoding='utf-8')
    assert r'With conference date \%' in tex and r'\begin{tabular}{lrrrr}' in tex
    assert r'\label{tab:coverage_of_new_titles}' in tex
