"""
Guards the scheduled-pipeline CI file against drifting from config.json and
from the orchestrator CLI. A publisher that is configured but missing from the
matrix would silently never be collected on schedule.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope='module')
def ci():
    return yaml.safe_load((ROOT / '.gitlab-ci.yml').read_text(encoding='utf-8'))


@pytest.fixture(scope='module')
def config():
    return json.loads((ROOT / 'config.json').read_text(encoding='utf-8'))


def test_matrix_matches_configured_publishers(ci, config):
    matrix = ci['publisher']['parallel']['matrix'][0]['PUBLISHER']
    configured = [p['abbreviation'] for p in config['publishers']]
    assert sorted(matrix) == sorted(configured)


def test_pipeline_jobs_only_run_on_schedules(ci):
    for job in ('init-database', 'springer', 'publisher'):
        rules = ci[job]['rules']
        assert all('schedule' in r['if'] for r in rules), job


def test_pipeline_jobs_are_serialised_on_the_database(ci):
    assert ci['springer']['resource_group'] == ci['publisher']['resource_group']


def test_init_runs_before_the_pipeline_jobs(ci):
    stages = ci['stages']
    assert stages.index(ci['init-database']['stage']) < stages.index(ci['springer']['stage'])
    assert stages.index(ci['init-database']['stage']) < stages.index(ci['publisher']['stage'])


def test_jobs_are_incremental_with_a_result_cap(ci):
    for job in ('springer', 'publisher'):
        cmd = ' '.join(ci[job]['script'])
        assert '--incremental' in cmd and '--max-results' in cmd, job


def test_springer_job_excludes_publishers(ci):
    assert '--no-publishers' in ' '.join(ci['springer']['script'])


@pytest.mark.parametrize('job', ['springer', 'publisher'])
def test_job_commands_are_accepted_by_the_orchestrator(ci, job):
    """Run the exact CI command with --dry-run: catches renamed/removed flags."""
    cmd = ci[job]['script'][0].replace('$PUBLISHER', 'WILEY').replace('"', '')
    args = cmd.split()[2:]          # drop: python orchestrator.py
    result = subprocess.run(
        [sys.executable, 'orchestrator.py', *args, '--dry-run'],
        cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'Configuration is valid' in result.stdout


def test_schedule_target_selects_exactly_one_group(ci):
    """Evaluate each job's rule for every SCHEDULE_TARGET value."""
    def runs(job, target):
        expr = ci[job]['rules'][0]['if']
        t = target
        return ('SCHEDULE_TARGET == null' in expr and t is None) or any(
            f'$SCHEDULE_TARGET == "{v}"' in expr and t == v
            for v in ('', 'springer', 'publishers'))

    for target, expected in [(None, {'springer', 'publisher'}), ('', {'springer', 'publisher'}),
                             ('springer', {'springer'}), ('publishers', {'publisher'})]:
        assert {j for j in ('springer', 'publisher') if runs(j, target)} == expected, target
