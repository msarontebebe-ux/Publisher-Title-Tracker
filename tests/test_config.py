"""
Tests for PipelineConfig — guards against the config-key bug (Phase 0 bug #2).

Before the fix, email/delay were read from the wrong keys, silently disabling
CrossRef's polite pool. These tests make that class of mistake impossible
to reintroduce unnoticed.
"""

import pytest

from orchestrator import PipelineConfig


@pytest.fixture(scope='module')
def config():
    return PipelineConfig('config.json')


def test_email_read_from_crossref_api_section(config):
    # The value must come from crossref_api.email, not a top-level 'email' key
    assert config.email == 'saripari61@gmail.com'
    assert 'example.com' not in config.email   # the old fallback placeholder


def test_delay_read_from_crossref_api_section(config):
    # config.json stores delay at crossref_api.delay (was: bogus 'delay_seconds')
    assert config.delay == pytest.approx(1.0)


def test_all_eight_series_load(config):
    assert len(config.series_list) == 8
    assert 'LNCS' in config.series_list
    assert 'AISC' in config.series_list


def test_series_lookup_case_insensitive(config):
    info = config.get_series_info('lncs')
    assert info is not None
    assert info['name'] == 'Lecture Notes in Computer Science'


def test_unknown_series_returns_none(config):
    assert config.get_series_info('NOPE') is None
