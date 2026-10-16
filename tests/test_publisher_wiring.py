"""Wiring tests for publisher streams — config shape and orchestrator stage."""

from unittest.mock import patch

import pytest

from orchestrator import PipelineConfig, PublisherScrapeStage


@pytest.fixture(scope='module')
def config():
    return PipelineConfig('config.json')


def test_four_publishers_configured(config):
    assert config.publisher_list == ['ELSEVIER', 'WILEY', 'IEEE', 'ACM']


def test_every_publisher_has_prefix_and_types(config):
    for abbr in config.publisher_list:
        info = config.get_publisher_info(abbr)
        assert info['crossref_prefixes'], abbr
        assert info['types'], abbr


def test_publisher_lookup_case_insensitive(config):
    assert config.get_publisher_info('wiley')['name'] == 'Wiley'
    assert config.get_publisher_info('NOPE') is None


def test_publisher_names_do_not_collide_with_series(config):
    # watermark / pipeline_runs key on the stream *name*; a clash would share state
    series_names = {s['name'] for s in config.config['springer_series']}
    assert not series_names & {config.get_publisher_info(a)['name']
                               for a in config.publisher_list}


def test_stage_calls_scrape_publisher_with_config():
    with patch('pipeline.runner.scrape_publisher') as mock_scrape:
        mock_scrape.return_value = ['a', 'b']
        stage = PublisherScrapeStage()
        ok = stage.execute(publisher='wiley', max_results=50,
                           email='t@example.com', delay=0, run_type='incremental')
        assert ok is True
        kwargs = mock_scrape.call_args.kwargs
        assert kwargs['pub_config']['abbreviation'] == 'WILEY'
        assert kwargs['run_type'] == 'incremental'
        assert stage.metrics['records_fetched'] == 2


def test_unknown_publisher_fails_the_stage():
    stage = PublisherScrapeStage()
    assert stage.execute(publisher='NOPE') is False
