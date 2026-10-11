"""
Tests for orchestrator wiring — guards against the import bug (Phase 0 bug #1).

Before the fix, ScrapeStage imported a non-existent `scrape_series` function,
so every scheduled run died at the scrape stage. These tests run the real
ScrapeStage code with a faked scraper: zero network, zero database.
"""

from unittest.mock import patch, MagicMock

import pytest

from orchestrator import ScrapeStage, PipelineConfig, PipelineOrchestrator


class TestScrapeStageWiring:

    def test_calls_real_function_with_correct_arguments(self):
        """The stage must invoke main_crossref.scrape_single_series."""
        with patch('main_crossref.scrape_single_series') as mock_scrape:
            mock_scrape.return_value = ['pub1', 'pub2']
            stage = ScrapeStage()
            ok = stage.execute(
                series='LNCS', year_start=2026, year_end=2026,
                max_results=100, email='t@example.com', delay=1.0,
                run_type='incremental',
            )
            assert ok is True
            mock_scrape.assert_called_once()
            kwargs = mock_scrape.call_args.kwargs
            assert kwargs['series_config']['abbreviation'] == 'LNCS'
            assert kwargs['year_start'] == 2026
            assert kwargs['max_results'] == 100
            assert kwargs['run_type'] == 'incremental'

    def test_metrics_recorded(self):
        with patch('main_crossref.scrape_single_series') as mock_scrape:
            mock_scrape.return_value = ['a', 'b', 'c']
            stage = ScrapeStage()
            stage.execute(series='CCIS', year_start=2026, max_results=10,
                          email='t@example.com', delay=1.0)
            assert stage.metrics['records_fetched'] == 3

    def test_unknown_series_fails_the_stage(self):
        with patch('main_crossref.scrape_single_series'):
            stage = ScrapeStage()
            ok = stage.execute(series='NOTASERIES', year_start=2026,
                               max_results=10, email='t@example.com', delay=1.0)
            assert ok is False
            assert stage.status == 'failed'

    def test_scraper_built_with_config_email(self):
        """The polite-pool email must reach the scraper client (bug #2)."""
        with patch('main_crossref.scrape_single_series'), \
             patch('crossref_scraper.CrossRefScraper') as mock_client:
            mock_client.return_value = MagicMock()
            stage = ScrapeStage()
            stage.execute(series='LNCS', year_start=2026, max_results=10,
                          email='polite@example.com', delay=2.5)
            assert mock_client.call_args.kwargs['email'] == 'polite@example.com'
            assert mock_client.call_args.kwargs['delay'] == 2.5


class TestSeriesNameResolution:

    def test_gold_stage_receives_full_series_name(self):
        """Bug #3: gold/quality filter on the FULL name stored in the DB."""
        config = PipelineConfig('config.json')
        orch = PipelineOrchestrator(config)

        with patch('orchestrator.GoldLayerStage') as mock_gold_cls, \
             patch('orchestrator.QualityCheckStage') as mock_quality_cls, \
             patch('orchestrator.ScrapeStage'):
            mock_gold_cls.return_value.execute.return_value = True
            mock_quality_cls.return_value.execute.return_value = True

            orch.run_pipeline(['LNCS'], year_start=2026, skip_scrape=True)

            gold_series = mock_gold_cls.return_value.execute.call_args.kwargs['series']
            quality_series = mock_quality_cls.return_value.execute.call_args.kwargs['series']
            assert gold_series == 'Lecture Notes in Computer Science'
            assert quality_series == 'Lecture Notes in Computer Science'
