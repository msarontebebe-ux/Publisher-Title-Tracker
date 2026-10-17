"""
Pipeline orchestrator for Springer publication data pipeline.

Runs the complete ETL workflow:
1. Scrape data from CrossRef API (Bronze layer)
2. Clean and structure data (Silver layer)
3. Compute aggregations (Gold layer)
4. Run quality checks

Usage:
    python orchestrator.py                    # Run all series
    python orchestrator.py --series LNCS      # Run specific series
    python orchestrator.py --publisher WILEY  # Run a publisher stream (CrossRef prefix + types)
    python orchestrator.py --no-publishers    # All Springer series, no publishers
    python orchestrator.py --dry-run          # Validate without executing
    python orchestrator.py --incremental      # Incremental mode (current year only)
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any
import time

from pipeline.config import load_config, find_series_by_abbr

# Configure logging
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create formatters
console_formatter = logging.Formatter(
    '%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
file_formatter = logging.Formatter(
    '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Setup root logger
logger = logging.getLogger('orchestrator')
logger.setLevel(logging.INFO)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# File handler with timestamp
log_file = LOG_DIR / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)


class PipelineConfig:
    """Pipeline configuration and constants."""
    
    def __init__(self, config_path: str = "config.json"):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
    
    @property
    def series_list(self) -> List[str]:
        """Get list of all series abbreviations."""
        return [s['abbreviation'] for s in self.config.get('springer_series', [])]
    
    @property
    def publisher_list(self) -> List[str]:
        """Get list of all tracked publisher abbreviations (config.json `publishers`)."""
        return [p['abbreviation'] for p in self.config.get('publishers', [])]

    def get_publisher_info(self, abbreviation: str) -> Optional[Dict[str, Any]]:
        """Get full publisher information (case-insensitive)."""
        for p in self.config.get('publishers', []):
            if p['abbreviation'].upper() == abbreviation.upper():
                return p
        return None

    @property
    def email(self) -> str:
        """Get email for API polite pool (lives under crossref_api in config.json)."""
        return self.config.get('crossref_api', {}).get('email', 'research@example.com')
    
    @property
    def delay(self) -> float:
        """Get delay between API requests (lives under crossref_api in config.json)."""
        return self.config.get('crossref_api', {}).get('delay', 1.0)
    
    def get_series_info(self, abbreviation: str) -> Optional[Dict[str, Any]]:
        """Get full series information."""
        return find_series_by_abbr(abbreviation, self.config)


class PipelineStage:
    """Base class for pipeline stages."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f'orchestrator.{name}')
        self.start_time = None
        self.end_time = None
        self.status = "pending"
        self.error = None
        self.metrics = {}
    
    def execute(self, **kwargs) -> bool:
        """Execute the pipeline stage. Returns True on success."""
        self.start_time = datetime.now(timezone.utc)
        self.logger.info(f"Starting stage: {self.name}")
        
        try:
            self._run(**kwargs)
            self.status = "success"
            self.end_time = datetime.now(timezone.utc)
            duration = (self.end_time - self.start_time).total_seconds()
            self.logger.info(f"✓ Stage completed in {duration:.1f}s")
            return True
            
        except Exception as exc:
            self.status = "failed"
            self.error = str(exc)
            self.end_time = datetime.now(timezone.utc)
            self.logger.error(f"✗ Stage failed: {exc}", exc_info=True)
            return False
    
    def _run(self, **kwargs):
        """Override in subclasses."""
        raise NotImplementedError
    
    def get_summary(self) -> Dict[str, Any]:
        """Get stage execution summary."""
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            'name': self.name,
            'status': self.status,
            'duration_seconds': duration,
            'error': self.error,
            'metrics': self.metrics,
        }


class ScrapeStage(PipelineStage):
    """Scrape data from CrossRef API."""
    
    def __init__(self):
        super().__init__("scrape")
    
    def _run(self, series: str, year_start: int, year_end: Optional[int] = None,
             max_results: int = 10000, email: str = None, delay: float = 1.0,
             run_type: str = 'full'):
        """Run the scraping process."""
        from pipeline.runner import scrape_single_series
        from pipeline.scraper import CrossRefScraper
        from pipeline.storage import DataLakeStorageHandler

        config = load_config()
        series_info = find_series_by_abbr(series, config)

        if not series_info:
            raise ValueError(f"Series {series} not found in config.json")
        
        self.logger.info(f"Scraping {series} ({series_info['name']})")
        self.logger.info(f"Year range: {year_start}-{year_end or 'present'}")
        
        # scrape_single_series expects a scraper client and a storage handler;
        # it performs the Bronze/Silver/Gold writes and run logging itself.
        scraper = CrossRefScraper(
            email=email or config.get('crossref_api', {}).get('email', 'research@example.com'),
            delay=delay
        )
        storage = DataLakeStorageHandler(
            base_dir=config.get('output', {}).get('directory', 'data')
        )
        
        results = scrape_single_series(
            series_config=series_info,
            scraper=scraper,
            storage=storage,
            year_start=year_start,
            year_end=year_end,
            max_results=max_results,
            run_type=run_type
        )
        
        self.metrics['records_fetched'] = len(results) if results else 0
        self.logger.info(f"Fetched {self.metrics['records_fetched']} records")


class PublisherScrapeStage(PipelineStage):
    """Scrape one publisher (DOI prefix + work types) from CrossRef."""

    def __init__(self):
        super().__init__("scrape_publisher")

    def _run(self, publisher: str, year_start: Optional[int] = None,
             year_end: Optional[int] = None, max_results: int = 10000,
             email: str = None, delay: float = 1.0, run_type: str = 'full'):
        from pipeline.runner import scrape_publisher
        from pipeline.scraper import CrossRefScraper
        from pipeline.storage import DataLakeStorageHandler

        config = load_config()
        info = next((p for p in config.get('publishers', [])
                     if p['abbreviation'].upper() == publisher.upper()), None)
        if not info:
            raise ValueError(f"Publisher {publisher} not found in config.json")

        self.logger.info(f"Scraping publisher {publisher} ({info['name']})")
        scraper = CrossRefScraper(
            email=email or config.get('crossref_api', {}).get('email', 'research@example.com'),
            delay=delay)
        storage = DataLakeStorageHandler(
            base_dir=config.get('output', {}).get('directory', 'data'))
        results = scrape_publisher(
            pub_config=info, scraper=scraper, storage=storage,
            year_start=year_start, year_end=year_end,
            max_results=max_results, run_type=run_type)
        self.metrics['records_fetched'] = len(results) if results else 0
        self.logger.info(f"Fetched {self.metrics['records_fetched']} records")


class QualityCheckStage(PipelineStage):
    """Run data quality checks."""
    
    def __init__(self):
        super().__init__("quality_check")
    
    def _run(self, series: Optional[str] = None):
        """Run quality checks and update quality_metrics table."""
        from pipeline.gold import compute_quality_metrics
        from pipeline.db import DatabaseConfig
        
        if not DatabaseConfig.is_cloud_configured():
            self.logger.info("No cloud DB configured — skipping quality check (file-only mode)")
            return
        
        session = DatabaseConfig.get_session()
        try:
            self.logger.info(f"Computing quality metrics for {series or 'all series'}")
            compute_quality_metrics(session, series_filter=series)
            session.commit()
            
            # Fetch metrics — bound parameters, never string interpolation
            from sqlalchemy import text
            if series:
                result = session.execute(
                    text("SELECT * FROM quality_metrics "
                         "WHERE series = :series "
                         "ORDER BY validated_at DESC LIMIT 1"),
                    {'series': series}
                ).fetchone()
            else:
                result = session.execute(
                    text("SELECT * FROM quality_metrics "
                         "ORDER BY validated_at DESC LIMIT 1")
                ).fetchone()
            
            if result:
                self.metrics['doi_coverage'] = float(result.doi_coverage_pct or 0)
                self.metrics['event_date_coverage'] = float(result.event_date_coverage_pct or 0)
                self.logger.info(f"Quality: DOI {self.metrics['doi_coverage']:.1f}%, "
                               f"Event dates {self.metrics['event_date_coverage']:.1f}%")
        finally:
            session.close()


class GoldLayerStage(PipelineStage):
    """Compute gold layer aggregations."""
    
    def __init__(self):
        super().__init__("gold_layer")
    
    def _run(self, series: Optional[str] = None):
        """Refresh gold layer tables."""
        from pipeline.gold import refresh_gold
        from pipeline.db import DatabaseConfig
        
        if not DatabaseConfig.is_cloud_configured():
            self.logger.info("No cloud DB configured — skipping gold refresh (file-only mode)")
            return
        
        self.logger.info(f"Refreshing gold layer for {series or 'all series'}")
        refresh_gold(series_filter=series)
        self.logger.info("Gold layer updated")


class PipelineOrchestrator:
    """Main orchestrator for the ETL pipeline."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = logger
        self.stages: List[PipelineStage] = []
        self.pipeline_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    def run_pipeline(self, series_list: List[str], year_start: int, 
                    year_end: Optional[int] = None, max_results: int = 10000,
                    skip_scrape: bool = False, run_type: str = 'full',
                    publisher_list: Optional[List[str]] = None) -> bool:
        """
        Run the complete pipeline for specified series.
        
        Args:
            run_type: 'full' or 'incremental' — recorded in pipeline_runs
                      so the table reflects how each run was invoked.
        
        Returns True if all stages succeeded.
        """
        self.logger.info("=" * 70)
        self.logger.info(f"Pipeline Execution Started | ID: {self.pipeline_id}")
        self.logger.info(f"Series: {', '.join(series_list)}")
        self.logger.info(f"Year range: {year_start}-{year_end or 'present'}")
        self.logger.info(f"Run type: {run_type}")
        self.logger.info("=" * 70)
        
        overall_success = True
        pipeline_start = datetime.now(timezone.utc)
        
        for series in series_list:
            self.logger.info(f"\n{'='*70}")
            self.logger.info(f"Processing series: {series}")
            self.logger.info(f"{'='*70}")
            
            # Gold/quality tables store the FULL series name (e.g. 'Lecture Notes
            # in Computer Science'), not the abbreviation — resolve it once here
            # so those stages filter on values that actually exist in the DB.
            series_info = self.config.get_series_info(series)
            series_full_name = series_info['name'] if series_info else series
            
            # Stage 1: Scrape
            if not skip_scrape:
                scrape_stage = ScrapeStage()
                if not scrape_stage.execute(
                    series=series,
                    year_start=year_start,
                    year_end=year_end,
                    max_results=max_results,
                    email=self.config.email,
                    delay=self.config.delay,
                    run_type=run_type
                ):
                    overall_success = False
                    self.logger.error(f"Scraping failed for {series}, skipping remaining stages")
                    continue
                self.stages.append(scrape_stage)
                
                # Wait between series to be polite to API
                if len(series_list) > 1:
                    time.sleep(2)
            
            # A scrape already refreshes gold and the quality metrics for its series
            # (pipeline.runner._persist_and_log), so these two stages only run on
            # their own, with --skip-scrape.
            if not skip_scrape:
                continue

            # Stage 2: Gold Layer
            gold_stage = GoldLayerStage()
            if not gold_stage.execute(series=series_full_name):
                overall_success = False
                self.logger.warning(f"Gold layer computation failed for {series}")
            self.stages.append(gold_stage)
            
            # Stage 3: Quality Check
            quality_stage = QualityCheckStage()
            if not quality_stage.execute(series=series_full_name):
                overall_success = False
                self.logger.warning(f"Quality check failed for {series}")
            self.stages.append(quality_stage)
        
        # Publisher streams (Elsevier, Wiley, IEEE, ACM ...) share the same
        # Bronze/Silver/Events/Gold path; gold/quality are refreshed by the
        # runner's shared write path, so only the scrape stage is needed here.
        for publisher in (publisher_list or []):
            self.logger.info(f"\n{'='*70}")
            self.logger.info(f"Processing publisher: {publisher}")
            self.logger.info(f"{'='*70}")
            if skip_scrape:
                continue
            pub_stage = PublisherScrapeStage()
            # Publishers are windowed by the watermark (incremental) or by an
            # explicit year range only when the user passed one.
            ok = pub_stage.execute(
                publisher=publisher,
                year_start=None if run_type == 'incremental' else year_start,
                year_end=None if run_type == 'incremental' else year_end,
                max_results=max_results,
                email=self.config.email,
                delay=self.config.delay,
                run_type=run_type)
            self.stages.append(pub_stage)
            if not ok:
                overall_success = False
                self.logger.error(f"Scraping failed for publisher {publisher}")
            time.sleep(2)

        # Pipeline summary
        pipeline_end = datetime.now(timezone.utc)
        duration = (pipeline_end - pipeline_start).total_seconds()
        
        self.logger.info("\n" + "=" * 70)
        self.logger.info("Pipeline Execution Summary")
        self.logger.info("=" * 70)
        self.logger.info(f"Status: {'SUCCESS ✓' if overall_success else 'FAILED ✗'}")
        self.logger.info(f"Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
        self.logger.info(f"Series processed: {len(series_list)}")
        self.logger.info(f"Total stages: {len(self.stages)}")
        
        # Stage breakdown
        success_count = sum(1 for s in self.stages if s.status == "success")
        failed_count = sum(1 for s in self.stages if s.status == "failed")
        self.logger.info(f"Stages succeeded: {success_count}/{len(self.stages)}")
        if failed_count > 0:
            self.logger.info(f"Stages failed: {failed_count}")
        
        # Failed stages detail
        failed_stages = [s for s in self.stages if s.status == "failed"]
        if failed_stages:
            self.logger.info("\nFailed stages:")
            for stage in failed_stages:
                self.logger.info(f"  - {stage.name}: {stage.error}")
        
        self.logger.info(f"\nLog file: {log_file}")
        self.logger.info("=" * 70)
        
        return overall_success
    
    def get_pipeline_summary(self) -> Dict[str, Any]:
        """Get complete pipeline execution summary."""
        return {
            'pipeline_id': self.pipeline_id,
            'stages': [s.get_summary() for s in self.stages],
            'total_stages': len(self.stages),
            'successful_stages': sum(1 for s in self.stages if s.status == "success"),
            'failed_stages': sum(1 for s in self.stages if s.status == "failed"),
        }


def main():
    parser = argparse.ArgumentParser(
        description='Orchestrate Springer publication data pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all series, incremental mode (current year only)
  python orchestrator.py --incremental
  
  # Run specific series from 2020
  python orchestrator.py --series LNCS --year-start 2020
  
  # Run multiple series
  python orchestrator.py --series LNCS LNAI --year-start 2024
  
  # Dry run to validate configuration
  python orchestrator.py --dry-run
  
  # Skip scraping, only refresh gold layer and quality checks
  python orchestrator.py --skip-scrape
        """
    )
    
    parser.add_argument('--series', nargs='+', 
                       help='Series to process (e.g., LNCS LNAI). Default: all series')
    parser.add_argument('--publisher', nargs='+',
                       help='Publisher streams to process (e.g., WILEY ELSEVIER IEEE ACM). '
                            'Default: all series AND all publishers')
    parser.add_argument('--year-start', type=int, 
                       help='Start year for scraping (default: current year)')
    parser.add_argument('--year-end', type=int,
                       help='End year for scraping (default: current year)')
    parser.add_argument('--max-results', type=int, default=10000,
                       help='Maximum results per series (default: 10000)')
    parser.add_argument('--incremental', action='store_true',
                       help='Incremental mode: only scrape current year')
    parser.add_argument('--no-publishers', action='store_true',
                       help='Without --series/--publisher, run only the Springer series '
                            '(used by the CI job that handles Springer on its own)')
    parser.add_argument('--skip-scrape', action='store_true',
                       help='Skip scraping, only run gold layer and quality checks')
    parser.add_argument('--dry-run', action='store_true',
                       help='Validate configuration without executing')
    parser.add_argument('--config', default='config.json',
                       help='Path to config.json (default: config.json)')
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config = PipelineConfig(args.config)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {args.config}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in configuration file: {e}")
        sys.exit(1)
    
    # Determine publishers to process
    publisher_list = []
    if args.publisher:
        publisher_list = [p.upper() for p in args.publisher]
        invalid = [p for p in publisher_list if p not in config.publisher_list]
        if invalid:
            logger.error(f"Invalid publisher: {', '.join(invalid)}")
            logger.info(f"Available publishers: {', '.join(config.publisher_list)}")
            sys.exit(1)
    elif not args.series and not args.no_publishers:
        publisher_list = config.publisher_list   # default run covers everything

    # Determine series to process
    if args.series:
        series_list = [s.upper() for s in args.series]
        # Validate series
        invalid = [s for s in series_list if s not in config.series_list]
        if invalid:
            logger.error(f"Invalid series: {', '.join(invalid)}")
            logger.info(f"Available series: {', '.join(config.series_list)}")
            sys.exit(1)
    elif args.publisher:
        series_list = []   # only the requested publishers
    else:
        series_list = config.series_list
    
    # Determine year range
    current_year = datetime.now().year
    if args.incremental:
        year_start = current_year
        year_end = current_year
    else:
        year_start = args.year_start or current_year
        year_end = args.year_end or current_year
    
    # Dry run mode
    if args.dry_run:
        logger.info("DRY RUN MODE - No data will be modified")
        logger.info(f"Would process series: {', '.join(series_list)}")
        logger.info(f"Would process publishers: {', '.join(publisher_list)}")
        logger.info(f"Year range: {year_start}-{year_end}")
        logger.info(f"Max results per series: {args.max_results}")
        logger.info(f"Skip scraping: {args.skip_scrape}")
        logger.info("Configuration is valid ✓")
        sys.exit(0)
    
    # Create and run orchestrator
    orchestrator = PipelineOrchestrator(config)
    success = orchestrator.run_pipeline(
        series_list=series_list,
        year_start=year_start,
        year_end=year_end,
        max_results=args.max_results,
        skip_scrape=args.skip_scrape,
        run_type='incremental' if args.incremental else 'full',
        publisher_list=publisher_list
    )
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
