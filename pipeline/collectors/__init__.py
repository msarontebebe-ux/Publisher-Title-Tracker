"""Collectors — one per data source (see base.Collector)."""
import os

from pipeline.collectors.base import Collector
from pipeline.collectors.crossref import CrossRefPublisherCollector

__all__ = ['Collector', 'CrossRefPublisherCollector', 'build_collector']


def build_collector(stream_config, scraper):
    """
    Pick the collector for a stream from config.json (`source`).

      source = "ieee_api"  -> IeeeXploreCollector when IEEE_API_KEY is set,
                              otherwise the CrossRef collector (so a missing key
                              never breaks the scheduled run)
      anything else        -> CrossRefPublisherCollector
    """
    if stream_config.get('source') == 'ieee_api':
        from pipeline.config import ensure_dotenv
        ensure_dotenv()
        key = os.getenv('IEEE_API_KEY')
        if key:
            from pipeline.collectors.ieee import IeeeXploreCollector
            return IeeeXploreCollector(stream_config, scraper, api_key=key)
        print("  IEEE_API_KEY not set - falling back to CrossRef for this stream")
    return CrossRefPublisherCollector(stream_config, scraper)
