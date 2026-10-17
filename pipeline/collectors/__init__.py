"""Collectors — one per data source (see base.Collector)."""
import os

from pipeline.collectors.base import Collector
from pipeline.collectors.crossref import CrossRefPublisherCollector

__all__ = ['Collector', 'CrossRefPublisherCollector', 'build_collector',
           'build_springer_api_collector']


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


def build_springer_api_collector(config):
    """
    SpringerApiCollector for the tracked series, or None when it is switched off
    (`springer_api.enabled`) or SPRINGER_API_KEY is not set - the Springer series
    then run on CrossRef alone, exactly as before.

    LNAI and LNBI (`scrape_via_issn: false`) are left out: CrossRef files their
    volumes under LNCS, and the CrossRef run keeps handling them that way.
    """
    api_cfg = config.get('springer_api') or {}
    if not api_cfg.get('enabled'):
        return None
    from pipeline.config import ensure_dotenv
    ensure_dotenv()
    key = os.getenv('SPRINGER_API_KEY')
    if not key:
        return None
    from pipeline.collectors.springer_api import SpringerApiCollector
    series_by_id = {s['springer_series_id']: s['name']
                    for s in config.get('springer_series', [])
                    if s.get('springer_series_id') and s.get('scrape_via_issn', True)}
    return SpringerApiCollector(dict(api_cfg, name='Springer API', series_by_id=series_by_id),
                                api_key=key)
