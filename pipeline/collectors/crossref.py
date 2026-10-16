"""CrossRef collector — tracks a publisher by DOI prefix and work types."""

from typing import Any, Dict, List, Optional, Tuple

from pipeline.collectors.base import Collector
from pipeline.models import ScrapedPublication
from pipeline.scraper import CrossRefScraper


class CrossRefPublisherCollector(Collector):
    """
    Stream config keys:
      crossref_prefixes : list of DOI prefixes owned by the publisher (OR-ed)
      types             : CrossRef work types to track (OR-ed), e.g.
                          proceedings, journal-issue, book, monograph, edited-book
    """

    def __init__(self, stream_config: Dict[str, Any], scraper: CrossRefScraper):
        super().__init__(stream_config)
        self.scraper = scraper

    def build_filter(self) -> str:
        prefixes = self.config.get('crossref_prefixes') or []
        types = self.config.get('types') or []
        if not prefixes or not types:
            raise ValueError(
                f"Stream '{self.name}' needs crossref_prefixes and types in config.json")
        parts = [f'prefix:{p}' for p in prefixes] + [f'type:{t}' for t in types]
        return ','.join(parts)

    def fetch(self, from_created_date: Optional[str] = None,
              year_start: Optional[int] = None, year_end: Optional[int] = None,
              max_results: int = 1000) -> Tuple[List[ScrapedPublication], List[Dict[Any, Any]]]:
        result = self.scraper.search_by_filter(
            self.build_filter(), year_start, year_end, max_results,
            from_created_date=from_created_date)
        self.fetch_ok = getattr(self.scraper, 'fetch_ok', True) is not False
        return result
