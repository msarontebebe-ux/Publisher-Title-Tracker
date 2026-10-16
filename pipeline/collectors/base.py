"""Abstract collector interface — one collector per data source.

A collector knows HOW to fetch new/changed titles for one tracked stream
(a Springer series, or a publisher such as Elsevier/Wiley/IEEE/ACM). Everything
after fetching (Bronze/Silver/Events/Gold, watermark, run log) is shared and
lives in pipeline.runner, so adding a source never touches the ETL code.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from pipeline.models import ScrapedPublication


class Collector(ABC):
    """Fetches titles for one stream and returns (publications, raw_items)."""

    def __init__(self, stream_config: Dict[str, Any]):
        self.config = stream_config
        #: False when the last fetch was incomplete (error, quota or cap reached);
        #: the runner then logs 'partial' and does NOT advance the watermark
        self.fetch_ok = True

    @property
    def name(self) -> str:
        return self.config['name']

    @abstractmethod
    def fetch(self, from_created_date: Optional[str] = None,
              year_start: Optional[int] = None, year_end: Optional[int] = None,
              max_results: int = 1000) -> Tuple[List[ScrapedPublication], List[Dict[Any, Any]]]:
        """Return (cleaned publications, raw source items).

        from_created_date (YYYY-MM-DD) selects the delta since the watermark and
        takes precedence over the year window.
        """
