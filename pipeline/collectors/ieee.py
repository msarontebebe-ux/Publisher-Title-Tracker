"""
IEEE Xplore collector (Metadata API) — conferences and journals.

The API returns one record per PAPER, but this tracker follows TITLES, i.e. one
record per proceedings volume (or per journal issue). So papers are grouped:

  * conference papers  -> grouped by `publication_number`; the proceedings DOI is
    the paper DOI minus its last segment (10.1109/ICMEW69720.2026.11720727 ->
    10.1109/icmew69720.2026), which is exactly the DOI CrossRef registers for the
    volume, so IEEE titles are not duplicated across sources;
  * journal papers     -> grouped by (`publication_number`, `is_number`) = one issue.
    CrossRef holds no DOI for IEEE issues, so the identifier is 'ieee:issue:<pn>:<is>'.

Books stay on CrossRef (book / edited-book / monograph): the API returns book
chapters whose DOIs do not map to a single book DOI.

Calls are budgeted by DailyQuota (free keys: 200/day). The key is only ever sent
as a request parameter and is scrubbed from every message we print.
"""

import re
import time
from collections import Counter, OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from pipeline.api_quota import DailyQuota
from pipeline.collectors.base import Collector
from pipeline.models import ScrapedPublication

BASE_URL = 'https://ieeexploreapi.ieee.org/api/v1/search/articles'
PAGE_SIZE = 200            # API maximum per call
BOOK_TYPES = ('book', 'edited-book', 'monograph')

_MONTHS = {m: i for i, m in enumerate(
    ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'], 1)}
_D, _M, _Y = r'(\d{1,2})', r'([A-Za-z]+)\.?', r'(\d{4})'
_DASH = r'\s*[-\u2013\u2014]\s*'


def _month(name: str) -> Optional[int]:
    return _MONTHS.get(name.strip().lower()[:3])


def parse_conference_dates(text: Optional[str]) -> Optional[Tuple[str, str]]:
    """
    Parse IEEE's free-text dates into (start_iso, end_iso).

    Handles '5-9 July 2026', '2-4 Sept. 2026', '28 Aug.-2 Sept. 2026',
    '28 Dec. 2026-1 Jan. 2027' and a single day '1 Sept. 2026'.
    Returns None when the text is not understood (callers then keep only the year).
    """
    if not text:
        return None
    t = text.strip()
    patterns = [
        (rf'^{_D}\s+{_M}\s+{_Y}{_DASH}{_D}\s+{_M}\s+{_Y}$', 'dmy_dmy'),
        (rf'^{_D}\s+{_M}{_DASH}{_D}\s+{_M}\s+{_Y}$', 'dm_dmy'),
        (rf'^{_D}{_DASH}{_D}\s+{_M}\s+{_Y}$', 'd_dmy'),
        (rf'^{_D}\s+{_M}\s+{_Y}$', 'dmy'),
    ]
    for pattern, kind in patterns:
        m = re.match(pattern, t)
        if not m:
            continue
        g = m.groups()
        try:
            if kind == 'dmy_dmy':
                s = (int(g[2]), _month(g[1]), int(g[0])); e = (int(g[5]), _month(g[4]), int(g[3]))
            elif kind == 'dm_dmy':
                s = (int(g[4]), _month(g[1]), int(g[0])); e = (int(g[4]), _month(g[3]), int(g[2]))
            elif kind == 'd_dmy':
                s = (int(g[3]), _month(g[2]), int(g[0])); e = (int(g[3]), _month(g[2]), int(g[1]))
            else:
                s = e = (int(g[2]), _month(g[1]), int(g[0]))
            if None in (s[1], e[1]):
                return None
            return (f'{s[0]:04d}-{s[1]:02d}-{s[2]:02d}', f'{e[0]:04d}-{e[1]:02d}-{e[2]:02d}')
        except (ValueError, TypeError):
            return None
    return None


def proceedings_doi(paper_dois: List[str]) -> Optional[str]:
    """Volume DOI = paper DOI minus its last segment, lower-cased (CrossRef style)."""
    stems = Counter(d.rpartition('.')[0].lower() for d in paper_dois if d and '.' in d)
    return stems.most_common(1)[0][0] if stems else None


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class IeeeXploreCollector(Collector):
    """Stream config keys: ieee_daily_limit (200), ieee_reserve (20), types."""

    def __init__(self, stream_config: Dict[str, Any], scraper, api_key: str,
                 quota: Optional[DailyQuota] = None, session=None, delay: float = 0.3):
        super().__init__(stream_config)
        self.scraper = scraper              # CrossRefScraper, used for books only
        self._key = api_key
        self.session = session or requests.Session()
        self.delay = delay
        self.quota = quota or DailyQuota(
            'ieee-xplore',
            limit=int(stream_config.get('ieee_daily_limit', 200)),
            reserve=int(stream_config.get('ieee_reserve', 20)))
        self.calls_made = 0

    # -- helpers ---------------------------------------------------------
    def _scrub(self, text: str) -> str:
        return str(text).replace(self._key, '<KEY>') if self._key else str(text)

    def _get_page(self, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """One budgeted API call. Returns parsed JSON, or None (fetch_ok -> False)."""
        if not self.quota.consume():
            print(f"  IEEE: daily call budget used up ({self.quota.budget}) - stopping")
            self.fetch_ok = False
            return None
        self.calls_made += 1
        query = dict(params, apikey=self._key, format='json')
        try:
            resp = self.session.get(BASE_URL, params=query, timeout=40)
            if resp.status_code != 200:
                print(f"  IEEE: HTTP {resp.status_code} - stopping")
                self.fetch_ok = False
                return None
            return resp.json()
        except Exception as exc:          # message may contain the URL incl. apikey
            print(f"  IEEE: request failed ({type(exc).__name__}): {self._scrub(exc)[:150]}")
            self.fetch_ok = False
            return None

    def _fetch_papers(self, content_type: str, start: str, end: str) -> List[Dict[str, Any]]:
        papers: List[Dict[str, Any]] = []
        record = 1
        while True:
            data = self._get_page({'content_type': content_type, 'start_date': start,
                                   'end_date': end, 'max_records': PAGE_SIZE,
                                   'start_record': record})
            if data is None:
                break
            batch = data.get('articles') or []
            papers.extend(batch)
            total = _int(data.get('total_records')) or 0
            print(f"  IEEE {content_type}: {len(papers)}/{total} papers")
            record += len(batch)
            if not batch or len(papers) >= total:
                break
            time.sleep(self.delay)
        return papers

    # -- grouping --------------------------------------------------------
    def _proceedings(self, papers: List[Dict[str, Any]]) -> List[ScrapedPublication]:
        groups: 'OrderedDict[str, List[Dict[str, Any]]]' = OrderedDict()
        for p in papers:
            if p.get('publication_number') and p.get('doi'):
                groups.setdefault(str(p['publication_number']), []).append(p)
        out = []
        for pn, items in groups.items():
            doi = proceedings_doi([i['doi'] for i in items])
            if not doi:
                continue
            first = items[0]
            name = first.get('publication_title') or doi
            dates = parse_conference_dates(first.get('conference_dates'))
            year = _int(first.get('publication_year'))
            start = dates[0] if dates else None
            out.append(ScrapedPublication(
                title=name, authors=[], year=year, venue=name, volume=None, pages=None,
                doi=doi, ee=f'https://ieeexplore.ieee.org/xpl/conhome/{pn}/proceeding',
                isbn=(first.get('isbn') or '').replace('-', '') or None,
                publisher='IEEE', work_type='proceedings',
                event_date_start=start, event_date_end=dates[1] if dates else None,
                event_year=int(start[:4]) if start else year,
                event_month=int(start[5:7]) if start else None,
                event_date_confidence='high' if dates else ('low' if year else None),
                event_name=name, event_location=first.get('conference_location')))
        return out

    def _journal_issues(self, papers: List[Dict[str, Any]]) -> List[ScrapedPublication]:
        groups: 'OrderedDict[Tuple[str, str], Dict[str, Any]]' = OrderedDict()
        for p in papers:
            if p.get('publication_number') and p.get('is_number'):
                groups.setdefault((str(p['publication_number']), str(p['is_number'])), p)
        out = []
        for (pn, isn), p in groups.items():
            vol, iss = p.get('volume'), p.get('issue')
            name = p.get('publication_title') or 'IEEE journal'
            title = name + (f' Vol. {vol}' if vol else '') + (f' No. {iss}' if iss else '')
            out.append(ScrapedPublication(
                title=title, authors=[], year=_int(p.get('publication_year')), venue=name,
                volume=str(vol) if vol else None, pages=None, doi=f'ieee:issue:{pn}:{isn}',
                ee=f'https://ieeexplore.ieee.org/xpl/tocresult.jsp?isnumber={isn}',
                isbn=None, publisher='IEEE', work_type='journal-issue'))
        return out

    # -- Collector API ---------------------------------------------------
    def fetch(self, from_created_date: Optional[str] = None,
              year_start: Optional[int] = None, year_end: Optional[int] = None,
              max_results: int = 1000) -> Tuple[List[ScrapedPublication], List[Dict[Any, Any]]]:
        self.fetch_ok = True
        today = datetime.now(timezone.utc).date()
        if from_created_date:
            start = from_created_date.replace('-', '')
        else:
            start = (today - timedelta(days=7)).strftime('%Y%m%d')
        end = today.strftime('%Y%m%d')
        print(f"Searching IEEE Xplore API: inserted {start}..{end} "
              f"(budget left today: {self.quota.remaining()})")

        conf = self._fetch_papers('Conferences', start, end)
        jour = self._fetch_papers('Journals', start, end)
        pubs = self._proceedings(conf) + self._journal_issues(jour)
        raw: List[Dict[Any, Any]] = list(conf) + list(jour)

        # Books: CrossRef (see module docstring).
        book_types = [t for t in (self.config.get('types') or []) if t in BOOK_TYPES]
        if book_types:
            book_cfg = dict(self.config, types=book_types)
            from pipeline.collectors.crossref import CrossRefPublisherCollector
            books = CrossRefPublisherCollector(book_cfg, self.scraper)
            b_pubs, b_raw = books.fetch(from_created_date=from_created_date,
                                        year_start=year_start, year_end=year_end,
                                        max_results=max_results)
            seen = {p.doi for p in pubs}
            pubs += [p for p in b_pubs if p.doi not in seen]
            raw += b_raw
            if not books.fetch_ok:
                self.fetch_ok = False

        print(f"  IEEE: {len(pubs)} titles from {len(conf) + len(jour)} papers, "
              f"{self.calls_made} API call(s) this run")
        if len(pubs) > max_results:
            print(f"  Truncated: {len(pubs)} titles, limit {max_results}")
            self.fetch_ok = False
        return pubs[:max_results], raw
