"""
Springer Nature Meta API collector (meta/v2) - early detection of new volumes.

The API returns one record per CHAPTER, never per book, and has no filter for a
book series. This tracker follows TITLES (volumes), so the collector:

  * asks for every book chapter that went online since the watermark date
    (`type:Book onlinedatefrom:<date>`, available on the Basic plan);
  * keeps the chapters of the tracked series (`seriesId`, matched against
    `springer_series_id` in config.json);
  * groups them into volumes. The volume DOI is the chapter DOI without its
    trailing `_<n>` (10.1007/978-3-032-41086-3_52 -> 10.1007/978-3-032-41086-3),
    which is the DOI CrossRef registers for the volume.

A volume is visible here as soon as its first chapter is online, usually before
CrossRef has the volume DOI. CrossRef stays the authority for title, editors and
ISBN (the API only has the short book name, no subtitle or part number); the API
adds what CrossRef lacks: the proceedings label (`genre`) and structured
conference data. See pipeline.runner.merge_api_volumes.

Calls are budgeted by DailyQuota (Basic plan: 500/day, 25 records per call). The
key is only sent as a request parameter and is scrubbed from every message.
"""

import re
import time
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from pipeline.api_quota import DailyQuota
from pipeline.collectors.base import Collector
from pipeline.models import ScrapedAuthor, ScrapedPublication

BASE_URL = 'https://api.springernature.com/meta/v2/json'
SOURCE = 'springer-api'     # label stored on events built from this API (same name as the quota provider)
_CHAPTER_DOI = re.compile(r'^(?P<book>.+)_(?P<n>\d+)$')


def volume_doi(chapter_doi: Optional[str]) -> Optional[str]:
    """Volume DOI = chapter DOI minus its trailing _<n>; None when it has no such suffix."""
    m = _CHAPTER_DOI.match(chapter_doi or '')
    return m.group('book') if m else None


def _given_family(name: str) -> str:
    """'Maisch, Bettina' -> 'Bettina Maisch' (the order CrossRef records use)."""
    if ',' in name:
        family, given = name.split(',', 1)
        return f'{given.strip()} {family.strip()}'.strip()
    return name.strip()


def _iso(parts: Any) -> Optional[str]:
    """[{'year': '2026', 'month': '10', 'day': '05'}] -> '2026-10-05' (None if incomplete/invalid)."""
    try:
        p = parts[0]
        return date(int(p['year']), int(p['month']), int(p['day'])).isoformat()
    except (TypeError, ValueError, KeyError, IndexError):
        return None


def _first(chapters: List[Dict[str, Any]], key: str) -> Any:
    """First non-empty value of `key` across the chapters (sorted, so it is deterministic)."""
    for c in chapters:
        if c.get(key):
            return c[key]
    return None


def _event_fields(chapters: List[Dict[str, Any]]) -> Dict[str, Any]:
    info = next((c['conferenceInfo'][0] for c in chapters if c.get('conferenceInfo')), None)
    if not info:
        return {}
    start, end = _iso(info.get('confEventDateStart')), _iso(info.get('confEventDateEnd'))
    abbr = info.get('confEventAbbreviation')
    name = (f'{abbr} {start[:4]}' if abbr and start else None) or info.get('confSeriesName')
    place = None
    try:
        loc = info['confEventLocation'][0]
        place = ', '.join(x for x in (loc.get('city'), loc['country'][0]['value']) if x) or None
    except (KeyError, IndexError, TypeError):
        pass
    return {
        'event_name': name, 'event_location': place,
        'event_date_start': start, 'event_date_end': end,
        'event_year': int(start[:4]) if start else None,
        'event_month': int(start[5:7]) if start else None,
        'event_date_confidence': 'high' if start else None,
    }


class SpringerApiCollector(Collector):
    """
    Stream config keys: series_by_id ({springer_series_id: series name} of the
    tracked series), daily_limit (500), reserve (25), page_size (25: Basic plan
    maximum), initial_days (7: look-back when there is no watermark).
    """

    def __init__(self, stream_config: Dict[str, Any], api_key: str,
                 quota: Optional[DailyQuota] = None, session=None, delay: float = 0.7):
        super().__init__(stream_config)
        self._key = api_key
        self.session = session or requests.Session()
        self.delay = delay
        self.page_size = int(stream_config.get('page_size', 25))
        self.series_by_id = {str(k): v for k, v in (stream_config.get('series_by_id') or {}).items()}
        self.quota = quota or DailyQuota(
            SOURCE,
            limit=int(stream_config.get('daily_limit', 500)),
            reserve=int(stream_config.get('reserve', 25)))
        self.calls_made = 0

    # -- helpers ---------------------------------------------------------
    def _scrub(self, text: Any) -> str:
        return str(text).replace(self._key, '<KEY>') if self._key else str(text)

    def _get_page(self, query: str, start: int) -> Optional[Dict[str, Any]]:
        """One budgeted call. Parsed JSON, {} for 'nothing found', None on failure (fetch_ok -> False)."""
        if not self.quota.consume():
            print(f"  Springer API: daily call budget used up ({self.quota.budget}) - stopping")
            self.fetch_ok = False
            return None
        self.calls_made += 1
        params = {'api_key': self._key, 'q': query, 's': start, 'p': self.page_size}
        try:
            resp = self.session.get(BASE_URL, params=params, timeout=40)
            if resp.status_code == 404:        # "No data was found for the given query"
                return {}
            if resp.status_code != 200:
                print(f"  Springer API: HTTP {resp.status_code} - stopping")
                self.fetch_ok = False
                return None
            return resp.json()
        except Exception as exc:               # the message may contain the URL incl. api_key
            print(f"  Springer API: request failed ({type(exc).__name__}): {self._scrub(exc)[:150]}")
            self.fetch_ok = False
            return None

    def _fetch_chapters(self, since: str, max_chapters: int) -> List[Dict[str, Any]]:
        chapters: List[Dict[str, Any]] = []
        start = 1
        query = f'type:Book onlinedatefrom:{since}'
        while True:
            data = self._get_page(query, start)
            if not data:
                break
            batch = data.get('records') or []
            chapters.extend(batch)
            try:
                total = int(data['result'][0]['total'])
            except (KeyError, IndexError, TypeError, ValueError):
                total = 0
            print(f"  Springer API: {len(chapters)}/{total} chapters")
            start += len(batch)
            if not batch or len(chapters) >= total:
                break
            if len(chapters) >= max_chapters:
                print(f"  Truncated: {total} chapters available, limit {max_chapters}")
                self.fetch_ok = False
                break
            time.sleep(self.delay)
        return chapters

    # -- grouping --------------------------------------------------------
    def volumes(self, chapters: List[Dict[str, Any]]) -> List[ScrapedPublication]:
        groups: 'OrderedDict[str, List[Dict[str, Any]]]' = OrderedDict()
        for c in sorted(chapters, key=lambda c: c.get('doi') or ''):
            series = self.series_by_id.get(str(c.get('seriesId') or ''))
            book = volume_doi(c.get('doi'))
            if series and book:
                groups.setdefault(book, []).append(c)
        out = []
        for book, chs in groups.items():
            names = [_given_family(e['bookEditor']) for e in (_first(chs, 'bookEditors') or [])
                     if e.get('bookEditor')]
            series = self.series_by_id[str(_first(chs, 'seriesId'))]
            genre = _first(chs, 'genre') or ''
            proceedings = 'Proceedings' in genre or any(
                'ConferencePaper' in (c.get('contentType') or '') for c in chs)
            pub_date = _first(chs, 'publicationDate') or ''
            out.append(ScrapedPublication(
                title=_first(chs, 'publicationName') or book,
                authors=[ScrapedAuthor(name=n) for n in names],
                year=int(pub_date[:4]) if pub_date[:4].isdigit() else None,
                venue=series, series=series, volume=None, pages=None,
                doi=book, ee=f'https://doi.org/{book}',
                isbn=_first(chs, 'printIsbn') or _first(chs, 'isbn'),
                publisher=_first(chs, 'publisherName') or 'Springer',
                work_type='proceedings' if proceedings else 'book',
                source=SOURCE, **_event_fields(chs)))
        return out

    # -- Collector API ---------------------------------------------------
    def fetch(self, from_created_date: Optional[str] = None,
              year_start: Optional[int] = None, year_end: Optional[int] = None,
              max_results: int = 5000) -> Tuple[List[ScrapedPublication], List[Dict[Any, Any]]]:
        """Volumes of the tracked series that had a chapter go online since `from_created_date`."""
        self.fetch_ok = True
        since = from_created_date or (
            datetime.now(timezone.utc).date() - timedelta(days=int(self.config.get('initial_days', 7)))
        ).isoformat()
        print(f"Searching Springer Meta API: chapters online since {since} "
              f"(budget left today: {self.quota.remaining()})")
        chapters = self._fetch_chapters(since, max_results)
        pubs = self.volumes(chapters)
        print(f"  Springer API: {len(pubs)} tracked volume(s) from {len(chapters)} chapters, "
              f"{self.calls_made} API call(s) this run")
        return pubs, chapters
