"""CrossRef API scraper for Springer publications."""

import requests
import time
import re
from typing import List, Optional, Dict, Any, Tuple
from urllib.parse import urlencode
from pipeline.models import ScrapedPublication as Publication, ScrapedAuthor as Author
from pipeline.event_dates import ConferenceDateExtractor
from pipeline.events import decode_entities


class CrossRefScraper:
    """Scraper for fetching publication metadata from CrossRef API."""
    
    def __init__(self, email: str = "research@example.com", delay: float = 1.0):
        """
        Initialize CrossRef scraper.
        
        Args:
            email: Your email for polite pool (gets faster API access)
            delay: Delay between requests in seconds
        """
        self.base_url = "https://api.crossref.org/works"
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': f'SpringerSeriesScraper/1.0 (mailto:{email})'
        })
        
        # Store raw API responses for data lake bronze layer
        self.raw_responses = []
        # False when the last search was cut short (network/parse error or
        # more results existed than max_results). The runner uses it to avoid
        # advancing the watermark over data that was never fetched.
        self.fetch_ok = True
    
    def search_by_filter(self, base_filter: str, year_start: Optional[int] = None,
                         year_end: Optional[int] = None, max_results: int = 1000,
                         from_created_date: Optional[str] = None) -> Tuple[List[Publication], List[Dict[Any, Any]]]:
        """
        Search CrossRef works with an arbitrary base filter (e.g. "issn:X,type:book"
        or "prefix:10.1016,type:edited-book"). Repeated filter names are OR-ed by CrossRef.
        Uses cursor-based pagination to bypass CrossRef's 10K offset limit.
        
        Args:
            base_filter: CrossRef `filter` expression without date constraints
            year_start: Optional start year filter
            year_end: Optional end year filter
            max_results: Maximum number of results to fetch
            from_created_date: Watermark date (YYYY-MM-DD). Despite the name it is
                sent to CrossRef as `from-update-date` ("deposited or redeposited
                at or after"), so incremental runs see records that were created
                OR changed since the last run. `from-created-date` would only
                return brand-new DOIs and never an update to an older title.
                The caller separates new titles from old ones using each
                record's `created` date. When set, it replaces the year filters
                (they serve different purposes and must not combine).
            
        Returns:
            Tuple of (List of Publication objects, List of raw API responses)
        """
        publications = []
        self.raw_responses = []  # Reset raw responses
        self.fetch_ok = True
        cursor = '*'  # Start cursor for deep pagination
        rows = 100  # Items per request (max 1000, but 100 is safer)
        
        print(f"Searching CrossRef: {base_filter}")
        
        while len(publications) < max_results:
            params = {
                'filter': base_filter,
                'rows': rows,
                'cursor': cursor,  # Use cursor instead of offset for deep pagination
                'select': 'DOI,title,subtitle,author,editor,published-print,published-online,container-title,volume,issue,page,ISBN,ISSN,publisher,type,event,created'
            }
            
            # Date filtering. A watermark (from_created_date) selects the true
            # delta since the last run and takes precedence over year filters.
            if from_created_date:
                params['filter'] += f',from-update-date:{from_created_date}'
                print(f"  Incremental: records deposited/updated since {from_created_date}")
            elif year_start and year_end:
                params['filter'] += f',from-pub-date:{year_start},until-pub-date:{year_end}'
            elif year_start:
                params['filter'] += f',from-pub-date:{year_start}'
            elif year_end:
                params['filter'] += f',until-pub-date:{year_end}'
            
            try:
                response = self.session.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                message = data.get('message', {})
                items = message.get('items', [])
                next_cursor = message.get('next-cursor')
                total_results = message.get('total-results', 0)
                
                if not items:
                    print(f"  No more results (fetched {len(publications)} total)")
                    break
                
                # Store raw API responses for Bronze layer
                for item in items:
                    self.raw_responses.append(item)  # Keep completely raw
                    pub = self._parse_publication(item)
                    if pub:
                        publications.append(pub)
                
                print(f"  Fetched {len(publications)}/{min(total_results, max_results)} publications...")
                
                # Check if we've reached the end or max results
                if len(publications) >= max_results and total_results > max_results:
                    print(f"  Truncated: {total_results} available, limit {max_results}")
                    self.fetch_ok = False
                if not next_cursor or len(publications) >= max_results:
                    print(f"  Completed: {len(publications)} publications")
                    break
                
                cursor = next_cursor  # Move to next page
                
                # Be polite to CrossRef servers
                time.sleep(self.delay)
                
            except requests.RequestException as e:
                print(f"  Error fetching data: {e}")
                self.fetch_ok = False
                break
            except Exception as e:
                print(f"  Error parsing response: {e}")
                self.fetch_ok = False
                break
        
        return publications[:max_results], self.raw_responses[:max_results]
    
    def search_by_issn(self, issn: str, year_start: Optional[int] = None,
                       year_end: Optional[int] = None, max_results: int = 1000,
                       from_created_date: Optional[str] = None) -> Tuple[List[Publication], List[Dict[Any, Any]]]:
        """Search book-level records (proceedings volumes) of a series by ISSN."""
        return self.search_by_filter(
            f'issn:{issn},type:book', year_start, year_end, max_results,
            from_created_date=from_created_date)

    def search_by_series_title(self, series_title: str, publisher: str = "Springer",
                               year_start: Optional[int] = None, 
                               year_end: Optional[int] = None,
                               max_results: int = 1000) -> List[Publication]:
        """
        Search publications by series title (alternative when ISSN unknown).
        
        Args:
            series_title: Title of the series
            publisher: Publisher name (default: Springer)
            year_start: Optional start year filter
            year_end: Optional end year filter
            max_results: Maximum number of results
            
        Returns:
            List of Publication objects
        """
        publications = []
        self.fetch_ok = True
        offset = 0
        rows = 100
        
        print(f"Searching CrossRef for series: {series_title}")
        
        while len(publications) < max_results:
            params = {
                'query.container-title': series_title,
                'rows': rows,
                'offset': offset,
                'sort': 'published',
                'order': 'desc'
            }
            
            # NOTE: no publisher-name filter here — CrossRef rejects it (HTTP 400).
            # The container-title query alone is specific enough for series titles;
            # year filters below narrow it further when requested.
            
            # Add year filter (build from scratch — there is no base filter to
            # extend, since CrossRef rejects the publisher-name filter)
            year_filters = []
            if year_start:
                year_filters.append(f'from-pub-date:{year_start}')
            if year_end:
                year_filters.append(f'until-pub-date:{year_end}')
            if year_filters:
                params['filter'] = ','.join(year_filters)
            
            try:
                response = self.session.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                message = data.get('message', {})
                items = message.get('items', [])
                total_results = message.get('total-results', 0)
                
                if not items:
                    print(f"  No more results (fetched {len(publications)} total)")
                    break
                
                for item in items:
                    pub = self._parse_publication(item)
                    if pub and series_title.lower() in str(pub.series).lower():
                        publications.append(pub)
                
                print(f"  Fetched {len(publications)}/{min(total_results, max_results)} publications...")
                
                offset += len(items)
                
                if offset >= total_results or len(publications) >= max_results:
                    print(f"  Completed: {len(publications)} publications")
                    break
                
                time.sleep(self.delay)
                
            except requests.RequestException as e:
                print(f"  Error fetching data: {e}")
                self.fetch_ok = False
                break
            except Exception as e:
                print(f"  Error parsing response: {e}")
                self.fetch_ok = False
                break
        
        return publications[:max_results]
    
    def _extract_volume_number(self, item: Dict[str, Any]) -> Optional[str]:
        """
        Extract volume number from title, DOI, or other metadata.
        
        Args:
            item: Raw CrossRef item
            
        Returns:
            Volume number as string or None
        """
        # Try direct volume field first
        volume = item.get('volume')
        if volume:
            return str(volume)
        
        # Extract from title (lightweight parsing, avoid false matches like years)
        title_list = item.get('title', [])
        if title_list:
            title = title_list[0]
            # Match explicit volume patterns (must be 4-5 digits or preceded by Vol/Volume)
            patterns = [
                r'Volume\s+(\d{4,5})',  # Volume 14764
                r'Vol\.\s+(\d{4,5})',   # Vol. 14764
                r'Vol\s+(\d{4,5})',     # Vol 14764
                r'LNCS\s+(\d{4,5})',    # LNCS 14764
                r'LNAI\s+(\d{4,5})',    # LNAI 14764
                r'CCIS\s+(\d{4,5})',    # CCIS 14764
            ]
            for pattern in patterns:
                match = re.search(pattern, title, re.IGNORECASE)
                if match:
                    return match.group(1)
        
        # Try extracting from subtitle or alternative title fields
        subtitle = item.get('subtitle', [])
        if subtitle and subtitle[0]:
            title_patterns = [r'Volume\s+(\d{4,5})', r'Vol\.\s+(\d{4,5})', r'LNCS\s+(\d{4,5})']
            for pattern in title_patterns:
                match = re.search(pattern, subtitle[0], re.IGNORECASE)
                if match:
                    return match.group(1)
        
        return None
    
    def _parse_publication(self, item: Dict[str, Any]) -> Optional[Publication]:
        """
        Parse a CrossRef API item into a Publication object.
        
        Args:
            item: Raw item data from CrossRef API
            
        Returns:
            Publication object or None if parsing fails
        """
        try:
            # Extract title
            title_list = item.get('title') or []
            title = title_list[0] if title_list else None
            if not title:
                # Journal issues have no title of their own: build one from
                # the journal name + volume/issue so the record is readable.
                container = (item.get('container-title') or [None])[0]
                if container:
                    vol, iss = item.get('volume'), item.get('issue')
                    title = container + (f" Vol. {vol}" if vol else '') + (f" No. {iss}" if iss else '')
                else:
                    title = 'Unknown Title'
            # CrossRef sometimes returns titles HTML-encoded ('A &amp; B'); store plain text.
            title = decode_entities(title)
            
            # Extract authors/editors (books have editors, book-chapters have authors)
            authors_data = item.get('author', []) or item.get('editor', [])
            authors = []
            for author_data in authors_data:
                given = author_data.get('given', '')
                family = author_data.get('family', '')
                name = decode_entities(f"{given} {family}".strip())
                if not name:
                    name = decode_entities(author_data.get('name', 'Unknown'))
                authors.append(Author(name=name))
            
            # Note: For proceedings volumes (type:book), these are typically editors, not authors
            
            # Extract year
            year = None
            published = item.get('published-print') or item.get('published-online') or item.get('created')
            if published:
                date_parts = published.get('date-parts', [[]])
                if date_parts and date_parts[0]:
                    year = date_parts[0][0]
            
            # Extract venue/series
            container_title = item.get('container-title', [])
            series = container_title[0] if container_title else None
            
            # Extract other metadata with enhanced volume detection
            volume = self._extract_volume_number(item)
            pages = item.get('page')
            doi = item.get('DOI')
            
            # Electronic edition URL
            ee = f"https://doi.org/{doi}" if doi else None
            
            # ISBN
            isbn_list = item.get('ISBN', [])
            isbn = isbn_list[0] if isbn_list else None
            
            # Publisher
            publisher = item.get('publisher', 'Springer')
            
            # Extract event dates — try sources in priority order
            subtitle_list = item.get('subtitle', [])
            subtitle = decode_entities(subtitle_list[0]) if subtitle_list else None

            event_info = ConferenceDateExtractor.extract_from_crossref(item)
            if not event_info or event_info.get('confidence') == 'low':
                # Subtitle carries the full conference string, e.g.
                # "33rd International Conference, ..., October 21-22, 2024, Proceedings"
                if subtitle:
                    sub_info = ConferenceDateExtractor.extract_from_title(subtitle)
                    if sub_info and sub_info.get('confidence', '') > (event_info or {}).get('confidence', ''):
                        event_info = sub_info
            if not event_info or event_info.get('confidence') == 'low':
                title_info = ConferenceDateExtractor.extract_from_title(title)
                if title_info and title_info.get('confidence', '') > (event_info or {}).get('confidence', ''):
                    event_info = title_info
            
            publication = Publication(
                title=title,
                authors=authors,
                year=year,
                venue=series,
                series=series,
                volume=volume,
                pages=pages,
                doi=doi,
                ee=ee,
                isbn=isbn,
                publisher=publisher,
                work_type=item.get('type'),
                created=(item.get('created') or {}).get('date-time'),
                event_date_start=event_info.get('event_date_start') if event_info else None,
                event_date_end=event_info.get('event_date_end') if event_info else None,
                event_year=event_info.get('event_year') if event_info else None,
                event_month=event_info.get('event_month') if event_info else None,
                event_date_confidence=event_info.get('confidence') if event_info else None,
                event_name=decode_entities(event_info.get('event_name')) if event_info else None,
                event_location=decode_entities(event_info.get('event_location')) if event_info else None,
            )
            
            return publication
            
        except Exception as e:
            print(f"  Warning: Failed to parse publication: {e}")
            return None
    
    def get_publication_by_doi(self, doi: str) -> Optional[Publication]:
        """
        Get a specific publication by DOI.
        
        Args:
            doi: Digital Object Identifier
            
        Returns:
            Publication object or None if not found
        """
        url = f"{self.base_url}/{doi}"
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            item = data.get('message', {})
            
            return self._parse_publication(item)
            
        except Exception as e:
            print(f"Error fetching DOI {doi}: {e}")
            return None
