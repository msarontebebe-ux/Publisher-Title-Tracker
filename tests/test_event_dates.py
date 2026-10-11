"""
Tests for ConferenceDateExtractor — the original contribution of this project.

Each test mirrors a real Springer proceedings title pattern. When these pass,
the regex extraction behaves as documented in the thesis; when one fails after
a code change, the change broke a promised behaviour.
"""

from event_date_extractor import ConferenceDateExtractor as E


class TestDateRangePatterns:
    """'May 15-17, 2024' family — the most common Springer layout."""

    def test_month_dd_dd_yyyy_hyphen(self):
        result = E.extract_from_title(
            '33rd International Conference, Vienna, Austria, October 21-22, 2024, Proceedings'
        )
        assert result is not None
        assert result['event_date_start'] == '2024-10-21'
        assert result['event_date_end'] == '2024-10-22'
        assert result['event_year'] == 2024
        assert result['event_month'] == 10
        assert result['confidence'] == 'high'

    def test_month_dd_dd_yyyy_en_dash(self):
        """En-dash (U+2013) separators appear in real titles and must work."""
        result = E.extract_from_title(
            'International Symposium, Reykjavik, Iceland, September 16–20, 2024'
        )
        assert result is not None
        assert result['event_date_start'] == '2024-09-16'
        assert result['event_date_end'] == '2024-09-20'

    def test_dd_dd_month_yyyy(self):
        """European order: '15-17 May 2024'."""
        result = E.extract_from_title(
            'Workshop on Semantics, Turin, Italy, 15-17 May 2024'
        )
        assert result is not None
        assert result['event_date_start'] == '2024-05-15'
        assert result['event_date_end'] == '2024-05-17'


class TestSingleAndPartialDates:

    def test_single_day(self):
        result = E.extract_from_title('One-Day Event, Zurich, Switzerland, May 15, 2024')
        assert result is not None
        assert result['event_date_start'] == '2024-05-15'
        assert result['event_date_end'] is None
        assert result['confidence'] == 'high'

    def test_month_year_only(self):
        """Month-only mentions get day 01 and medium confidence."""
        result = E.extract_from_title('Annual Meeting, Budapest, Hungary, March 2024')
        assert result is not None
        assert result['event_date_start'] == '2024-03-01'
        assert result['confidence'] == 'medium'


class TestNegativeCases:

    def test_no_date_returns_none(self):
        assert E.extract_from_title('Advances in Databases: Theory and Practice') is None

    def test_empty_title(self):
        assert E.extract_from_title('') is None

    def test_impossible_date_rejected(self):
        """February 30 does not exist — must not silently produce 2024-02-30."""
        assert E.extract_from_title('Conference, March, February 30, 2024') is None


class TestEventNameAndLocation:

    def test_event_name_after_dash(self):
        assert E.extract_event_name('Computer Vision – ECCV 2024 Workshops') == 'ECCV 2024'

    def test_event_name_multi_word(self):
        name = E.extract_event_name('Machine Learning – ECML PKDD 2023 Workshops')
        assert name is not None
        assert '2023' in name

    def test_location_before_date(self):
        title = '33rd International Conference, Vienna, Austria, October 21-22, 2024, Proceedings'
        assert E.extract_event_location(title) == 'Vienna, Austria'

    def test_location_none_without_date(self):
        """Location anchors on the date position — no date, no location."""
        assert E.extract_event_location('A Book With No Date Anywhere') is None


class TestCrossrefFallback:

    def test_explicit_event_field_wins(self):
        item = {
            'title': ['Some Volume'],
            'subtitle': [''],
            'event': {
                'name': 'ICSE 2024',
                'location': 'Lisbon, Portugal',
                'start': '2024-04-14',
                'end': '2024-04-20',
            },
        }
        result = E.extract_from_crossref(item)
        assert result['event_date_start'] == '2024-04-14'
        assert result['event_date_end'] == '2024-04-20'
        assert result['event_name'] == 'ICSE 2024'
        assert result['confidence'] == 'high'

    def test_publication_year_fallback_is_low_confidence(self):
        item = {
            'title': ['Proceedings of Something'],
            'subtitle': [''],
            'published-print': {'date-parts': [[2023, 6]]},
        }
        result = E.extract_from_crossref(item)
        assert result['event_date_start'] is None
        assert result['event_year'] == 2023
        assert result['confidence'] == 'low'
