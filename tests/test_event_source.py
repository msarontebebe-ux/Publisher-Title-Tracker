"""
The event log records WHICH source produced a title (crossref / ieee-xplore),
while the DOI stays the identity across sources. Also guards the file-only
storage fallback, which broke when new record fields were added.
"""

import csv
import tempfile
from unittest.mock import MagicMock

from pipeline.collectors.ieee import IeeeXploreCollector
from pipeline.models import ScrapedAuthor, ScrapedPublication
from pipeline.storage import DataLakeStorageHandler
from tests.test_ieee_collector import CONFIG, KEY, FakeSession, make


def _pub(doi, **extra):
    fields = dict(title='T', doi=doi, year=2026, series='IEEE', publisher='IEEE',
                  authors=[ScrapedAuthor(name='Alice')])
    fields.update(extra)
    return ScrapedPublication(**fields)


def _events(url):
    from sqlalchemy import create_engine, text
    engine = create_engine(url)
    with engine.connect() as conn:
        rows = conn.execute(text(
            'SELECT source_record_id, source, event_type FROM events ORDER BY id')).fetchall()
    engine.dispose()
    return rows


class TestRecordSource:

    def test_default_source_is_crossref(self):
        assert _pub('10.1/a').to_dict()['source'] == 'crossref'

    def test_ieee_api_records_carry_the_ieee_label(self):
        pubs, _ = make().fetch()
        assert pubs and all(p.source == 'ieee-xplore' for p in pubs)

    def test_ieee_books_from_crossref_keep_the_crossref_label(self):
        assert 'source' in ScrapedPublication.__dataclass_fields__
        assert _pub('10.1109/book', work_type='book').source == 'crossref'


class TestEventSource:

    def test_event_stores_the_record_source(self, db_env):
        from pipeline.runner import _write_events
        _write_events([_pub('10.1109/a', source='ieee-xplore'), _pub('10.1109/b')], 'IEEE')
        assert _events(db_env) == [('10.1109/a', 'ieee-xplore', 'title.added'),
                                   ('10.1109/b', 'crossref', 'title.added')]

    def test_same_doi_from_a_second_source_is_not_new_again(self, db_env):
        """CrossRef then IEEE API describe the same proceedings DOI: one title.added."""
        from pipeline.runner import _write_events
        _write_events([_pub('10.1109/x')], 'IEEE')                          # via CrossRef
        result = _write_events([_pub('10.1109/x', source='ieee-xplore')], 'IEEE')  # via API
        assert result.added == 0 and result.updated == 0 and result.unchanged == 1
        assert [r[2] for r in _events(db_env)] == ['title.added']

    def test_source_does_not_change_the_fingerprint(self):
        from pipeline.events import payload_hash
        a = _pub('10.1109/x').to_dict()
        b = _pub('10.1109/x', source='ieee-xplore').to_dict()
        assert payload_hash(a) == payload_hash(b)

    def test_changed_metadata_from_other_source_is_an_update(self, db_env):
        from pipeline.runner import _write_events
        _write_events([_pub('10.1109/x')], 'IEEE')
        result = _write_events([_pub('10.1109/x', source='ieee-xplore',
                                     event_location='Bangkok, Thailand')], 'IEEE')
        assert result.updated == 1
        assert [r[2] for r in _events(db_env)] == ['title.added', 'title.updated']


class TestFileFallbackStorage:

    def test_csv_and_json_write_with_new_fields(self):
        handler = DataLakeStorageHandler(base_dir=tempfile.mkdtemp())
        pubs = [_pub('10.1109/a', work_type='proceedings', source='ieee-xplore')]
        csv_path = handler.save_silver_csv(pubs, 'IEEE')
        handler.save_silver_json(pubs, 'IEEE')
        with open(csv_path, encoding='utf-8-sig', newline='') as f:
            row = next(csv.DictReader(f))
        assert row['work_type'] == 'proceedings'
        assert 'source' not in row
