"""Tests for the legacy work_type backfill (test database only, CrossRef faked)."""

from datetime import datetime

import pytest

import backfill_work_type
from pipeline.events import payload_hash

LNCS = 'Lecture Notes in Computer Science'


@pytest.fixture()
def rows(db_env):
    from pipeline.db import DatabaseConfig
    from pipeline.models import Publication
    s = DatabaseConfig.get_session()
    now = datetime.utcnow()
    data = [
        ('10.1007/a', LNCS, None), ('10.1007/b', LNCS, None),     # legacy Springer rows
        ('10.1007/c', LNCS, 'book'),                              # already typed
        ('10.1002/w', 'Wiley', None),                             # other publisher: untouched
    ]
    for doi, series, wt in data:
        s.add(Publication(doi=doi, title=doi, series=series, work_type=wt,
                          publisher=series, fetched_at=now))
    s.commit()
    s.close()


def _types():
    from pipeline.db import DatabaseConfig
    from sqlalchemy import text
    s = DatabaseConfig.get_session()
    out = dict(s.execute(text('SELECT doi, work_type FROM publications')).fetchall())
    s.close()
    return out


def all_book(dois):
    return {d.lower(): 'book' for d in dois}


def test_dry_run_writes_nothing(rows):
    r = backfill_work_type.run(dry_run=True, fetch_types=all_book, pause=0)
    assert r['missing'] == 2 and r['updated'] == 0 and r['types']['book'] == 2
    assert _types()['10.1007/a'] is None


def test_backfill_updates_only_legacy_springer_rows(rows):
    r = backfill_work_type.run(fetch_types=all_book, pause=0)
    assert r['updated'] == 2
    t = _types()
    assert t['10.1007/a'] == t['10.1007/b'] == 'book'
    assert t['10.1002/w'] is None            # other publishers are left alone


def test_each_row_gets_its_own_crossref_type(rows):
    types = {'10.1007/a': 'book', '10.1007/b': 'book-chapter'}
    backfill_work_type.run(fetch_types=lambda d: {x.lower(): types[x] for x in d}, pause=0)
    t = _types()
    assert t['10.1007/a'] == 'book' and t['10.1007/b'] == 'book-chapter'


def test_rows_crossref_does_not_know_stay_empty(rows):
    r = backfill_work_type.run(
        fetch_types=lambda d: {x.lower(): 'book' for x in d if x.endswith('/a')}, pause=0)
    assert r['not_found'] == 1 and r['updated'] == 1
    t = _types()
    assert t['10.1007/a'] == 'book' and t['10.1007/b'] is None


def test_second_run_finds_nothing(rows):
    backfill_work_type.run(fetch_types=all_book, pause=0)
    r = backfill_work_type.run(fetch_types=all_book, pause=0)
    assert r['missing'] == 0 and r['updated'] == 0


def test_chunks_are_committed_independently(rows, monkeypatch):
    """An interruption after the first chunk must keep that chunk's work."""
    monkeypatch.setattr(backfill_work_type, 'CHUNK', 1)
    calls = []

    def flaky(dois):
        calls.append(list(dois))
        if len(calls) == 2:
            raise RuntimeError('network down')
        return {d.lower(): 'book' for d in dois}

    with pytest.raises(RuntimeError):
        backfill_work_type.run(fetch_types=flaky, pause=0)
    t = _types()
    assert t['10.1007/a'] == 'book' and t['10.1007/b'] is None
    r = backfill_work_type.run(fetch_types=all_book, pause=0)     # resume
    assert r['updated'] == 1 and _types()['10.1007/b'] == 'book'


def test_work_type_never_changes_the_event_fingerprint():
    """The reason the backfill creates no title.updated events."""
    base = {'doi': '10.1007/a', 'title': 'T', 'year': 2024, 'series': LNCS}
    assert payload_hash(base) == payload_hash(dict(base, work_type='book'))
