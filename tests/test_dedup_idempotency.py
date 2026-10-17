"""
Integration test: DOI deduplication must be idempotent (NFR4).

Loads the same publications twice into a REAL PostgreSQL sandbox and asserts
the second load inserts zero rows. This is the automated form of the manual
validation the thesis previously performed by eye.

Requires a reachable test database (see tests/conftest.py for the URL).
Skips politely when none is available — CI always provides one.
"""

from pipeline.models import ScrapedPublication as Pub, ScrapedAuthor as Author


def _make_publications():
    """Two realistic fake proceedings volumes."""
    return [
        Pub(
            title='Advances in Test Frameworks, Vol. 1',
            authors=[Author(name='Alice Author'), Author(name='Bob Writer')],
            year=2024, series='Test Series', volume='1', pages='1-100',
            doi='10.1007/978-3-000-00000-1_1', publisher='Springer',
            ee='https://doi.org/10.1007/978-3-000-00000-1_1',
            isbn='978-3-000-00000-1',
        ),
        Pub(
            title='Advances in Test Frameworks, Vol. 2',
            authors=[Author(name='Alice Author'), Author(name='Carol Checker')],
            year=2024, series='Test Series', volume='2', pages='1-120',
            doi='10.1007/978-3-000-00000-2_1', publisher='Springer',
            ee='https://doi.org/10.1007/978-3-000-00000-2_1',
            isbn='978-3-000-00000-2',
        ),
    ]


def _count(session_factory):
    from sqlalchemy import text
    session = session_factory()
    try:
        return session.execute(text('SELECT COUNT(*) FROM publications')).scalar()
    finally:
        session.close()


def test_first_load_inserts_everything(db_env):
    from pipeline.runner import _write_silver
    from pipeline.db import DatabaseConfig

    inserted = _write_silver(_make_publications(), 'Test Series')
    assert inserted.inserted == 2
    assert inserted.failed == 0
    assert _count(DatabaseConfig.get_session) == 2


def test_second_load_inserts_zero(db_env):
    """Same DOIs again → nothing new. The heart of 'increment detection'."""
    from pipeline.runner import _write_silver
    from pipeline.db import DatabaseConfig

    # First load (module-scoped test_db persists across tests in this module)
    _write_silver(_make_publications(), 'Test Series')

    # Second identical load — must be a no-op
    inserted_again = _write_silver(_make_publications(), 'Test Series')
    assert inserted_again.inserted == 0
    assert inserted_again.failed == 0
    assert _count(DatabaseConfig.get_session) == 2


def test_partial_overlap_inserts_only_new(db_env):
    """Mixed batch: 1 known DOI + 1 brand-new DOI → exactly 1 insert."""
    from pipeline.runner import _write_silver
    from pipeline.db import DatabaseConfig

    _write_silver(_make_publications(), 'Test Series')

    new_only = _make_publications()[1:]
    new_only[0].doi = '10.1007/978-3-000-00000-3_1'   # make it genuinely new
    inserted = _write_silver(new_only, 'Test Series')

    assert inserted.inserted == 1
    assert inserted.failed == 0
    assert _count(DatabaseConfig.get_session) == 3


def test_publication_without_doi_is_skipped(db_env):
    """FR3: DOI is the identity — records without one must not be stored."""
    from pipeline.runner import _write_silver
    from pipeline.db import DatabaseConfig

    before = _count(DatabaseConfig.get_session)
    orphan = _make_publications()[0]
    orphan.doi = None
    inserted = _write_silver([orphan], 'Test Series')

    assert inserted.inserted == 0
    assert inserted.failed == 0
    assert _count(DatabaseConfig.get_session) == before


def test_authors_deduplicated_by_name(db_env):
    """Alice Author appears in multiple volumes — stored once, linked twice."""
    from pipeline.runner import _write_silver
    from pipeline.db import DatabaseConfig
    from sqlalchemy import text

    _write_silver(_make_publications(), 'Test Series')

    session = DatabaseConfig.get_session()
    try:
        alice = session.execute(
            text("SELECT COUNT(*) FROM authors WHERE name = 'Alice Author'")
        ).scalar()
        links = session.execute(
            text("SELECT COUNT(*) FROM publication_authors pa "
                 "JOIN authors a ON a.id = pa.author_id "
                 "WHERE a.name = 'Alice Author'")
        ).scalar()
    finally:
        session.close()

    assert alice == 1      # one author row
    assert links == 2      # linked to both volumes


def test_failed_batch_is_reported_not_silently_success(db_env, monkeypatch):
    """Regression guard for the 'partial writes logged as success' bug.

    If a batch cannot commit, the run must report failed>0 (so the caller
    logs 'partial'), NOT a clean success. Previously this failed silently:
    the except block printed a message but the run was still recorded as
    'success', making pipeline_runs untrustworthy.
    """
    from pipeline.runner import _write_silver
    from sqlalchemy.orm import Session

    # Force every batch commit to raise. The initial session that loads
    # existing DOIs only does execute()/close() — never commit() — so it is
    # unaffected; only the batch write is broken, which is the real scenario.
    def boom(self):
        raise RuntimeError("simulated commit failure (disk full)")

    monkeypatch.setattr(Session, "commit", boom)

    result = _write_silver(_make_publications(), 'Test Series')

    # The failure must be surfaced, not swallowed:
    assert result.failed >= 1          # at least one batch rolled back
    assert result.inserted == 0        # nothing was actually persisted
    assert result.errors               # and an error message was captured
    assert "simulated commit failure" in result.errors[0]


def _pub(doi, created=None, title='T'):
    from pipeline.models import ScrapedPublication, ScrapedAuthor
    return ScrapedPublication(title=title, doi=doi, year=2026, created=created,
                              series='Lecture Notes in Computer Science',
                              authors=[ScrapedAuthor(name='Alice')])


class TestIncrementalWithLegacyTitles:
    """End to end through _persist_and_log: a title stored before the event log,
    re-deposited by the publisher, must not be announced as new."""

    def _run(self, pubs, since='2026-10-13'):
        from unittest.mock import MagicMock
        from pipeline.runner import _persist_and_log, _utcnow
        _persist_and_log(pubs, [], 'Lecture Notes in Computer Science', 'LNCS', _utcnow(),
                         'incremental', MagicMock(), new_since=since)

    def _events(self, url):
        from sqlalchemy import create_engine, text
        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(text('SELECT source_record_id, event_type FROM events '
                                     'ORDER BY id')).fetchall()
        engine.dispose()
        return [tuple(r) for r in rows]

    def test_legacy_old_and_new_titles_are_each_handled_correctly(self, db_env):
        from pipeline.db import DatabaseConfig
        from pipeline.models import Publication
        session = DatabaseConfig.get_session()
        session.add(Publication(doi='10.1/legacy', title='Legacy', year=2020,
                                series='Lecture Notes in Computer Science'))
        session.commit()
        session.close()

        self._run([_pub('10.1/legacy', created='2020-01-01T00:00:00Z'),    # stored, no event
                   _pub('10.1/untracked', created='2019-01-01T00:00:00Z'),  # old, never stored
                   _pub('10.1/fresh', created='2026-10-14T00:00:00Z')])     # genuinely new

        assert self._events(db_env) == [('10.1/legacy', 'title.baseline'),
                                        ('10.1/fresh', 'title.added')]
        from sqlalchemy import create_engine, text
        engine = create_engine(db_env)
        with engine.connect() as conn:
            stored = {r[0] for r in conn.execute(text('SELECT doi FROM publications'))}
        engine.dispose()
        assert stored == {'10.1/legacy', '10.1/fresh'}      # the untracked old title was not stored

    def test_second_run_with_a_changed_legacy_title_emits_one_update(self, db_env):
        from pipeline.db import DatabaseConfig
        from pipeline.models import Publication
        session = DatabaseConfig.get_session()
        session.add(Publication(doi='10.1/legacy', title='Legacy', year=2020,
                                series='Lecture Notes in Computer Science'))
        session.commit()
        session.close()
        self._run([_pub('10.1/legacy', created='2020-01-01T00:00:00Z')])
        self._run([_pub('10.1/legacy', created='2020-01-01T00:00:00Z', title='Legacy, corrected')])
        assert [t for _, t in self._events(db_env)] == ['title.baseline', 'title.updated']


class TestGoldPrunesOrphans:
    """Gold rows of a series that no longer exists in publications must go away on
    the next refresh, even though refreshes only ever recompute one series."""

    def _seed(self, url):
        from sqlalchemy import create_engine, text
        engine = create_engine(url)
        with engine.begin() as conn:
            for ser, year, doi in [('Alive', 2024, '10.1/a1'), ('Alive', 2025, '10.1/a2')]:
                conn.execute(text("INSERT INTO publications (doi, title, year, series) "
                                  "VALUES (:d, 'T', :y, :s)"), {'d': doi, 'y': year, 's': ser})
            # leftovers of a series that was renamed away
            conn.execute(text("INSERT INTO gold.series_stats (series, total_publications) VALUES ('Ghost', 9)"))
            conn.execute(text("INSERT INTO gold.yearly_trends (series, year, publication_count) VALUES ('Ghost', 2020, 9)"))
            conn.execute(text("INSERT INTO gold.top_authors (series, author_name, publication_count) VALUES ('Ghost', 'X', 9)"))
            conn.execute(text("INSERT INTO quality_metrics (series, total_records) VALUES ('Ghost', 9)"))
            # a year that no longer has any title, inside a series that still exists
            conn.execute(text("INSERT INTO gold.yearly_trends (series, year, publication_count) VALUES ('Alive', 1999, 4)"))
        engine.dispose()

    def _series_in(self, url, table):
        from sqlalchemy import create_engine, text
        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(text(f'SELECT DISTINCT series FROM {table} ORDER BY 1')).fetchall()
        engine.dispose()
        return [r[0] for r in rows]

    def test_refreshing_one_series_removes_other_series_leftovers(self, db_env):
        from pipeline.gold import refresh_gold
        self._seed(db_env)
        refresh_gold(series_filter='Alive')          # the scheduled jobs refresh ONE series
        for table in ('gold.series_stats', 'gold.yearly_trends', 'gold.top_authors', 'quality_metrics'):
            assert 'Ghost' not in self._series_in(db_env, table), table
        assert self._series_in(db_env, 'gold.series_stats') == ['Alive']
        assert self._series_in(db_env, 'quality_metrics') == ['Alive']

    def test_a_year_without_titles_is_removed_but_real_years_stay(self, db_env):
        from sqlalchemy import create_engine, text
        from pipeline.gold import refresh_gold
        self._seed(db_env)
        refresh_gold(series_filter='Alive')
        engine = create_engine(db_env)
        with engine.connect() as conn:
            years = [r[0] for r in conn.execute(text(
                "SELECT year FROM gold.yearly_trends WHERE series = 'Alive' ORDER BY year"))]
        engine.dispose()
        assert years == [2024, 2025]

    def test_live_series_are_never_pruned(self, db_env):
        from pipeline.gold import refresh_gold
        self._seed(db_env)
        refresh_gold(series_filter='Alive')
        refresh_gold(series_filter='Alive')          # idempotent
        assert self._series_in(db_env, 'gold.series_stats') == ['Alive']

    def test_prune_reports_how_many_rows_it_removed(self, db_env):
        from pipeline.db import DatabaseConfig
        from pipeline.gold import prune_orphans
        self._seed(db_env)
        session = DatabaseConfig.get_session()
        try:
            assert prune_orphans(session) == 5      # 4 Ghost rows + the orphan year
            session.commit()
            assert prune_orphans(session) == 0      # nothing left to remove
        finally:
            session.close()
