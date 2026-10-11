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
