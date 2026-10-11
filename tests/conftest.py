"""
Shared pytest configuration.

Adds the project root to sys.path (so tests can import project modules
without installing the package) and provides the database fixture used
by integration tests.
"""

import os
import sys
from pathlib import Path

# Project root = parent of tests/ — needed for `import models_db` etc.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

# Dedicated throwaway test database — NEVER the production Neon DB.
# CI provides its own ephemeral Postgres via service containers.
TEST_DATABASE_URL = os.environ.get(
    'TEST_DATABASE_URL',
    'postgresql+psycopg2://springer:springer_test@localhost:5432/springer_test'
)


@pytest.fixture(scope='module')
def test_db():
    """
    Module-scoped fixture that prepares a clean test database.

    Yields the database URL on success; skips the test module entirely
    when no test database is reachable (so `pytest` stays green on
    machines without Postgres — CI always has one).
    """
    from sqlalchemy import create_engine, text

    try:
        engine = create_engine(TEST_DATABASE_URL)
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
    except Exception as exc:
        pytest.skip(f'Test database not reachable: {exc}')

    # Fresh slate: drop all tables (and medallion schemas) from prior runs
    from models_db import Base
    with engine.connect() as conn:
        conn.execute(text('DROP SCHEMA IF EXISTS bronze CASCADE'))
        conn.execute(text('DROP SCHEMA IF EXISTS gold CASCADE'))
        conn.commit()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    yield TEST_DATABASE_URL

    engine.dispose()


@pytest.fixture()
def db_env(test_db, monkeypatch):
    """
    Points the application's DatabaseConfig at the TEST database and
    clears all tables, so every test starts from a known-empty state —
    never touching production.
    """
    from sqlalchemy import create_engine, text
    engine = create_engine(test_db)
    with engine.connect() as conn:
        conn.execute(text('TRUNCATE publication_authors, publications, authors, pipeline_runs, quality_metrics RESTART IDENTITY CASCADE'))
        conn.commit()
    engine.dispose()

    monkeypatch.setenv('DATABASE_URL', test_db)
    yield test_db
