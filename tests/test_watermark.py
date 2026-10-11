"""
Tests for the high-watermark (Phase 4) — incremental extraction bookmark.

Unit tests cover the date formatting; integration tests prove the core
guarantee against a real Postgres:
  * no bookmark initially -> get_watermark returns None
  * after advance_watermark -> get_watermark returns that timestamp
  * advancing again updates (does not duplicate) the bookmark
"""

from datetime import datetime

from pipeline.watermark import (
    get_watermark, advance_watermark, watermark_to_created_date,
)


class TestDateFormat:

    def test_none_watermark_formats_to_none(self):
        assert watermark_to_created_date(None) is None

    def test_datetime_formats_to_date(self):
        dt = datetime(2026, 10, 11, 15, 30, 0)
        assert watermark_to_created_date(dt) == '2026-10-11'


class TestWatermarkPersistence:
    SERIES = 'Lecture Notes in Computer Science'

    def test_no_watermark_initially(self, db_env):
        assert get_watermark(self.SERIES) is None

    def test_advance_then_get(self, db_env):
        when = datetime(2026, 10, 1, 3, 0, 0)
        stored = advance_watermark(self.SERIES, when=when)
        assert stored == when
        assert get_watermark(self.SERIES) == when

    def test_advance_twice_updates_not_duplicates(self, db_env):
        first = datetime(2026, 10, 1, 3, 0, 0)
        second = datetime(2026, 10, 2, 3, 0, 0)
        advance_watermark(self.SERIES, when=first)
        advance_watermark(self.SERIES, when=second)
        # Only the latest should be stored (upsert, not a second row).
        assert get_watermark(self.SERIES) == second

        from sqlalchemy import create_engine, text
        engine = create_engine(db_env)
        with engine.connect() as conn:
            n = conn.execute(text(
                'SELECT COUNT(*) FROM pipeline_state WHERE series = :s'
            ), {'s': self.SERIES}).scalar()
        engine.dispose()
        assert n == 1
