"""
Daily call budget for rate-limited APIs (IEEE Xplore free keys: 200 calls/day).

The counter is stored in the database (table api_usage) so it survives between
separate CI jobs. Without a database it falls back to an in-memory counter,
which is only correct within one process - fine for local experiments.

A `reserve` is subtracted from the limit so manual testing, retries and clock
differences between us and the provider never push the key over its quota.
"""

from datetime import date, datetime, timezone
from typing import Callable, Dict, Tuple


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


class MemoryUsage:
    """Per-process counter (tests, and runs without a database)."""

    def __init__(self):
        self._calls: Dict[Tuple[str, date], int] = {}

    def get(self, provider: str, day: date) -> int:
        return self._calls.get((provider, day), 0)

    def increment(self, provider: str, day: date) -> int:
        key = (provider, day)
        self._calls[key] = self._calls.get(key, 0) + 1
        return self._calls[key]


class DbUsage:
    """Counter persisted in the api_usage table (atomic upsert)."""

    def __init__(self):
        from sqlalchemy import text
        from pipeline.db import DatabaseConfig
        from pipeline.models import ApiUsage
        self._text = text
        self._engine = DatabaseConfig.create_engine_from_env()
        # Older databases may not have the table yet; creating it is idempotent.
        ApiUsage.__table__.create(self._engine, checkfirst=True)

    def get(self, provider: str, day: date) -> int:
        with self._engine.connect() as conn:
            row = conn.execute(self._text(
                'SELECT calls FROM api_usage WHERE provider = :p AND day = :d'),
                {'p': provider, 'd': day}).fetchone()
        return row[0] if row else 0

    def increment(self, provider: str, day: date) -> int:
        with self._engine.begin() as conn:
            return conn.execute(self._text(
                'INSERT INTO api_usage (provider, day, calls) VALUES (:p, :d, 1) '
                'ON CONFLICT (provider, day) DO UPDATE SET calls = api_usage.calls + 1 '
                'RETURNING calls'), {'p': provider, 'd': day}).scalar()


def default_usage():
    """DB-backed when a database is configured and reachable, else in-memory."""
    from pipeline.db import DatabaseConfig
    if DatabaseConfig.is_cloud_configured():
        try:
            return DbUsage()
        except Exception as exc:
            print(f"  API quota: database counter unavailable ({type(exc).__name__}); "
                  f"using an in-memory counter for this run")
    return MemoryUsage()


class DailyQuota:
    """Budget = limit - reserve calls per UTC day for one provider."""

    def __init__(self, provider: str, limit: int, reserve: int = 0,
                 usage=None, today: Callable[[], date] = utc_today):
        self.provider = provider
        self.budget = max(0, int(limit) - int(reserve))
        self.usage = usage if usage is not None else default_usage()
        self._today = today

    def used(self) -> int:
        return self.usage.get(self.provider, self._today())

    def remaining(self) -> int:
        return max(0, self.budget - self.used())

    def consume(self) -> bool:
        """Reserve one call. Returns False (and spends nothing) when the budget is gone."""
        if self.remaining() <= 0:
            return False
        self.usage.increment(self.provider, self._today())
        return True
