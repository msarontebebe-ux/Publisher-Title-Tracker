"""
High-watermark helpers for incremental extraction (Phase 4).

A "watermark" is a single stored timestamp per series — the moment of the last
SUCCESSFUL incremental run. It is the bookmark that lets each run fetch only
what changed since last time, instead of re-scanning everything.

Two operations:
  * get_watermark(series)      -> datetime | None   (read the bookmark)
  * advance_watermark(series)  -> datetime          (move the bookmark to NOW,
                                                     only after a success)

Semantics that keep this safe:
  * A missing watermark (first ever run) returns None. The caller then falls
    back to a sensible default window (e.g. the current year) so the first run
    still captures recent titles.
  * The watermark is advanced ONLY on success, so a failed run re-tries the
    same window and never silently skips data.

CrossRef maps the watermark to its `from-created-date` filter, which selects
records *created/updated in CrossRef* after the watermark — i.e. things that
are new or changed since our last look.
"""

from datetime import datetime
from typing import Optional

from pipeline.db import DatabaseConfig


def get_watermark(series: str) -> Optional[datetime]:
    """Return the stored last-success timestamp for a series, or None."""
    from sqlalchemy import text as sa_text
    try:
        session = DatabaseConfig.get_session()
    except Exception:
        return None
    try:
        row = session.execute(sa_text(
            "SELECT last_success_at FROM pipeline_state WHERE series = :s"
        ), {'s': series}).fetchone()
        return row[0] if row else None
    except Exception:
        # Table may not exist yet on a brand-new DB — treat as "no watermark".
        return None
    finally:
        session.close()


def advance_watermark(series: str, when: Optional[datetime] = None) -> Optional[datetime]:
    """
    Set the watermark for a series to `when` (default: now), creating the row
    if needed. Returns the timestamp that was stored, or None on failure.
    """
    from sqlalchemy import text as sa_text
    when = when or datetime.utcnow()
    try:
        session = DatabaseConfig.get_session()
    except Exception:
        return None
    try:
        session.execute(sa_text("""
            INSERT INTO pipeline_state (series, last_success_at, updated_at)
            VALUES (:s, :t, :t)
            ON CONFLICT (series)
            DO UPDATE SET last_success_at = EXCLUDED.last_success_at,
                          updated_at      = EXCLUDED.updated_at
        """), {'s': series, 't': when})
        session.commit()
        return when
    except Exception:
        session.rollback()
        return None
    finally:
        session.close()


def watermark_to_created_date(watermark: Optional[datetime]) -> Optional[str]:
    """
    Format a watermark as a CrossRef `from-created-date` value (YYYY-MM-DD).
    Returns None when there is no watermark (caller uses its default window).
    """
    if watermark is None:
        return None
    return watermark.strftime('%Y-%m-%d')
