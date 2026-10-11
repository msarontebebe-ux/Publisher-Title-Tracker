"""
Shared configuration helpers — single source of truth.

Centralises three concerns that were previously duplicated across
runner.py, orchestrator.py and api_server.py:

  * ensure_dotenv()   — load .env once so DATABASE_URL is visible everywhere
  * load_config()     — read config.json
  * series lookups    — abbreviation <-> full-name resolution

This module is intentionally dependency-free (stdlib + python-dotenv only)
so it can be imported by every other module without creating import cycles.
"""

import json
from typing import Dict, Any, Optional

DEFAULT_CONFIG_PATH = "config.json"

# Load .env at import time so DATABASE_URL (and friends) are visible to every
# module that imports pipeline.config — replaces the four copy-pasted
# try/except load_dotenv() blocks that used to live in runner/db/api/enrich.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def ensure_dotenv() -> None:
    """Idempotently load .env into the environment (safe to call repeatedly)."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass


def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Load configuration from JSON file."""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_series_map(config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, str]:
    """Return {ABBREVIATION: full_name} for every configured series."""
    cfg = load_config(config_path)
    return {s['abbreviation'].upper(): s['name'] for s in cfg.get('springer_series', [])}


def find_series_by_abbr(abbr: str, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return the full series dict for an abbreviation (case-insensitive), or None."""
    for s in config.get('springer_series', []):
        if s['abbreviation'].upper() == abbr.upper():
            return s
    return None


def resolve_series_name(abbr_or_name: str, series_map: Dict[str, str] = None) -> str:
    """
    Accept either an abbreviation (LNCS) or a full name and always return the
    full name as stored in the DB. Pass a preloaded `series_map` to avoid
    re-reading config.json on every call (used by the request-path API).
    """
    if not abbr_or_name:
        return abbr_or_name
    if series_map is None:
        series_map = load_series_map()
    return series_map.get(abbr_or_name.strip().upper(), abbr_or_name)
