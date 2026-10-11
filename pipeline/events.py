"""
Event layer — change detection over publication records.

Turns a stream of scraped records into an append-only log of *changes*:

    title.added    — a DOI observed for the first time
    title.updated  — a DOI observed before, but its canonical payload changed

The core primitive is a stable fingerprint (`payload_hash`) computed over a
record's canonical fields. Two records with the same DOI:

  * same fingerprint  -> nothing changed -> no event (idempotent re-run)
  * different fingerprint -> title.updated

Design notes
------------
* The fingerprint deliberately EXCLUDES volatile fields (fetched_at, ids) so
  that merely re-fetching the same title never looks like a change.
* Fields are normalised (strings trimmed, dates to ISO, authors sorted by
  name) before hashing, so cosmetic ordering differences don't create noise.
* Persisting an event uses INSERT ... ON CONFLICT DO NOTHING against the
  UNIQUE(source, source_record_id, payload_hash) constraint, so idempotency is
  enforced by the database, not by racy application code.
"""

import hashlib
import json
import uuid
from datetime import date, datetime
from typing import Any, Dict, Optional

# Canonical fields that define a title's identity/content for change detection.
# Everything here is stable metadata; volatile transport fields are excluded.
CANONICAL_FIELDS = (
    'doi', 'title', 'year', 'series', 'volume', 'pages', 'isbn', 'publisher',
    'url', 'event_date_start', 'event_date_end', 'event_year', 'event_month',
    'event_date_confidence', 'event_name', 'event_location',
)

# Event type constants
TITLE_ADDED = 'title.added'
TITLE_UPDATED = 'title.updated'


def _normalise(value: Any) -> Any:
    """Coerce a value to a stable, hashable, JSON-serialisable form."""
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, str):
        return value.strip()
    return value


def canonical_record(pub: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the canonical, order-independent representation of a publication
    used both for the fingerprint and for the stored event payload.

    Authors are reduced to a sorted list of names so author ordering does not
    affect the fingerprint.
    """
    record = {field: _normalise(pub.get(field)) for field in CANONICAL_FIELDS}

    authors = pub.get('authors') or []
    names = []
    for a in authors:
        if isinstance(a, dict):
            name = a.get('name')
        else:
            name = a
        if name:
            names.append(str(name).strip())
    record['authors'] = sorted(n for n in names if n)

    return record


def payload_hash(pub: Dict[str, Any]) -> str:
    """Return the sha256 hex fingerprint of a record's canonical form."""
    canonical = canonical_record(pub)
    # sort_keys=True guarantees a deterministic byte sequence regardless of
    # dict insertion order; ensure_ascii keeps the encoding stable.
    blob = json.dumps(canonical, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()


def new_event_id() -> str:
    """A unique, sortable-ish identifier for an event row."""
    return uuid.uuid4().hex


def classify_event_type(pub: Dict[str, Any], existing_hash: Optional[str]) -> Optional[str]:
    """
    Decide what event (if any) a scraped record represents.

    Args:
        pub:           the scraped publication dict.
        existing_hash: the payload_hash last stored for this DOI, or None if
                       the DOI has never been seen.

    Returns:
        TITLE_ADDED   if the DOI is new,
        TITLE_UPDATED if the DOI is known but its fingerprint changed,
        None          if nothing changed (no event should be written).
    """
    current = payload_hash(pub)
    if existing_hash is None:
        return TITLE_ADDED
    if existing_hash != current:
        return TITLE_UPDATED
    return None
