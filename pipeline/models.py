"""
SQLAlchemy ORM models — full medallion architecture in Neon PostgreSQL.

Schemas:
  bronze  — raw_responses      : raw API JSON, one row per scrape batch
  public  — publications       : silver layer (cleaned, structured)
            authors
            publication_authors
            series
            pipeline_runs
            quality_metrics
            events              : append-only change log (title.added / title.updated)
  gold    — series_stats       : per-series aggregations
            yearly_trends      : publications per year per series
            top_authors        : author publication counts

In-memory transport objects (used by the scraper before DB write):
  ScrapedAuthor, ScrapedPublication
"""

from dataclasses import dataclass, field, asdict as _dc_asdict
from typing import List, Optional as Opt
from datetime import datetime


# ---------------------------------------------------------------------------
# In-memory transport dataclasses (scraper -> DB writer)
# ---------------------------------------------------------------------------

@dataclass
class ScrapedAuthor:
    """Lightweight author record produced by the scraper."""
    name: str
    dblp_url: Opt[str] = None

    def to_dict(self):
        return _dc_asdict(self)


@dataclass
class ScrapedPublication:
    """Lightweight publication record produced by the scraper."""
    title: str
    authors: List[ScrapedAuthor]
    year: Opt[int] = None
    venue: Opt[str] = None
    series: Opt[str] = None
    volume: Opt[str] = None
    pages: Opt[str] = None
    doi: Opt[str] = None
    ee: Opt[str] = None
    isbn: Opt[str] = None
    publisher: str = "Springer"
    dblp_key: Opt[str] = None
    dblp_url: Opt[str] = None
    event_date_start: Opt[str] = None
    event_date_end: Opt[str] = None
    event_year: Opt[int] = None
    event_month: Opt[int] = None
    event_date_confidence: Opt[str] = None
    event_name: Opt[str] = None
    event_location: Opt[str] = None
    fetched_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self):
        data = _dc_asdict(self)
        data['authors'] = [a.to_dict() for a in self.authors]
        return data

    def __str__(self):
        names = ", ".join(a.name for a in self.authors[:3])
        if len(self.authors) > 3:
            names += " et al."
        return f"{self.title} - {names} ({self.year})"
from sqlalchemy import (
    Column, Integer, String, Text, Date, DateTime,
    ForeignKey, Table, Numeric, JSON, event, UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.schema import DDL

Base = declarative_base()

# ---------------------------------------------------------------------------
# Schema creation — fires before tables are created
# ---------------------------------------------------------------------------
event.listen(
    Base.metadata,
    'before_create',
    DDL('CREATE SCHEMA IF NOT EXISTS bronze'),
)
event.listen(
    Base.metadata,
    'before_create',
    DDL('CREATE SCHEMA IF NOT EXISTS gold'),
)


# ---------------------------------------------------------------------------
# BRONZE — raw API responses stored as JSON
# ---------------------------------------------------------------------------

class BronzeRawResponse(Base):
    """
    One row per scrape batch.
    Stores the full raw CrossRef API response as JSON so you can always
    re-process from the original source data.
    """
    __tablename__ = 'raw_responses'
    __table_args__ = {'schema': 'bronze'}

    id            = Column(Integer, primary_key=True, autoincrement=True)
    series_abbr   = Column(String(20),  nullable=False, index=True)
    series_name   = Column(String(200))
    fetched_at    = Column(DateTime, default=datetime.utcnow, index=True)
    record_count  = Column(Integer)
    raw_data      = Column(JSON, nullable=False)   # list of raw CrossRef items


# ---------------------------------------------------------------------------
# SILVER — cleaned, structured publication data (public schema)
# ---------------------------------------------------------------------------

# Many-to-many: publications ↔ authors
publication_authors = Table(
    'publication_authors',
    Base.metadata,
    Column('publication_id', Integer, ForeignKey('publications.id', ondelete='CASCADE'), primary_key=True),
    Column('author_id',      Integer, ForeignKey('authors.id',      ondelete='CASCADE'), primary_key=True),
    Column('author_order',   Integer),
)


class Publication(Base):
    __tablename__ = 'publications'

    id        = Column(Integer, primary_key=True, autoincrement=True)
    doi       = Column(String(255), unique=True, nullable=False, index=True)
    title     = Column(Text, nullable=False)
    year      = Column(Integer, index=True)
    series    = Column(String(100), index=True)
    volume    = Column(String(20))
    pages     = Column(String(50))
    isbn      = Column(String(50))
    publisher = Column(String(100), default='Springer')
    url       = Column(Text)

    event_date_start      = Column(Date)
    event_date_end        = Column(Date)
    event_year            = Column(Integer, index=True)
    event_month           = Column(Integer)
    event_date_confidence = Column(String(20))   # high / medium / low
    event_name            = Column(String(255))  # e.g. "ECCV 2024"
    event_location        = Column(String(255))  # e.g. "Milan, Italy"

    fetched_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    authors = relationship('Author', secondary=publication_authors, back_populates='publications')

    def to_dict(self):
        return {
            'id':                    self.id,
            'doi':                   self.doi,
            'title':                 self.title,
            'year':                  self.year,
            'series':                self.series,
            'volume':                self.volume,
            'pages':                 self.pages,
            'isbn':                  self.isbn,
            'publisher':             self.publisher,
            'url':                   self.url,
            'event_date_start':      self.event_date_start.isoformat() if self.event_date_start else None,
            'event_date_end':        self.event_date_end.isoformat()   if self.event_date_end   else None,
            'event_year':            self.event_year,
            'event_month':           self.event_month,
            'event_date_confidence': self.event_date_confidence,
            'event_name':            self.event_name,
            'event_location':        self.event_location,
            'authors':               [{'name': a.name} for a in self.authors],
            'fetched_at':            self.fetched_at.isoformat() if self.fetched_at else None,
        }


class Author(Base):
    __tablename__ = 'authors'

    id              = Column(Integer, primary_key=True, autoincrement=True)
    name            = Column(String(255), nullable=False, index=True)
    normalized_name = Column(String(255), index=True)

    publications = relationship('Publication', secondary=publication_authors, back_populates='authors')


class Series(Base):
    __tablename__ = 'series'

    abbreviation       = Column(String(50), primary_key=True)
    full_name          = Column(String(255), nullable=False)
    issn               = Column(String(20))
    issn_electronic    = Column(String(20))
    springer_series_id = Column(String(50))
    springer_url       = Column(Text)
    description        = Column(Text)

    def to_dict(self):
        return {
            'abbreviation':    self.abbreviation,
            'full_name':       self.full_name,
            'issn':            self.issn,
            'issn_electronic': self.issn_electronic,
            'springer_url':    self.springer_url,
        }


class QualityMetric(Base):
    __tablename__ = 'quality_metrics'

    id                      = Column(Integer, primary_key=True, autoincrement=True)
    series                  = Column(String(200), index=True)
    total_records           = Column(Integer)
    doi_coverage_pct        = Column(Numeric(5, 2))
    event_date_coverage_pct = Column(Numeric(5, 2))  # has event_date_start (day-level)
    event_year_coverage_pct = Column(Numeric(5, 2))  # has event_year (from published-print)
    author_coverage_pct     = Column(Numeric(5, 2))
    validated_at            = Column(DateTime, default=datetime.utcnow, index=True)


class PipelineRun(Base):
    __tablename__ = 'pipeline_runs'

    id              = Column(Integer, primary_key=True, autoincrement=True)
    run_type        = Column(String(50))   # full / incremental / manual
    series          = Column(String(255))
    records_fetched = Column(Integer)
    records_new     = Column(Integer)
    status          = Column(String(20), index=True)   # success / partial / failed
    error_message   = Column(Text)
    started_at      = Column(DateTime, index=True)
    completed_at    = Column(DateTime)

    def to_dict(self):
        return {
            'id':              self.id,
            'run_type':        self.run_type,
            'series':          self.series,
            'records_fetched': self.records_fetched,
            'records_new':     self.records_new,
            'status':          self.status,
            'started_at':      self.started_at.isoformat()   if self.started_at   else None,
            'completed_at':    self.completed_at.isoformat() if self.completed_at else None,
        }


# ---------------------------------------------------------------------------
# EVENTS — append-only change log (the "diary" / event-sourcing projection)
# ---------------------------------------------------------------------------
#
# One row per OBSERVED CHANGE to a title, never edited or deleted:
#   title.added    — a DOI seen for the first time
#   title.updated  — a DOI seen before, but its canonical payload changed
#
# The UNIQUE(source, source_record_id, payload_hash) constraint makes the log
# idempotent BY SCHEMA: re-running the pipeline over unchanged data cannot
# create a duplicate event, because the identical (doi, fingerprint) pair is
# rejected. Change detection is therefore an addition from an update, proven
# by the database itself rather than by application logic.

class TitleEvent(Base):
    __tablename__ = 'events'
    __table_args__ = (
        # Idempotency is enforced here: the same (source, doi, fingerprint)
        # triple can never be inserted twice. NOTE: per-column indexes come
        # from index=True on the columns below; only indexes that do NOT map
        # 1:1 to a single column belong here (to avoid DuplicateTable).
        UniqueConstraint('source', 'source_record_id', 'payload_hash',
                         name='uq_events_source_record_hash'),
    )

    id              = Column(Integer, primary_key=True, autoincrement=True)
    event_id        = Column(String(64), unique=True, index=True)   # uuid4 hex
    event_type      = Column(String(20), nullable=False, index=True)  # title.added | title.updated
    source          = Column(String(50), default='crossref')        # data source / publisher connector
    source_record_id= Column(String(255), nullable=False, index=True)  # the DOI
    series          = Column(String(100), index=True)
    publisher       = Column(String(100))
    payload_hash    = Column(String(64), nullable=False)            # sha256 of canonical record
    payload         = Column(JSON)                                  # the canonical record snapshot
    published_at    = Column(Date)                                  # publication/event date, if known
    observed_at     = Column(DateTime)                              # when the source produced the data
    detected_at     = Column(DateTime, default=datetime.utcnow, index=True)  # when WE noticed it
    delivered_at    = Column(DateTime, nullable=True)               # when served to a consumer (latency)

    def to_dict(self):
        return {
            'event_id':        self.event_id,
            'event_type':      self.event_type,
            'source':          self.source,
            'doi':             self.source_record_id,
            'series':          self.series,
            'publisher':       self.publisher,
            'payload_hash':    self.payload_hash,
            'payload':         self.payload,
            'published_at':    self.published_at.isoformat()   if self.published_at else None,
            'observed_at':     self.observed_at.isoformat()    if self.observed_at  else None,
            'detected_at':     self.detected_at.isoformat()    if self.detected_at  else None,
            'delivered_at':    self.delivered_at.isoformat()   if self.delivered_at else None,
        }


# ---------------------------------------------------------------------------
# PIPELINE STATE — the high-watermark (the "bookmark") for incremental runs
# ---------------------------------------------------------------------------
#
# One row per series storing last_success_at: the moment of the last SUCCESSFUL
# incremental run. The next incremental run asks the source for only what
# changed after this timestamp (CrossRef from-created-date), instead of
# re-scanning the whole current year. The watermark advances ONLY after a
# successful run, so a failed run re-tries the same window and never loses data.

class PipelineState(Base):
    __tablename__ = 'pipeline_state'

    series          = Column(String(100), primary_key=True)
    last_success_at = Column(DateTime, nullable=False, index=True)
    updated_at      = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'series':          self.series,
            'last_success_at': self.last_success_at.isoformat() if self.last_success_at else None,
            'updated_at':      self.updated_at.isoformat()      if self.updated_at      else None,
        }


# ---------------------------------------------------------------------------
# GOLD — pre-computed aggregations (analytics-ready)
# ---------------------------------------------------------------------------

class GoldSeriesStat(Base):
    """Per-series summary — recomputed after every scrape."""
    __tablename__ = 'series_stats'
    __table_args__ = {'schema': 'gold'}

    series                = Column(String(100), primary_key=True)
    total_publications    = Column(Integer)
    total_unique_authors  = Column(Integer)
    year_min              = Column(Integer)
    year_max              = Column(Integer)
    avg_authors_per_paper = Column(Numeric(5, 2))
    computed_at           = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'series':                self.series,
            'total_publications':    self.total_publications,
            'total_unique_authors':  self.total_unique_authors,
            'year_min':              self.year_min,
            'year_max':              self.year_max,
            'avg_authors_per_paper': float(self.avg_authors_per_paper) if self.avg_authors_per_paper else 0,
            'computed_at':           self.computed_at.isoformat() if self.computed_at else None,
        }


class GoldYearlyTrend(Base):
    """Publications per year per series — powers trend charts."""
    __tablename__ = 'yearly_trends'
    __table_args__ = {'schema': 'gold'}

    series            = Column(String(100), primary_key=True)
    year              = Column(Integer,     primary_key=True)
    publication_count = Column(Integer)
    computed_at       = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'series':            self.series,
            'year':              self.year,
            'publication_count': self.publication_count,
        }


class GoldTopAuthor(Base):
    """Top authors by publication count per series."""
    __tablename__ = 'top_authors'
    __table_args__ = {'schema': 'gold'}

    id                = Column(Integer, primary_key=True, autoincrement=True)
    series            = Column(String(100), index=True)
    author_name       = Column(String(255))
    publication_count = Column(Integer)
    computed_at       = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'series':            self.series,
            'author_name':       self.author_name,
            'publication_count': self.publication_count,
        }
