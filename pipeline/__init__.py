"""
Springer proceedings increment-monitoring pipeline.

Medallion (Bronze -> Silver -> Gold) batch data pipeline that monitors new
conference-proceedings volumes from Springer series (LNCS, CCIS, LNAI, ...)
via the public CrossRef API, stores them in PostgreSQL (Neon), and serves
"prírastky" (increments) to users through a REST API.

Package layout
--------------
pipeline.models        SQLAlchemy ORM models + in-memory transport dataclasses
pipeline.db            DatabaseConfig — engine/session from environment
pipeline.event_dates   ConferenceDateExtractor — regex event-date mining
pipeline.scraper       CrossRefScraper — CrossRef REST client (cursor paging)
pipeline.storage       DataLakeStorageHandler — Bronze/Silver/Gold file layers
pipeline.gold          Gold aggregations + data-quality metrics
pipeline.runner        Scrape orchestration + Silver/Gold DB writes (the ETL)

Entry points live at the repository root so deployment is unchanged:
    orchestrator.py            python orchestrator.py [--incremental]
    init_database.py           python init_database.py
    api_server.py              gunicorn api_server:app
    main_crossref.py           python main_crossref.py   (thin shim)
"""

__version__ = "1.0.0"
