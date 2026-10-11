"""
Backward-compatible entry point.

The scraping orchestration now lives in the `pipeline` package
(pipeline.runner). This thin shim keeps the classic invocation working:

    python main_crossref.py --series LNCS --year-start 2024

All logic (load_config, scrape_single_series, scrape_all_series, main, ...)
is imported from pipeline.runner, so behaviour is identical to before the
package restructure — only the import path changed.
"""

from pipeline.runner import (  # noqa: F401  (re-exported for compatibility)
    main,
    load_config,
    scrape_single_series,
    scrape_all_series,
    SilverResult,
)

if __name__ == '__main__':
    main()
