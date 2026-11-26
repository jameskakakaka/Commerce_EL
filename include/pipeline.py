"""
Airflow-free runner for the whole pipeline.

Executes the same task functions the DAGs call, in the same order:
extract -> transform -> data-quality gate (quarantine) -> load.
Used by scripts/run_local.py, CI and the end-to-end test.
"""

from __future__ import annotations

import logging
from pathlib import Path

from include.config import parse_date_str, processed_path, quarantine_path, raw_path
from include.tasks.extract.scraper import FakeStoreTransactionScraper
from include.tasks.load.duckdb_load import load_processed_data
from include.tasks.quality.checks import quarantine, validate_file
from include.tasks.transform.processing import clean

logger = logging.getLogger(__name__)


def run_pipeline(fixtures_dir: Path | str | None = None) -> dict:
    """Run extract/transform/DQ/load. Pass fixtures_dir to run fully offline."""
    written = FakeStoreTransactionScraper(fixtures_dir=fixtures_dir).extract_transactions()
    dates = sorted(path.stem.split("_")[-1] for path in written)

    passed: list[str] = []
    quarantined: dict[str, list[str]] = {}
    for date_str in dates:
        run_date = parse_date_str(date_str)
        clean(raw_path(run_date), processed_path(run_date))
        violations = validate_file(processed_path(run_date))
        if violations:
            quarantine(processed_path(run_date), quarantine_path(run_date), violations)
            quarantined[date_str] = violations
        else:
            passed.append(date_str)

    loaded = [load_processed_data(d) for d in passed]
    summary = {
        "partitions": len(dates),
        "loaded_partitions": len(loaded),
        "rows_loaded": sum(r["rows"] for r in loaded),
        "quarantined": quarantined,
    }
    logger.info("Pipeline summary: %s", summary)
    return summary
