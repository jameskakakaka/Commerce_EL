"""
Central path configuration for the pipeline.

Every module resolves file locations through these helpers, so the landing zone
and the DuckDB warehouse can be relocated with environment variables:

- LANDING_DIR  : root of the date-partitioned file landing zone
- DUCKDB_PATH  : path to the DuckDB warehouse file

Values are read on every call (not at import time) so tests and containers can
override them via the environment.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path

# include/config.py -> parents[1] is the project root (also /usr/local/airflow in Astro)
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def landing_dir() -> Path:
    return Path(os.getenv("LANDING_DIR", str(PROJECT_ROOT / "landing")))


def duckdb_path() -> Path:
    return Path(os.getenv("DUCKDB_PATH", str(PROJECT_ROOT / "warehouse" / "fakeshop.duckdb")))


def _partition(zone: str, prefix: str, run_date: date) -> Path:
    return (
        landing_dir()
        / zone
        / run_date.strftime("%Y")
        / run_date.strftime("%m")
        / f"{prefix}_{run_date.strftime('%Y%m%d')}.csv"
    )


def raw_path(run_date: date) -> Path:
    """landing/raw/YYYY/MM/raw_data_YYYYMMDD.csv"""
    return _partition("raw", "raw_data", run_date)


def processed_path(run_date: date) -> Path:
    """landing/processed/YYYY/MM/clean_data_YYYYMMDD.csv"""
    return _partition("processed", "clean_data", run_date)


def quarantine_path(run_date: date) -> Path:
    """landing/quarantine/YYYY/MM/clean_data_YYYYMMDD.csv"""
    return _partition("quarantine", "clean_data", run_date)


def parse_date_str(date_str: str) -> date:
    """Parse the YYYYMMDD partition key used across DAGs."""
    return datetime.strptime(date_str, "%Y%m%d").date()


# Shared Airflow default_args (plain dict, so this module stays Airflow-free).
DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}
