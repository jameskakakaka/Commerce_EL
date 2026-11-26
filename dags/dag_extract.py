"""
DAG 1: Extract.

Pulls users, products and carts from the FakeStore API, flattens them into one
row per cart line item and writes date-partitioned raw CSVs:
landing/raw/YYYY/MM/raw_data_YYYYMMDD.csv
"""

import json
import logging
from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable

from include.config import DEFAULT_ARGS
from include.tasks.extract.scraper import FakeStoreTransactionScraper

logger = logging.getLogger(__name__)

RAW_DATASET = Dataset("file:///landing/raw")


@dag(
    dag_id="01_fakeshop_extract",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fakeshop", "extract"],
)
def extract_dag():
    @task(outlets=[RAW_DATASET])
    def extract_transactions() -> list[str]:
        """Run the scraper and publish the partition dates it wrote."""
        written_paths = FakeStoreTransactionScraper().extract_transactions()
        dates = sorted(path.stem.split("_")[-1] for path in written_paths)

        # Hand-off to the downstream (dataset-triggered) DAGs.
        # max_active_runs=1 on every DAG keeps this single slot from being overwritten mid-run.
        Variable.set("LAST_EXTRACT_DATES", json.dumps(dates))
        logger.info("Extracted %d partitions: %s", len(dates), dates)
        return dates

    extract_transactions()


extract_dag()
