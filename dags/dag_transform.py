"""
DAG 2: Transform.

Triggered by the raw dataset. Cleans every partition written by the last
extract in parallel (dynamic task mapping) and writes:
landing/processed/YYYY/MM/clean_data_YYYYMMDD.csv
"""

import json
import logging
from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable

from include.config import DEFAULT_ARGS, parse_date_str, processed_path, raw_path
from include.tasks.transform.processing import clean

logger = logging.getLogger(__name__)

RAW_DATASET = Dataset("file:///landing/raw")
PROCESSED_DATASET = Dataset("file:///landing/processed")


@dag(
    dag_id="02_fakeshop_transform",
    start_date=datetime(2024, 1, 1),
    schedule=[RAW_DATASET],
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fakeshop", "transform"],
)
def transform_dag():
    @task()
    def get_dates_to_process() -> list[str]:
        return json.loads(Variable.get("LAST_EXTRACT_DATES", default_var="[]"))

    @task()
    def clean_date(date_str: str) -> str:
        """Run the pandas cleaning logic for one partition."""
        run_date = parse_date_str(date_str)
        clean(raw_path(run_date), processed_path(run_date))
        return date_str

    @task(outlets=[PROCESSED_DATASET])
    def publish_processed(cleaned: list[str]) -> list[str]:
        """Single fan-in task so the load DAG is triggered once per transform run."""
        dates = sorted(cleaned)
        logger.info("Processed %d partitions: %s", len(dates), dates)
        return dates

    publish_processed(clean_date.expand(date_str=get_dates_to_process()))


transform_dag()
