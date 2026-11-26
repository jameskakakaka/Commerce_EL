"""
DAG 3: Load (data-quality gate + DuckDB).

Triggered by the processed dataset.
  1. Map:    validate every processed partition in parallel against the data
             contract. Failing files are moved to landing/quarantine/ and the
             task is marked *skipped*, so one bad day never blocks the others.
  2. Reduce: load the partitions that passed, sequentially (DuckDB allows a
             single writer process), each in its own transaction.
"""

import json
import logging
from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

from include.config import DEFAULT_ARGS, parse_date_str, processed_path, quarantine_path
from include.tasks.load.duckdb_load import load_processed_data
from include.tasks.quality.checks import quarantine, validate_file

logger = logging.getLogger(__name__)

PROCESSED_DATASET = Dataset("file:///landing/processed")
WAREHOUSE_DATASET = Dataset("duckdb://warehouse/transactions")


@dag(
    dag_id="03_fakeshop_load_duckdb",
    start_date=datetime(2024, 1, 1),
    schedule=[PROCESSED_DATASET],
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["fakeshop", "load", "dq-gating"],
)
def load_dag():
    @task()
    def get_dates_to_load() -> list[str]:
        return json.loads(Variable.get("LAST_EXTRACT_DATES", default_var="[]"))

    @task()
    def validate_data_quality(date_str: str) -> str:
        """Check one partition against the data contract; quarantine it on failure."""
        run_date = parse_date_str(date_str)
        csv_path = processed_path(run_date)
        if not csv_path.exists():
            raise AirflowSkipException(f"No processed file for {date_str}")

        violations = validate_file(csv_path)
        if violations:
            dest = quarantine(csv_path, quarantine_path(run_date), violations)
            raise AirflowSkipException(f"{date_str} quarantined to {dest}: {violations}")

        logger.info("%s passed all data-quality checks", date_str)
        return date_str

    @task(trigger_rule="none_failed", outlets=[WAREHOUSE_DATASET])
    def load_all_dates(validated_dates: list[str]) -> list[dict]:
        """Load only the partitions that passed, one at a time."""
        dates = sorted(d for d in validated_dates if d)
        if not dates:
            raise AirflowSkipException("No partition passed the data-quality gate")
        return [load_processed_data(d) for d in dates]

    load_all_dates(validate_data_quality.expand(date_str=get_dates_to_load()))


load_dag()
