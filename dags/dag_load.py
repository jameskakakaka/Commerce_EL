"""
DAG 3: Load Pipeline (DuckDB + Data Gating)
"""
import json
import os
import pandas as pd
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException

# Import our modular load function
from include.tasks.load.duckdb_load import load_processed_data

PROCESSED_DATASET = Dataset("file://landing/processed_data")
Dataset("file://landing/processed_data")
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
LANDING_DIR = Path(os.getenv("LANDING_DIR", _PROJECT_ROOT / "landing"))


# Setting up the DAG for Airflow
@dag(
    dag_id = "03_fakeshop_load_duckdb",
    start_date = datetime(2024, 1, 1),
    schedule = [PROCESSED_DATASET],
    catchup = False,
    tags = ["fakeshop", "load", "dq-gating"],
)

def load_dag():

    @task()
    def get_dates_to_load() -> list[str]:
        """Reads the exact dates that were just processed."""
        raw = Variable.get("LAST_EXTRACT_DATES", default_var="[]")
        return json.loads(raw)

    @task()
    def validate_data_quality(date_str: str) -> str:
        """
        Reads the CSV and checks for critical data errors.
        This runs dynamically in parallel!
        """
        run_date = datetime.strptime(date_str, "%Y%m%d").date()
        csv_path = (
            LANDING_DIR
            / "processed"
            / run_date.strftime("%Y")
            / run_date.strftime("%m")
            / f"clean_data_{run_date.strftime('%Y%m%d')}.csv"
        )

        if not csv_path.exists():
            raise AirflowSkipException(f"Missing file for {date_str}. Skipping.")

        # Load the CSV into Pandas to inspect the data
        df = pd.read_csv(csv_path)

        # --- THE DATA CONTRACT ---
        # Rule 1: Revenue cannot be negative
        if (df['total_amount'] < 0).any():
            raise ValueError(f"🚨 DQ ALERT: Negative total_amount found in {date_str}!")
        
        # Rule 2: Critical IDs cannot be missing
        if df['transaction_id'].isnull().any():
            raise ValueError(f"🚨 DQ ALERT: Null transaction_id found in {date_str}!")

        print(f"✅ {date_str} passed all Data Quality checks.")
        return date_str

    @task()
    def load_all_dates(validated_dates: list[str]) -> list[str]:
        """Loads ONLY the validated dates sequentially to prevent DuckDB locks."""
        results = []
        for d in validated_dates:
            if d: # Double-check it wasn't skipped
                results.append(load_processed_data(d))
        return results

    # --- Wire up the Map-Reduce Execution ---
    dates = get_dates_to_load()
    
    # Step 1 (Map): Run DQ checks in parallel across all dates
    validated_dates = validate_data_quality.expand(date_str=dates)
    
    # Step 2 (Reduce): Load only the survivors sequentially
    load_all_dates(validated_dates)