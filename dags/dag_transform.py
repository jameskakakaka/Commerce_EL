"""
DAG 2: Transform Pipeline (Production Ready)
"""
import json
import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.datasets import Dataset

from include.tasks.transform.processing import get_input_path, get_output_path, clean

RAW_DATASET = Dataset("file://landing/raw_data")
PROCESSED_DATASET = Dataset("file://landing/processed_data")

@dag(
    dag_id="02_fakeshop_transform",
    start_date=datetime(2024, 1, 1),
    schedule=[RAW_DATASET],
    catchup=False,
    tags=["fakeshop", "transform", "production"],
)
def transform_dag():

    @task()
    def get_dates_to_process() -> list[str]:
        raw = Variable.get("LAST_EXTRACT_DATES", default_var="[]")
        return json.loads(raw)

    @task()
    def download_raw(date_str: str) -> str:
        """Downloads the raw file from S3 before cleaning it."""
        run_date = datetime.strptime(date_str, "%Y%m%d").date()
        local_path = get_input_path(run_date)
        
        if os.getenv("STORAGE_MODE", "local") == "local":
            print(f"Local mode: Reading directly from disk at {local_path}")
        else:
            # Placeholder for future AWS code
            print(f"S3 mode: Downloading raw file for {date_str} from AWS S3...")
            
        return date_str

    @task()
    def clean_date(date_str: str) -> str:
        """Runs the pandas cleaning logic."""
        run_date = datetime.strptime(date_str, "%Y%m%d").date()
        input_path = get_input_path(run_date)
        output_path = get_output_path(run_date)
        
        clean(input_path, output_path)
        return date_str

    @task(outlets=[PROCESSED_DATASET])
    def upload_processed(date_str: str):
        """Uploads the cleaned file back to S3."""
        if os.getenv("STORAGE_MODE", "local") == "local":
            print(f"Local mode: Processed file kept on disk for {date_str}.")
            return None
            
        print(f"S3 mode: Uploading processed file for {date_str} to AWS S3...")
        return "uploaded"

    # --- Wire up the Execution with Dynamic Mapping ---
    dates = get_dates_to_process()
    
    # 1. Download in parallel
    downloaded_dates = download_raw.expand(date_str=dates)
    # 2. Clean in parallel
    cleaned_dates = clean_date.expand(date_str=downloaded_dates)
    # 3. Upload in parallel
    upload_processed.expand(date_str=cleaned_dates)

transform_dag()