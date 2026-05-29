

'''
DAG 1 : Extract pipeline (Taskflow API)
'''
# Importing necessary libraries 
import json
import os 
from datetime import datetime 
from airflow.decorators import dag, task 
from airflow.models import Variable 
from airflow.datasets import Dataset 

# Importing from scraper 
from include.tasks.extract.scraper import FakeStoreTransactionScraper 

# Setting up the directory for the raw dataset
RAW_DATASET = Dataset("file://landing/raw_data")

# Setting up @dag 
@dag(
    dag_id="01_fakeshop_extract",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["fakeshop", "extract", "production"],
)

# Defining the extract_dag function
def extract_dag():

    # Setting up @task 
    @task()
    def extract_transactions() -> list[str]:
        """
        Run the Python scraper and returns the dates downloaded from the sources
        """
        # Path for extracting 
        scraper = FakeStoreTransactionScraper()
        written_paths = scraper.extract_transactions()

        # Dates for downloading 
        dates = [path.stem.split('_')[-1] for path in written_paths]
        dates.sort()

        # Variables 
        Variable.set("LAST_EXTRACT_DATES", json.dumps(dates))
        return dates 


    # Uploading the files 
    @task(outlets=[RAW_DATASET])
    def upload_raw(written_dates: list[str]):
        """
        Upload raw files to S3. Skipps automatically if running in local 
        """
        # Toggle 
        if os.getenv("STORAGE_MODE", "local") == "local":
            print("STORAGE_MODE is 'local'. Skipping AWS S3 upload.")
            return []
            
        # Placeholder for future AWS s3
        print(f"STORAGE_MODE is 's3'. Uploading {len(written_dates)} files to AWS bucket...")
        return written_dates

    # Wire up the execution (Chain the tasks)
    dates = extract_transactions()
    upload_raw(dates)

extract_dag()
