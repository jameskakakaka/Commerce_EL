

"""
Transform raw API transactions into clean CSV

Reads from: /raw/YYYY/MM/raw_data_YYYYMMDD.csv
Writes to: /processed/YYYY/MM/clean_data_YYYYMMDD.csv
"""

# Importing necessary libraries 
import logging # Monitor the terminator 
import os # Control the pathways and systems of the files 
import argparse 
import pandas as pd # Parse the files 

from datetime import date, datetime, timedelta 
from pathlib import Path 

# Logging basics to monitor changes in the terminator 
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

from include.config import landing_dir, processed_path, raw_path

# Kept as aliases for the CLI below and for backwards compatibility.
get_input_path = raw_path
get_output_path = processed_path


# Run the cleaning pipeline on a raw CSV and write the cleaned output
def clean(input_path:Path, output_path:Path) -> Path:
    """
    Run the cleaning pipeline on a raw CSV and write the cleaned output
    """
    df = pd.read_csv(input_path)
    logger.info("Loaded %d rows from %s", len(df), input_path)

    # Apply the modular transformation steps 
    df = _clean_data_types(df)
    df = _drop_invalid_records(df)
    df = _add_sale_tier(df)
    df = _standardize_text(df)
    df = _add_updated_at(df)

    # Ensure the destination exists and save 
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    logger.info("Saved %d rows → %s", len(df), output_path)
    return output_path


# Transformation steps
def _clean_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures numeric columns are floats and IDs are strings
    """
    df['unit_price'] = df['unit_price'].astype(float)
    df['total_amount'] = df['total_amount'].astype(float)
    df['transaction_id'] = df['transaction_id'].astype(str)
    return df

def _drop_invalid_records(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows missing crititcal IDs
    """
    return df.dropna(subset=['transaction_id', 'product_id'])


def _add_sale_tier(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies business logics to categorize items and high-value items
    """
    df['sale_tier'] = df['total_amount'].apply(
        lambda x: 'High Value' if x >= 100.0 else 'Standard'
    )
    return df

def _standardize_text(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures categorical text is consistently cased
    """
    df['category'] = df['category'].str.upper()
    df['customer_name'] = df['customer_name'].str.title()
    return df

def _add_updated_at(df: pd.DataFrame) -> pd.DataFrame:
    """Adds data lineage metadata."""
    df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df

# CLI (Command Line Interface for Airflow)

if __name__ == "__main__":
    # Setting up the argument parser for the script 
    parser = argparse.ArgumentParser(description="Clean raw transactions data")
    # Adding the arguments for the run_date, yesterday, and all
    parser.add_argument("--run_date", type=date.fromisoformat, help="Run date YYYY-MM-DD")
    parser.add_argument("--yesterday", action="store_true", help="Process yesterday's data")
    parser.add_argument("--all", action="store_true", help="Process all raw files under landing/raw")
    # Parsing the arguments 
    args = parser.parse_args()
    
    # Check if the all argument is passed, process all the files 
    if args.all:
        raw_files = sorted((landing_dir() / "raw").glob("*/*/raw_data_*.csv")) # Get all the raw files under landing/raw   
        if not raw_files:
            raise FileNotFoundError("No raw data files found under landing/raw")

        # Process each file 
        for raw_file in raw_files:
            # Get the date from the file name 
            date_str = raw_file.stem.replace("raw_data_", "")
            run_date = datetime.strptime(date_str, "%Y%m%d").date()
            clean(raw_file, get_output_path(run_date)) # Clean the data 

    # Otherwise, process a specific date for Ariflow 
    else:
        if args.run_date is not None:
            run_date = args.run_date
        elif args.yesterday:
            run_date = date.today() - timedelta(days=1)
        else:
            parser.error("Provide --run_date, --yesterday, or --all")

        input_path = get_input_path(run_date)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        clean(input_path, get_output_path(run_date))