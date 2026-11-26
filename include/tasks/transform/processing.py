"""
Transform raw transactions into clean CSVs.

Reads from:  landing/raw/YYYY/MM/raw_data_YYYYMMDD.csv
Writes to:   landing/processed/YYYY/MM/clean_data_YYYYMMDD.csv
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from include.config import landing_dir, processed_path, raw_path

logger = logging.getLogger(__name__)

HIGH_VALUE_THRESHOLD = 100.0
REQUIRED_COLUMNS = {
    "transaction_id",
    "cart_id",
    "transaction_date",
    "user_id",
    "customer_name",
    "product_id",
    "category",
    "unit_price",
    "quantity",
    "total_amount",
}

# Kept as aliases for backwards compatibility.
get_input_path = raw_path
get_output_path = processed_path


def clean(input_path: Path, output_path: Path, now: datetime | None = None) -> Path:
    """Run the cleaning pipeline on a raw CSV and write the cleaned output."""
    df = pd.read_csv(input_path)
    logger.info("Loaded %d rows from %s", len(df), input_path)

    df = transform(df, now=now)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Saved %d rows -> %s", len(df), output_path)
    return output_path


def transform(df: pd.DataFrame, now: datetime | None = None) -> pd.DataFrame:
    """Pure transformation: DataFrame in, cleaned DataFrame out."""
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Raw data is missing columns: {sorted(missing)}")

    df = _drop_invalid_records(df.copy())
    df = _clean_data_types(df)
    df = _add_sale_tier(df)
    df = _standardize_text(df)
    return _add_updated_at(df, now)


def _drop_invalid_records(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows missing critical IDs.

    Must run before casting to str, otherwise NaN becomes the literal
    string 'nan' and the row survives.
    """
    df["transaction_id"] = df["transaction_id"].replace(r"^\s*$", np.nan, regex=True)
    before = len(df)
    df = df.dropna(subset=["transaction_id", "product_id"])
    if dropped := before - len(df):
        logger.warning("Dropped %d rows with missing transaction_id/product_id", dropped)
    return df


def _clean_data_types(df: pd.DataFrame) -> pd.DataFrame:
    df["transaction_id"] = df["transaction_id"].astype(str).str.strip()
    df["product_id"] = df["product_id"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    df["total_amount"] = df["total_amount"].astype(float)
    return df


def _add_sale_tier(df: pd.DataFrame) -> pd.DataFrame:
    df["sale_tier"] = np.where(df["total_amount"] >= HIGH_VALUE_THRESHOLD, "High Value", "Standard")
    return df


def _standardize_text(df: pd.DataFrame) -> pd.DataFrame:
    df["category"] = df["category"].str.strip().str.upper()
    df["customer_name"] = df["customer_name"].str.strip().str.title()
    return df


def _add_updated_at(df: pd.DataFrame, now: datetime | None = None) -> pd.DataFrame:
    """Lineage metadata: when this row was processed (UTC)."""
    now = now or datetime.now(timezone.utc)
    df["updated_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
    return df


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    parser = argparse.ArgumentParser(description="Clean raw transactions data")
    parser.add_argument("--run_date", type=date.fromisoformat, help="Run date YYYY-MM-DD")
    parser.add_argument("--yesterday", action="store_true", help="Process yesterday's data")
    parser.add_argument("--all", action="store_true", help="Process all raw files")
    args = parser.parse_args()

    if args.all:
        raw_files = sorted((landing_dir() / "raw").glob("*/*/raw_data_*.csv"))
        if not raw_files:
            raise FileNotFoundError("No raw data files found under landing/raw")
        for raw_file in raw_files:
            run_date = datetime.strptime(raw_file.stem.replace("raw_data_", ""), "%Y%m%d").date()
            clean(raw_file, processed_path(run_date))
        return

    if args.run_date is not None:
        run_date = args.run_date
    elif args.yesterday:
        run_date = date.today() - timedelta(days=1)
    else:
        parser.error("Provide --run_date, --yesterday, or --all")

    input_path = raw_path(run_date)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    clean(input_path, processed_path(run_date))


if __name__ == "__main__":
    main()
