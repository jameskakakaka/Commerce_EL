"""
Data contract for processed partitions.

`find_violations` is a pure function so the rules can be unit-tested without
Airflow. Files that break the contract are moved to the quarantine zone
together with a `.reasons.txt` file explaining why.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

AMOUNT_TOLERANCE = 0.01
CONTRACT_COLUMNS = (
    "transaction_id",
    "product_id",
    "unit_price",
    "quantity",
    "total_amount",
)


def find_violations(df: pd.DataFrame) -> list[str]:
    """Return a human-readable list of contract violations (empty list = pass)."""
    missing = [c for c in CONTRACT_COLUMNS if c not in df.columns]
    if missing:
        return [f"missing columns: {missing}"]
    if df.empty:
        return ["partition is empty"]

    expected_total = (df["unit_price"] * df["quantity"]).round(2)
    checks = {
        "null transaction_id": df["transaction_id"].isna(),
        "duplicate transaction_id": df["transaction_id"].duplicated(keep=False),
        "null product_id": df["product_id"].isna(),
        "negative total_amount": df["total_amount"] < 0,
        "non-positive quantity": df["quantity"] <= 0,
        "non-positive unit_price": df["unit_price"] <= 0,
        "total_amount != unit_price * quantity": (expected_total - df["total_amount"]).abs()
        > AMOUNT_TOLERANCE,
    }
    return [f"{name} ({int(mask.sum())} rows)" for name, mask in checks.items() if mask.any()]


def validate_file(csv_path: Path) -> list[str]:
    return find_violations(pd.read_csv(csv_path))


def quarantine(csv_path: Path, destination: Path, violations: list[str]) -> Path:
    """Move a failing partition out of the load path and record the reasons."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    os.replace(csv_path, destination)  # atomic on the same filesystem; overwrites
    destination.with_suffix(".reasons.txt").write_text("\n".join(violations) + "\n", "utf-8")
    logger.error("Quarantined %s -> %s: %s", csv_path, destination, violations)
    return destination
