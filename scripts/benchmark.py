"""
Benchmark the pipeline's transform -> DQ -> load stages on synthetic data.

The FakeStore API only returns a few dozen line items, so this script generates
synthetic transactions with the *same raw schema* and runs the real task
functions on them in an isolated temp directory.

    python scripts/benchmark.py                       # 10k, 100k, 1M rows
    python scripts/benchmark.py --rows 50000 --days 10
    python scripts/benchmark.py --dbt                 # also time `dbt build`

Results are printed as JSON and written to docs/benchmark_results.json.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import tempfile
import time
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from include.config import parse_date_str, processed_path, raw_path  # noqa: E402
from include.tasks.extract.scraper import write_partitions  # noqa: E402
from include.tasks.load.duckdb_load import load_processed_data  # noqa: E402
from include.tasks.quality.checks import validate_file  # noqa: E402
from include.tasks.transform.processing import clean  # noqa: E402

CATEGORIES = np.array(["electronics", "jewelery", "men's clothing", "women's clothing"])


def synthetic_raw(n_rows: int, n_days: int, seed: int = 42) -> pd.DataFrame:
    """Vectorised generator producing the scraper's raw schema."""
    rng = np.random.default_rng(seed)
    n_products = 200
    prices = rng.uniform(5, 500, n_products).round(2)

    cart_id = np.arange(n_rows) // 3 + 1  # ~3 line items per cart
    product_id = (np.arange(n_rows) % 3) + 1 + (cart_id * 7) % (n_products - 3)
    quantity = rng.integers(1, 10, n_rows)
    unit_price = prices[product_id - 1]
    day = (cart_id % n_days).astype(int)
    dates = pd.Timestamp("2020-01-01") + pd.to_timedelta(day, unit="D")

    def ids(values: np.ndarray) -> pd.Series:
        return pd.Series(values).astype(str)

    return pd.DataFrame(
        {
            "transaction_id": "CART-" + ids(cart_id) + "-PROD-" + ids(product_id),
            "cart_id": cart_id,
            "transaction_date": dates.strftime("%Y-%m-%dT00:00:00.000Z"),
            "user_id": cart_id % 1000 + 1,
            "customer_name": "Synthetic User",
            "product_id": product_id,
            "product_title": "Synthetic Product, Size M",
            "category": CATEGORIES[product_id % len(CATEGORIES)],
            "unit_price": unit_price,
            "quantity": quantity,
            "total_amount": (unit_price * quantity).round(2),
        }
    )


def _timed(fn, *args, **kwargs):
    start = time.perf_counter()
    result = fn(*args, **kwargs)
    return result, time.perf_counter() - start


def run_once(n_rows: int, n_days: int, with_dbt: bool) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        os.environ["LANDING_DIR"] = str(Path(tmp) / "landing")
        os.environ["DUCKDB_PATH"] = str(Path(tmp) / "warehouse.duckdb")

        df = synthetic_raw(n_rows, n_days)
        written, t_write = _timed(write_partitions, df)
        dates = sorted(p.stem.split("_")[-1] for p in written)
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)

        t_transform = t_dq = t_load = 0.0
        for d in dates:
            run_date = parse_date_str(d)
            _, t = _timed(clean, raw_path(run_date), processed_path(run_date), now=now)
            t_transform += t
            violations, t = _timed(validate_file, processed_path(run_date))
            t_dq += t
            assert not violations, violations
            _, t = _timed(load_processed_data, d)
            t_load += t

        # Idempotency: reload every partition and check the row count is unchanged.
        _, t_reload = _timed(lambda: [load_processed_data(d) for d in dates])
        with duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True) as conn:
            rows_in_db = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        assert rows_in_db == n_rows, (rows_in_db, n_rows)

        result = {
            "rows": n_rows,
            "partitions": len(dates),
            "seconds": {
                "write_raw": round(t_write, 3),
                "transform": round(t_transform, 3),
                "dq_checks": round(t_dq, 3),
                "load": round(t_load, 3),
                "reload_all_idempotent": round(t_reload, 3),
            },
            "rows_per_second": {
                "transform": round(n_rows / t_transform),
                "dq_checks": round(n_rows / t_dq),
                "load": round(n_rows / t_load),
            },
            "rows_after_reload": rows_in_db,
            "duplicates_after_reload": rows_in_db - n_rows,
        }

        if with_dbt:
            sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
            from run_local import run_dbt

            _, t_dbt = _timed(run_dbt)
            result["seconds"]["dbt_build"] = round(t_dbt, 3)
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark transform/DQ/load stages")
    parser.add_argument("--rows", type=int, nargs="+", default=[10_000, 100_000, 1_000_000])
    parser.add_argument("--days", type=int, default=30, help="number of daily partitions")
    parser.add_argument("--dbt", action="store_true", help="also time dbt build")
    parser.add_argument("--out", type=Path, default=PROJECT_ROOT / "docs/benchmark_results.json")
    args = parser.parse_args()

    report = {
        "generated_at": date.today().isoformat(),
        "machine": {
            "os": f"{platform.system()} {platform.release()}",
            "cpu": platform.processor() or platform.machine(),
            "logical_cpus": os.cpu_count(),
            "python": platform.python_version(),
            "pandas": pd.__version__,
            "duckdb": duckdb.__version__,
        },
        "note": "Synthetic data in the raw FakeStore schema; single process, local disk.",
        "runs": [run_once(n, args.days, args.dbt) for n in args.rows],
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + "\n", "utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
