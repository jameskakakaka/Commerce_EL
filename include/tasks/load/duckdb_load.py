"""
Load processed CSV partitions into the DuckDB warehouse.

Each partition is loaded with delete-then-insert inside a single transaction,
so re-running a date replaces its rows (idempotent) and a failed load leaves
the previous data untouched (atomic).
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from include.config import duckdb_path, parse_date_str, processed_path

logger = logging.getLogger(__name__)

TABLE = "transactions"

# Column order and types of the raw warehouse table (the dbt source).
SCHEMA: list[tuple[str, str]] = [
    ("transaction_id", "VARCHAR"),
    ("cart_id", "INTEGER"),
    ("transaction_date", "DATE"),
    ("user_id", "INTEGER"),
    ("customer_name", "VARCHAR"),
    ("product_id", "INTEGER"),
    ("product_title", "VARCHAR"),
    ("category", "VARCHAR"),
    ("unit_price", "DOUBLE"),
    ("quantity", "INTEGER"),
    ("total_amount", "DOUBLE"),
    ("sale_tier", "VARCHAR"),
    ("updated_at", "TIMESTAMP"),
]

_COLUMNS = ", ".join(name for name, _ in SCHEMA)
_CREATE_SQL = f"CREATE TABLE IF NOT EXISTS {TABLE} ({', '.join(f'{n} {t}' for n, t in SCHEMA)})"


def _select_expr(name: str, sql_type: str) -> str:
    # The CSV is read as all-VARCHAR and cast explicitly, so the table contract does
    # not depend on DuckDB's type sniffing. ISO timestamps ('2020-03-02T00:00:00.000Z')
    # are truncated to the calendar date to avoid timezone-dependent casts.
    if name == "transaction_date":
        return "CAST(LEFT(transaction_date, 10) AS DATE)"
    return f"CAST({name} AS {sql_type})"


_INSERT_SQL = (
    f"INSERT INTO {TABLE} ({_COLUMNS}) "
    f"SELECT {', '.join(_select_expr(n, t) for n, t in SCHEMA)} "
    "FROM read_csv(?, header = true, all_varchar = true)"
)


def load_processed_data(date_str: str, db_path: Path | None = None) -> dict:
    """Load one day's processed CSV. Returns a small summary for logs/XCom."""
    run_date = parse_date_str(date_str)
    csv_path = processed_path(run_date)
    if not csv_path.exists():
        logger.warning("No processed file for %s at %s", date_str, csv_path)
        return {"date": date_str, "status": "skipped", "rows": 0}

    db_path = Path(db_path) if db_path else duckdb_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(_CREATE_SQL)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(f"DELETE FROM {TABLE} WHERE transaction_date = ?", [run_date])
            conn.execute(_INSERT_SQL, [str(csv_path)])
            # Every row in the file must belong to the partition being loaded,
            # otherwise the next re-run of a *different* date would not clean it up.
            foreign = conn.execute(
                f"SELECT COUNT(*) FROM read_csv(?, header = true, all_varchar = true) "
                f"WHERE CAST(LEFT(transaction_date, 10) AS DATE) <> ?",
                [str(csv_path), run_date],
            ).fetchone()[0]
            if foreign:
                raise ValueError(f"{foreign} rows in {csv_path.name} do not belong to {run_date}")
            rows = conn.execute(
                f"SELECT COUNT(*) FROM {TABLE} WHERE transaction_date = ?", [run_date]
            ).fetchone()[0]
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    logger.info("Loaded %d rows into %s for %s", rows, TABLE, date_str)
    return {"date": date_str, "status": "loaded", "rows": rows}
