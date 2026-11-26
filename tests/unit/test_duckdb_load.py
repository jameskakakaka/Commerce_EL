from datetime import date, datetime, timezone

import duckdb
import pandas as pd
import pytest

from include.config import processed_path
from include.tasks.load.duckdb_load import SCHEMA, load_processed_data
from include.tasks.transform.processing import transform

DAY = date(2020, 3, 2)
DATE_STR = "20200302"


def _write_processed(rows, run_date=DAY):
    base = {
        "cart_id": 1,
        "transaction_date": f"{run_date.isoformat()}T00:00:00.000Z",
        "user_id": 1,
        "customer_name": "John Doe",
        "product_id": 1,
        "product_title": 'Backpack, "15 inch"',  # commas and quotes must survive
        "category": "bags",
        "unit_price": 10.0,
        "quantity": 1,
        "total_amount": 10.0,
    }
    df = pd.DataFrame([{**base, "transaction_id": tid} for tid in rows])
    out = transform(df, now=datetime(2026, 1, 1, tzinfo=timezone.utc))
    path = processed_path(run_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False)
    return path


def _query(db, sql):
    with duckdb.connect(str(db), read_only=True) as conn:
        return conn.execute(sql).fetchall()


def test_load_creates_table_with_contract_schema(workspace):
    _write_processed(["A", "B"])

    result = load_processed_data(DATE_STR)

    assert result == {"date": DATE_STR, "status": "loaded", "rows": 2}
    cols = _query(
        workspace["db"],
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'transactions' ORDER BY ordinal_position",
    )
    assert cols == [(n, t) for n, t in SCHEMA]
    assert _query(workspace["db"], "SELECT DISTINCT product_title FROM transactions") == [
        ('Backpack, "15 inch"',)
    ]


def test_reloading_a_date_is_idempotent(workspace):
    _write_processed(["A", "B", "C"])

    for _ in range(3):
        load_processed_data(DATE_STR)

    assert _query(workspace["db"], "SELECT COUNT(*) FROM transactions") == [(3,)]


def test_reload_replaces_only_that_partition(workspace):
    _write_processed(["A", "B"])
    _write_processed(["X"], run_date=date(2020, 1, 1))
    load_processed_data(DATE_STR)
    load_processed_data("20200101")

    _write_processed(["A"])  # the source shrank for 2020-03-02
    load_processed_data(DATE_STR)

    assert _query(
        workspace["db"], "SELECT transaction_date, COUNT(*) FROM transactions GROUP BY 1 ORDER BY 1"
    ) == [(date(2020, 1, 1), 1), (DAY, 1)]


def test_failed_load_rolls_back_and_keeps_previous_rows(workspace):
    _write_processed(["A", "B"])
    load_processed_data(DATE_STR)

    # A file under the 2020-03-02 partition that contains a row from another day.
    path = processed_path(DAY)
    df = pd.read_csv(path)
    df.loc[1, "transaction_date"] = "2020-01-01T00:00:00.000Z"
    df.to_csv(path, index=False)

    with pytest.raises(ValueError, match="do not belong"):
        load_processed_data(DATE_STR)

    assert _query(workspace["db"], "SELECT COUNT(*) FROM transactions") == [(2,)]


def test_missing_file_is_skipped(workspace):
    assert load_processed_data("20991231") == {"date": "20991231", "status": "skipped", "rows": 0}
    assert not workspace["db"].exists()
