import json

import duckdb

from include.pipeline import run_pipeline


def _rows(db):
    with duckdb.connect(str(db), read_only=True) as conn:
        return conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]


def test_end_to_end_offline_run_is_idempotent(workspace, fixtures_dir):
    first = run_pipeline(fixtures_dir=fixtures_dir)
    second = run_pipeline(fixtures_dir=fixtures_dir)

    assert first == second
    assert first["partitions"] == 4
    assert first["quarantined"] == {}
    assert _rows(workspace["db"]) == first["rows_loaded"] == 14


def test_bad_partition_is_quarantined_and_others_still_load(workspace, fixtures_dir, tmp_path):
    # Copy the fixtures and reference a product that does not exist on 2020-01-01,
    # which the extractor prices at 0 -> breaks the unit_price > 0 contract.
    bad = tmp_path / "bad_fixtures"
    bad.mkdir()
    for name in ("users", "products", "carts"):
        data = json.loads((fixtures_dir / f"{name}.json").read_text("utf-8"))
        if name == "carts":
            for cart in data:
                if cart["date"].startswith("2020-01-01"):
                    cart["products"].append({"productId": 9999, "quantity": 1})
        (bad / f"{name}.json").write_text(json.dumps(data), "utf-8")

    summary = run_pipeline(fixtures_dir=bad)

    assert list(summary["quarantined"]) == ["20200101"]
    assert summary["loaded_partitions"] == 3
    quarantined = workspace["landing"] / "quarantine/2020/01/clean_data_20200101.csv"
    assert quarantined.exists()
    assert "unit_price" in quarantined.with_suffix(".reasons.txt").read_text()
    assert _rows(workspace["db"]) == summary["rows_loaded"]
