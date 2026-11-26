import pandas as pd
import pytest

from include.tasks.quality.checks import find_violations, quarantine, validate_file


def _good():
    return pd.DataFrame(
        {
            "transaction_id": ["A", "B"],
            "product_id": [1, 2],
            "unit_price": [10.0, 2.5],
            "quantity": [2, 3],
            "total_amount": [20.0, 7.5],
        }
    )


def test_valid_partition_passes():
    assert find_violations(_good()) == []


@pytest.mark.parametrize(
    ("column", "value", "rule"),
    [
        ("transaction_id", None, "null transaction_id"),
        ("transaction_id", "A", "duplicate transaction_id"),
        ("product_id", None, "null product_id"),
        ("total_amount", -1.0, "negative total_amount"),
        ("quantity", 0, "non-positive quantity"),
        ("unit_price", 0.0, "non-positive unit_price"),
        ("total_amount", 99.0, "total_amount != unit_price * quantity"),
    ],
)
def test_each_rule_is_enforced(column, value, rule):
    df = _good()
    df.loc[1, column] = value
    violations = find_violations(df)
    assert any(v.startswith(rule) for v in violations), violations


def test_missing_columns_and_empty_partition():
    assert find_violations(_good().drop(columns=["quantity"]))[0].startswith("missing columns")
    assert find_violations(_good().iloc[0:0]) == ["partition is empty"]


def test_quarantine_moves_file_and_records_reasons(tmp_path):
    src = tmp_path / "processed" / "clean.csv"
    src.parent.mkdir()
    bad = _good()
    bad.loc[0, "total_amount"] = -5
    bad.to_csv(src, index=False)

    violations = validate_file(src)
    dest = quarantine(src, tmp_path / "quarantine" / "clean.csv", violations)

    assert not src.exists()
    assert dest.exists()
    assert "negative total_amount" in dest.with_suffix(".reasons.txt").read_text()
