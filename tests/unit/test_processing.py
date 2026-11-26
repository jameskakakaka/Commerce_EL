from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from include.tasks.transform.processing import HIGH_VALUE_THRESHOLD, clean, transform

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _raw(**overrides):
    row = {
        "transaction_id": "CART-1-PROD-1",
        "cart_id": 1,
        "transaction_date": "2020-03-02T00:00:00.000Z",
        "user_id": 1,
        "customer_name": "  john doe ",
        "product_id": 1,
        "product_title": "Backpack",
        "category": " men's clothing",
        "unit_price": 50.0,
        "quantity": 1,
        "total_amount": 50.0,
    }
    row.update(overrides)
    return row


def test_rows_with_missing_ids_are_dropped():
    # Regression: astype(str) used to turn NaN into 'nan' before dropna ran.
    df = pd.DataFrame(
        [
            _raw(),
            _raw(transaction_id=np.nan),
            _raw(transaction_id="   "),
            _raw(transaction_id="CART-9-PROD-9", product_id=np.nan),
        ]
    )

    out = transform(df, now=NOW)

    assert out["transaction_id"].tolist() == ["CART-1-PROD-1"]
    assert "nan" not in out["transaction_id"].str.lower().tolist()


@pytest.mark.parametrize(
    ("amount", "tier"),
    [(HIGH_VALUE_THRESHOLD - 0.01, "Standard"), (HIGH_VALUE_THRESHOLD, "High Value")],
)
def test_sale_tier_threshold(amount, tier):
    out = transform(pd.DataFrame([_raw(total_amount=amount)]), now=NOW)
    assert out.loc[0, "sale_tier"] == tier


def test_text_is_standardised_and_lineage_added():
    out = transform(pd.DataFrame([_raw()]), now=NOW)

    assert out.loc[0, "category"] == "MEN'S CLOTHING"
    assert out.loc[0, "customer_name"] == "John Doe"
    assert out.loc[0, "updated_at"] == "2026-01-01 12:00:00"


def test_transform_does_not_mutate_input():
    df = pd.DataFrame([_raw()])
    before = df.copy()
    transform(df, now=NOW)
    pd.testing.assert_frame_equal(df, before)


def test_missing_columns_raise():
    with pytest.raises(ValueError, match="unit_price"):
        transform(pd.DataFrame([_raw()]).drop(columns=["unit_price"]))


def test_clean_writes_output_file(tmp_path):
    src = tmp_path / "raw.csv"
    dst = tmp_path / "nested" / "clean.csv"
    pd.DataFrame([_raw(), _raw(transaction_id="CART-1-PROD-2")]).to_csv(src, index=False)

    clean(src, dst, now=NOW)

    out = pd.read_csv(dst)
    assert len(out) == 2
    assert {"sale_tier", "updated_at"} <= set(out.columns)
