import json
import logging
from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests

from include.tasks.extract.scraper import (
    RAW_COLUMNS,
    ExtractError,
    FakeStoreTransactionScraper,
    flatten_carts,
    write_partitions,
)


def _load(fixtures_dir, name):
    return json.loads((fixtures_dir / f"{name}.json").read_text("utf-8"))


@pytest.fixture
def api_data(fixtures_dir):
    return {name: _load(fixtures_dir, name) for name in ("users", "products", "carts")}


def test_flatten_carts_one_row_per_line_item(api_data):
    df = flatten_carts(api_data["users"], api_data["products"], api_data["carts"])

    expected_rows = sum(len(c["products"]) for c in api_data["carts"])
    assert len(df) == expected_rows
    assert list(df.columns) == RAW_COLUMNS
    assert df["transaction_id"].is_unique
    assert (df["total_amount"] == (df["unit_price"] * df["quantity"]).round(2)).all()


def test_flatten_carts_joins_names_and_products():
    users = [{"id": 7, "name": {"firstname": "ada", "lastname": "lovelace"}}]
    products = [{"id": 3, "title": "Widget", "price": 2.5, "category": "tools"}]
    carts = [
        {
            "id": 1,
            "userId": 7,
            "date": "2020-01-01T00:00:00.000Z",
            "products": [{"productId": 3, "quantity": 4}],
        }
    ]

    row = flatten_carts(users, products, carts).iloc[0]

    assert row["transaction_id"] == "CART-1-PROD-3"
    assert row["customer_name"] == "Ada Lovelace"
    assert row["total_amount"] == 10.0


def test_unknown_product_is_kept_with_zero_price_and_logged(caplog):
    carts = [
        {
            "id": 1,
            "userId": 1,
            "date": "2020-01-01T00:00:00.000Z",
            "products": [{"productId": 999, "quantity": 1}],
        }
    ]
    with caplog.at_level(logging.WARNING):
        df = flatten_carts([], [], carts)

    assert df.loc[0, "unit_price"] == 0.0
    assert df.loc[0, "customer_name"] == "Unknown User"
    assert "999" in caplog.text


def test_write_partitions_is_idempotent(workspace, api_data):
    df = flatten_carts(api_data["users"], api_data["products"], api_data["carts"])

    first = write_partitions(df)
    second = write_partitions(df)

    assert first == second
    assert len(first) == df["transaction_date"].str[:10].nunique()
    total = sum(len(pd.read_csv(p)) for p in first)
    assert total == len(df)  # overwritten, not appended
    assert all(p.is_relative_to(workspace["landing"] / "raw") for p in first)


def test_write_partitions_rejects_empty_frame(workspace):
    with pytest.raises(ExtractError):
        write_partitions(pd.DataFrame(columns=RAW_COLUMNS))


def test_fetch_raises_extract_error_on_http_failure():
    session = MagicMock()
    session.get.side_effect = requests.ConnectionError("boom")
    scraper = FakeStoreTransactionScraper(session=session)

    with pytest.raises(ExtractError, match="users"):
        scraper._fetch_api_endpoint("users")


@pytest.mark.parametrize("payload", [[], {"error": "nope"}])
def test_fetch_rejects_empty_or_non_list_payload(payload):
    session = MagicMock()
    session.get.return_value.json.return_value = payload
    scraper = FakeStoreTransactionScraper(session=session)

    with pytest.raises(ExtractError):
        scraper._fetch_api_endpoint("carts")


def test_extract_from_fixtures_end_to_end(workspace, fixtures_dir):
    written = FakeStoreTransactionScraper(fixtures_dir=fixtures_dir).extract_transactions()

    assert [p.name for p in written] == [
        "raw_data_20200101.csv",
        "raw_data_20200102.csv",
        "raw_data_20200301.csv",
        "raw_data_20200302.csv",
    ]
