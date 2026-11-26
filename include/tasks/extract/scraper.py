"""
Extract FakeStore API data into date-partitioned raw CSVs.

Joins users + products onto cart line items (one row per product in a cart) and
writes one file per cart date:
landing/raw/YYYY/MM/raw_data_YYYYMMDD.csv
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from include.config import raw_path

logger = logging.getLogger(__name__)

RAW_COLUMNS = [
    "transaction_id",
    "cart_id",
    "transaction_date",
    "user_id",
    "customer_name",
    "product_id",
    "product_title",
    "category",
    "unit_price",
    "quantity",
    "total_amount",
]

ENDPOINTS = ("users", "products", "carts")


class ExtractError(RuntimeError):
    """Raised when the source cannot be read or returns unusable data."""


def build_session(retries: int = 3, backoff_factor: float = 1.0) -> requests.Session:
    """HTTP session with exponential backoff on transient errors (429/5xx)."""
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "fakeshop-pipeline/1.0"
    return session


def flatten_carts(
    users: list[dict[str, Any]],
    products: list[dict[str, Any]],
    carts: list[dict[str, Any]],
) -> pd.DataFrame:
    """Denormalise carts into one row per cart line item."""
    user_lookup = {
        u["id"]: f"{u['name']['firstname']} {u['name']['lastname']}".title() for u in users
    }
    product_lookup = {p["id"]: p for p in products}

    rows: list[dict[str, Any]] = []
    unknown_products: set[int] = set()
    for cart in carts:
        for item in cart["products"]:
            product_id = item["productId"]
            product = product_lookup.get(product_id)
            if product is None:
                # Keep the row so the data-quality gate can quarantine it
                # (unit_price = 0 breaks the contract) instead of dropping it silently.
                unknown_products.add(product_id)
                product = {"title": "Unknown Product", "price": 0.0, "category": "Unknown"}

            price = float(product["price"])
            quantity = int(item["quantity"])
            rows.append(
                {
                    "transaction_id": f"CART-{cart['id']}-PROD-{product_id}",
                    "cart_id": cart["id"],
                    "transaction_date": cart["date"],
                    "user_id": cart["userId"],
                    "customer_name": user_lookup.get(cart["userId"], "Unknown User"),
                    "product_id": product_id,
                    "product_title": product["title"],
                    "category": product["category"],
                    "unit_price": price,
                    "quantity": quantity,
                    "total_amount": round(price * quantity, 2),
                }
            )

    if unknown_products:
        logger.warning("Carts reference unknown product ids: %s", sorted(unknown_products))
    return pd.DataFrame(rows, columns=RAW_COLUMNS)


def write_partitions(df: pd.DataFrame) -> list[Path]:
    """Write one raw CSV per transaction date. Re-running overwrites (idempotent)."""
    if df.empty:
        raise ExtractError("No transactions generated; check the source API.")

    partition_dates = pd.to_datetime(df["transaction_date"].str[:10]).dt.date
    saved: list[Path] = []
    for txn_date, group in df.groupby(partition_dates):
        output_path = raw_path(txn_date)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        group.to_csv(output_path, index=False)
        saved.append(output_path)
        logger.info("Saved %d line items -> %s", len(group), output_path)
    return saved


class FakeStoreTransactionScraper:
    """Reads users/products/carts from the FakeStore API (or local JSON fixtures)."""

    BASE_URL = "https://fakestoreapi.com"

    def __init__(
        self,
        session: requests.Session | None = None,
        base_url: str | None = None,
        timeout: float = 10,
        fixtures_dir: Path | str | None = None,
    ):
        self._session = session or build_session()
        self._base_url = (base_url or self.BASE_URL).rstrip("/")
        self._timeout = timeout
        self._fixtures_dir = Path(fixtures_dir) if fixtures_dir else None

    def _fetch_api_endpoint(self, endpoint: str) -> list[dict[str, Any]]:
        if self._fixtures_dir is not None:
            data = json.loads((self._fixtures_dir / f"{endpoint}.json").read_text("utf-8"))
        else:
            url = f"{self._base_url}/{endpoint}"
            logger.info("Fetching %s", url)
            try:
                response = self._session.get(url, timeout=self._timeout)
                response.raise_for_status()
                data = response.json()
            except (requests.RequestException, ValueError) as exc:
                raise ExtractError(f"Failed to fetch {url}: {exc}") from exc

        if not isinstance(data, list) or not data:
            raise ExtractError(f"Endpoint '{endpoint}' returned no records")
        return data

    def extract_transactions(self) -> list[Path]:
        """Fetch, join and persist. Returns the partition files written."""
        users, products, carts = (self._fetch_api_endpoint(e) for e in ENDPOINTS)
        return write_partitions(flatten_carts(users, products, carts))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    written = FakeStoreTransactionScraper().extract_transactions()
    logger.info("Wrote %d daily transaction files.", len(written))
