#!/usr/bin/env python3
"""Download Olist Brazilian E-Commerce dataset from Kaggle and extract
referentially-consistent CSV subsets for E2E QA testing.

Usage:
    pip install kagglehub pandas
    python backend/tests/fixtures/kaggle_data/download_kaggle_data.py

Requires Kaggle API credentials in ~/.kaggle/kaggle.json
(get from https://www.kaggle.com/settings → API → Create New Token).
"""

from __future__ import annotations

import sys
from pathlib import Path

FIXTURE_DIR = Path(__file__).resolve().parent

# Subset sizes (referentially consistent)
ORDERS_SAMPLE = 500
MAX_ITEMS = 800  # cap for order_items


def main() -> int:
    try:
        import kagglehub  # type: ignore
    except ImportError:
        print("ERROR: kagglehub not installed. Run: pip install kagglehub", file=sys.stderr)
        return 1

    try:
        import pandas as pd  # type: ignore
    except ImportError:
        print("ERROR: pandas not installed. Run: pip install pandas", file=sys.stderr)
        return 1

    # ── Download ──────────────────────────────────────────────────────
    print("Downloading Olist dataset from Kaggle...")
    dataset_path = Path(kagglehub.dataset_download("olistbr/brazilian-ecommerce"))
    print(f"  Downloaded to: {dataset_path}")

    # ── Load full CSVs ────────────────────────────────────────────────
    orders_full = pd.read_csv(dataset_path / "olist_orders_dataset.csv")
    customers_full = pd.read_csv(dataset_path / "olist_customers_dataset.csv")
    items_full = pd.read_csv(dataset_path / "olist_order_items_dataset.csv")
    products_full = pd.read_csv(dataset_path / "olist_products_dataset.csv")
    sellers_full = pd.read_csv(dataset_path / "olist_sellers_dataset.csv")

    print(f"  Full dataset: {len(orders_full)} orders, {len(customers_full)} customers, "
          f"{len(items_full)} items, {len(products_full)} products, {len(sellers_full)} sellers")

    # ── Referentially-consistent subset ───────────────────────────────
    # 1. Sample orders (prefer diverse statuses)
    orders = orders_full.sample(n=min(ORDERS_SAMPLE, len(orders_full)), random_state=42)

    # 2. Keep only referenced customers
    customer_ids = set(orders["customer_id"].unique())
    customers = customers_full[customers_full["customer_id"].isin(customer_ids)]

    # 3. Keep only referenced order_items
    order_ids = set(orders["order_id"].unique())
    items = items_full[items_full["order_id"].isin(order_ids)]
    if len(items) > MAX_ITEMS:
        items = items.head(MAX_ITEMS)

    # 4. Keep only referenced products
    product_ids = set(items["product_id"].unique())
    products = products_full[products_full["product_id"].isin(product_ids)]

    # 5. Keep only referenced sellers
    seller_ids = set(items["seller_id"].unique())
    sellers = sellers_full[sellers_full["seller_id"].isin(seller_ids)]

    # ── Write subsets ─────────────────────────────────────────────────
    out_map = {
        "olist_orders.csv": orders,
        "olist_customers.csv": customers,
        "olist_order_items.csv": items,
        "olist_products.csv": products,
        "olist_sellers.csv": sellers,
    }

    for filename, df in out_map.items():
        out_path = FIXTURE_DIR / filename
        df.to_csv(out_path, index=False)
        print(f"  WROTE: {out_path} ({len(df)} rows, {len(df.columns)} cols)")

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\nSubsets extracted ({len(orders)} orders → "
          f"{len(customers)} customers, {len(items)} items, "
          f"{len(products)} products, {len(sellers)} sellers)")
    print("Status distribution:")
    for status, count in orders["order_status"].value_counts().items():
        print(f"  {status}: {count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
