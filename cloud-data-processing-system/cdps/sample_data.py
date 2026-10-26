"""Generates sample source systems: orders (CSV), products (JSON), customers (SQLite).

Orders include deliberately bad rows (duplicates, bad dates, negative quantities, unknown
products/customers) so the quality and quarantine logic has something to catch.
"""
import csv
import json
import random
import sqlite3
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path

PRODUCTS = [
    ("SKU-100", "Wireless Mouse", "Electronics", 799.0),
    ("SKU-101", "Mechanical Keyboard", "Electronics", 3499.0),
    ("SKU-102", "USB-C Hub", "Electronics", 1899.0),
    ("SKU-200", "Notebook A5", "Stationery", 120.0),
    ("SKU-201", "Gel Pen Pack", "Stationery", 90.0),
    ("SKU-300", "Desk Lamp", "Home", 1299.0),
    ("SKU-301", "Water Bottle", "Home", 450.0),
    ("SKU-302", "Backpack", "Home", 1799.0),
]
CUSTOMERS = [
    ("C001", "Asha Rao", "India"), ("C002", "Ben Carter", "USA"), ("C003", "Chen Wei", "Singapore"),
    ("C004", "Divya Nair", "India"), ("C005", "Emma Stone", "UK"), ("C006", "Farid Khan", "UAE"),
    ("C007", "Grace Lee", "USA"), ("C008", "Hiro Tanaka", "Japan"), ("C009", "Isha Mehta", "India"),
    ("C010", "Jack Brown", "UK"),
]


def create_sample_data(out_dir: str = "sample_data", n_orders: int = 200, seed: int = 7) -> dict:
    d = Path(out_dir)
    d.mkdir(parents=True, exist_ok=True)
    rnd = random.Random(seed)

    (d / "products.json").write_text(json.dumps({"products": [
        {"sku": s, "name": n, "category": c, "list_price": p} for s, n, c, p in PRODUCTS]}, indent=2))

    db = d / "customers.db"
    db.unlink(missing_ok=True)
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("CREATE TABLE customers (customer_id TEXT PRIMARY KEY, name TEXT, country TEXT)")
        conn.executemany("INSERT INTO customers VALUES (?,?,?)", CUSTOMERS)
        conn.commit()

    start = date(2026, 9, 1)
    rows = []
    for i in range(1, n_orders + 1):
        sku, _, _, price = rnd.choice(PRODUCTS)
        rows.append({
            "order_id": f"O{i:05d}",
            "order_date": (start + timedelta(days=rnd.randint(0, 29))).isoformat(),
            "customer_id": rnd.choice(CUSTOMERS)[0],
            "sku": sku,
            "quantity": rnd.randint(1, 5),
            "unit_price": price,
            "discount": rnd.choice([0, 0, 0, 0.05, 0.10, 0.20]),
        })
    # injected problems
    rows[5] = {**rows[5], "quantity": 0}                       # INVALID_QUANTITY
    rows[9] = {**rows[9], "order_date": "31/09/2026"}          # MALFORMED_FIELD
    rows[14] = {**rows[14], "order_id": ""}                    # MISSING_ORDER_ID
    rows[20] = {**rows[20], "discount": 1.5}                   # INVALID_DISCOUNT
    rows[30] = {**rows[30], "sku": "SKU-999"}                  # UNKNOWN_PRODUCT (found at integration)
    rows[40] = {**rows[40], "customer_id": "C999"}             # UNKNOWN_CUSTOMER (found at integration)
    rows.append(dict(rows[60]))                                # DUPLICATE_ORDER
    rows.append(dict(rows[61]))                                # DUPLICATE_ORDER

    with open(d / "orders.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    return {"orders": len(rows), "products": len(PRODUCTS), "customers": len(CUSTOMERS)}
