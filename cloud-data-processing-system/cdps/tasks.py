"""Pipeline task implementations: extract, clean, integrate, aggregate, load."""
import csv
import io
import json
import sqlite3
from collections import defaultdict
from contextlib import closing
from datetime import datetime

from .connectors import fetch


def _dump(rows) -> bytes:
    return json.dumps(rows, indent=2, default=str).encode()


def _to_csv(rows: list[dict], columns: list[str]) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode()


# ---------- extract ----------
def make_extract(name: str, src: dict):
    failed_once: set = set()

    def extract(ctx):
        if name in ctx.flaky_once and name not in failed_once:  # demo of retry behaviour
            failed_once.add(name)
            raise ConnectionError(f"simulated transient failure reading '{name}'")
        rows = fetch(src)
        ctx.storage.put(f"raw/{name}/{ctx.run_id}.json", _dump(rows))
        return rows

    return extract


# ---------- clean ----------
def validate_order(r: dict) -> tuple[dict | None, str | None]:
    """Return (clean_row, None) or (None, rejection_reason)."""
    order_id = (r.get("order_id") or "").strip()
    if not order_id:
        return None, "MISSING_ORDER_ID"
    try:
        qty = int(str(r["quantity"]).strip())
        price = float(str(r["unit_price"]).strip())
        discount = float(str(r.get("discount") or 0).strip())
        date = datetime.strptime(str(r["order_date"]).strip(), "%Y-%m-%d").date().isoformat()
    except (KeyError, ValueError, TypeError):
        return None, "MALFORMED_FIELD"
    if qty <= 0:
        return None, "INVALID_QUANTITY"
    if price <= 0:
        return None, "INVALID_PRICE"
    if not 0 <= discount < 1:
        return None, "INVALID_DISCOUNT"
    return {"order_id": order_id, "order_date": date, "customer_id": str(r.get("customer_id", "")).strip(),
            "sku": str(r.get("sku", "")).strip(), "quantity": qty, "unit_price": price,
            "discount": discount}, None


def clean_orders(ctx):
    good, bad, seen = [], [], set()
    for r in ctx.results["extract_orders"]:
        row, reason = validate_order(r)
        if row and row["order_id"] in seen:
            row, reason = None, "DUPLICATE_ORDER"
        if row:
            seen.add(row["order_id"])
            good.append(row)
        else:
            bad.append({**r, "_reason": reason})
    ctx.stats["orders_rejected_cleaning"] = len(bad)
    ctx.storage.put(f"staging/orders_clean/{ctx.run_id}.json", _dump(good))
    if bad:
        ctx.storage.put(f"quarantine/orders/{ctx.run_id}.json", _dump(bad))
    return good


# ---------- integrate ----------
FACT_COLUMNS = ["order_id", "order_date", "customer_id", "customer_name", "country", "sku",
                "product_name", "category", "quantity", "unit_price", "discount", "revenue"]


def integrate(ctx):
    products = {p["sku"]: p for p in ctx.results["extract_products"]}
    customers = {str(c["customer_id"]): c for c in ctx.results["extract_customers"]}
    facts, orphans = [], []
    for o in ctx.results["clean_orders"]:
        p, c = products.get(o["sku"]), customers.get(o["customer_id"])
        if p is None or c is None:
            orphans.append({**o, "_reason": "UNKNOWN_PRODUCT" if p is None else "UNKNOWN_CUSTOMER"})
            continue
        facts.append({**o, "customer_name": c["name"], "country": c["country"],
                      "product_name": p["name"], "category": p["category"],
                      "revenue": round(o["quantity"] * o["unit_price"] * (1 - o["discount"]), 2)})
    ctx.stats["orders_unmatched"] = len(orphans)
    ctx.storage.put(f"staging/fact_orders/{ctx.run_id}.json", _dump(facts))
    if orphans:
        ctx.storage.put(f"quarantine/integration/{ctx.run_id}.json", _dump(orphans))
    return facts


# ---------- aggregate ----------
def _group(facts, key_fn, key_name):
    g = defaultdict(lambda: {"orders": 0, "units": 0, "revenue": 0.0, "customers": set()})
    for f in facts:
        a = g[key_fn(f)]
        a["orders"] += 1
        a["units"] += f["quantity"]
        a["revenue"] += f["revenue"]
        a["customers"].add(f["customer_id"])
    return [{key_name: k, "orders": a["orders"], "units": a["units"], "revenue": round(a["revenue"], 2),
             "customers": len(a["customers"])} for k, a in sorted(g.items())]


def agg_daily(ctx):
    return _group(ctx.results["integrate"], lambda f: f["order_date"], "order_date")


def agg_category(ctx):
    rows = _group(ctx.results["integrate"], lambda f: f["category"], "category")
    return sorted(rows, key=lambda r: -r["revenue"])


def agg_country(ctx):
    rows = _group(ctx.results["integrate"], lambda f: f["country"], "country")
    return sorted(rows, key=lambda r: -r["revenue"])


# ---------- load ----------
CURATED = {
    "fact_orders": ("integrate", FACT_COLUMNS),
    "daily_sales": ("agg_daily", ["order_date", "orders", "units", "revenue", "customers"]),
    "sales_by_category": ("agg_category", ["category", "orders", "units", "revenue", "customers"]),
    "sales_by_country": ("agg_country", ["country", "orders", "units", "revenue", "customers"]),
}


def load(ctx):
    db_path = ctx.config.get("warehouse", "warehouse.db")
    loaded = {}
    with closing(sqlite3.connect(db_path)) as db:
        for table, (task, cols) in CURATED.items():
            rows = ctx.results[task]
            ctx.storage.put(f"curated/{table}/{ctx.run_id}.csv", _to_csv(rows, cols))
            ctx.storage.put(f"curated/{table}/latest.csv", _to_csv(rows, cols))
            db.execute(f"DROP TABLE IF EXISTS {table}")
            db.execute(f"CREATE TABLE {table} ({', '.join(cols)})")
            db.executemany(f"INSERT INTO {table} VALUES ({','.join('?' * len(cols))})",
                           [tuple(r[c] for c in cols) for r in rows])
            loaded[table] = len(rows)
        db.commit()
    ctx.stats["loaded_tables"] = loaded
    return {"database": db_path, "tables": loaded}
