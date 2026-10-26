"""Builds the pipeline DAG and runs it.

extract_orders ──> clean_orders ─┐
extract_products ────────────────┼─> integrate ─┬─> agg_daily ────┐
extract_customers ───────────────┘               ├─> agg_category ─┼─> load
                                                  └─> agg_country ──┘
"""
import json
import uuid
from datetime import datetime, timezone

from . import tasks as T
from .orchestrator import Context, Orchestrator, Task
from .storage import build_storage


def build_pipeline(config: dict, max_workers: int = 4) -> Orchestrator:
    src = config["sources"]
    dag = [
        Task("extract_orders", T.make_extract("orders", {**src["orders"]})),
        Task("extract_products", T.make_extract("products", {**src["products"]})),
        Task("extract_customers", T.make_extract("customers", {**src["customers"]})),
        Task("clean_orders", T.clean_orders, ["extract_orders"], retries=0),
        Task("integrate", T.integrate, ["clean_orders", "extract_products", "extract_customers"], retries=0),
        Task("agg_daily", T.agg_daily, ["integrate"], retries=0),
        Task("agg_category", T.agg_category, ["integrate"], retries=0),
        Task("agg_country", T.agg_country, ["integrate"], retries=0),
        Task("load", T.load, ["integrate", "agg_daily", "agg_category", "agg_country"]),
    ]
    return Orchestrator(dag, max_workers=max_workers)


def run_pipeline(config: dict, max_workers: int = 4, flaky_once: tuple = ()) -> tuple[dict, Context]:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:4]
    storage = build_storage(config.get("storage", {}))
    ctx = Context(run_id, storage, config)
    ctx.flaky_once = set(flaky_once)
    record = build_pipeline(config, max_workers).run(ctx)
    storage.put(f"meta/runs/{run_id}.json", json.dumps(record, indent=2).encode())  # run log / lineage
    return record, ctx
