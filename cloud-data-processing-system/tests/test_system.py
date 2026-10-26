import os
import sqlite3
import tempfile
import unittest
from contextlib import closing

from cdps.orchestrator import Context, Orchestrator, Task
from cdps.pipeline import run_pipeline
from cdps.sample_data import create_sample_data
from cdps.tasks import validate_order


def ctx():
    return Context("t", None, {})


class OrchestratorTests(unittest.TestCase):
    def test_dependency_order_and_results(self):
        order = []
        tasks = [
            Task("c", lambda c: order.append("c") or [3], ["a", "b"]),
            Task("a", lambda c: order.append("a") or [1]),
            Task("b", lambda c: order.append("b") or [2]),
        ]
        rec = Orchestrator(tasks).run(ctx())
        self.assertEqual(rec["status"], "SUCCESS")
        self.assertEqual(order[-1], "c")

    def test_retry_then_success(self):
        calls = {"n": 0}

        def flaky(c):
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("boom")
            return [1]

        rec = Orchestrator([Task("t", flaky, retries=2, retry_delay=0.001)]).run(ctx())
        self.assertEqual(rec["tasks"][0]["attempts"], 3)
        self.assertEqual(rec["status"], "SUCCESS")

    def test_failure_skips_downstream(self):
        def bad(c):
            raise ValueError("nope")

        tasks = [Task("a", bad, retries=1, retry_delay=0.001), Task("b", lambda c: [1], ["a"]),
                 Task("c", lambda c: [1])]
        rec = Orchestrator(tasks).run(ctx())
        st = {t["task"]: t["status"] for t in rec["tasks"]}
        self.assertEqual(st, {"a": "FAILED", "b": "SKIPPED", "c": "SUCCESS"})
        self.assertEqual(rec["status"], "FAILED")

    def test_cycle_and_unknown_dep(self):
        with self.assertRaises(ValueError):
            Orchestrator([Task("a", lambda c: 1, ["b"]), Task("b", lambda c: 1, ["a"])])
        with self.assertRaises(ValueError):
            Orchestrator([Task("a", lambda c: 1, ["zzz"])])


class ValidationTests(unittest.TestCase):
    base = {"order_id": "O1", "order_date": "2026-09-01", "customer_id": "C1", "sku": "S",
            "quantity": "2", "unit_price": "10", "discount": "0.1"}

    def test_valid(self):
        row, reason = validate_order(self.base)
        self.assertIsNone(reason)
        self.assertEqual(row["quantity"], 2)

    def test_rejections(self):
        cases = {"quantity": ("0", "INVALID_QUANTITY"), "unit_price": ("-5", "INVALID_PRICE"),
                 "discount": ("1.5", "INVALID_DISCOUNT"), "order_date": ("31/09/2026", "MALFORMED_FIELD"),
                 "order_id": ("", "MISSING_ORDER_ID")}
        for field, (value, expected) in cases.items():
            self.assertEqual(validate_order({**self.base, field: value})[1], expected, field)


class EndToEndTest(unittest.TestCase):
    def test_pipeline_with_injected_failure(self):
        with tempfile.TemporaryDirectory() as d:
            create_sample_data(os.path.join(d, "src"))
            config = {
                "storage": {"type": "local", "bucket_dir": os.path.join(d, "bucket")},
                "warehouse": os.path.join(d, "wh.db"),
                "sources": {
                    "orders": {"type": "csv", "path": os.path.join(d, "src/orders.csv")},
                    "products": {"type": "json", "path": os.path.join(d, "src/products.json"),
                                 "records_key": "products"},
                    "customers": {"type": "sqlite", "path": os.path.join(d, "src/customers.db"),
                                  "query": "SELECT customer_id, name, country FROM customers"},
                },
            }
            record, c = run_pipeline(config, flaky_once=("customers",))
            self.assertEqual(record["status"], "SUCCESS")
            attempts = {t["task"]: t["attempts"] for t in record["tasks"]}
            self.assertEqual(attempts["extract_customers"], 2)  # retried once
            # every input row must end up either in the fact table or in quarantine
            total_in = len(c.results["extract_orders"])
            facts = len(c.results["integrate"])
            quarantined = record["stats"]["orders_rejected_cleaning"] + record["stats"]["orders_unmatched"]
            self.assertEqual(facts + quarantined, total_in)
            with closing(sqlite3.connect(config["warehouse"])) as db:
                revenue_db = db.execute("SELECT ROUND(SUM(revenue),2) FROM sales_by_category").fetchone()[0]
            self.assertAlmostEqual(revenue_db, sum(f["revenue"] for f in c.results["integrate"]), places=2)


if __name__ == "__main__":
    unittest.main()
