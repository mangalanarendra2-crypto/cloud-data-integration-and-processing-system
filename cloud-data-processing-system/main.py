"""CLI entry point.

Usage:
    python setup_sample_data.py                      # create sample sources
    python main.py                                   # run the pipeline
    python main.py --demo-failure extract_customers  # inject a transient failure to see retries
"""
import argparse
import json
import sys

from cdps.pipeline import run_pipeline


def main():
    ap = argparse.ArgumentParser(description="Cloud Data Integration and Processing System")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--workers", type=int, default=4, help="parallel task slots")
    ap.add_argument("--demo-failure", nargs="*", default=[], metavar="SOURCE",
                    help="sources (orders, products, customers) that fail once to demonstrate retries")
    a = ap.parse_args()

    with open(a.config, encoding="utf-8") as f:
        config = json.load(f)

    flaky = [s.replace("extract_", "") for s in a.demo_failure]
    record, ctx = run_pipeline(config, a.workers, flaky_once=tuple(flaky))

    print(f"Run {record['run_id']}: {record['status']} in {record['duration_s']}s\n")
    print(f"{'task':<20}{'status':<10}{'tries':>6}{'secs':>8}{'rows':>7}")
    for t in record["tasks"]:
        rows = "" if t["rows"] is None else t["rows"]
        print(f"{t['task']:<20}{t['status']:<10}{t['attempts']:>6}{t['duration_s']:>8}{rows:>7}")
        if t["error"] and t["status"] != "SUCCESS":
            print(f"    -> {t['error']}")

    s = record["stats"]
    print(f"\nQuarantined: {s.get('orders_rejected_cleaning', 0)} at cleaning, "
          f"{s.get('orders_unmatched', 0)} at integration")
    if record["status"] == "SUCCESS":
        facts = ctx.results["integrate"]
        print(f"Total revenue: {sum(f['revenue'] for f in facts):,.2f} over {len(facts)} orders")
        print("\nSales by category:")
        for r in ctx.results["agg_category"]:
            print(f"  {r['category']:<14}{r['revenue']:>12,.2f}  ({r['orders']} orders)")
        print("\nSales by country:")
        for r in ctx.results["agg_country"]:
            print(f"  {r['country']:<14}{r['revenue']:>12,.2f}  ({r['orders']} orders)")
        print(f"\nWarehouse: {config.get('warehouse')}   Run log: meta/runs/{record['run_id']}.json")
    sys.exit(0 if record["status"] == "SUCCESS" else 1)


if __name__ == "__main__":
    main()
