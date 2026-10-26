# Cloud Data Integration and Processing System (base)

A small cloud-style data platform: it integrates data from several systems, processes it through a
dependency-aware job pipeline, quarantines bad records, and publishes analytics tables to object
storage and a SQL warehouse. Standard library only (Python 3.10+).

```
extract_orders ──> clean_orders ─┐
extract_products ────────────────┼─> integrate ─┬─> agg_daily ────┐
extract_customers ───────────────┘               ├─> agg_category ─┼─> load -> curated/ + warehouse.db
                                                  └─> agg_country ──┘
        raw/ -> staging/ -> curated/      bad rows -> quarantine/      run logs -> meta/runs/
```

## Run
```bash
python setup_sample_data.py                          # orders CSV, products JSON, customers SQLite
python main.py                                       # run the pipeline
python main.py --demo-failure extract_customers      # inject a transient failure to see retries
python -m unittest discover -s tests -t . -v         # tests
```

## What's in it
- **Orchestrator** (`cdps/orchestrator.py`): runs tasks in dependency order, executes independent tasks in
  parallel, retries with exponential backoff, skips tasks downstream of a failure, detects cycles, and
  records a run log (status, attempts, duration, rows) saved to `meta/runs/<run_id>.json`.
- **Storage zones** (`cdps/storage.py`): `raw/` (untouched extracts), `staging/` (cleaned/joined),
  `curated/` (publishable tables, versioned by run plus `latest.csv`), `quarantine/` (rejected rows with a reason).
  Defaults to a local folder acting as a bucket; set `"storage": {"type": "s3", "bucket": "...", "region": "..."}`
  (and `pip install boto3`) to use AWS S3.
- **Data quality** (`cdps/tasks.py`): rejects missing IDs, malformed dates/numbers, non-positive quantity or price,
  invalid discounts and duplicate orders; orders referencing unknown products/customers are quarantined at the
  integration step. Every input row ends up either in `fact_orders` or in quarantine.
- **Processing**: joins orders with products and customers, computes revenue, and builds daily, category and
  country aggregates.
- **Warehouse**: `warehouse.db` tables `fact_orders`, `daily_sales`, `sales_by_category`, `sales_by_country`.

## Extending
Add a source in `config.json` and a matching extract task in `cdps/pipeline.py`; add processing steps as
functions that read `ctx.results[...]` and register them with `Task(name, fn, deps)`. Revenue uses floats rounded
to 2 decimals for simplicity; use `Decimal` or integer cents for real accounting.

Production next steps: a real scheduler (Airflow/Prefect/cron), incremental loads with watermarks,
a columnar format (Parquet) and a cloud warehouse (BigQuery/Redshift/Snowflake), alerting on failures,
and secrets management for source credentials.
