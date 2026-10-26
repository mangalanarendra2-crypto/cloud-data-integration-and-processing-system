"""Source connectors. Each returns a list of dict records."""
import csv
import json
import sqlite3


def fetch_csv(cfg: dict) -> list[dict]:
    with open(cfg["path"], newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fetch_json(cfg: dict) -> list[dict]:
    with open(cfg["path"], encoding="utf-8") as f:
        data = json.load(f)
    key = cfg.get("records_key")
    return data[key] if key else data


def fetch_sqlite(cfg: dict) -> list[dict]:
    conn = sqlite3.connect(cfg["path"])
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(cfg["query"])]
    finally:
        conn.close()


CONNECTORS = {"csv": fetch_csv, "json": fetch_json, "sqlite": fetch_sqlite}


def fetch(cfg: dict) -> list[dict]:
    fn = CONNECTORS.get(cfg["type"])
    if fn is None:
        raise ValueError(f"Unknown source type: {cfg['type']}")
    return fn(cfg)
