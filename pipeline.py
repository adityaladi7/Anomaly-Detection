"""
ETL Pipeline
Validates raw events, applies transformations, detects anomalies,
and loads structured output into DuckDB.
"""

import json
import os
import duckdb
import pandas as pd
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional


# ─── Validation ──────────────────────────────────────────────────────────────

REQUIRED_FIELDS = [
    "event_id", "timestamp", "user_id", "product_id",
    "quantity", "unit_price", "total_value", "session_clicks", "base_price"
]


@dataclass
class ValidationResult:
    valid: bool
    errors: list = field(default_factory=list)


def validate_event(event: dict) -> ValidationResult:
    errors = []

    # Required fields
    for field_name in REQUIRED_FIELDS:
        if field_name not in event or event[field_name] is None:
            errors.append(f"Missing required field: {field_name}")

    if errors:
        return ValidationResult(valid=False, errors=errors)

    # Type & range checks
    if not isinstance(event["quantity"], int) or event["quantity"] < 1:
        errors.append("quantity must be a positive integer")

    if not isinstance(event["unit_price"], (int, float)) or event["unit_price"] <= 0:
        errors.append("unit_price must be a positive number")

    if not isinstance(event["session_clicks"], int) or event["session_clicks"] < 0:
        errors.append("session_clicks must be a non-negative integer")

    # Referential integrity
    expected_total = round(event["unit_price"] * event["quantity"], 2)
    if abs(event["total_value"] - expected_total) > 0.05:
        errors.append(
            f"total_value mismatch: got {event['total_value']}, expected {expected_total}"
        )

    # Timestamp parseable
    try:
        datetime.fromisoformat(event["timestamp"])
    except (ValueError, TypeError):
        errors.append(f"Invalid timestamp format: {event['timestamp']}")

    return ValidationResult(valid=len(errors) == 0, errors=errors)


# ─── Anomaly Detection ────────────────────────────────────────────────────────

PRICE_SPIKE_MULTIPLIER = 2.5
BULK_ORDER_THRESHOLD = 30
BOT_CLICK_THRESHOLD = 150


def detect_anomaly(event: dict) -> dict:
    """
    Rule-based anomaly detection.
    Returns anomaly flags and a severity score (0–100).
    """
    flags = []
    severity = 0

    price_ratio = event["unit_price"] / event["base_price"]
    if price_ratio >= PRICE_SPIKE_MULTIPLIER:
        flags.append("price_spike")
        severity += min(int((price_ratio - 2.5) * 20), 50)

    if event["quantity"] >= BULK_ORDER_THRESHOLD:
        flags.append("bulk_order")
        severity += min(int((event["quantity"] - 30) / 5), 30)

    if event["session_clicks"] >= BOT_CLICK_THRESHOLD:
        flags.append("bot_pattern")
        severity += min(int((event["session_clicks"] - 150) / 10), 30)

    severity = min(severity, 100)
    is_anomaly = len(flags) > 0

    return {
        "anomaly_flags": ",".join(flags) if flags else "none",
        "anomaly_count": len(flags),
        "severity_score": severity,
        "is_anomaly": is_anomaly,
    }


# ─── Transform ────────────────────────────────────────────────────────────────

def transform_event(event: dict) -> dict:
    """Enrich and flatten event for warehouse loading."""
    ts = datetime.fromisoformat(event["timestamp"])
    anomaly = detect_anomaly(event)

    return {
        "event_id": event["event_id"],
        "timestamp": event["timestamp"],
        "hour_of_day": ts.hour,
        "day_of_week": ts.strftime("%A"),
        "user_id": event["user_id"],
        "product_id": event["product_id"],
        "product_name": event.get("product_name", ""),
        "category": event.get("category", ""),
        "quantity": event["quantity"],
        "unit_price": event["unit_price"],
        "base_price": event["base_price"],
        "price_ratio": round(event["unit_price"] / event["base_price"], 4),
        "total_value": event["total_value"],
        "session_clicks": event["session_clicks"],
        **anomaly,
        "processed_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
    }


# ─── Load ─────────────────────────────────────────────────────────────────────

def load_to_duckdb(records: list, db_path: str = "data/pipeline.duckdb"):
    """Load transformed records into DuckDB."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    df = pd.DataFrame(records)

    con = duckdb.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            event_id VARCHAR PRIMARY KEY,
            timestamp VARCHAR,
            hour_of_day INTEGER,
            day_of_week VARCHAR,
            user_id VARCHAR,
            product_id VARCHAR,
            product_name VARCHAR,
            category VARCHAR,
            quantity INTEGER,
            unit_price DOUBLE,
            base_price DOUBLE,
            price_ratio DOUBLE,
            total_value DOUBLE,
            session_clicks INTEGER,
            anomaly_flags VARCHAR,
            anomaly_count INTEGER,
            severity_score INTEGER,
            is_anomaly BOOLEAN,
            processed_at VARCHAR
        )
    """)

    # Upsert pattern - skip duplicates
    con.execute("INSERT OR IGNORE INTO orders SELECT * FROM df")
    total = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    anomalies = con.execute("SELECT COUNT(*) FROM orders WHERE is_anomaly = TRUE").fetchone()[0]
    con.close()

    print(f"[Load] DuckDB: {total} total records | {anomalies} anomalies detected")
    return total, anomalies


# ─── Pipeline Orchestration ───────────────────────────────────────────────────

def run_pipeline(input_path: str, db_path: str = "data/pipeline.duckdb"):
    """Full ETL run: read → validate → transform → load."""
    print(f"\n[ETL] Starting pipeline from: {input_path}")

    raw_events = []
    with open(input_path) as f:
        for line in f:
            line = line.strip()
            if line:
                raw_events.append(json.loads(line))

    print(f"[ETL] Loaded {len(raw_events)} raw events")

    valid_events, invalid_events = [], []
    for event in raw_events:
        result = validate_event(event)
        if result.valid:
            valid_events.append(event)
        else:
            invalid_events.append({"event": event, "errors": result.errors})

    print(f"[ETL] Validation: {len(valid_events)} valid | {len(invalid_events)} invalid")

    if invalid_events:
        invalid_path = input_path.replace(".jsonl", "_invalid.jsonl")
        with open(invalid_path, "w") as f:
            for item in invalid_events:
                f.write(json.dumps(item) + "\n")
        print(f"[ETL] Invalid events written to: {invalid_path}")

    transformed = [transform_event(e) for e in valid_events]
    total, anomalies = load_to_duckdb(transformed, db_path)

    return {
        "raw_count": len(raw_events),
        "valid_count": len(valid_events),
        "invalid_count": len(invalid_events),
        "anomaly_count": anomalies,
        "total_loaded": total,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/raw_events.jsonl")
    parser.add_argument("--db", default="data/pipeline.duckdb")
    args = parser.parse_args()

    summary = run_pipeline(args.input, args.db)
    print(f"\n[ETL] Summary: {summary}")
