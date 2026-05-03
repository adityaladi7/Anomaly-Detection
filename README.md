# 🔍 Real-Time E-Commerce Anomaly Detection Pipeline

[![CI/CD](https://github.com/adityaladi7/realtime-anomaly-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/adityaladi7/realtime-anomaly-pipeline/actions)
![Python](https://img.shields.io/badge/Python-3.11%2B-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-0.10-yellow)
![Kafka](https://img.shields.io/badge/Apache_Kafka-Streaming-black)
![Tests](https://img.shields.io/badge/Tests-30%20passing-brightgreen)

A production-grade streaming data pipeline that ingests real-time e-commerce order events via **Apache Kafka**, validates and transforms them through an **ETL layer**, detects anomalies (price spikes, bulk orders, bot patterns) using rule-based scoring, stores results in **DuckDB**, and surfaces findings through a **Streamlit monitoring dashboard** — all gated behind a **GitHub Actions CI/CD** pipeline.

---

## Architecture

```
┌─────────────────┐     Kafka Topic      ┌──────────────────┐
│  Event Producer │ ──────────────────► │  Kafka Consumer  │
│  (Synthetic     │  ecommerce-orders    │  (File fallback  │
│   Order Events) │                      │   in CI mode)    │
└─────────────────┘                      └────────┬─────────┘
                                                  │
                                    ┌─────────────▼──────────────┐
                                    │         ETL Pipeline        │
                                    │  ┌────────────────────────┐ │
                                    │  │  1. Validate           │ │
                                    │  │     - Null checks      │ │
                                    │  │     - Type checks      │ │
                                    │  │     - Referential integ│ │
                                    │  ├────────────────────────┤ │
                                    │  │  2. Transform          │ │
                                    │  │     - Enrich fields    │ │
                                    │  │     - Price ratio calc │ │
                                    │  │     - Temporal features│ │
                                    │  ├────────────────────────┤ │
                                    │  │  3. Anomaly Detection  │ │
                                    │  │     - Price spike      │ │
                                    │  │     - Bulk order       │ │
                                    │  │     - Bot pattern      │ │
                                    │  │     - Severity scoring │ │
                                    │  └────────────┬───────────┘ │
                                    └───────────────┼─────────────┘
                                                    │
                             ┌──────────────────────▼──────────────────┐
                             │              DuckDB Warehouse            │
                             │   orders table · upsert / deduplication │
                             └──────────────────────┬──────────────────┘
                                                    │
                                    ┌───────────────▼──────────────┐
                                    │   Streamlit Dashboard         │
                                    │   KPIs · Charts · Alert log   │
                                    └───────────────────────────────┘
```

---

## CI/CD Pipeline (GitHub Actions)

Every push to `main` or `dev` triggers a 3-stage pipeline:

```
Lint (flake8) → Validate & Test (pytest, 30 tests) → Build & Package
```

The pipeline runs in **CI mode** without a live Kafka broker — the producer writes events to a JSONL file, and the ETL pipeline reads from it. This allows full integration testing in GitHub Actions without any infrastructure.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Streaming | Apache Kafka (kafka-python) |
| ETL / Validation | Python (pandas) |
| Warehouse | DuckDB |
| Testing | pytest, pytest-cov |
| Linting | flake8 |
| CI/CD | GitHub Actions |
| Dashboard | Streamlit, Plotly |

---

## Anomaly Detection Rules

| Anomaly Type | Detection Rule | Severity Contribution |
|---|---|---|
| **Price Spike** | `unit_price > 2.5× base_price` | Up to 50 points |
| **Bulk Order** | `quantity ≥ 30 units` | Up to 30 points |
| **Bot Pattern** | `session_clicks ≥ 150` | Up to 30 points |
| **Combined** | Multiple flags on one event | Additive, capped at 100 |

Events below threshold are labelled `"none"` — clean records flow straight to the warehouse.

Invalid events (null fields, type mismatches, total_value inconsistencies) are **quarantined** to a separate `_invalid.jsonl` file and never loaded into DuckDB.

---

## Project Structure

```
realtime-anomaly-pipeline/
├── producer/
│   └── event_producer.py       # Kafka producer + file fallback
├── etl/
│   └── pipeline.py             # Validate → Transform → Anomaly detect → Load
├── tests/
│   └── test_pipeline.py        # 30 pytest tests (unit + integration + e2e)
├── dashboard/
│   └── app.py                  # Streamlit monitoring dashboard
├── .github/
│   └── workflows/
│       └── ci.yml              # GitHub Actions: lint → test → build
├── data/                       # Generated at runtime (gitignored)
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run in file mode (no Kafka required)
```bash
# Generate 100 synthetic events
python producer/event_producer.py --mode file --output data/raw_events.jsonl --n 100

# Run ETL pipeline
python etl/pipeline.py --input data/raw_events.jsonl --db data/pipeline.duckdb

# Launch dashboard
streamlit run dashboard/app.py
```

### 3. Run with Kafka (live streaming mode)
```bash
# Start Kafka (Docker)
docker-compose up -d

# Stream events to Kafka topic
python producer/event_producer.py --mode kafka --topic ecommerce-orders --n 500

# Launch dashboard
streamlit run dashboard/app.py
```

### 4. Run tests
```bash
pytest tests/ -v --cov=etl --cov=producer --cov-report=term-missing
```

---

## Test Coverage

```
30 tests | 4 test classes | 0.72s runtime

TestValidation       (9 tests)  — null checks, type checks, referential integrity
TestAnomalyDetection (7 tests)  — each anomaly type, severity cap, boundary conditions
TestTransform        (4 tests)  — field enrichment, price ratio, temporal features
TestProducer         (5 tests)  — event generation, file output, anomaly seeding
TestEndToEnd         (5 tests)  — full pipeline, DuckDB load, deduplication, quarantine
```

---

## Key Design Decisions

**DuckDB over SQLite** — DuckDB is columnar and optimised for analytical queries (aggregations, window functions) common in data engineering. Zero-dependency embedded deployment with full SQL support.

**File fallback for CI** — Kafka requires a broker; GitHub Actions runners don't have one. The producer/consumer pattern is preserved in code but the pipeline degrades gracefully to JSONL files in CI, enabling full automated testing without infrastructure.

**Severity scoring** — Binary anomaly flags don't tell operators which events need immediate attention. A 0–100 severity score (additive across flag types, capped at 100) lets the dashboard surface the highest-risk events first.

**Quarantine pattern** — Invalid events are never silently dropped. They're written to `_invalid.jsonl` with their validation errors attached, so data quality issues are traceable and recoverable.

---

## Author  

**Aditya Gaur** · [github.com/adityaladi7](https://github.com/adityaladi7) · [linkedin.com/in/adityagaur](https://linkedin.com/in/adityagaur)
