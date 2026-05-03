"""
Test Suite for Real-Time Anomaly Detection Pipeline
Tests: validation logic, anomaly detection, ETL transforms, end-to-end pipeline
"""

import pytest
import json
import os
import tempfile
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.event_producer import generate_event, produce_to_file
from etl.pipeline import (
    validate_event, detect_anomaly, transform_event,
    load_to_duckdb, run_pipeline, ValidationResult
)


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def valid_event():
    return {
        "event_id": "test-001",
        "timestamp": "2025-05-01T12:00:00",
        "user_id": "U1234",
        "product_id": "P001",
        "product_name": "Laptop",
        "category": "Electronics",
        "quantity": 1,
        "unit_price": 999.99,
        "total_value": 999.99,
        "session_clicks": 10,
        "base_price": 999.99,
        "injected_anomaly": "none",
    }


@pytest.fixture
def price_spike_event(valid_event):
    e = valid_event.copy()
    e["unit_price"] = 4999.99
    e["total_value"] = 4999.99
    return e


@pytest.fixture
def bulk_order_event(valid_event):
    e = valid_event.copy()
    e["quantity"] = 100
    e["total_value"] = round(999.99 * 100, 2)
    return e


@pytest.fixture
def bot_pattern_event(valid_event):
    e = valid_event.copy()
    e["session_clicks"] = 300
    return e


@pytest.fixture
def temp_db(tmp_path):
    return str(tmp_path / "test_pipeline.duckdb")


@pytest.fixture
def temp_events_file(tmp_path):
    path = str(tmp_path / "raw_events.jsonl")
    produce_to_file(path, n_events=50)
    return path


# ─── Validation Tests ─────────────────────────────────────────────────────────

class TestValidation:

    def test_valid_event_passes(self, valid_event):
        result = validate_event(valid_event)
        assert result.valid is True
        assert result.errors == []

    def test_missing_required_field_fails(self, valid_event):
        del valid_event["event_id"]
        result = validate_event(valid_event)
        assert result.valid is False
        assert any("event_id" in e for e in result.errors)

    def test_missing_quantity_fails(self, valid_event):
        del valid_event["quantity"]
        result = validate_event(valid_event)
        assert result.valid is False

    def test_zero_quantity_fails(self, valid_event):
        valid_event["quantity"] = 0
        result = validate_event(valid_event)
        assert result.valid is False
        assert any("quantity" in e for e in result.errors)

    def test_negative_price_fails(self, valid_event):
        valid_event["unit_price"] = -10.0
        result = validate_event(valid_event)
        assert result.valid is False

    def test_total_value_mismatch_fails(self, valid_event):
        valid_event["total_value"] = 9999.00  # Wrong
        result = validate_event(valid_event)
        assert result.valid is False
        assert any("mismatch" in e for e in result.errors)

    def test_invalid_timestamp_fails(self, valid_event):
        valid_event["timestamp"] = "not-a-date"
        result = validate_event(valid_event)
        assert result.valid is False

    def test_negative_session_clicks_fails(self, valid_event):
        valid_event["session_clicks"] = -5
        result = validate_event(valid_event)
        assert result.valid is False

    def test_multiple_missing_fields_all_reported(self):
        result = validate_event({})
        assert result.valid is False
        assert len(result.errors) > 1


# ─── Anomaly Detection Tests ──────────────────────────────────────────────────

class TestAnomalyDetection:

    def test_normal_event_no_anomaly(self, valid_event):
        result = detect_anomaly(valid_event)
        assert result["is_anomaly"] is False
        assert result["anomaly_flags"] == "none"
        assert result["severity_score"] == 0

    def test_price_spike_detected(self, price_spike_event):
        result = detect_anomaly(price_spike_event)
        assert result["is_anomaly"] is True
        assert "price_spike" in result["anomaly_flags"]
        assert result["severity_score"] > 0

    def test_bulk_order_detected(self, bulk_order_event):
        result = detect_anomaly(bulk_order_event)
        assert result["is_anomaly"] is True
        assert "bulk_order" in result["anomaly_flags"]

    def test_bot_pattern_detected(self, bot_pattern_event):
        result = detect_anomaly(bot_pattern_event)
        assert result["is_anomaly"] is True
        assert "bot_pattern" in result["anomaly_flags"]

    def test_severity_score_capped_at_100(self, price_spike_event):
        price_spike_event["unit_price"] = 999999.99
        price_spike_event["session_clicks"] = 9999
        price_spike_event["quantity"] = 9999
        price_spike_event["total_value"] = round(999999.99 * 9999, 2)
        result = detect_anomaly(price_spike_event)
        assert result["severity_score"] <= 100

    def test_multiple_flags_combined(self, valid_event):
        valid_event["unit_price"] = 5000.0
        valid_event["total_value"] = 5000.0 * 100
        valid_event["quantity"] = 100
        valid_event["session_clicks"] = 300
        result = detect_anomaly(valid_event)
        assert result["anomaly_count"] >= 2

    def test_boundary_bulk_order(self, valid_event):
        """Exactly at threshold should trigger."""
        valid_event["quantity"] = 30
        valid_event["total_value"] = round(valid_event["unit_price"] * 30, 2)
        result = detect_anomaly(valid_event)
        assert "bulk_order" in result["anomaly_flags"]


# ─── Transform Tests ──────────────────────────────────────────────────────────

class TestTransform:

    def test_transform_adds_required_fields(self, valid_event):
        result = transform_event(valid_event)
        assert "hour_of_day" in result
        assert "day_of_week" in result
        assert "price_ratio" in result
        assert "processed_at" in result
        assert "anomaly_flags" in result

    def test_price_ratio_calculated_correctly(self, valid_event):
        valid_event["unit_price"] = 1999.98
        valid_event["base_price"] = 999.99
        valid_event["total_value"] = 1999.98
        result = transform_event(valid_event)
        assert abs(result["price_ratio"] - 2.0) < 0.01

    def test_hour_of_day_range(self, valid_event):
        result = transform_event(valid_event)
        assert 0 <= result["hour_of_day"] <= 23

    def test_anomaly_fields_present(self, price_spike_event):
        result = transform_event(price_spike_event)
        assert result["is_anomaly"] is True
        assert result["severity_score"] > 0


# ─── Producer Tests ───────────────────────────────────────────────────────────

class TestProducer:

    def test_generate_event_has_required_fields(self):
        event = generate_event()
        for field in ["event_id", "timestamp", "user_id", "product_id",
                      "quantity", "unit_price", "total_value", "session_clicks", "base_price"]:
            assert field in event

    def test_produce_to_file_creates_file(self, tmp_path):
        path = str(tmp_path / "events.jsonl")
        produce_to_file(path, n_events=20)
        assert os.path.exists(path)

    def test_produce_to_file_correct_count(self, tmp_path):
        path = str(tmp_path / "events.jsonl")
        produce_to_file(path, n_events=30)
        with open(path) as f:
            lines = [l for l in f.readlines() if l.strip()]
        assert len(lines) == 30

    def test_produce_to_file_valid_json(self, tmp_path):
        path = str(tmp_path / "events.jsonl")
        produce_to_file(path, n_events=10)
        with open(path) as f:
            for line in f:
                event = json.loads(line)
                assert "event_id" in event

    def test_produce_includes_all_anomaly_types(self, tmp_path):
        """Guaranteed anomaly seeding covers all types."""
        path = str(tmp_path / "events.jsonl")
        produce_to_file(path, n_events=20)
        with open(path) as f:
            events = [json.loads(l) for l in f]
        anomaly_types = {e["injected_anomaly"] for e in events}
        assert "price_spike" in anomaly_types
        assert "bulk_order" in anomaly_types
        assert "bot_pattern" in anomaly_types


# ─── End-to-End Pipeline Test ─────────────────────────────────────────────────

class TestEndToEnd:

    def test_full_pipeline_runs(self, temp_events_file, temp_db):
        summary = run_pipeline(temp_events_file, temp_db)
        assert summary["raw_count"] == 50
        assert summary["valid_count"] > 0
        assert summary["total_loaded"] > 0

    def test_pipeline_detects_anomalies(self, temp_events_file, temp_db):
        """Seeded file always has anomalies — pipeline should catch them."""
        summary = run_pipeline(temp_events_file, temp_db)
        assert summary["anomaly_count"] > 0

    def test_pipeline_loads_to_duckdb(self, temp_events_file, temp_db):
        import duckdb
        run_pipeline(temp_events_file, temp_db)
        con = duckdb.connect(temp_db)
        count = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        con.close()
        assert count > 0

    def test_pipeline_deduplication(self, temp_events_file, temp_db):
        """Running pipeline twice should not double-count records."""
        s1 = run_pipeline(temp_events_file, temp_db)
        s2 = run_pipeline(temp_events_file, temp_db)
        assert s1["total_loaded"] == s2["total_loaded"]

    def test_invalid_events_quarantined(self, tmp_path, temp_db):
        """Corrupt events should be written to invalid file, not loaded."""
        path = str(tmp_path / "mixed.jsonl")
        good = generate_event()
        bad = {"event_id": "bad-001", "user_id": "U999"}  # missing fields

        with open(path, "w") as f:
            f.write(json.dumps(good) + "\n")
            f.write(json.dumps(bad) + "\n")

        summary = run_pipeline(path, temp_db)
        assert summary["valid_count"] == 1
        assert summary["invalid_count"] == 1

        invalid_path = path.replace(".jsonl", "_invalid.jsonl")
        assert os.path.exists(invalid_path)
