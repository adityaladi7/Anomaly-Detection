"""
E-Commerce Event Producer
Simulates real-time order/clickstream events via Kafka.
Falls back to file-based queue when Kafka is not available (CI/testing mode).
"""

import json
import random
import time
import os
import uuid
from datetime import datetime, timezone

UTC = timezone.utc

PRODUCTS = [
    {"id": "P001", "name": "Laptop", "base_price": 999.99, "category": "Electronics"},
    {"id": "P002", "name": "Headphones", "base_price": 149.99, "category": "Electronics"},
    {"id": "P003", "name": "Running Shoes", "base_price": 89.99, "category": "Apparel"},
    {"id": "P004", "name": "Desk Chair", "base_price": 299.99, "category": "Furniture"},
    {"id": "P005", "name": "Coffee Maker", "base_price": 59.99, "category": "Kitchen"},
    {"id": "P006", "name": "Yoga Mat", "base_price": 34.99, "category": "Sports"},
    {"id": "P007", "name": "Smartphone", "base_price": 799.99, "category": "Electronics"},
    {"id": "P008", "name": "Backpack", "base_price": 49.99, "category": "Apparel"},
]

ANOMALY_TYPES = ["price_spike", "bulk_order", "bot_pattern", "none"]


def generate_event(force_anomaly=None):
    """Generate a single synthetic order event."""
    product = random.choice(PRODUCTS)
    anomaly = force_anomaly or random.choices(
        ANOMALY_TYPES, weights=[5, 8, 5, 82]
    )[0]

    quantity = 1
    price = product["base_price"] * random.uniform(0.95, 1.05)
    user_id = f"U{random.randint(1000, 9999)}"
    session_clicks = random.randint(3, 25)

    # Inject anomalies
    if anomaly == "price_spike":
        price = product["base_price"] * random.uniform(3.0, 8.0)
    elif anomaly == "bulk_order":
        quantity = random.randint(50, 200)
    elif anomaly == "bot_pattern":
        session_clicks = random.randint(200, 500)
        quantity = random.randint(10, 30)

    unit_price = round(price, 2)
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(UTC).replace(tzinfo=None).isoformat(),
        "user_id": user_id,
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": unit_price,
        "total_value": round(unit_price * quantity, 2),
        "session_clicks": session_clicks,
        "base_price": product["base_price"],
        "injected_anomaly": anomaly,  # ground truth label for validation
    }
    return event


def produce_to_file(output_path, n_events=100):
    """Write events to JSONL file (used in CI/testing mode)."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    events = []
    for i in range(n_events):
        # Guarantee at least a few of each anomaly type
        if i < 5:
            event = generate_event(force_anomaly="price_spike")
        elif i < 10:
            event = generate_event(force_anomaly="bulk_order")
        elif i < 15:
            event = generate_event(force_anomaly="bot_pattern")
        else:
            event = generate_event()
        events.append(event)

    with open(output_path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    print(f"[Producer] Written {len(events)} events to {output_path}")
    return events


def produce_to_kafka(topic, n_events=None, delay=0.5):
    """Stream events to Kafka topic (used in live mode)."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        raise RuntimeError("kafka-python not installed. Run: pip install kafka-python")

    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    count = 0
    print(f"[Producer] Streaming to Kafka topic '{topic}'...")
    try:
        while n_events is None or count < n_events:
            event = generate_event()
            producer.send(topic, event)
            print(f"[Producer] Sent event {event['event_id']} | anomaly={event['injected_anomaly']}")
            count += 1
            time.sleep(delay)
    except KeyboardInterrupt:
        print(f"\n[Producer] Stopped after {count} events.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["file", "kafka"], default="file")
    parser.add_argument("--output", default="data/raw_events.jsonl")
    parser.add_argument("--topic", default="ecommerce-orders")
    parser.add_argument("--n", type=int, default=100)
    args = parser.parse_args()

    if args.mode == "file":
        produce_to_file(args.output, args.n)
    else:
        produce_to_kafka(args.topic, args.n)
