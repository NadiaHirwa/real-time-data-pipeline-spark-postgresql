"""
data_generator.py

Simulates an e-commerce platform producing a continuous stream of
user events (view, purchase). Writes a new, timestamped CSV file into
data/incoming/ every GENERATOR_INTERVAL_SECONDS, where Spark
Structured Streaming picks it up (see spark_streaming.py).

A small, deliberate fraction of events violate the data contract
(docs/data_contract.md) on purpose - a generator that only ever
produces perfectly valid data can never prove the pipeline's
rejection/quarantine logic actually works.
"""

import csv
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

sys.path.append(str(Path(__file__).resolve().parent))
import config
from monitoring_logger import get_logger

logger = get_logger(__name__)
fake = Faker()

CSV_COLUMNS = [
    "event_id", "user_id", "product_id", "event_type",
    "price", "quantity", "category", "event_timestamp",
]

CATEGORIES = ["Electronics", "Books", "Clothing", "Home & Kitchen", "Toys", "Sports"]

# What fraction of events should be deliberately invalid, and how.
# Kept small and realistic: real bad data is the exception, not the norm.
BAD_EVENT_RATE = 0.05


def _make_good_event() -> dict:
    """Generate one event that satisfies the data contract."""
    event_type = random.choices(config.ALLOWED_EVENT_TYPES, weights=[80, 20])[0]
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 9999),
        "product_id": random.randint(100, 999),
        "event_type": event_type,
        "price": round(random.uniform(5, 500), 2),
        "quantity": random.randint(1, 3) if event_type == "purchase" else 1,
        "category": random.choice(CATEGORIES),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _make_bad_event() -> dict:
    """
    Generate one event that deliberately violates exactly one rule
    from docs/data_contract.md, chosen at random. This exists purely
    to exercise the pipeline's rejection path with realistic-looking
    (but invalid) data.
    """
    event = _make_good_event()
    violation = random.choice(
        ["negative_price", "zero_quantity", "bad_event_type", "missing_user_id", "future_timestamp"]
    )

    if violation == "negative_price":
        event["price"] = -round(random.uniform(1, 100), 2)
    elif violation == "zero_quantity":
        event["quantity"] = 0
    elif violation == "bad_event_type":
        event["event_type"] = random.choice(["click", "wishlist", "unknown"])
    elif violation == "missing_user_id":
        event["user_id"] = ""
    elif violation == "future_timestamp":
        event["event_timestamp"] = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
    return event


def generate_batch(n: int) -> list[dict]:
    """Generate a batch of n events, mixing in a small rate of bad ones."""
    batch = []
    for _ in range(n):
        if random.random() < BAD_EVENT_RATE:
            batch.append(_make_bad_event())
        else:
            batch.append(_make_good_event())
    return batch


def write_batch_to_csv(batch: list[dict]) -> Path:
    """Write one batch of events to a new, uniquely-named CSV file."""
    filename = f"events_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}.csv"
    path = config.INCOMING_DIR / filename

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(batch)

    return path


def run(max_iterations: int | None = None) -> None:
    """
    Run the generator loop indefinitely (or for max_iterations, useful
    for testing), writing one CSV file per iteration.
    """
    config.ensure_directories()
    logger.info(
        "Data generator started. Writing every %ds, %d events/file, %.0f%% deliberately invalid.",
        config.GENERATOR_INTERVAL_SECONDS,
        config.GENERATOR_EVENTS_PER_FILE,
        BAD_EVENT_RATE * 100,
    )

    iterations = 0
    try:
        while max_iterations is None or iterations < max_iterations:
            batch = generate_batch(config.GENERATOR_EVENTS_PER_FILE)
            path = write_batch_to_csv(batch)
            logger.info("Wrote %d events to %s", len(batch), path.name)

            iterations += 1
            time.sleep(config.GENERATOR_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Data generator stopped by user after %d files.", iterations)


if __name__ == "__main__":
    run()