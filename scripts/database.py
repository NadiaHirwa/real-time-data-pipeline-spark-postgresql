"""
database.py

Connection helper and verification queries for PostgreSQL. This
module is used for setup verification and manual/automated checks -
NOT for the streaming writes themselves, which happen directly inside
spark_streaming.py via Spark's JDBC writer (see docs/engineering_decisions.md
for why: Spark's JDBC connection is managed independently by the JVM,
not through this psycopg2 connection).
"""

import sys
from pathlib import Path

import psycopg2

sys.path.append(str(Path(__file__).resolve().parent))
import config
from monitoring_logger import get_logger

logger = get_logger(__name__)


def get_connection():
    """Open a new psycopg2 connection using the configured credentials."""
    return psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
    )


def test_connection() -> bool:
    """Attempt to connect and run a trivial query. Returns True on success."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        logger.info("Database connection successful.")
        return True
    except Exception:
        logger.exception("Database connection failed.")
        return False


def run_query(query: str) -> list[tuple]:
    """Run a read-only query and return all rows."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()


# --- Verification queries (see docs/data_contract.md) ---

def row_count() -> int:
    return run_query("SELECT COUNT(*) FROM events;")[0][0]


def rejected_count() -> int:
    return run_query("SELECT COUNT(*) FROM rejected_events;")[0][0]


def latest_events(n: int = 10) -> list[tuple]:
    return run_query(f"SELECT * FROM events ORDER BY ingested_at DESC LIMIT {n};")


def constraint_violations() -> list[tuple]:
    """
    Should always return zero rows. A non-empty result means invalid
    data reached the events table despite the CHECK constraints and
    Spark-side validation - a genuine bug worth investigating.
    """
    return run_query("SELECT * FROM events WHERE price < 0 OR quantity <= 0;")


def duplicate_event_ids() -> list[tuple]:
    """Should always return zero rows, proving the PRIMARY KEY/upsert worked."""
    return run_query(
        "SELECT event_id, COUNT(*) FROM events GROUP BY event_id HAVING COUNT(*) > 1;"
    )


def rows_per_minute() -> list[tuple]:
    """Throughput check: how many rows were inserted per minute."""
    return run_query(
        """
        SELECT date_trunc('minute', ingested_at) AS minute, COUNT(*)
        FROM events GROUP BY minute ORDER BY minute;
        """
    )


def rejection_reason_breakdown() -> list[tuple]:
    """
    Counts rejected rows by reason - the core input for
    docs/data_quality_report.md. Grouping by reason turns the raw
    rejected_events table into an actionable summary: which specific
    contract rule is triggering most often.
    """
    return run_query(
        """
        SELECT rejection_reason, COUNT(*) AS count
        FROM rejected_events
        GROUP BY rejection_reason
        ORDER BY count DESC;
        """
    )


if __name__ == "__main__":
    if test_connection():
        print(f"Total events: {row_count()}")
        print(f"Total rejected: {rejected_count()}")
        print(f"Constraint violations (should be 0): {len(constraint_violations())}")
        print(f"Duplicate event_ids (should be 0): {len(duplicate_event_ids())}")