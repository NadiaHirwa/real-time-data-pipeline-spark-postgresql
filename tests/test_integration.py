"""
test_integration.py

Integration test exercising the REAL write path (write_valid_to_postgres
+ merge_staging_to_events) against a live PostgreSQL instance, unlike
test_spark_streaming.py which tests validation logic in isolation with
no database involved.

Requires a running PostgreSQL matching the connection details in .env
(or the DB_* environment variables set by CI - see
.github/workflows/ci.yml). Skipped automatically if no database is
reachable, so this file doesn't break local test runs for anyone
who hasn't got Postgres running.
"""

import sys
import uuid
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession, Row

sys.path.append(str(Path(__file__).resolve().parent.parent / "scripts"))

import config
import database
from spark_streaming import write_valid_to_postgres, merge_staging_to_events


def _db_available() -> bool:
    try:
        return database.test_connection()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _db_available(), reason="No reachable PostgreSQL instance")


# @pytest.fixture(scope="module")
# def spark():
#     session = (
#         SparkSession.builder
#         .appName("TestIntegration")
#         .master("local[1]")
#         .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
#         .getOrCreate()
#     )
#     yield session
#     session.stop()


@pytest.fixture(autouse=True)
def clean_tables():
    """Ensure a clean slate before and after each test in this file."""
    database.execute_statement("TRUNCATE TABLE events, staging_events;")
    yield
    database.execute_statement("TRUNCATE TABLE events, staging_events;")


@pytest.mark.integration
def test_valid_rows_reach_events_table_via_staging(spark):
    """
    The full write path: stage via Spark's bulk JDBC writer, then
    merge into events. This is the actual code path spark_streaming.py
    uses in production - not a simplified re-implementation of it.
    """
    run_id = str(uuid.uuid4())
    batch_id = 0

    df = spark.createDataFrame([
        Row(
            event_id="11111111-1111-1111-1111-111111111111",
            user_id=1, product_id=2, event_type="view",
            price=9.99, quantity=1, category="Books",
            event_timestamp=datetime(2026, 1, 1, 12, 0, 0),
        )
    ])

    write_valid_to_postgres(df, config.JDBC_URL, run_id, batch_id)
    merge_staging_to_events(run_id, batch_id)

    rows = database.run_query("SELECT event_id, price FROM events;")
    assert len(rows) == 1
    assert str(rows[0][0]) == "11111111-1111-1111-1111-111111111111"


@pytest.mark.integration
def test_merge_is_idempotent_on_duplicate_event_id(spark):
    """
    Running the same event_id through the write path twice (e.g. a
    retried batch after a transient failure) must not create a
    duplicate row - this is what ON CONFLICT DO NOTHING is for.
    """
    row = Row(
        event_id="22222222-2222-2222-2222-222222222222",
        user_id=1, product_id=2, event_type="purchase",
        price=19.99, quantity=1, category="Toys",
        event_timestamp=datetime(2026, 1, 1, 12, 0, 0),
    )
    df = spark.createDataFrame([row])

    run_id_1 = str(uuid.uuid4())
    write_valid_to_postgres(df, config.JDBC_URL, run_id_1, 0)
    merge_staging_to_events(run_id_1, 0)

    run_id_2 = str(uuid.uuid4())
    write_valid_to_postgres(df, config.JDBC_URL, run_id_2, 0)
    merge_staging_to_events(run_id_2, 0)

    rows = database.run_query(
        "SELECT COUNT(*) FROM events WHERE event_id = '22222222-2222-2222-2222-222222222222';"
    )
    assert rows[0][0] == 1


@pytest.mark.integration
def test_staging_table_is_empty_after_successful_merge(spark):
    """Staged rows for a batch should be deleted once merged - see docs/engineering_decisions.md."""
    run_id = str(uuid.uuid4())
    df = spark.createDataFrame([
        Row(
            event_id="33333333-3333-3333-3333-333333333333",
            user_id=1, product_id=2, event_type="view",
            price=5.00, quantity=1, category="Sports",
            event_timestamp=datetime(2026, 1, 1, 12, 0, 0),
        )
    ])

    write_valid_to_postgres(df, config.JDBC_URL, run_id, 0)
    merge_staging_to_events(run_id, 0)

    remaining = database.run_query(
        "SELECT COUNT(*) FROM staging_events WHERE run_id = %s;" % f"'{run_id}'"
    )
    assert remaining[0][0] == 0