"""
test_edge_cases.py

Permanent automated tests for three scenarios originally verified
manually, once, with a throwaway script (see docs/test_cases.md rows
5, 20, 21 for the full narrative). Converting them into real,
repeatable pytest tests means anyone - a supervisor, a reviewer, a
future version of this project - can re-run these and get the same
answer, rather than trusting a one-time manual claim.
"""

import sys
import uuid
from pathlib import Path

import pytest
from pyspark.sql import Row

sys.path.append(str(Path(__file__).resolve().parent.parent / "scripts"))

from spark_streaming import (
    EVENT_SCHEMA,
    cast_and_normalize,
    tag_validation_result,
    split_valid_and_rejected,
)


def test_duplicate_event_id_within_one_batch_is_dropped_to_one_row(spark):
    """
    Row 5 in test_cases.md. Two rows sharing the identical event_id,
    presented as one micro-batch, must collapse to exactly one row
    on the valid side - dropDuplicates() runs inside
    split_valid_and_rejected().
    """
    shared_id = str(uuid.uuid4())
    df = spark.createDataFrame(
        [
            Row(
                event_id=shared_id, user_id="111", product_id="222", event_type="view",
                price="49.99", quantity="1", category="Books",
                event_timestamp="2026-01-01 12:00:00", _corrupt_record=None,
            ),
            Row(
                event_id=shared_id, user_id="333", product_id="444", event_type="purchase",
                price="29.99", quantity="2", category="Toys",
                event_timestamp="2026-01-01 12:00:05", _corrupt_record=None,
            ),
        ],
        schema=EVENT_SCHEMA,
    )

    tagged = tag_validation_result(cast_and_normalize(df))
    valid_df, _ = split_valid_and_rejected(tagged)

    matching_rows = valid_df.filter(valid_df.event_id == shared_id).collect()
    assert len(matching_rows) == 1


def test_csv_missing_a_column_entirely_is_rejected_as_malformed(spark, tmp_path):
    """
    Row 20 in test_cases.md. A CSV whose HEADER is missing a column
    entirely (not just an empty value) triggers Spark's PERMISSIVE
    mode to flag the whole row as structurally corrupt, rather than
    aligning by name and leaving the missing field null. This is a
    real, specific result - not the "missing_or_invalid_quantity"
    outcome originally guessed before this was actually tested.
    """
    csv_path = tmp_path / "missing_column.csv"
    csv_path.write_text(
        "event_id,user_id,product_id,event_type,price,category,event_timestamp\n"
        "eeee8888-8888-8888-8888-888888888888,555,666,view,39.99,Sports,2026-01-01 12:00:00\n",
        encoding="utf-8",
    )

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(EVENT_SCHEMA)
        .csv(str(csv_path))
    )

    tagged = tag_validation_result(cast_and_normalize(df))
    row = tagged.collect()[0]
    assert row["rejection_reason"] == "malformed_csv_row"


def test_large_batch_of_5000_rows_processes_without_error(spark):
    """
    Row 21 in test_cases.md. A single batch of 5,000 rows (250x the
    typical 20-row test size) must pass through casting and
    validation without error, and produce exactly 5,000 tagged rows.
    This does not test the full streaming file-watch mechanism (that
    requires main.py stream, exercised manually - see
    scripts/manual_tests/), only that the transformation logic itself
    scales without breaking.
    """
    rows = [
        Row(
            event_id=str(uuid.uuid4()), user_id=str(1000 + i), product_id=str(100 + (i % 900)),
            event_type="view" if i % 5 else "purchase", price=str(round(9.99 + i * 0.01, 2)),
            quantity="1", category="Books", event_timestamp="2026-01-01 12:00:00",
            _corrupt_record=None,
        )
        for i in range(5000)
    ]
    df = spark.createDataFrame(rows, schema=EVENT_SCHEMA)

    tagged = tag_validation_result(cast_and_normalize(df))
    valid_df, rejected_df = split_valid_and_rejected(tagged)

    assert valid_df.count() + rejected_df.count() == 5000


def test_null_event_type_is_rejected_not_silently_accepted(spark):
    """
    Real bug found by comparing against a peer implementation: Spark's
    ~col.isin(...) evaluates to NULL (not True) when col itself is
    NULL, and a .when(NULL, ...) condition never fires. If
    tag_validation_result()'s event_type check were written as plain
    ~F.col("event_type").isin(...) without an explicit isNull() guard,
    a genuinely NULL event_type could silently fall through validation
    entirely and be marked VALID if no other rule happened to also
    catch that row. This test uses a row where EVERY other field is
    otherwise perfectly valid, so if this specific check has the bug,
    nothing else would catch it and the row would incorrectly pass.
    """
    row = Row(
        event_id=str(uuid.uuid4()), user_id="123", product_id="456",
        event_type=None, price="19.99", quantity="1", category="Books",
        event_timestamp="2026-01-01 12:00:00", _corrupt_record=None,
    )
    df = spark.createDataFrame([row], schema=EVENT_SCHEMA)

    tagged = tag_validation_result(cast_and_normalize(df))
    result = tagged.collect()[0]

    assert result["rejection_reason"] is not None, (
        "A row with event_type=None was NOT rejected - this is the exact "
        "null-unsafe isin() bug: ~col.isin(...) evaluates to NULL when col "
        "is NULL, so the .when() condition never fires and the row is "
        "incorrectly treated as valid."
    )
    assert result["rejection_reason"] == "invalid_event_type"