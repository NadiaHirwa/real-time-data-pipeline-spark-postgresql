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
    project_rejected_for_write,
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


def test_null_event_id_is_rejected_not_silently_accepted(spark):
    """
    A second instance of the same bug class as
    test_null_event_type_is_rejected_not_silently_accepted, found by
    checking every other .when() condition in tag_validation_result()
    for the same pattern. F.col("event_id").rlike(pattern) follows
    the same three-valued logic as isin(): NULL.rlike(...) evaluates
    to NULL, not False, so ~NULL is still NULL and the .when()
    condition never fires. Since no OTHER check in this function
    tests event_id's nullness, a row with event_id=None could fall
    through every single check and be marked VALID - on the PRIMARY
    KEY column, which is more severe than the event_type instance of
    this same bug.
    """
    row = Row(
        event_id=None, user_id="123", product_id="456",
        event_type="view", price="19.99", quantity="1", category="Books",
        event_timestamp="2026-01-01 12:00:00", _corrupt_record=None,
    )
    df = spark.createDataFrame([row], schema=EVENT_SCHEMA)

    tagged = tag_validation_result(cast_and_normalize(df))
    result = tagged.collect()[0]

    assert result["rejection_reason"] is not None, (
        "A row with event_id=None was NOT rejected - the same null-unsafe "
        "rlike() pattern as the event_type bug, but on the PRIMARY KEY "
        "column."
    )
    assert result["rejection_reason"] == "invalid_event_id_format"


def test_rejected_row_preserves_original_value_that_failed_to_cast(spark):
    """
    Diagnostic-quality gap found by comparing against a peer
    implementation. cast_and_normalize() try_casts each numeric field,
    so an unparseable value becomes NULL - correct for validation, but
    it used to mean rejected_events recorded only that price failed,
    never that the offending value was "not_a_number". An analyst
    reviewing quarantine could see the verdict but not the evidence.

    This asserts against project_rejected_for_write(), the actual
    projection write_rejected_to_postgres() sends to the database, so
    it tests what really gets stored rather than a re-implementation
    of it. Every field here is deliberately valid except price, so the
    row is rejected for exactly one reason.
    """
    row = Row(
        event_id=str(uuid.uuid4()), user_id="123", product_id="456",
        event_type="view", price="not_a_number", quantity="1", category="Books",
        event_timestamp="2026-01-01 12:00:00", _corrupt_record=None,
    )
    df = spark.createDataFrame([row], schema=EVENT_SCHEMA)

    tagged = tag_validation_result(cast_and_normalize(df))
    _, rejected_df = split_valid_and_rejected(tagged)

    # The typed column is still NULL - validation depends on that, and
    # this test must not accidentally "fix" it by weakening the cast.
    assert rejected_df.collect()[0]["price"] is None

    written = project_rejected_for_write(rejected_df).collect()[0]

    assert written["price"] == "not_a_number", (
        f"rejected_events would record price={written['price']!r} instead of "
        "the original 'not_a_number' - the value that caused the rejection "
        "is exactly what quarantine needs to preserve."
    )
    assert written["rejection_reason"] == "invalid_or_negative_price"
    # Fields that cast cleanly still round-trip their original text.
    assert written["user_id"] == "123"
    assert written["quantity"] == "1"


def test_malformed_row_preserves_raw_csv_text_in_corrupt_record(spark, tmp_path):
    """
    Companion to the test above, for the one rejection type that has
    no usable field values at all. A structurally broken CSV row is
    tagged malformed_csv_row, and every parsed column is null - so the
    raw text Spark captured in _corrupt_record is the ONLY evidence of
    what arrived. It was computed and carried through validation, then
    silently dropped at write time because write_rejected_to_postgres()
    never selected it.
    """
    csv_path = tmp_path / "malformed.csv"
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
    _, rejected_df = split_valid_and_rejected(tagged)
    written = project_rejected_for_write(rejected_df).collect()[0]

    assert written["rejection_reason"] == "malformed_csv_row"
    assert written["corrupt_record"] is not None, (
        "A malformed_csv_row rejection reached rejected_events with no "
        "corrupt_record text - the only diagnostic such a row has."
    )
    # The captured text must be the actual offending line, not a placeholder.
    assert "eeee8888-8888-8888-8888-888888888888" in written["corrupt_record"]
    assert "39.99" in written["corrupt_record"]