"""
test_spark_streaming.py

Unit tests for the validation logic in spark_streaming.py, covering
every rule in docs/data_contract.md directly - this is the core
correctness logic of the whole pipeline, so it's tested independently
of the actual streaming/file-watching machinery around it.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession, Row

sys.path.append(str(Path(__file__).resolve().parent.parent / "scripts"))

from spark_streaming import cast_and_normalize, tag_validation_result, EVENT_SCHEMA

@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("TestValidation").master("local[1]").getOrCreate()
    yield session
    session.stop()


def make_raw_row(**overrides) -> Row:
    """
    Build one raw (all-string) event row, matching EVENT_SCHEMA, with
    sensible valid defaults that individual tests override to trigger
    exactly one contract violation at a time.

    _corrupt_record defaults to None (no corruption) - added after
    EVENT_SCHEMA gained this column for malformed-CSV-row detection
    (see docs/data_contract.md); without it, these manually-built
    test rows lack the column entirely, since Spark infers a
    DataFrame's schema from the Row objects' own fields when no
    explicit schema is given, not from EVENT_SCHEMA itself.
    """
    defaults = {
        "event_id": "11111111-1111-1111-1111-111111111111",
        "user_id": "123",
        "product_id": "456",
        "event_type": "view",
        "price": "19.99",
        "quantity": "1",
        "category": "Books",
        "event_timestamp": "2026-01-01 12:00:00",
        "_corrupt_record": None,
    }
    defaults.update(overrides)
    return Row(**defaults)


def validate(spark, **overrides) -> str | None:
    """Run one row through cast_and_normalize + tag_validation_result, return its rejection_reason."""
    df = spark.createDataFrame([make_raw_row(**overrides)], schema=EVENT_SCHEMA)
    result = tag_validation_result(cast_and_normalize(df))
    return result.collect()[0]["rejection_reason"]


def test_fully_valid_row_is_not_rejected(spark):
    assert validate(spark) is None


def test_missing_user_id_is_rejected(spark):
    assert validate(spark, user_id="") == "missing_or_invalid_user_id"


def test_non_numeric_user_id_is_rejected(spark):
    """try_cast() turns an unparseable value into null, which the missing-value rule then catches."""
    assert validate(spark, user_id="not_a_number") == "missing_or_invalid_user_id"


def test_missing_product_id_is_rejected(spark):
    assert validate(spark, product_id="") == "missing_or_invalid_product_id"


def test_invalid_event_type_is_rejected(spark):
    assert validate(spark, event_type="click") == "invalid_event_type"


def test_event_type_normalization_is_case_and_whitespace_insensitive(spark):
    """'  Purchase  ' must be treated identically to 'purchase' - see data_contract.md normalization rules."""
    assert validate(spark, event_type="  Purchase  ") is None


def test_negative_price_is_rejected(spark):
    assert validate(spark, price="-10.00") == "invalid_or_negative_price"


def test_zero_price_is_accepted(spark):
    """Zero is a valid, if unusual, price (e.g. a free promotional item) - only negative values are rejected."""
    assert validate(spark, price="0.00") is None


def test_zero_quantity_is_rejected(spark):
    assert validate(spark, quantity="0") == "invalid_or_zero_quantity"


def test_negative_quantity_is_rejected(spark):
    assert validate(spark, quantity="-1") == "invalid_or_zero_quantity"


def test_unparseable_timestamp_is_rejected(spark):
    assert validate(spark, event_timestamp="not a real date") == "unparseable_timestamp"


def test_far_future_timestamp_is_rejected(spark):
    assert validate(spark, event_timestamp="2099-01-01 00:00:00") == "future_timestamp"


def test_first_matching_rule_wins_when_multiple_violations_exist(spark):
    """
    A row with BOTH a missing user_id and an invalid event_type should
    report only the FIRST rule checked (missing_or_invalid_user_id),
    per tag_validation_result()'s fixed check order - see
    docs/data_contract.md's note on this deliberate simplification.
    """
    assert validate(spark, user_id="", event_type="click") == "missing_or_invalid_user_id"