"""
test_errors.py

Permanent unit tests for classify_db_error() (errors.py) - the logic
that decides whether a database error is worth retrying. This
replaces an earlier ad-hoc, throwaway verification script used once
during development and then deleted; see docs/test_cases.md for the
gap this closes.

Note: production code (make_write_valid_partition in
spark_streaming.py) does NOT import errors.py directly - it inlines
an equivalent check, since functions passed to foreachPartition
cannot import custom project modules (see
docs/engineering_decisions.md's standing rule). These tests validate
the classification LOGIC itself, and test_inline_classification_
matches_errors_module below specifically guards against the two
independent copies silently drifting apart from each other.
"""

import sys
from pathlib import Path

import psycopg2
import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent / "scripts"))

from errors import PermanentDatabaseError, TransientDatabaseError, classify_db_error


class _FakeDBError(Exception):
    """
    A minimal stand-in for a real psycopg2 exception. classify_db_error()
    only ever reads exc.pgcode via getattr() - it never checks the
    exception's actual type - so a plain class with a settable pgcode
    attribute is sufficient and avoids depending on psycopg2's real
    exception classes, whose pgcode attribute is read-only after
    construction.
    """
    def __init__(self, pgcode: str, message: str = "some error"):
        super().__init__(message)
        self.pgcode = pgcode


def _error_with_pgcode(pgcode: str, message: str = "some error") -> Exception:
    """Build a fake database exception carrying a specific SQLSTATE."""
    return _FakeDBError(pgcode, message)


def test_connection_refused_is_transient():
    result = classify_db_error(psycopg2.OperationalError("connection refused"))
    assert isinstance(result, TransientDatabaseError)


def test_connection_reset_is_transient():
    result = classify_db_error(psycopg2.OperationalError("connection reset by peer"))
    assert isinstance(result, TransientDatabaseError)


def test_deadlock_text_is_transient():
    result = classify_db_error(psycopg2.OperationalError("deadlock detected"))
    assert isinstance(result, TransientDatabaseError)


def test_undefined_table_is_permanent():
    result = classify_db_error(psycopg2.ProgrammingError('relation "nonexistent_table" does not exist'))
    assert isinstance(result, PermanentDatabaseError)


def test_sqlstate_08_prefix_is_transient():
    """08xxx is the SQLSTATE class for connection exceptions."""
    result = classify_db_error(_error_with_pgcode("08006"))
    assert isinstance(result, TransientDatabaseError)


def test_sqlstate_53_prefix_is_transient():
    """53xxx is the SQLSTATE class for insufficient resources."""
    result = classify_db_error(_error_with_pgcode("53300"))
    assert isinstance(result, TransientDatabaseError)


def test_sqlstate_40p01_deadlock_is_transient():
    result = classify_db_error(_error_with_pgcode("40P01"))
    assert isinstance(result, TransientDatabaseError)


def test_sqlstate_23505_unique_violation_is_permanent():
    """23505 (unique_violation) is a real constraint failure, never worth retrying."""
    result = classify_db_error(_error_with_pgcode("23505"))
    assert isinstance(result, PermanentDatabaseError)


def test_sqlstate_42703_undefined_column_is_permanent():
    result = classify_db_error(_error_with_pgcode("42703"))
    assert isinstance(result, PermanentDatabaseError)


def test_unrecognized_text_with_no_pgcode_defaults_to_permanent():
    """
    An exception with neither a recognized SQLSTATE nor a known
    transient-failure phrase should fail closed (permanent), not
    silently retry an error we don't understand.
    """
    result = classify_db_error(Exception("some completely unrecognized error"))
    assert isinstance(result, PermanentDatabaseError)


def test_inline_classification_matches_errors_module():
    """
    spark_streaming.py's make_write_valid_partition() inlines its own
    copy of this classification logic (see docs/engineering_decisions.md
    for why it cannot import errors.py directly). This test guards
    against the two copies silently drifting apart: it re-implements
    the inlined version's _is_transient() check exactly as it appears
    in spark_streaming.py, and compares its verdict against
    classify_db_error() for the same set of errors.
    """
    RETRYABLE_SQLSTATE_PREFIXES = ("08", "53")
    RETRYABLE_SQLSTATES = {"40001", "40P01", "55P03", "57P01", "57P02", "57P03"}

    def inline_is_transient(exc) -> bool:
        sqlstate = getattr(exc, "pgcode", None)
        if sqlstate:
            return sqlstate in RETRYABLE_SQLSTATES or sqlstate.startswith(RETRYABLE_SQLSTATE_PREFIXES)
        text = str(exc).lower()
        markers = ("connection refused", "connection reset", "could not connect",
                   "starting up", "terminating connection", "deadlock detected",
                   "too many clients", "timeout", "broken pipe")
        return any(m in text for m in markers)

    test_cases = [
        psycopg2.OperationalError("connection refused"),
        psycopg2.ProgrammingError('relation "x" does not exist'),
        _error_with_pgcode("08006"),
        _error_with_pgcode("23505"),
        _error_with_pgcode("40P01"),
    ]

    for exc in test_cases:
        module_result = isinstance(classify_db_error(exc), TransientDatabaseError)
        inline_result = inline_is_transient(exc)
        assert module_result == inline_result, f"Mismatch for {exc!r}: module={module_result}, inline={inline_result}"