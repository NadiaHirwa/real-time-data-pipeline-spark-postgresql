"""
errors.py

Explicit error taxonomy for database failures. Distinguishing a
RETRYABLE infrastructure failure (connection refused, deadlock) from
a PERMANENT data/configuration failure (bad table name, constraint
violation) is what lets retry logic decide whether backing off is
even worth trying. A bare `except Exception: retry` would sit in a
loop for minutes against a typo in a table name.

Adapted from a pattern reviewed in a peer's implementation of the
same assignment - see docs/engineering_decisions.md for attribution
and reasoning.
"""

from __future__ import annotations


class PipelineError(Exception):
    """Base class for every error this project raises deliberately."""


class TransientDatabaseError(PipelineError):
    """
    Connection refused, server restarting, deadlock, serialization
    failure. Worth retrying with backoff.
    """


class PermanentDatabaseError(PipelineError):
    """
    Undefined table, bad credentials, constraint violation, syntax
    error. Retrying cannot help; fail loudly and immediately.
    """


# PostgreSQL SQLSTATE classes that a retry can plausibly resolve.
#   08xxx  connection exception
#   40001  serialization failure
#   40P01  deadlock detected
#   53xxx  insufficient resources (out of memory, too many connections)
#   55P03  lock_not_available
#   57P01/02/03  admin/crash shutdown, cannot connect now
RETRYABLE_SQLSTATE_PREFIXES = ("08", "53")
RETRYABLE_SQLSTATES = frozenset({"40001", "40P01", "55P03", "57P01", "57P02", "57P03"})

_TRANSIENT_TEXT_MARKERS = (
    "connection refused",
    "connection reset",
    "could not connect",
    "the database system is starting up",
    "terminating connection",
    "deadlock detected",
    "too many clients",
    "timeout",
    "broken pipe",
)


def classify_db_error(exc: BaseException) -> PipelineError:
    """
    Map a psycopg2 (or any driver) exception onto the taxonomy above.
    Uses the SQLSTATE code when available (psycopg2 exposes this via
    exc.pgcode); falls back to matching known transient-failure
    phrases in the error message when it isn't.
    """
    sqlstate = getattr(exc, "pgcode", None)
    if sqlstate:
        if sqlstate in RETRYABLE_SQLSTATES or sqlstate.startswith(RETRYABLE_SQLSTATE_PREFIXES):
            return TransientDatabaseError(f"[SQLSTATE {sqlstate}] {exc}")
        return PermanentDatabaseError(f"[SQLSTATE {sqlstate}] {exc}")

    text = str(exc).lower()
    if any(marker in text for marker in _TRANSIENT_TEXT_MARKERS):
        return TransientDatabaseError(str(exc))
    return PermanentDatabaseError(str(exc))