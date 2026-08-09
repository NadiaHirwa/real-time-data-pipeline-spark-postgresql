"""
retry.py

Bounded exponential backoff with full jitter, applied ONLY to errors
the taxonomy in errors.py classifies as transient. A missing table
or a bad password fails on the first attempt - retrying those just
delays the real error by tens of seconds and buries the actual cause.

Adapted from a pattern reviewed in a peer's implementation of the
same assignment - see docs/engineering_decisions.md for attribution.
"""

from __future__ import annotations

import random
import sys
import time
from pathlib import Path
from typing import Callable, TypeVar

sys.path.append(str(Path(__file__).resolve().parent))
from errors import PermanentDatabaseError, TransientDatabaseError, classify_db_error
from monitoring_logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BASE_DELAY = 0.5
DEFAULT_MAX_DELAY = 15.0


def with_retry(
    fn: Callable[[], T],
    *,
    what: str,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
) -> T:
    """
    Run fn(), retrying only errors classified as transient by
    classify_db_error(). Uses FULL jitter (sleep = random(0, backoff))
    rather than a fixed backoff: if several batches/partitions
    reconnect after the same outage, identical sleep schedules would
    make them all retry in lockstep and hammer the database together.
    """
    last: BaseException | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as exc:
            classified = classify_db_error(exc)

            if isinstance(classified, PermanentDatabaseError):
                logger.error("%s failed permanently: %s", what, classified)
                raise classified from exc

            last = classified
            if attempt == max_attempts:
                break

            backoff = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay = random.uniform(0.0, backoff)
            logger.warning(
                "%s failed (attempt %d/%d): %s - retrying in %.2fs",
                what, attempt, max_attempts, classified, delay,
            )
            time.sleep(delay)

    logger.error("%s exhausted %d attempts", what, max_attempts)
    raise TransientDatabaseError(f"{what} failed after {max_attempts} attempts: {last}") from last