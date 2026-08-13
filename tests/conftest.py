"""
conftest.py

pytest automatically discovers this file and shares its fixtures
across every test file in this folder. This exists because Spark
only allows ONE SparkSession per process, and pytest runs the whole
test suite in a single process - if each test file creates its own
SparkSession independently, whichever one happens to run FIRST
(alphabetically, by default) "wins," and any configuration set by
a LATER file's session-creation call is silently ignored, since
getOrCreate() just returns the already-existing session.

This was discovered as a real bug: adding test_edge_cases.py (which
created a session with no PostgreSQL driver configured) caused
test_integration.py's tests to fail with ClassNotFoundException,
even though test_integration.py's own fixture correctly configured
the driver - because by the time it ran, test_edge_cases.py's
undriven session already existed. A single shared, correctly
configured fixture here fixes this permanently, regardless of file
naming or execution order.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.append(str(Path(__file__).resolve().parent.parent / "scripts"))

from spark_streaming import with_postgres_driver


@pytest.fixture(scope="session")
def spark():
    # The driver is attached by the same helper the streaming job uses,
    # rather than hardcoding spark.jars.packages here. That keeps this
    # fixture correct in both environments: Maven natively, and the
    # jar baked into the Docker image when running inside a container
    # (where a Maven fetch would need network access on every run).
    session = with_postgres_driver(
        SparkSession.builder
        .appName("TestSuite")
        .master("local[1]")
    ).getOrCreate()
    yield session
    session.stop()