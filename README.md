# Real-Time E-Commerce Streaming Pipeline

A data pipeline that simulates an e-commerce platform's user activity, streams it in near-real-time using Apache Spark Structured Streaming, validates it against a defined data contract, and stores the results in PostgreSQL - with invalid records routed to a queryable quarantine table rather than silently dropped.

See [`docs/project_overview.md`](docs/project_overview.md) for a fuller description of what this is and why it's built this way.

## Deliverables

| File | What it is |
|---|---|
| [`scripts/data_generator.py`](scripts/data_generator.py) | Generates realistic e-commerce events as CSV files (the producer) |
| [`scripts/spark_streaming.py`](scripts/spark_streaming.py) | Reads, validates, transforms, and writes events to PostgreSQL via a staging-table + SQL-merge upsert (the consumer) |
| [`scripts/metrics_listener.py`](scripts/metrics_listener.py) | StreamingQueryListener writing Spark's own per-batch timing to stream_metrics automatically |
| [`scripts/database.py`](scripts/database.py) | Connection handling and SQL verification queries |
| [`scripts/config.py`](scripts/config.py) | Centralized configuration - single source of truth for paths, credentials, and tunable settings |
| [`scripts/errors.py`](scripts/errors.py) | Error taxonomy distinguishing transient (retryable) from permanent database failures |
| [`sql/postgres_setup.sql`](sql/postgres_setup.sql) | Database, table, and view creation, including constraints matching the data contract |
| [`sql/postgres_setup_ci.sql`](sql/postgres_setup_ci.sql) | CI-specific schema (omits CREATE DATABASE, since the CI Postgres container creates it automatically) |
| [`main.py`](main.py) | CLI dispatcher (generator / stream / verify / test / clean / status) |
| [`.github/workflows/ci.yml`](.github/workflows/ci.yml) | Runs the full 27-test suite against a real PostgreSQL service container on every push |
| [`tests/test_spark_streaming.py`](tests/test_spark_streaming.py) | 13 tests covering every data contract validation rule |
| [`tests/test_integration.py`](tests/test_integration.py) | 3 tests exercising the real staging+merge write path against a live PostgreSQL |
| [`tests/test_errors.py`](tests/test_errors.py) | 11 tests covering the error classification logic, including a drift-guard against the inlined worker-side copy |
| [`postgres_connection_details.txt`](postgres_connection_details.txt) | Connection details (placeholder values - see the file itself and docs/engineering_decisions.md for why) |
| [`docs/project_overview.md`](docs/project_overview.md) | What this system does and how its pieces fit together |
| [`docs/user_guide.md`](docs/user_guide.md) | Step-by-step setup and run instructions - start here to actually run it |
| [`docs/architecture.md`](docs/architecture.md) | Full system design, technology justification, Spark/Postgres configuration, with diagrams |
| [`docs/sequence_and_state.md`](docs/sequence_and_state.md) | Step-by-step batch processing sequence and single-record lifecycle, with diagrams |
| [`docs/engineering_decisions.md`](docs/engineering_decisions.md) | Every significant technical decision, its reasoning, alternatives considered, and trade-offs - start here if you only read one file |
| [`docs/data_dictionary.md`](docs/data_dictionary.md) | Every column, its type, and its meaning |
| [`docs/data_contract.md`](docs/data_contract.md) | The enforceable validation rules every record must satisfy |
| [`docs/performance_methodology.md`](docs/performance_methodology.md) | How performance was measured |
| [`docs/performance_metrics.md`](docs/performance_metrics.md) | Real results from an 800-event controlled test run, including an honestly-diagnosed trigger-interval issue |
| [`docs/data_quality_report.md`](docs/data_quality_report.md) | Rejection breakdown by reason, integrity check results |
| [`docs/test_cases.md`](docs/test_cases.md) | Detailed manual test plan, including explicitly marked untested scenarios |
| [`docs/acceptance_tests.md`](docs/acceptance_tests.md) | Functional requirements checked against actual results |
| [`docs/error_handling_and_recovery.md`](docs/error_handling_and_recovery.md) | What happens when something goes wrong, and how the system recovers |
| [`docs/risks_and_limitations.md`](docs/risks_and_limitations.md) | Known gaps, honestly documented |
| [`docs/future_improvements.md`](docs/future_improvements.md) | Concrete ideas for extending this project |
| [`docs/scope.md`](docs/scope.md) | What is and is not covered |
| [`docs/assumptions_and_constraints.md`](docs/assumptions_and_constraints.md) | What was assumed vs. what was imposed |
| [`docs/naming_conventions.md`](docs/naming_conventions.md) | File, code, and database naming patterns used throughout |
| [`docs/retention_policy.md`](docs/retention_policy.md) | How long each category of data is kept |
| [`diagrams/`](diagrams/) | Architecture, sequence, and state diagrams (PNG) |
| `v_rejection_summary`, `v_batch_performance`, `v_pipeline_health` | SQL views (in sql/postgres_setup.sql) backing the data quality and performance reports with reusable queries, rather than one-off hand-typed SQL |

## Quick Start

```
pip install -r requirements.txt
cp .env.example .env          # then fill in your PostgreSQL credentials
# run sql/postgres_setup.sql against PostgreSQL (see docs/user_guide.md)
python main.py status         # confirm everything is wired up correctly

# in one terminal:
python main.py stream

# in another terminal:
python main.py generator
```

Full setup and troubleshooting: docs/user_guide.md.

## Key Results

- 844 of 880 events (95.9%) correctly validated and stored across cumulative testing, with 0 constraint violations and 0 duplicate IDs found across an entire day of repeated, overlapping runs, restarts, and mid-development crashes
- The staging table + SQL merge rewrite (replacing an earlier per-partition psycopg2 approach) cut last-file latency from ~13.4s to ~4.2s (~3.2x faster) and reduced trigger-interval overruns from 17 of 18 batches to 1 of 24
- CI runs the full 27-test suite against a real PostgreSQL service container on every push - this caught a real, otherwise-invisible bug (a missing `pytest` entry in requirements.txt, masked locally by Anaconda's pre-installed copy)
- Two real, previously-unknown gaps were found through deliberate edge-case testing and documented honestly rather than hidden: `stream_metrics.num_input_rows` reports a consistent, unexplained 3x multiple of the true row count, and zero-row files (empty or header-only CSVs) are read correctly but never archived

See [`docs/performance_metrics.md`](docs/performance_metrics.md), [`docs/data_quality_report.md`](docs/data_quality_report.md), and [`docs/risks_and_limitations.md`](docs/risks_and_limitations.md) for full detail.

## Tools

Python, Apache Spark 4.2.0 (Structured Streaming), PostgreSQL, psycopg2, Faker, pytest, python-dotenv, GitHub Actions. Developed and tested on Windows with Java 17 (Eclipse Temurin); CI runs on Ubuntu, confirming the pipeline is not accidentally Windows-specific.