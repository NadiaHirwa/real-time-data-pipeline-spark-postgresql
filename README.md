# Real-Time E-Commerce Streaming Pipeline

A data pipeline that simulates an e-commerce platform's user activity, streams it in near-real-time using Apache Spark Structured Streaming, validates it against a defined data contract, and stores the results in PostgreSQL - with invalid records routed to a queryable quarantine table rather than silently dropped.

See [`docs/project_overview.md`](docs/project_overview.md) for a fuller description of what this is and why it's built this way.

## Deliverables

| File | What it is |
|---|---|
| [`scripts/data_generator.py`](scripts/data_generator.py) | Generates realistic e-commerce events as CSV files (the producer) |
| [`scripts/spark_streaming.py`](scripts/spark_streaming.py) | Reads, validates, transforms, and writes events to PostgreSQL (the consumer) |
| [`scripts/database.py`](scripts/database.py) | Connection handling and SQL verification queries |
| [`scripts/config.py`](scripts/config.py) | Centralized configuration - single source of truth for paths, credentials, and tunable settings |
| [`sql/postgres_setup.sql`](sql/postgres_setup.sql) | Database and table creation, including constraints matching the data contract |
| [`main.py`](main.py) | CLI dispatcher (generator / stream / verify / test / clean / status) |
| [`tests/test_spark_streaming.py`](tests/test_spark_streaming.py) | 13 automated tests covering every data contract rule |
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

- 769 of 800 events (96.1%) correctly validated and stored in a single controlled test run, with 0 constraint violations and 0 duplicate IDs found across an entire day of repeated, overlapping testing
- A genuine performance issue (the configured 5-second trigger interval being too aggressive for the write pattern) was found, diagnosed, and documented with a proposed fix - not hidden
- A real bug (empty-string vs. null on zero-match array joins) was found and fixed via testing during development

See docs/performance_metrics.md and docs/data_quality_report.md for full detail.

## Tools

Python, Apache Spark 4.2.0 (Structured Streaming), PostgreSQL, psycopg2, Faker, pytest, python-dotenv. Developed and tested on Windows with Java 17 (Eclipse Temurin).