# Future Improvements

Concrete, non-exhaustive ideas for extending this project, organized by which part of the system they'd affect. None of these are implemented; this is a forward-looking list, distinct from risks_and_limitations.md's list of current, known gaps.

## Ingestion

- Replace CSV-file simulation with a real Kafka topic, making this an actually-realistic streaming source rather than a simulation
- Add schema evolution handling (e.g. Avro schemas with a schema registry) if the event structure needs to change over time without breaking the pipeline

## Processing

- Tune TRIGGER_INTERVAL_SECONDS based on the findings in performance_metrics.md (currently too aggressive for the tested write pattern)
- Implement a connection pool (e.g. psycopg2.pool.ThreadedConnectionPool) instead of opening a fresh connection per partition per batch, to reduce the dominant source of per-batch latency identified during testing
- Add windowed aggregations (e.g. "purchases per minute") as a second, derived output alongside the raw event table - ties directly into DEM05's Structured Streaming windowing/watermarking content
- Log ALL violated rules per row, not just the first matching one (see the deliberate simplification noted in data_contract.md)

## Storage

- Add a retention policy with automated cleanup for data/processed_archive/ and logs/, rather than leaving them to grow indefinitely (see retention_policy.md)
- Move database credentials from a checked-in postgres_connection_details.txt to a proper secrets manager, even for a local/demo deployment

## Operations

- Containerize the whole pipeline (Docker Compose for Postgres + the Spark job) for consistent setup across machines - explicitly listed as a bonus in the original project guidance, not pursued here since PostgreSQL was already installed and working natively
- Add automated alerting (e.g. a Slack webhook) on repeated batch failures, rather than relying on someone actively watching logs
- Add a CI pipeline (GitHub Actions) running the pytest suite on every push, matching the pattern used in the companion TMDB Spark project

## Testing

- Deliberately test the untested system-level scenarios listed in test_cases.md (database outage mid-batch, checkpoint deletion, corrupted CSV) rather than relying on Spark's documented-but-unverified-here guarantees
- Run the performance test multiple times and report a distribution rather than a single sample (see performance_methodology.md's stated limitation)

## Architecture

- Explore Delta Lake or Apache Iceberg as the storage layer instead of a plain PostgreSQL table, connecting this project back to the table-format concepts covered in this course's earlier Spark module sections
- Introduce Apache Airflow to orchestrate scheduled maintenance tasks (archive cleanup, performance report generation) around the always-on streaming job