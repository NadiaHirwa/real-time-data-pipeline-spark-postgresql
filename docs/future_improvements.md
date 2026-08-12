# Future Improvements

Concrete, non-exhaustive ideas for extending this project, organized by which part of the system they'd affect. None of these are implemented; this is a forward-looking list, distinct from risks_and_limitations.md's list of current, known gaps.

## Ingestion

- Replace CSV-file simulation with a real Kafka topic, making this an actually-realistic streaming source rather than a simulation
- Add schema evolution handling (e.g. Avro schemas with a schema registry) if the event structure needs to change over time without breaking the pipeline

## Processing

- Tune TRIGGER_INTERVAL_SECONDS based on the findings in performance_metrics.md - the staging+merge rewrite substantially reduced how often this matters (only 1 of 24 batches fell behind in the post-rewrite test, versus 17 of 18 before), but the original mismatch between configured and actual cadence was never formally corrected
- ~~Implement a connection pool instead of opening a fresh connection per partition per batch~~ **Done** - superseded by the staging table + SQL merge rewrite, which eliminated per-partition connections entirely (see engineering_decisions.md and performance_metrics.md)
- Add windowed aggregations (e.g. "purchases per minute") as a second, derived output alongside the raw event table - ties directly into DEM05's Structured Streaming windowing/watermarking content
- Log ALL violated rules per row, not just the first matching one (see the deliberate simplification noted in data_contract.md)
- ~~Investigate the stream_metrics.num_input_rows 3x multiplier~~ **Resolved** - re-investigated and found to be a measurement artifact (unfiltered queries mixing rows from multiple run_ids), not a real Spark bug; see risks_and_limitations.md for the corrected finding
- Deliberately test the retry/backoff logic against a genuine PostgreSQL outage (e.g. stopping the service mid-batch), rather than only unit-testing the error classification logic in isolation (see test_cases.md)

## Storage

- Fix the zero-row file archiving gap properly. THREE approaches have now been investigated and all three reverted, each documented in risks_and_limitations.md: (1) deriving the file list from row content, which cannot see a zero-row file at all, since no row exists referencing it; (2) DataFrame.inputFiles(), which returns empty for a zero-row Structured Streaming micro-batch even though it works correctly on an equivalent plain batch read; and (3) Spark's own cleanSource=archive option, which DOES correctly identify zero-row files (unlike the first two) but fails on Windows with "Mkdirs failed" when moving them, because Spark's SourceFileArchiver nests the source file's full absolute path - drive letter included - under the archive directory, producing a path Windows cannot create. A real fix therefore needs one of two things: (a) testing cleanSource=archive on Linux or macOS, where there is no drive-letter prefix to cause that collision, to establish whether it works correctly there - currently an untested open question, not a known outcome; or (b) implementing filesystem-level tracking as originally proposed (list data/incoming/ before a trigger runs, archive exactly those files once the batch succeeds) rather than relying on any DataFrame-level or Spark-internal metadata. Option (b) remains a genuinely larger change (coordinating a pre-trigger file listing with a post-success archive step) than originally estimated, which is why it wasn't completed in this version
- Add a retention policy with automated cleanup for data/processed_archive/ and logs/, rather than leaving them to grow indefinitely (see retention_policy.md)
- Move database credentials from a checked-in postgres_connection_details.txt to a proper secrets manager, even for a local/demo deployment

## Operations

- Containerize the whole pipeline (Docker Compose for Postgres + the Spark job) for consistent setup across machines - explicitly listed as a bonus in the original project guidance, not pursued here since PostgreSQL was already installed and working natively
- Add automated alerting (e.g. a Slack webhook) on repeated batch failures, rather than relying on someone actively watching logs
- ~~Add a CI pipeline running the test suite on every push~~ **Done** - .github/workflows/ci.yml runs the full 27-test suite against a real PostgreSQL service container on every push (see architecture.md)

## Testing

- Deliberately test the remaining untested system-level scenarios in test_cases.md (database outage mid-batch, checkpoint deletion) rather than relying on Spark's documented-but-unverified-here guarantees
- Run the performance test multiple times and report a distribution rather than a single sample (see performance_methodology.md's stated limitation)
- Fix the zero-row file archiving gap discovered during testing (see the Storage section above) and add a regression test for it

## Architecture

- Explore Delta Lake or Apache Iceberg as the storage layer instead of a plain PostgreSQL table, connecting this project back to the table-format concepts covered in this course's earlier Spark module sections
- Introduce Apache Airflow to orchestrate scheduled maintenance tasks (archive cleanup, performance report generation) around the always-on streaming job