# Acceptance Tests

High-level pass/fail checks tied directly to the Functional Requirements listed in the project's planning notes. Distinct from test_cases.md (detailed scenarios) and the automated unit tests (isolated logic checks) - this is the "did we build what we set out to build" checklist.

| Requirement | Acceptance Test | Result |
|---|---|---|
| FR1: Generate realistic e-commerce events | data_generator.py produces view/purchase events with product/user/timestamp info, using Faker-backed realistic values | PASS |
| FR2: Continuously write events as CSV files | Generator writes a new timestamped CSV every GENERATOR_INTERVAL_SECONDS | PASS - confirmed over a sustained 40-file, ~2-minute run |
| FR3: Detect new CSV files automatically | Spark Structured Streaming picks up new files without manual intervention | PASS |
| FR4: Enforce an explicit schema | EVENT_SCHEMA declared explicitly in spark_streaming.py; no schema inference used | PASS |
| FR5: Clean and validate incoming data | cast_and_normalize() and tag_validation_result() applied to every micro-batch | PASS |
| FR6: Reject invalid records into quarantine | Rejected rows routed to the rejected_events table (see note below) | PASS |
| FR7: Store valid events in PostgreSQL | Valid rows upserted into the events table | PASS |
| FR8: Archive processed files | Source files moved to data/processed_archive/ after successful writes | PASS, WITH A KNOWN EXCEPTION: zero-row files (empty or header-only CSV) are read correctly but never archived, since archiving is driven by row content, not file listing - see risks_and_limitations.md and test_cases.md rows 17-18 |
| FR9: Produce logs | Every module logs via the shared monitoring_logger.py, to console and logs/pipeline.log | PASS |
| FR10: Recover from a restart via checkpointing | checkpointLocation configured and used by the streaming query | PASS (mechanism confirmed present and functioning across normal restarts; a deliberate mid-batch-crash restart test was not performed - see test_cases.md #25) |
| FR11: Measure and report performance | performance_metrics.md and performance_methodology.md document a real, measured 800-event test run | PASS |
| FR12: Provide SQL verification queries | database.py exposes row_count(), constraint_violations(), duplicate_event_ids(), etc., runnable via main.py verify | PASS |

## Note on FR6

The original master plan specified a data/rejected/ CSV folder for quarantined records. During implementation, this was extended to write rejected rows directly to a rejected_events table instead of separate CSV files, since a queryable table supports the data quality reporting and SQL verification requirements (FR12) far better than scattered CSV files would. This is a deliberate scope adjustment, not an unmet requirement - see engineering_decisions.md.

## Additional Requirements (Added During Development, Beyond the Original 12)

| Requirement | Acceptance Test | Result |
|---|---|---|
| FR13: Retry transient database failures with backoff | Inline retry logic in make_write_valid_partition() classifies errors as transient/permanent and retries only transient ones, up to 5 attempts | PASS for the classification logic (11 unit tests in test_errors.py); NOT tested end-to-end against a genuine database outage - see test_cases.md #26 |
| FR14: Automatically collect per-batch performance metrics | MetricsListener writes Spark's own internal timing to stream_metrics after every batch, without manual log-reading | PASS for timing fields; num_input_rows has a known, documented 3x discrepancy - see risks_and_limitations.md |
| FR15: Detect structurally malformed CSV rows | _corrupt_record column + PERMISSIVE mode catches rows with the wrong field count, tagged malformed_csv_row | PASS - confirmed via a targeted manual test, see test_cases.md #19 |
| FR16: Validate event_id format and realistic price/quantity bounds | UUID regex check and MAX_PRICE/MAX_QUANTITY bounds added to tag_validation_result() | PASS - confirmed via targeted manual tests, see test_cases.md #22-24 |
| FR17: Run the full test suite automatically on every push | GitHub Actions CI with a real PostgreSQL service container | PASS - confirmed passing, see architecture.md |

## Overall Result

11 of the original 12 functional requirements fully and directly verified, with FR8 carrying one documented exception (zero-row file archiving) and FR10 verified for its core mechanism but not stress-tested against a deliberate mid-batch failure. Of the 5 additional requirements added during development, 3 are fully verified (FR15-17) and 2 have a partial result honestly documented (FR13's end-to-end outage behavior untested; FR14's row-count field has a known discrepancy).