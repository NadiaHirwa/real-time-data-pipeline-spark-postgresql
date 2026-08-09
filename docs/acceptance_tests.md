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
| FR8: Archive processed files | Source files moved to data/processed_archive/ after successful writes | PASS |
| FR9: Produce logs | Every module logs via the shared monitoring_logger.py, to console and logs/pipeline.log | PASS |
| FR10: Recover from a restart via checkpointing | checkpointLocation configured and used by the streaming query | PASS (mechanism confirmed present and functioning across normal restarts; a deliberate mid-batch-crash restart test was not performed - see test_cases.md #22) |
| FR11: Measure and report performance | performance_metrics.md and performance_methodology.md document a real, measured 800-event test run | PASS |
| FR12: Provide SQL verification queries | database.py exposes row_count(), constraint_violations(), duplicate_event_ids(), etc., runnable via main.py verify | PASS |

## Note on FR6

The original master plan specified a data/rejected/ CSV folder for quarantined records. During implementation, this was extended to write rejected rows directly to a rejected_events table instead of separate CSV files, since a queryable table supports the data quality reporting and SQL verification requirements (FR12) far better than scattered CSV files would. This is a deliberate scope adjustment, not an unmet requirement - see engineering_decisions.md.

## Overall Result

11 of 12 functional requirements fully and directly verified; FR10 verified for its core mechanism (checkpoint location configured, used, and surviving normal restarts) but not stress-tested against a deliberate mid-batch failure.