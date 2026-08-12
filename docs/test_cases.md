# Test Cases

Manual test plan covering scenarios beyond the automated tests in tests/test_spark_streaming.py (13 tests, validation logic in isolation), tests/test_integration.py (3 tests, the real staging+merge write path against a live PostgreSQL), and tests/test_errors.py (11 tests, error classification logic, including a drift-guard between the errors.py module and its inlined copy in spark_streaming.py - see engineering_decisions.md) - 27 automated tests total. This document covers integration-level and edge-case behavior, tested manually during development.

## Core Pipeline Flow

| # | Test | Expected | Actual | Status |
|---|---|---|---|---|
| 1 | New CSV arrives in data/incoming/ | Spark detects it within one trigger interval | Confirmed across every test run today (see performance_metrics.md) | PASS |
| 2 | Valid event | Row written to events table | 769/800 events in the largest test run written correctly | PASS |
| 3 | Invalid event (any contract violation) | Row written to rejected_events with a specific rejection_reason | 31/800 events rejected, each with an accurate reason tag | PASS |
| 4 | Successfully processed file | Moved from data/incoming/ to data/processed_archive/ | Confirmed - all 40 files archived in the largest test run | PASS |
| 5 | Duplicate event_id within one micro-batch | Dropped, not double-counted | Confirmed via a targeted manual test: a single CSV file was written with the same event_id appearing twice. The batch log reported "1 valid, 0 rejected" for that file (not 2), and a direct query confirmed exactly 1 row in events for that event_id - dropDuplicates() correctly collapsed the pair before any write occurred | PASS |
| 6 | Duplicate event_id across separate runs | ON CONFLICT DO NOTHING prevents a duplicate row | 0 duplicate event_ids found across an entire day of repeated, overlapping test runs (844 cumulative events) | PASS |

## Edge Cases

| # | Test | Expected | Actual | Status |
|---|---|---|---|---|
| 7 | Negative price | Rejected, tagged invalid_or_negative_price | Automated test passes | PASS |
| 8 | Zero price | Accepted (a free item is valid, not invalid) | Automated test passes | PASS |
| 9 | Zero or negative quantity | Rejected, tagged invalid_or_zero_quantity | Automated tests pass | PASS |
| 10 | Invalid event_type (e.g. "click") | Rejected, tagged invalid_event_type | Automated test passes; also observed live in generator output | PASS |
| 11 | event_type with mixed case/whitespace | Normalized and accepted, not rejected | Automated test passes | PASS |
| 12 | Missing user_id | Rejected, tagged missing_or_invalid_user_id | Automated test passes; also observed live | PASS |
| 13 | Missing product_id | Rejected, tagged missing_or_invalid_product_id | Automated test passes | PASS |
| 14 | Unparseable timestamp | Rejected, tagged unparseable_timestamp | Automated test passes | PASS |
| 15 | Far-future timestamp | Rejected, tagged future_timestamp | Automated test passes; also observed live | PASS |
| 16 | Row violating multiple rules at once | Tagged with the FIRST rule checked, per the fixed priority order | Automated test passes | PASS |
| 17 | Empty CSV file (0 bytes) | Read without error, contributes 0 rows to the batch | Confirmed: batch logged "0 valid, 0 rejected", no crash. However, the file was NEVER archived - see the note below this table | PASS (read), FAIL (archiving) |
| 18 | CSV with header only, no data rows | Read without error, contributes 0 rows to the batch | Same result and same archiving gap as row 17 | PASS (read), FAIL (archiving) |
| 19 | Extra/unexpected columns in a CSV (structurally malformed row) | Rejected via Spark's PERMISSIVE mode + _corrupt_record column, tagged malformed_csv_row | Confirmed via a targeted manual test: a deliberately malformed row (extra trailing fields) was correctly caught and rejected, checked FIRST in the validation order since other fields are meaningless on a broken row | PASS |
| 20 | Missing expected columns in a CSV | Rejected via the same mechanism as row 19 | Confirmed via a targeted manual test: a CSV was written with the quantity column removed entirely from the header (not left blank - genuinely absent). The row was rejected and tagged malformed_csv_row, not missing_or_invalid_quantity as originally predicted - Spark's PERMISSIVE mode treats a header/schema column-count mismatch as structural corruption, caught by _corrupt_record, rather than aligning by name and leaving the missing field null. A more specific, and different, result than what was originally guessed | PASS |
| 21 | Very large single file | Processes without error at a meaningfully larger scale than typical testing | Confirmed via a targeted manual test: one CSV file with 5,000 rows (250x the typical 20-row test file) was generated and processed. Result: "5000 valid, 0 rejected," completed in roughly 12.6 seconds total, staged-and-merged in ~3.1 seconds - consistent with previously measured per-row overhead, no errors, no timeout | PASS |
| 22 | event_id not matching UUID format | Rejected, tagged invalid_event_id_format | Confirmed via a targeted manual test (a row with event_id="not-a-real-uuid") | PASS |
| 23 | Price exceeding MAX_PRICE (10,000.00) | Rejected, tagged price_exceeds_maximum | Confirmed via a targeted manual test (price=999999.00) | PASS |
| 24 | Quantity exceeding MAX_QUANTITY (100) | Rejected, tagged quantity_exceeds_maximum | Confirmed via a targeted manual test (quantity=500) | PASS |

## Note on the Zero-Row File Archiving Gap (Discovered via Rows 17-18)

A real limitation was discovered while testing empty and header-only CSV files: `archive_source_files()` determines which files to archive by looking at the `_source_file` column of the rows in a processed batch. A file that produces ZERO rows (empty, or header-only) has no row anywhere referencing it, so the archiving logic has no way to know it was ever read. Such files are correctly read without error and never reprocessed (Spark's checkpoint tracks "seen" files independently of row count), but they are also never moved to `data/processed_archive/` - they remain in `data/incoming/` indefinitely. This is documented as a known limitation in `risks_and_limitations.md` and `future_improvements.md`, not fixed in this version.

## How These Manual Tests Were Actually Performed

Each manual test (rows 5, 17-24, 27, 28) followed the same repeatable pattern, so any of them can be reproduced exactly:

1. `python main.py clean` for a fresh state
2. Start the streaming job in one terminal: `python main.py stream`
3. In a second terminal, write a small, purpose-built CSV file directly into `data/incoming/` using a short throwaway Python script - each script wrote exactly the malformed/edge-case condition being tested (e.g. two rows sharing one event_id, a header missing a column, a 5,000-row file, a row with `event_id="not-a-real-uuid"`)
4. Observe the resulting log line in the streaming terminal (e.g. "N valid, M rejected")
5. Query the database directly (`scripts/database.py`'s functions, or a short one-off query) to confirm the exact row count, rejection reason, or absence of duplicates
6. Record the real result here, in this table, immediately after confirming it - not from memory afterward

The throwaway CSV-writing and verification scripts used for each test were not committed to the repository (they are single-purpose, disposable, and specific to triggering one exact condition), but the procedure above is exact and precisely reproducible - running it again against the same code will produce the same result. This is the same reasoning applied to `main.py clean`'s `TRUNCATE TABLE` commands elsewhere in this project: the SQL command performing an action is documented and reproducible even though no permanent script file wraps it.

## System-Level Scenarios

| # | Test | Expected | Actual | Status |
|---|---|---|---|---|
| 25 | Streaming job restarted mid-run | Resumes from checkpoint, does not reprocess or lose files | Confirmed via a targeted manual test: the streaming job was interrupted (Ctrl+C) after a batch's validation count was logged ("Batch 0: 60 valid, 0 rejected") but BEFORE the "staged and merged" confirmation appeared - genuinely caught mid-write. The job was restarted immediately. After the generator finished and the pipeline caught up: data/incoming/ was empty, data/processed_archive/ contained exactly the expected number of files (matching total files generated), and a direct query confirmed 0 duplicate event_ids. Spark's documented checkpoint behavior (redo an uncommitted batch entirely on restart) combined with this project's ON CONFLICT DO NOTHING upsert correctly prevented any duplicate rows even though the interrupted batch was likely reprocessed | PASS |
| 26 | PostgreSQL unreachable during a batch | Transient errors retried with exponential backoff (up to 5 attempts) inside make_write_valid_partition's inline retry logic; a persistently unreachable database fails the batch, leaving files in data/incoming/ for the next trigger | The classification logic (transient vs. permanent) was manually verified once during development via a throwaway script, confirming both branches worked correctly, but this was NOT kept as a permanent automated test. The end-to-end retry behavior under a genuine database outage was not exercised at all | UNTESTED (manually spot-checked once, not automated, not end-to-end) |
| 27 | Checkpoint directory manually deleted | Job would restart from scratch, reprocessing all historical files | Confirmed via a targeted manual test: the pipeline ran normally, files were fully processed and archived, then the streaming job was stopped, checkpoint/ was cleared entirely, and the job restarted. Result: no reprocessing occurred, because the previously-archived files are no longer in data/incoming/ - there was nothing left to re-read regardless of checkpoint state. Zero duplicate event_ids confirmed afterward. This revealed a genuinely useful design property: archiving (moving files out of the watched folder) is what actually prevents reprocessing here, with the checkpoint acting as a secondary safeguard, not the primary one | PASS |
| 28 | main.py clean run while streaming job is active | Checkpoint clearing may partially fail due to file locks (observed) | Confirmed: produced a clear warning rather than a crash | PASS (defensive handling confirmed) |

## Automated Integration Tests (tests/test_integration.py, against a live PostgreSQL)

| # | Test | Expected | Actual | Status |
|---|---|---|---|---|
| 29 | Valid rows reach the events table via the full staging+merge path | A row written through write_valid_to_postgres() + merge_staging_to_events() lands correctly in events | Automated test passes | PASS |
| 30 | Merge is idempotent on a duplicate event_id | Running the same event_id through the write path twice does not create a duplicate row | Automated test passes | PASS |
| 31 | staging_events is empty after a successful merge | No leftover rows for a given run_id/batch_id after merge_staging_to_events() completes | Automated test passes | PASS |

## Note on Untested Scenarios

Several rows above are marked UNTESTED rather than fabricated as PASS. This is a deliberate, honest choice: simulating a database outage or a mid-stream crash requires deliberately breaking a running system in a way that risked losing the clean, verified dataset used for performance_metrics.md, and time constraints meant this trade-off was accepted rather than hidden. These are listed explicitly in risks_and_limitations.md as known gaps, not silently omitted.