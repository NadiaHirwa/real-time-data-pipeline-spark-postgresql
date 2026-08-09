# Test Cases

Manual test plan covering scenarios beyond the automated unit tests in tests/test_spark_streaming.py (which cover the data contract validation logic in isolation - see that file for the 13 automated cases). This document covers integration-level and edge-case behavior, tested manually during development.

## Core Pipeline Flow

| # | Test | Expected | Actual | Status |
|---|---|---|---|---|
| 1 | New CSV arrives in data/incoming/ | Spark detects it within one trigger interval | Confirmed across every test run today (see performance_metrics.md) | PASS |
| 2 | Valid event | Row written to events table | 769/800 events in the largest test run written correctly | PASS |
| 3 | Invalid event (any contract violation) | Row written to rejected_events with a specific rejection_reason | 31/800 events rejected, each with an accurate reason tag | PASS |
| 4 | Successfully processed file | Moved from data/incoming/ to data/processed_archive/ | Confirmed - all 40 files archived in the largest test run | PASS |
| 5 | Duplicate event_id within one micro-batch | Dropped, not double-counted | Not explicitly triggered in generator output during testing (generator produces unique UUIDs) | UNTESTED (manual) |
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
| 17 | Empty CSV file | Not explicitly tested | - | UNTESTED |
| 18 | CSV with header only, no data rows | Not explicitly tested | - | UNTESTED |
| 19 | Extra/unexpected columns in a CSV | Rejected via Spark's PERMISSIVE mode + `_corrupt_record` column, tagged `malformed_csv_row` | Confirmed: a deliberately malformed row (extra fields) was correctly caught and rejected | PASS |
| 20 | Missing expected columns in a CSV | Not explicitly tested; likely produces nulls, triggering existing null-checks | - | UNTESTED |
| 21 | Very large single file | Not explicitly tested at scale beyond ~60 rows/file in testing | - | UNTESTED |

## System-Level Scenarios

| # | Test | Expected | Actual | Status |
|---|---|---|---|---|
| 22 | Streaming job restarted mid-run | Resumes from checkpoint, does not reprocess or lose files | Not deliberately tested with a mid-run restart; checkpoint mechanism is Spark's built-in guarantee, not custom logic | UNTESTED (relies on Spark's documented behavior) |
| 23 | PostgreSQL unreachable during a batch | Batch fails, files remain in data/incoming/ for retry, no crash of the whole streaming job | Not deliberately triggered (Postgres was kept running throughout testing) | UNTESTED |
| 24 | Checkpoint directory manually deleted | Job would restart from scratch, reprocessing all historical files | Not tested; expected behavior based on Spark's documented checkpoint semantics | UNTESTED |
| 25 | main.py clean run while streaming job is active | Checkpoint clearing may partially fail due to file locks (observed) | Confirmed: produced a clear warning rather than a crash | PASS (defensive handling confirmed) |
| 26 | event_id not matching UUID format | Rejected, tagged invalid_event_id_format | Confirmed via targeted test | PASS |
| 27 | Price/quantity exceeding realistic upper bounds | Rejected, tagged price_exceeds_maximum / quantity_exceeds_maximum | Confirmed via targeted test | PASS |


## Note on Untested Scenarios

Several rows above are marked UNTESTED rather than fabricated as PASS. This is a deliberate, honest choice: simulating a database outage or a mid-stream crash requires deliberately breaking a running system in a way that risked losing the clean, verified dataset used for performance_metrics.md, and time constraints meant this trade-off was accepted rather than hidden. These are listed explicitly in risks_and_limitations.md as known gaps, not silently omitted.