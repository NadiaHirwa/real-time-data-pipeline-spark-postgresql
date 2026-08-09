# Data Quality Report

Summarizes the quality of data processed through this pipeline, based on cumulative testing conducted during development (multiple runs, restarts, and one deliberate 800-event controlled test - see performance_metrics.md for that specific run's isolated numbers). Query results below reflect the full cumulative state of the database at the time of writing.

## Overall Volume

| Metric | Count | Percentage |
|---|---|---|
| Total events processed | 880 | 100% |
| Valid (written to events) | 844 | 95.9% |
| Rejected (written to rejected_events) | 36 | 4.1% |

## Rejection Breakdown by Reason

| Rejection Reason | Count | Share of Rejections |
|---|---|---|
| missing_or_invalid_user_id | 10 | 27.8% |
| invalid_or_negative_price | 9 | 25.0% |
| future_timestamp | 6 | 16.7% |
| invalid_event_type | 6 | 16.7% |
| invalid_or_zero_quantity | 5 | 13.9% |

**Observation:** the distribution is roughly even across all five deliberately-injected violation types in data_generator.py's _make_bad_event() function, which selects among them uniformly at random. This even spread is expected behavior confirming the generator's injection logic works as designed, rather than a finding about real-world data quality patterns (since this is synthetic test data, not genuine e-commerce traffic).

## Integrity Checks

| Check | Result | Query Used |
|---|---|---|
| Constraint violations in events table | 0 | SELECT * FROM events WHERE price < 0 OR quantity <= 0; |
| Duplicate event_ids in events table | 0 | SELECT event_id, COUNT(*) FROM events GROUP BY event_id HAVING COUNT(*) > 1; |

**Significance:** these two checks returning zero rows, across an entire day of repeated, overlapping, sometimes-crashed test runs (not a single clean pass), is meaningful evidence that:
1. Validation logic correctly prevents invalid data from ever reaching the events table (rather than relying on the database CHECK constraints alone as a last resort)
2. The ON CONFLICT DO NOTHING upsert strategy genuinely prevents duplicates under real, messy, repeated usage - not just a single controlled test

## Known Gaps in This Report

- No measurement of "late" events specifically (events whose event_timestamp significantly precedes ingested_at beyond normal processing latency) - all rejected timestamps observed were FUTURE timestamps, not unusually old ones, so this category was not exercised by testing
- No measurement of malformed CSV structure (missing columns, extra columns, corrupted files) - see test_cases.md's untested edge cases
- Percentages reflect cumulative testing data, not a single isolated run - see performance_metrics.md for the isolated 800-event test's own breakdown (769 valid, 31 rejected, 96.1%/3.9%), which is consistent with these cumulative figures