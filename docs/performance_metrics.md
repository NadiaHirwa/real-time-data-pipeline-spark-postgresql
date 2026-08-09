# Performance Metrics

See [`performance_methodology.md`](performance_methodology.md) for exactly how these numbers were measured.

## Test Run Summary

| Metric | Value |
|---|---|
| Files generated | 40 |
| Events per file | 20 |
| Total events generated | 800 |
| Valid events written to `events` | 769 (96.1%) |
| Rejected events written to `rejected_events` | 31 (3.9%) |
| Constraint violations found post-write | 0 |
| Duplicate `event_id`s found post-write | 0 |

## Timing

| Measurement | Value |
|---|---|
| Generator phase duration (first file to last file written) | 117.2s |
| Full pipeline completion (stream start to last Postgres write) | 141.1s |
| First-file latency (file created to written to Postgres) | ~14.5s |
| Last-file latency (file created to written to Postgres) | ~13.4s |
| End-to-end throughput | ~5.7 events/sec |

## Observed Issue: Trigger Interval Overrun

17 of 18 micro-batches logged a warning of this form:

```
WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger
interval is 5000 milliseconds, but spent 5146-14529 milliseconds
```

**What this means:** the configured `TRIGGER_INTERVAL_SECONDS=5` asks Spark to start a new micro-batch every 5 seconds, but actual processing (read, validate, upsert, archive) regularly took 5-14 seconds per batch - meaning Spark started the next batch immediately after finishing the previous one, rather than actually waiting for a 5-second gap. The pipeline did not lose data or fall meaningfully behind in an accumulating way (all 40 files were eventually processed and archived correctly), but it never achieved the intended 5-second cadence.

**Why this happened**, based on the batch sizes observed: batches ranged from 20 to 60 rows depending on how many files had accumulated between triggers, and larger batches (e.g. Batch 14 at 60 rows, Batch 11 at 60 rows) correlated with longer processing times (7-8+ seconds) - consistent with the earlier, separately-diagnosed finding that psycopg2 connection setup, not data volume, dominates processing time for small-to-medium batches (see the commit history for spark_streaming.py's repartition(4) fix).

**What this means practically:** for this specific generator rate (20 events every 3 seconds, roughly 6.7 events/sec produced), a 5-second trigger interval is too aggressive given the current per-batch Postgres write overhead. The system is not broken - it processes everything correctly, just not at the originally configured cadence.

## What Would Improve This (Not Implemented, Documented as Future Work)

- Increase `TRIGGER_INTERVAL_SECONDS` to 10-15s, reducing how often the fixed per-batch connection overhead is paid
- Use a connection pool (e.g. psycopg2.pool) instead of opening a fresh connection per partition per batch
- Batch multiple micro-batches' worth of rows into fewer, larger Postgres writes, trading a small amount of latency for significantly reduced per-write overhead

See [`future_improvements.md`](future_improvements.md) for the full list of potential enhancements.

## Data Quality

Across 800 generated events (with a deliberate ~5% invalid injection rate, per `data_generator.py`):

- 31 of 800 (3.9%) were rejected - close to, and consistent with, the intentional 5% injection rate (the exact rate varies per run since injection is randomized)
- 0 rejected events were later found to violate a constraint in the `events` table, confirming rejection correctly happened before any invalid data reached the valid dataset
- 0 duplicate event_ids were found across the full day's cumulative testing (multiple runs, restarts, and crashes), confirming the ON CONFLICT DO NOTHING upsert strategy works correctly under real, repeated, imperfect usage - not just a single clean run