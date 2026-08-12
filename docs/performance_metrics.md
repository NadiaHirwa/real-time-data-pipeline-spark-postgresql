# Performance Metrics

See [`performance_methodology.md`](performance_methodology.md) for exactly how these numbers were measured.

> **Note on the benchmark configuration:** both runs below predate `MAX_FILES_PER_TRIGGER` being wired into the pipeline. The setting existed in `config.py` and was documented as part of the test setup, but was never actually applied to the stream, so these runs were uncapped. Observed batch sizes stayed within 20-60 rows (1-3 files at 20 events per file), well under the 5-file cap, so the latency and throughput figures here are very likely unaffected - but the documented setup did not match what executed, and that is disclosed rather than left implicit. See `performance_methodology.md` for the fuller note.

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

## Update: Staging Table + Merge Rewrite (Idea 2 from Peer Review)

The write path was later rewritten to use Spark's bulk JDBC writer into a staging table, followed by a single SQL `INSERT...ON CONFLICT` merge, replacing the original per-partition `psycopg2` approach measured above (see docs/engineering_decisions.md). A second 800-event test run, under otherwise identical conditions, measured:

| Measurement | Before (per-partition psycopg2) | After (staging + merge) | Change |
|---|---|---|---|
| Last-file latency (file created to written to Postgres) | ~13.4s | ~4.2s | ~3.2x faster |
| Full pipeline completion | 141.1s | 121.4s | ~14% faster |
| Batches triggering "falling behind" warning | 17 of 18 (94%) | 1 of 24 (4%) | Trigger-interval overrun largely resolved as a side effect |
| Orphaned staging rows | N/A (no staging table existed) | 0 (this run) | - |

The latency improvement is substantially larger than the overall-completion improvement, because generator phase duration (~117s both times) is fixed by GENERATOR_INTERVAL_SECONDS and dominates the total - the write-path speedup shows up much more clearly in per-batch latency than in total wall-clock time for a fixed-length generator run.

Note: the staging+merge approach has its own documented limitation - if the merge step fails for a permanent (non-retryable) reason after staging has already succeeded, rows can be orphaned in staging_events indefinitely, since the two steps are separate transactions. This was observed once during development (see docs/risks_and_limitations.md) and requires manual cleanup; it is not automatically resolved.

## Observed Issue: Trigger Interval Overrun (Original Finding, Since Largely Resolved)

**Note:** this section describes the ORIGINAL test run, using the per-partition `psycopg2` write path. As shown in the Update section above, the staging+merge rewrite resolved most (though not all) of this issue as a side effect. This section is kept as an honest historical record of how the problem was originally found and diagnosed.

17 of 18 micro-batches logged a warning of this form:

```
WARN ProcessingTimeExecutor: Current batch is falling behind. The trigger
interval is 5000 milliseconds, but spent 5146-14529 milliseconds
```

**What this means:** the configured `TRIGGER_INTERVAL_SECONDS=5` asks Spark to start a new micro-batch every 5 seconds, but actual processing (read, validate, upsert, archive) regularly took 5-14 seconds per batch - meaning Spark started the next batch immediately after finishing the previous one, rather than actually waiting for a 5-second gap. The pipeline did not lose data or fall meaningfully behind in an accumulating way (all 40 files were eventually processed and archived correctly), but it never achieved the intended 5-second cadence.

**Why this happened**, based on the batch sizes observed: batches ranged from 20 to 60 rows depending on how many files had accumulated between triggers, and larger batches (e.g. Batch 14 at 60 rows, Batch 11 at 60 rows) correlated with longer processing times (7-8+ seconds) - consistent with the earlier, separately-diagnosed finding that psycopg2 connection setup, not data volume, dominates processing time for small-to-medium batches (see the commit history for spark_streaming.py's repartition(4) fix).

**What this means practically:** for this specific generator rate (20 events every 3 seconds, roughly 6.7 events/sec produced), a 5-second trigger interval is too aggressive given the current per-batch Postgres write overhead. The system is not broken - it processes everything correctly, just not at the originally configured cadence.

## What Would Improve This

- ~~Use a connection pool instead of opening a fresh connection per partition per batch~~ **Done** - superseded entirely by the staging table + SQL merge rewrite (see the Update section above), which eliminated per-partition connections altogether rather than pooling them
- ~~Batch multiple micro-batches' worth of rows into fewer, larger Postgres writes~~ **Effectively done** - the staging+merge approach achieves this same goal by a different mechanism (one bulk JDBC write + one SQL statement per batch, instead of many small connections)
- Increase `TRIGGER_INTERVAL_SECONDS` to 10-15s - **still not implemented**; now a much smaller concern than before (only 1 of 24 batches fell behind in the post-rewrite test run, versus 17 of 18 before), but the original mismatch between configured and actual cadence hasn't been formally corrected

See [`future_improvements.md`](future_improvements.md) for the full list of potential enhancements.

## Data Quality

Across 800 generated events (with a deliberate ~5% invalid injection rate, per `data_generator.py`):

- 31 of 800 (3.9%) were rejected - close to, and consistent with, the intentional 5% injection rate (the exact rate varies per run since injection is randomized)
- 0 rejected events were later found to violate a constraint in the `events` table, confirming rejection correctly happened before any invalid data reached the valid dataset
- 0 duplicate event_ids were found across the full day's cumulative testing (multiple runs, restarts, and crashes), confirming the ON CONFLICT DO NOTHING upsert strategy works correctly under real, repeated, imperfect usage - not just a single clean run