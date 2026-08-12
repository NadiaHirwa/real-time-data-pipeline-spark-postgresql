# Performance Methodology

Describes HOW the numbers in [`performance_metrics.md`](performance_metrics.md) were measured, so the results can be reproduced or challenged.

## Test Setup

```
Generator settings (.env):
  GENERATOR_EVENTS_PER_FILE = 20
  GENERATOR_INTERVAL_SECONDS = 3

Streaming settings (.env):
  TRIGGER_INTERVAL_SECONDS = 5
  MAX_FILES_PER_TRIGGER = 5

Test plan:
  - Run `python main.py clean` for a fresh state
  - Start the streaming job (Terminal 1): python main.py stream
  - Start the generator (Terminal 2), capped at 40 iterations:
    python -c "import sys; sys.path.append('scripts'); from data_generator import run; run(max_iterations=40)"
  - Wait for both processes to finish, then run: python main.py verify
```

This produces a controlled, bounded run (40 files, 800 events total) rather than an open-ended stream, so start and end times are well-defined and the test is reproducible.

**Disclosure about `MAX_FILES_PER_TRIGGER = 5` above:** this benchmark was actually run BEFORE that setting was wired into the pipeline. The value existed in `config.py` and `.env.example` at the time, and was listed here as part of the test setup, but `read_incoming_stream()` never passed it to Spark as a `maxFilesPerTrigger` option - it was dead configuration, so the run was in fact uncapped. It has since been connected (see `architecture.md`'s Spark Configuration section).

In practice this very likely did not affect the recorded numbers: the batch sizes observed during the run ranged from 20 to 60 rows (see `performance_metrics.md`'s trigger-interval section), and at 20 events per file that is 1-3 files per batch - comfortably under the 5-file cap, which therefore would never have bound even if it had been active. The numbers are reported as measured rather than re-run. This is disclosed as a shortfall in the benchmark's rigor - the documented configuration did not match the executed configuration - not as a known error in the results.

## What Was Measured, and How

**Generator phase duration**: timestamp of the first "Wrote N events" log line to the timestamp of the last, both emitted by `data_generator.py`'s own logger.

**Full pipeline completion time**: timestamp the streaming query started (`spark_streaming.py`'s "Streaming query started" log) to the timestamp of the final "wrote N valid rows" log line for the last batch.

**Per-file latency**: for a specific file, the difference between its "Wrote N events to {filename}" timestamp (generator log) and the "wrote N valid rows to events" timestamp of the batch that file was included in (streaming log). This is an approximation - it measures latency for the BATCH containing that file, not the individual row, since `foreachBatch` processes many files' rows together. First-file and last-file latency were measured this way as representative bounds, not an average across all 800 events individually.

**Throughput**: total events generated divided by total wall-clock time from generator start to final Postgres write.

**Constraint violations and duplicates**: `main.py verify`, which runs the SQL verification queries defined in [`data_contract.md`](data_contract.md) directly against the `events` table.

## Limitations of This Methodology

- Measured on a single local machine (`local[*]`), not a distributed cluster - these numbers describe THIS specific hardware, not general Spark Structured Streaming performance.
- Timestamps come from application-level log lines, not a dedicated tracing/instrumentation tool - accurate to roughly the resolution of Python's logging timestamps (milliseconds), but not designed for sub-second precision analysis.
- A single test run was measured, not an average across multiple runs. Given the consistency of the "falling behind" warning across nearly every batch (see [`performance_metrics.md`](performance_metrics.md)), this is treated as a reliable, repeatable pattern rather than a one-off anomaly - but a rigorous benchmark would run this multiple times and report a distribution, not a single sample.