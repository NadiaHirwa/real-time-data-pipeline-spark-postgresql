# Architecture

![Architecture Diagram](../diagrams/architecture.png)


## Why This Structure

Each script owns exactly one concern:

- data_generator.py - produces synthetic events, knows nothing about Spark, Postgres, or validation rules
- spark_streaming.py - reads, validates, transforms, and writes; the only file that knows about the data contract
- database.py - connection handling and read-only verification queries; used both by main.py verify and manually during development
- config.py - the single source of truth for paths, credentials, and tunable settings; every other module reads from here rather than calling os.getenv() directly
- main.py - a thin CLI layer over the above, added specifically so every operation is reachable through one consistent interface (see engineering_decisions.md for the reasoning behind adding this without combining the producer and consumer into one process)

## Technology Justification

| Technology | Chosen Because | Alternative Considered |
|---|---|---|
| Apache Spark Structured Streaming | Native checkpointing and file-source streaming support, directly matching this course module's content on Structured Streaming | Plain batch Spark on a schedule (rejected - see engineering_decisions.md) |
| PostgreSQL | ACID compliance, mature JDBC and psycopg2 support, already installed and working locally | MySQL, SQLite |
| Faker | Industry-standard realistic fake data generation, rather than hand-rolled random values | Hand-written random value generation |
| psycopg2 (not Spark's JDBC writer, for valid rows) | Enables ON CONFLICT DO NOTHING upserts; Spark's JDBC writer has no upsert support at all | Spark's standard JDBC writer (used for rejected_events, which needs no upsert logic) |

## Spark Configuration

```
Master:              local[*]
Checkpoint location:  checkpoint/ (configurable via CHECKPOINT_DIR)
Trigger interval:     5 seconds (configurable via TRIGGER_INTERVAL_SECONDS;
                       found to be too aggressive for the tested write
                       pattern - see performance_metrics.md)
Max files per trigger: 5 (configurable via MAX_FILES_PER_TRIGGER) - caps how
                       many files one micro-batch consumes. Without it, the
                       first batch after a backlog (a restart, a slow
                       consumer, or files accumulating between triggers)
                       reads every waiting file at once, risking a very slow
                       or memory-heavy first batch; capping it spreads the
                       backlog over several normal-sized batches
Schema:               explicit (EVENT_SCHEMA in spark_streaming.py);
                       inference is not possible for a streaming file
                       source that hasn't seen all its data yet
JDBC driver:           PostgreSQL JDBC 42.7.3, loaded one of two ways
                       depending on environment - see "Runtime
                       Environments" below. Natively and in CI it comes
                       from spark.jars.packages (Maven coordinates, not a
                       manually-downloaded local .jar - see
                       engineering_decisions.md); in Docker it is baked
                       into the image at build time
```

## Runtime Environments: Native and Dockerized

The project runs in either of two environments, and both execute the same application code - there is no Docker-specific branch of the pipeline, and no file that exists only for one path.

| | Native | Dockerized |
|---|---|---|
| Host requirements | Python, Java 17, Spark, PostgreSQL, JDBC .jar | Docker Desktop only |
| Components | Processes on the host | `postgres`, `app`, and `adminer` containers on a compose network |
| Database host | `localhost` | `postgres` (the compose service name) |
| JDBC driver | Resolved from Maven at session start | Baked into the image at build time |
| Status | How the project was developed and benchmarked | An additional, equivalent way to run it |

Exactly two things differ between the environments from the application's point of view, and both are handled by configuration rather than by conditional code.

**1. The JDBC driver, loaded by one of two paths.** `with_postgres_driver()` in `spark_streaming.py` attaches the driver to a SparkSession builder, and every caller - the streaming job and the pytest fixture in `tests/conftest.py` - goes through it:

- If `POSTGRES_JDBC_JAR` is set (the Docker image sets it to the jar it downloaded during build), it configures `spark.jars` with that local path. No Maven round-trip on container start: deterministic, works offline, and measurably faster.
- If it is unset (native and CI), it configures `spark.jars.packages` with the Maven coordinates, exactly as before.

The selection is driven by the *presence of one environment variable*, not by detecting Docker. That is the point: adding the containerized path required no change to how the native path resolves its driver, and a future third environment would need no new branch either. The single constraint is that the version in `POSTGRES_JDBC_COORDINATES` and the version the Dockerfile downloads must stay in sync; both are 42.7.3, and each is commented pointing at the other.

**2. `DB_HOST`, which is genuinely environment-specific.** Containers reach each other by service name on the compose network, so the database is at `postgres` inside Docker and `localhost` natively. This is the main environment difference application code has to get right, and it works because `config.py` reads `DB_HOST` via `os.getenv()` rather than hardcoding it: `docker-compose.yml` sets `DB_HOST: postgres` for the app service, and python-dotenv does not override variables already present in the environment, so the container's value wins over the `DB_HOST=localhost` in a mounted `.env`. The same `.env` therefore serves both paths without edits.

## PostgreSQL Configuration

```
Database:    ecommerce_events
Tables:      events, rejected_events, staging_events, stream_metrics
             (see "Data Storage" section below for each table's
             purpose and constraints)
Views:       v_rejection_summary, v_batch_performance, v_pipeline_health
Indexes:     idx_events_timestamp, idx_events_user_id,
             idx_events_product_id, idx_events_ingested_at,
             idx_staging_events_run_batch
Encoding:    default (UTF-8)
Port:        5432 (default, configurable via .env)
```

## Data Storage: Four Distinct Tables, Different Purposes

| Table | Written By | Constraints | Purpose |
|---|---|---|---|
| `events` | `merge_staging_to_events()` (SQL merge from staging) | Full CHECK constraints, PRIMARY KEY on `event_id` | The valid, queryable dataset |
| `rejected_events` | `write_rejected_to_postgres()` (Spark JDBC writer, plain append) | None - deliberately unconstrained | Quarantine, queryable for data quality analysis |
| `staging_events` | `write_valid_to_postgres()` (Spark JDBC writer, bulk append) | None - deliberately unconstrained | Transient landing zone; emptied by the merge step within the same batch |
| `stream_metrics` | `MetricsListener.onQueryProgress()` (direct psycopg2, driver-side) | `UNIQUE (run_id, batch_id)` | Automatic per-batch performance data (see `performance_metrics.md`) |

## The Staging + Merge Write Path (Idea 2, from peer review - see engineering_decisions.md)

```
valid_df (Spark DataFrame)
      │
      ▼
Spark's bulk JDBC writer (write_valid_to_postgres)
      │  appends rows tagged with run_id + batch_id
      ▼
staging_events table
      │
      ▼
merge_staging_to_events() - runs on the driver via psycopg2:
      1. pg_advisory_lock() - serializes concurrent merges
      2. INSERT INTO events SELECT ... FROM staging_events
         WHERE run_id = ? AND batch_id = ?
         ON CONFLICT (event_id) DO NOTHING
      3. DELETE FROM staging_events WHERE run_id = ? AND batch_id = ?
      4. pg_advisory_unlock()
```

This replaced an earlier per-partition `psycopg2` approach (one connection per Spark partition per batch), which measured ~13.4s last-file latency versus ~4.2s after this rewrite - see `performance_metrics.md` for the full before/after comparison.

## Automatic Metrics Collection (MetricsListener)

`scripts/metrics_listener.py` registers a `StreamingQueryListener` with the active `SparkSession` before the query starts. Spark calls its `onQueryProgress()` method automatically after every completed micro-batch, providing Spark's own internal timing (`batch_duration_ms`, `input_rows_per_second`, etc.) without requiring manual log-reading. This data is written directly to `stream_metrics`.

**Known limitation:** `num_input_rows` from this listener has been observed at a consistent 3x multiple of the true row count, for reasons not yet confirmed - see `risks_and_limitations.md`. Timing fields are unaffected and considered reliable.

## Continuous Integration

`.github/workflows/ci.yml` runs on every push to `main`, using a real PostgreSQL service container (not a mock) alongside Java and Python setup steps. This runs the full test suite (`tests/test_spark_streaming.py`'s 13 unit tests plus `tests/test_integration.py`'s 3 integration tests) against a genuinely fresh Ubuntu environment, distinct from local Windows development - this caught a real, otherwise-invisible bug (a missing `pytest` entry in `requirements.txt`, masked locally by Anaconda's pre-installed copy).

This project shares no code with the earlier tmdb-movies-analysis-spark project from the same course module (they are separate, independently-graded assignments), but does reuse proven PATTERNS learned there: a centralized config.py/.env approach, a shared logging module, ADR-style documentation of decisions, and the general discipline of one-commit-per-logical-change git history.