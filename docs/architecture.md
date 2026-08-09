# Architecture

![Architecture Diagram](../diagrams/architecture.png)

## Component Overview

```
                    ┌──────────────────────┐
                    │  data_generator.py   │
                    │  (producer, Python)   │
                    └──────────┬───────────┘
                               │ writes CSV every
                               │ GENERATOR_INTERVAL_SECONDS
                               ▼
                    ┌──────────────────────┐
                    │  data/incoming/       │
                    │  (landing zone)        │
                    └──────────┬───────────┘
                               │ watched by
                               ▼
        ┌──────────────────────────────────────────┐
        │        spark_streaming.py (consumer)       │
        │                                              │
        │  readStream (explicit schema, no inference) │
        │        │                                     │
        │        ▼                                     │
        │  cast_and_normalize()                        │
        │        │                                     │
        │        ▼                                     │
        │  tag_validation_result()                     │
        │        │                                     │
        │        ├──valid──────┐   ├──rejected──────┐  │
        │        ▼             │   ▼                │  │
        │  foreachBatch()      │   foreachBatch()    │  │
        │  psycopg2 upsert     │   Spark JDBC writer │  │
        └────────┬─────────────┴──────────┬──────────┘
                  │                        │
                  ▼                        ▼
        ┌──────────────────┐   ┌──────────────────────┐
        │  events table     │   │  rejected_events table │
        │  (PostgreSQL)      │   │  (PostgreSQL)          │
        └──────────────────┘   └──────────────────────┘
                  │
                  │ (after both writes succeed)
                  ▼
        ┌──────────────────────┐
        │ data/processed_archive/│
        └──────────────────────┘

  checkpoint/ tracks processing progress across restarts,
  independent of the above data flow.

  main.py provides a CLI dispatcher (generator/stream/verify/
  test/clean/status) over these components without combining
  them into one process - see engineering_decisions.md.
```

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
Schema:               explicit (EVENT_SCHEMA in spark_streaming.py);
                       inference is not possible for a streaming file
                       source that hasn't seen all its data yet
JDBC driver:           PostgreSQL JDBC 42.7.13, loaded via spark.jars
```

## PostgreSQL Configuration

```
Database:    ecommerce_events
Tables:      events (PRIMARY KEY event_id, CHECK constraints on
             event_type/price/quantity), rejected_events (no
             constraints, by design - see data_contract.md)
Indexes:     idx_events_timestamp, idx_events_user_id,
             idx_events_product_id, idx_events_ingested_at
Encoding:    default (UTF-8)
Port:        5432 (default, configurable via .env)
```

## Relationship to the Companion TMDB Spark Project

This project shares no code with the earlier tmdb-movies-analysis-spark project from the same course module (they are separate, independently-graded assignments), but does reuse proven PATTERNS learned there: a centralized config.py/.env approach, a shared logging module, ADR-style documentation of decisions, and the general discipline of one-commit-per-logical-change git history.