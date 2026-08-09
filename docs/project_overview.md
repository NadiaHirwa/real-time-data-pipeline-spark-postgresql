# Project Overview

## What This Is

A real-time-style data pipeline simulating an e-commerce platform tracking user activity. A Python script generates fake user events (product views and purchases), Apache Spark Structured Streaming picks them up as they arrive, cleans and validates them, and PostgreSQL stores the results - with invalid records routed to a separate quarantine table rather than silently dropped.

## The Three Core Components

1. **data_generator.py** (the producer) - writes a new CSV file of synthetic events into data/incoming/ every few seconds, including a deliberate small percentage of invalid records so the pipeline's rejection logic has something real to catch.

2. **spark_streaming.py** (the consumer) - watches data/incoming/ continuously. Every time new files appear, it reads them, casts and normalizes the data, checks every record against a defined data contract, and splits each batch into valid and rejected rows.

3. **PostgreSQL** (the storage layer) - valid rows are upserted into an events table (duplicate-safe, thanks to a unique constraint and ON CONFLICT DO NOTHING); rejected rows land in a separate rejected_events table, tagged with exactly why they were rejected.

A main.py command-line dispatcher ties these together for convenience (generator, stream, verify, test, clean, status), without combining the producer and consumer into a single process - they remain independent, exactly as a real producer/consumer pair would run in production.

## Why It's Built This Way

See docs/engineering_decisions.md for the full reasoning behind every significant technical choice. The short version: this project deliberately mirrors real production streaming pipeline concerns (schema enforcement, data quality validation, checkpointed recovery, measured performance) even though its "streaming" source is a simulated CSV folder rather than a genuine message queue like Kafka - a documented, accepted trade-off given the assignment's constraints, not an oversight.

## Where to Look Next

- docs/user_guide.md - how to actually run this project
- docs/architecture.md - the full system design, with diagrams
- docs/performance_metrics.md - real, measured results from an 800-event test run
- docs/engineering_decisions.md - every significant choice, its reasoning, and its trade-offs