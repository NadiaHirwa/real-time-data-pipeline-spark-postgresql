# Scope

## In Scope

- Real-time-style ingestion of e-commerce events via file-based micro-batching (CSV landing folder, watched by Spark Structured Streaming)
- Data cleaning, type casting, and normalization
- Validation against a defined data contract, with invalid records routed to a separate quarantine path (file and table)
- Storage of valid, transformed events in PostgreSQL
- Checkpointing for restart recovery
- File archiving after successful processing
- Logging across every pipeline stage
- Performance measurement (latency, throughput) under a controlled test load
- Data quality reporting (valid/rejected counts, constraint violations, duplicates)
- SQL-based verification of stored data

## Out of Scope

- **Kafka or any true message-queue system** - CSV files in a folder simulate streaming; this is an assignment constraint, not a design preference (see [`engineering_decisions.md`](engineering_decisions.md))
- **Cloud deployment** - the pipeline runs entirely on a single local machine
- **Distributed, multi-node Spark cluster** - `local[*]` only
- **Authentication or authorization** on any component (Postgres uses a single superuser locally; no API layer exists)
- **Dashboards or BI tooling** - verification happens via SQL queries and `main.py verify`, not a visual dashboard
- **Machine learning or predictive analytics** on the event data
- **Exactly-once delivery guarantees beyond what a unique constraint plus upsert provides** - the pipeline is at-least-once at the micro-batch level, with duplicate prevention handled at the database layer (see [`data_contract.md`](data_contract.md))