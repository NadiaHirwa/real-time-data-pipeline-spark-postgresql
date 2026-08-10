-- postgres_setup.sql
--
-- Creates the database and events table for the real-time streaming
-- pipeline. Column types and constraints match docs/data_dictionary.md
-- and docs/data_contract.md exactly - these three should never drift
-- out of sync.
--
-- Run the CREATE DATABASE line separately (see user_guide.md), since
-- PostgreSQL does not allow CREATE DATABASE inside the same session
-- as commands that use it. Everything after that can run in one go
-- once connected to the ecommerce_events database.

-- Step 1: run this once, connected to the default 'postgres' database
CREATE DATABASE ecommerce_events;

-- Step 2: connect to ecommerce_events, then run everything below

CREATE TABLE IF NOT EXISTS events (
    event_id         UUID PRIMARY KEY,
    user_id          INTEGER NOT NULL,
    product_id       INTEGER NOT NULL,
    event_type       VARCHAR(20) NOT NULL CHECK (event_type IN ('view', 'purchase')),
    price            NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    category         VARCHAR(50),
    event_timestamp  TIMESTAMP NOT NULL,
    ingested_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Indexes on the columns the verification queries and any future
-- analysis actually filter/sort by (see docs/data_contract.md's
-- SQL Verification Queries section)
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON events (user_id);
CREATE INDEX IF NOT EXISTS idx_events_product_id ON events (product_id);
CREATE INDEX IF NOT EXISTS idx_events_ingested_at ON events (ingested_at);

-- A dedicated table for rejected records, so quarantine data is
-- queryable, not just sitting as CSV files in data/rejected/.
-- Deliberately has NO constraints on event_type/price/etc, since
-- the entire point is to hold rows that violate those rules.
CREATE TABLE IF NOT EXISTS rejected_events (
    event_id         TEXT,
    user_id          TEXT,
    product_id       TEXT,
    event_type       TEXT,
    price            TEXT,
    quantity         TEXT,
    category         TEXT,
    event_timestamp  TEXT,
    rejection_reason TEXT NOT NULL,
    rejected_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Staging table for the bulk-write-then-merge upsert pattern used by
-- write_valid_to_postgres() (see docs/engineering_decisions.md).
-- Spark's JDBC writer bulk-appends here (fast - one operation, not
-- one psycopg2 connection per partition); a separate SQL statement
-- then merges from here into the real events table with an upsert.
-- run_id + batch_id let the merge step target exactly the rows THIS
-- batch just wrote, even if staging_events is shared across restarts
-- or (in principle) concurrent runs. Deliberately has NO constraints,
-- matching rejected_events' reasoning - validation already happened
-- before rows reach this table; constraints here would only get in
-- the way of the bulk write.
CREATE TABLE IF NOT EXISTS staging_events (
    run_id           TEXT NOT NULL,
    batch_id         BIGINT NOT NULL,
    event_id         TEXT,
    user_id          INTEGER,
    product_id       INTEGER,
    event_type       TEXT,
    price            NUMERIC(10, 2),
    quantity         INTEGER,
    category         TEXT,
    event_timestamp  TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_staging_events_run_batch
    ON staging_events (run_id, batch_id);

-- One row per micro-batch, written automatically by a
-- StreamingQueryListener (see spark_streaming.py's MetricsListener).
-- This is what performance_metrics.md should ultimately be built
-- from - Spark's own authoritative internal timing, rather than
-- manually reading log timestamps and doing arithmetic by hand.
CREATE TABLE IF NOT EXISTS stream_metrics (
    metric_id                 BIGSERIAL     PRIMARY KEY,
    run_id                    TEXT          NOT NULL,
    query_id                  TEXT          NOT NULL,
    batch_id                  BIGINT        NOT NULL,
    batch_timestamp           TIMESTAMPTZ   NOT NULL,
    num_input_rows            BIGINT,
    input_rows_per_second     DOUBLE PRECISION,
    processed_rows_per_second DOUBLE PRECISION,
    batch_duration_ms         BIGINT,
    add_batch_ms              BIGINT,
    get_batch_ms              BIGINT,
    trigger_execution_ms      BIGINT,
    recorded_at               TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (run_id, batch_id)
);