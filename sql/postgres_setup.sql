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