-- postgres_setup_ci.sql
--
-- CI-specific version of sql/postgres_setup.sql. Identical table and
-- index definitions, but omits the CREATE DATABASE line: GitHub
-- Actions' postgres service container already creates the
-- ecommerce_events database via its POSTGRES_DB environment
-- variable, so running CREATE DATABASE again here would fail with
-- "database already exists."
--
-- Keep this file's table/index definitions in sync with
-- postgres_setup.sql by hand whenever the schema changes - there is
-- no automated check enforcing this (see docs/risks_and_limitations.md).

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

CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON events (user_id);
CREATE INDEX IF NOT EXISTS idx_events_product_id ON events (product_id);
CREATE INDEX IF NOT EXISTS idx_events_ingested_at ON events (ingested_at);

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