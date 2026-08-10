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


-- Backs data_quality_report.md directly
CREATE OR REPLACE VIEW v_rejection_summary AS
SELECT rejection_reason, COUNT(*) AS count,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_rejections
FROM rejected_events
GROUP BY rejection_reason
ORDER BY count DESC;

-- Backs performance_metrics.md directly
CREATE OR REPLACE VIEW v_batch_performance AS
SELECT run_id, batch_id, batch_timestamp, batch_duration_ms,
       CASE WHEN batch_duration_ms > 5000 THEN 'over_trigger_interval' ELSE 'on_time' END AS trigger_status
FROM stream_metrics
ORDER BY batch_timestamp;

-- Quick sanity check, backs main.py status
CREATE OR REPLACE VIEW v_pipeline_health AS
SELECT
    (SELECT COUNT(*) FROM events) AS total_events,
    (SELECT COUNT(*) FROM rejected_events) AS total_rejected,
    (SELECT COUNT(*) FROM staging_events) AS orphaned_staging_rows,
    (SELECT COUNT(*) FROM events e WHERE EXISTS (
        SELECT 1 FROM events e2 WHERE e2.event_id = e.event_id GROUP BY e2.event_id HAVING COUNT(*) > 1
    )) AS duplicate_events;

CREATE INDEX IF NOT EXISTS idx_staging_events_run_batch
    ON staging_events (run_id, batch_id);