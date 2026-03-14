-- Nexus – PostgreSQL Schema
-- Initializes the data warehouse tables for the retail intelligence platform.

-- Raw order events (every event from Kafka lands here)
CREATE TABLE IF NOT EXISTS order_events (
    event_id        VARCHAR(64) NOT NULL,
    event_type      VARCHAR(32) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    order_id        VARCHAR(32) NOT NULL,
    product_id      VARCHAR(16) NOT NULL,
    product_name    VARCHAR(128) NOT NULL,
    category        VARCHAR(64) NOT NULL,
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10, 2) NOT NULL,
    total_amount    NUMERIC(10, 2) NOT NULL,
    region          VARCHAR(32) NOT NULL,
    payment_method  VARCHAR(32) NOT NULL,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (event_id, event_timestamp)
) PARTITION BY RANGE (event_timestamp);

-- Initial partition for current time range (example)
CREATE TABLE IF NOT EXISTS order_events_default PARTITION OF order_events DEFAULT;

-- Aggregated revenue metrics (written by Spark micro-batches)
CREATE TABLE IF NOT EXISTS revenue_metrics (
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    category        VARCHAR(64) NOT NULL,
    region          VARCHAR(32) NOT NULL,
    order_count     INTEGER NOT NULL,
    total_revenue   NUMERIC(12, 2) NOT NULL,
    avg_order_value NUMERIC(10, 2) NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end, category, region)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_order_events_timestamp ON order_events (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_order_events_product   ON order_events (product_id);
CREATE INDEX IF NOT EXISTS idx_order_events_category  ON order_events (category);
CREATE INDEX IF NOT EXISTS idx_order_events_region    ON order_events (region);
CREATE INDEX IF NOT EXISTS idx_order_events_payment   ON order_events (payment_method);
CREATE INDEX IF NOT EXISTS idx_revenue_metrics_window ON revenue_metrics (window_start);
CREATE INDEX IF NOT EXISTS idx_revenue_metrics_cat_reg ON revenue_metrics (category, region);
CREATE INDEX IF NOT EXISTS idx_order_events_cat_ts ON order_events (category, event_timestamp);

-- Detected anomalies (written by the anomaly detection service)
CREATE TABLE IF NOT EXISTS anomalies (
    id               SERIAL PRIMARY KEY,
    detected_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    category         VARCHAR(64) NOT NULL,
    region           VARCHAR(32) NOT NULL,
    actual_revenue   NUMERIC(12, 2) NOT NULL,
    expected_revenue NUMERIC(12, 2) NOT NULL,
    anomaly_score    NUMERIC(6, 4) NOT NULL,
    severity         VARCHAR(16) NOT NULL,
    status           VARCHAR(16) NOT NULL DEFAULT 'open',
    UNIQUE (window_start, category, region)
);

CREATE INDEX IF NOT EXISTS idx_anomalies_detected ON anomalies (detected_at);
CREATE INDEX IF NOT EXISTS idx_anomalies_status   ON anomalies (status);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON anomalies (severity);
CREATE INDEX IF NOT EXISTS idx_anomalies_cat_reg ON anomalies (category, region);

-- AI copilot investigation reports
CREATE TABLE IF NOT EXISTS copilot_reports (
    id                SERIAL PRIMARY KEY,
    anomaly_id        INTEGER REFERENCES anomalies(id) ON DELETE CASCADE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    severity          VARCHAR(16) NOT NULL,
    category          VARCHAR(64) NOT NULL,
    region            VARCHAR(32) NOT NULL,
    actual_revenue    NUMERIC(12, 2),
    expected_revenue  NUMERIC(12, 2),
    confidence        NUMERIC(4, 2) NOT NULL DEFAULT 0,
    estimated_loss    NUMERIC(12, 2) NOT NULL DEFAULT 0,
    root_cause        TEXT NOT NULL,
    recommended_action TEXT NOT NULL,
    full_report       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reports_anomaly ON copilot_reports (anomaly_id);
CREATE INDEX IF NOT EXISTS idx_reports_created ON copilot_reports (created_at);
CREATE INDEX IF NOT EXISTS idx_reports_severity ON copilot_reports (severity);

-- Feature store (computed by Spark, consumed by ML and copilot)
CREATE TABLE IF NOT EXISTS feature_store (
    computed_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    category                 VARCHAR(64) NOT NULL,
    region                   VARCHAR(32) NOT NULL,
    revenue_last_5m          NUMERIC(12, 2) NOT NULL DEFAULT 0,
    revenue_last_15m         NUMERIC(12, 2) NOT NULL DEFAULT 0,
    revenue_last_60m         NUMERIC(12, 2) NOT NULL DEFAULT 0,
    orders_last_5m           INTEGER NOT NULL DEFAULT 0,
    orders_last_15m          INTEGER NOT NULL DEFAULT 0,
    orders_last_60m          INTEGER NOT NULL DEFAULT 0,
    avg_order_value_last_15m NUMERIC(10, 2) NOT NULL DEFAULT 0,
    revenue_trend_pct        NUMERIC(8, 4) NOT NULL DEFAULT 0,
    PRIMARY KEY (computed_at, category, region)
);

CREATE INDEX IF NOT EXISTS idx_feature_store_cat    ON feature_store (category, region);

-- Model drift log (written by drift_monitor.py)
CREATE TABLE IF NOT EXISTS model_drift_log (
    id              SERIAL PRIMARY KEY,
    measured_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    window_hours    INTEGER NOT NULL,
    anomaly_rate    NUMERIC(6, 4) NOT NULL,
    avg_score       NUMERIC(6, 4) NOT NULL,
    score_std       NUMERIC(6, 4) NOT NULL,
    psi_revenue     NUMERIC(8, 4),
    psi_order_count NUMERIC(8, 4),
    drift_flag      BOOLEAN NOT NULL,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_drift_log_time ON model_drift_log (measured_at DESC);

-- Application configuration (controls demo scenarios)
CREATE TABLE IF NOT EXISTS app_config (
    key   VARCHAR(64) PRIMARY KEY,
    value VARCHAR(256) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Seed defaults
INSERT INTO app_config (key, value) VALUES ('simulate_stockout', 'false')
ON CONFLICT (key) DO NOTHING;

-- Create UNLOGGED table for high-throughput order_events to reduce write latency
-- Note: UNLOGGED tables lack durability but are much faster for transient data
-- Consider using with appropriate replication/backup strategy

-- Data retention policy: Archive old events after 30 days
-- Optional: Run this query periodically to clean up old order_events
-- DELETE FROM order_events WHERE ingested_at < NOW() - INTERVAL '30 days';
-- Or set up a scheduled job (pg_cron) to run automatically
--
-- PRODUCTION TIP: For high-volume systems, consider:
--  1. Table partitioning by time (e.g., RANGE on ingested_at)
--  2. Archive to cold storage (e.g., S3) before deletion
--  3. Retention-based TTL index
--  4. Connection pooling with PgBouncer or pgpool2

-- Anomaly archive table (optional, for compliance/auditing)
CREATE TABLE IF NOT EXISTS anomalies_archive (LIKE anomalies INCLUDING ALL);
-- Periodically: INSERT INTO anomalies_archive SELECT * FROM anomalies WHERE detected_at < NOW() - INTERVAL '90 days';
--               DELETE FROM anomalies WHERE detected_at < NOW() - INTERVAL '90 days';

-- Optimizations: Set shared_buffers and work_mem in postgresql.conf for better query performance
-- Recommended for 8GB+ RAM systems:
-- shared_buffers = 2GB
-- effective_cache_size = 6GB
-- work_mem = 50MB
-- maintenance_work_mem = 500MB
