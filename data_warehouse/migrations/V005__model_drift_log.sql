-- data_warehouse/migrations/V004__model_drift_log.sql
CREATE TABLE IF NOT EXISTS model_drift_log (
    id              SERIAL PRIMARY KEY,
    measured_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    window_hours    INTEGER NOT NULL,
    anomaly_rate    NUMERIC(6,4) NOT NULL,
    avg_score       NUMERIC(6,4) NOT NULL,
    score_std       NUMERIC(6,4) NOT NULL,
    psi_revenue     NUMERIC(8,6),
    psi_order_count NUMERIC(8,6),
    drift_flag      BOOLEAN NOT NULL DEFAULT FALSE,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_drift_log_measured
    ON model_drift_log (measured_at DESC);
CREATE INDEX IF NOT EXISTS idx_drift_log_flag
    ON model_drift_log (drift_flag)
    WHERE drift_flag = TRUE;
