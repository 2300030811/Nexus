-- data_warehouse/migrations/V005__dlq_events.sql
CREATE TABLE IF NOT EXISTS dlq_events (
    id           SERIAL PRIMARY KEY,
    event_data   JSONB NOT NULL,
    error        TEXT,
    retry_count  INTEGER NOT NULL DEFAULT 0,
    first_failed TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_retried TIMESTAMPTZ,
    resolved     BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_dlq_unresolved
    ON dlq_events (first_failed)
    WHERE resolved = FALSE;
