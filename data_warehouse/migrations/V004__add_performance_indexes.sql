-- data_warehouse/migrations/V004__add_performance_indexes.sql
-- Indexes to speed up dashboard queries and anomaly detection scans.

-- Speed up fetching latest anomalies for the API and dashboard
CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at ON anomalies (detected_at DESC);

-- Speed up checking if a metric window has already been scored
CREATE INDEX IF NOT EXISTS idx_anomalies_composite_lookup ON anomalies (window_start, category, region);

-- Speed up windowed revenue aggregations in the API/Dashboard
CREATE INDEX IF NOT EXISTS idx_order_events_timestamp_lookup ON order_events (event_timestamp DESC);

-- Speed up time-series lookups for specific categories/regions
CREATE INDEX IF NOT EXISTS idx_revenue_metrics_lookup ON revenue_metrics (window_start DESC, category, region);
