-- data_warehouse/migrations/V008__feature_store_performance.sql
-- Optimizes feature store purging and lookup queries.

-- Index for efficient purging (Phase 4.3 of implementation plan)
CREATE INDEX IF NOT EXISTS idx_feature_store_computed_at ON feature_store (computed_at DESC);

-- Composite index for fast feature retrieval during anomaly detection
CREATE INDEX IF NOT EXISTS idx_feature_store_lookup ON feature_store (computed_at DESC, category, region);
