-- Nexus Database Maintenance & Optimization Script
-- Adds partitioning, retention policies, and archival strategy

-- =============================================================================
-- Table Partitioning for order_events
-- =============================================================================

-- The order_events table is now partitioned by default in init.sql.
-- These helper functions will apply to 'order_events'.

-- Create monthly partitions for the current year + 3 months ahead
CREATE TABLE IF NOT EXISTS order_events_2026_03 PARTITION OF order_events
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE IF NOT EXISTS order_events_2026_04 PARTITION OF order_events
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE IF NOT EXISTS order_events_2026_05 PARTITION OF order_events
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE IF NOT EXISTS order_events_2026_06 PARTITION OF order_events
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

-- Create indexes on partitions are automatic in Newer PG, but we can index the parent
CREATE INDEX IF NOT EXISTS idx_order_events_timestamp_partitioned 
    ON order_events (event_timestamp);

-- Step 3: Migrate data from old table (if needed)
-- INSERT INTO order_events_partitioned SELECT * FROM order_events_old;

-- Step 4: Drop old table and rename
-- DROP TABLE order_events_old;
-- ALTER TABLE order_events_partitioned RENAME TO order_events;

-- =============================================================================
-- Helper Function: Create New Partitions Automatically
-- =============================================================================

CREATE OR REPLACE FUNCTION create_next_month_partition()
RETURNS void AS $$
DECLARE
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    -- Calculate next month
    start_date := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month');
    end_date := start_date + INTERVAL '1 month';
    
    partition_name := 'order_events_' || TO_CHAR(start_date, 'YYYY_MM');
    
    -- Check if partition already exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_class WHERE relname = partition_name
    ) THEN
        EXECUTE FORMAT(
            'CREATE TABLE %I PARTITION OF order_events FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            end_date
        );
        RAISE NOTICE 'Created partition: %', partition_name;
    ELSE
        RAISE NOTICE 'Partition % already exists', partition_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Data Retention: Archival Strategy
-- =============================================================================

-- Create archive table for old events (cold storage)
CREATE TABLE IF NOT EXISTS order_events_archive (
    LIKE order_events INCLUDING ALL
);

-- Function to archive events older than N days
CREATE OR REPLACE FUNCTION archive_old_events(days_threshold INTEGER DEFAULT 90)
RETURNS INTEGER AS $$
DECLARE
    archived_count INTEGER;
BEGIN
    -- Move old events to archive
    WITH moved AS (
        DELETE FROM order_events
        WHERE event_timestamp < NOW() - MAKE_INTERVAL(days => days_threshold)
        RETURNING *
    )
    INSERT INTO order_events_archive SELECT * FROM moved;
    
    GET DIAGNOSTICS archived_count = ROW_COUNT;
    RAISE NOTICE 'Archived % events older than % days', archived_count, days_threshold;
    
    RETURN archived_count;
END;
$$ LANGUAGE plpgsql;

-- Function to drop old partitions (DANGER: permanent deletion)
CREATE OR REPLACE FUNCTION drop_old_partition(months_old INTEGER DEFAULT 6)
RETURNS void AS $$
DECLARE
    partition_name TEXT;
    cutoff_date DATE;
BEGIN
    cutoff_date := DATE_TRUNC('month', CURRENT_DATE - MAKE_INTERVAL(months => months_old));
    partition_name := 'order_events_' || TO_CHAR(cutoff_date, 'YYYY_MM');
    
    IF EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE FORMAT('DROP TABLE %I', partition_name);
        RAISE NOTICE 'Dropped old partition: %', partition_name;
    ELSE
        RAISE NOTICE 'Partition % does not exist', partition_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Anomalies Table Optimization
-- =============================================================================

-- Add index on detected_at for faster dashboard queries
CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at_desc 
    ON anomalies (detected_at DESC);

-- Add composite index for category + region + status queries
CREATE INDEX IF NOT EXISTS idx_anomalies_category_region_status 
    ON anomalies (category, region, status);

-- =============================================================================
-- Vacuum and Analyze Schedule (Manual for now)
-- =============================================================================

-- Run these periodically (or set up pg_cron):
-- VACUUM ANALYZE order_events_partitioned;
-- VACUUM ANALYZE revenue_metrics;
-- VACUUM ANALYZE anomalies;
-- VACUUM ANALYZE copilot_reports;

-- =============================================================================
-- Usage Instructions
-- =============================================================================

-- To create next month's partition (run monthly):
-- SELECT create_next_month_partition();

-- To archive events older than 90 days:
-- SELECT archive_old_events(90);

-- To drop partitions older than 6 months (DANGEROUS - permanent):
-- SELECT drop_old_partition(6);

-- To check partition sizes:
-- SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
-- FROM pg_tables
-- WHERE tablename LIKE 'order_events_%'
-- ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
