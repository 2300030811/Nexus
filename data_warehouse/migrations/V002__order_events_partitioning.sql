-- V002__order_events_partitioning.sql
-- Optimizes storage and maintenance of high-volume order events by using PostgreSQL partitioning.

-- 1. Create a function to manage future partitions automatically
CREATE OR REPLACE FUNCTION create_next_month_partition()
RETURNS VOID AS $$
DECLARE
    next_month DATE := date_trunc('month', NOW() + INTERVAL '1 month')::DATE;
    partition_name TEXT := 'order_events_' || to_char(next_month, 'YYYY_MM');
    start_date TEXT := to_char(next_month, 'YYYY-MM-DD');
    end_date TEXT := to_char(next_month + INTERVAL '1 month', 'YYYY-MM-DD');
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = partition_name) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF order_events FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

-- 2. Refactor existing order_events table to be partitioned
-- Since we cannot alter a table to be partitioned, we rename and recreate.
-- (In a true production system with TBs of data, this would be a multi-step online migration).

DO $$ 
BEGIN
    -- Rename existing table if it's not already partitioned
    IF EXISTS (
        SELECT 1 FROM pg_class c 
        JOIN pg_namespace n ON n.oid = c.relnamespace 
        WHERE n.nspname = 'public' AND c.relname = 'order_events' 
          AND c.relkind != 'p' -- relkind 'p' means partitioned table
    ) THEN
        ALTER TABLE order_events RENAME TO order_events_v1;
        
        -- Create the partitioned table
        CREATE TABLE order_events (
            event_id        VARCHAR(64),
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
            PRIMARY KEY (event_id, event_timestamp) -- PK must include partition key
        ) PARTITION BY RANGE (event_timestamp);

        -- Create initial partitions for past, current, and next month
        EXECUTE format('CREATE TABLE order_events_default PARTITION OF order_events DEFAULT');
        
        -- Create current month partition
        DECLARE
            curr_month DATE := date_trunc('month', NOW())::DATE;
            curr_name TEXT := 'order_events_' || to_char(curr_month, 'YYYY_MM');
        BEGIN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF order_events FOR VALUES FROM (%L) TO (%L)',
                curr_name, to_char(curr_month, 'YYYY-MM-DD'), to_char(curr_month + INTERVAL '1 month', 'YYYY-MM-DD')
            );
        EXCEPTION WHEN others THEN
            RAISE NOTICE 'Partition % already exists', curr_name;
        END;

        -- Migrating data from old table to new partitioned table
        INSERT INTO order_events SELECT * FROM order_events_v1;
        
        -- Re-add indexes to the parent partitioned table (will apply to children)
        CREATE INDEX idx_order_events_product   ON order_events (product_id);
        CREATE INDEX idx_order_events_category  ON order_events (category);
        CREATE INDEX idx_order_events_region    ON order_events (region);
        CREATE INDEX idx_order_events_payment   ON order_events (payment_method);
        CREATE INDEX idx_order_events_cat_ts    ON order_events (category, event_timestamp);

        -- Drop old version
        DROP TABLE order_events_v1;
    END IF;
END $$;

-- Ensure next month's partition is ready immediately
SELECT create_next_month_partition();
