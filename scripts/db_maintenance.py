#!/usr/bin/env python3
"""
Nexus Database Maintenance Script
Performs VACUUM ANALYZE to update statistics and reclaim space.
"""
import os
import time
import psycopg2
from common.logging_utils import get_logger

logger = get_logger("nexus.db_maintenance")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB   = os.getenv("PG_DB",   "nexus")
PG_USER = os.getenv("PG_USER", "nexus")
PG_PASS = os.getenv("PG_PASSWORD", "nexus_password")

def maintain():
    logger.info("Starting database maintenance on %s...", PG_DB)
    
    try:
        # Maintenance commands cannot run inside a transaction block
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASS
        )
        conn.autocommit = True
        
        with conn.cursor() as cur:
            # 1. Update statistics for the query planner
            logger.info("Running ANALYZE...")
            cur.execute("ANALYZE VERBOSE;")
            
            # 2. Reclaim space and update visibility maps
            logger.info("Running VACUUM...")
            cur.execute("VACUUM VERBOSE order_events;")
            cur.execute("VACUUM VERBOSE revenue_metrics;")
            cur.execute("VACUUM VERBOSE anomalies;")
            
            # 3. Check for index bloat or unusual table sizes
            cur.execute("""
                SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) 
                FROM pg_catalog.pg_statio_user_tables 
                ORDER BY pg_total_relation_size(relid) DESC;
            """)
            logger.info("Table sizes:")
            for name, size in cur.fetchall():
                logger.info("  %s: %s", name, size)

        logger.info("Maintenance complete.")
        conn.close()
    except Exception as e:
        logger.error("Maintenance failed: %s", e)

if __name__ == "__main__":
    maintain()
