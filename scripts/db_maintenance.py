#!/usr/bin/env python3
"""
Nexus Database Maintenance Script
Performs VACUUM ANALYZE to update statistics and reclaim space.
"""
import os
import sys
import time
import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.logging_utils import get_logger
from common.db_utils import get_db_config

logger = get_logger("nexus.db_maintenance")


def maintain():
    cfg = get_db_config()
    logger.info("Starting database maintenance on %s...", cfg["dbname"])

    try:
        # Maintenance commands cannot run inside a transaction block
        conn = psycopg2.connect(**cfg)
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
