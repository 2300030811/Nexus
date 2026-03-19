"""
Database connection utilities with pooling support.
"""

import os
import time
from typing import Optional
import psycopg2
from psycopg2 import pool


from common.config import load_config


def get_db_config() -> dict:
    """Get database configuration from centralized config."""
    config = load_config()
    db = config['database']
    db['connect_timeout'] = 5
    return db


_connection_pool: Optional[pool.ThreadedConnectionPool] = None


def get_connection_pool(minconn: int = 1, maxconn: int = 10) -> pool.ThreadedConnectionPool:
    """
    Get or create a threaded connection pool with retry logic.
    """
    global _connection_pool
    if _connection_pool is None:
        config = get_db_config()
        attempts = 3
        last_err = None
        
        for i in range(attempts):
            try:
                _connection_pool = pool.ThreadedConnectionPool(
                    minconn=minconn,
                    maxconn=maxconn,
                    **config
                )
                return _connection_pool
            except Exception as e:
                last_err = e
                if i < attempts - 1:
                    time.sleep(2)  # Wait before retry
                    
        raise RuntimeError(f"Failed to create DB pool after {attempts} attempts: {last_err}")
    return _connection_pool


def get_single_connection():
    """
    Create a single database connection (no pooling).
    Use this for simple single-threaded services (e.g., producer, ML services).
    """
    config = get_db_config()
    return psycopg2.connect(**config)


def close_connection(conn) -> None:
    """Safely close a database connection."""
    try:
        if conn and not conn.closed:
            conn.close()
    except Exception:
        pass


def close_connection_pool() -> None:
    """Close all connections in the global pool."""
    global _connection_pool
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
