"""
Database connection utilities with pooling support.
"""

import os
from typing import Optional
import psycopg2
from psycopg2 import pool


def get_db_config() -> dict:
    """Get database configuration from environment variables."""
    return {
        'host': os.getenv("PG_HOST", "postgres"),
        'port': os.getenv("PG_PORT", "5432"),
        'dbname': os.getenv("PG_DB", "nexus"),
        'user': os.getenv("PG_USER", "nexus"),
        'password': os.getenv("PG_PASSWORD", "nexus_password"),
        'connect_timeout': 5,
    }


_connection_pool: Optional[pool.ThreadedConnectionPool] = None


def get_connection_pool(minconn: int = 1, maxconn: int = 10) -> pool.ThreadedConnectionPool:
    """
    Get or create a threaded connection pool.
    Use this for services that need concurrent database access (e.g., dashboard, API servers).
    """
    global _connection_pool
    if _connection_pool is None:
        config = get_db_config()
        _connection_pool = pool.ThreadedConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            **config
        )
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
