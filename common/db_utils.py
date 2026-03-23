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


class Database:
    """Explicitly owned database connection pool."""
    def __init__(self, minconn: int = 1, maxconn: int = 10):
        self._config = get_db_config()
        self._pool = None
        self._minconn = minconn
        self._maxconn = maxconn
        self.initialize_pool()

    def initialize_pool(self):
        attempts = 3
        last_err = None
        for i in range(attempts):
            try:
                self._pool = pool.ThreadedConnectionPool(
                    minconn=self._minconn,
                    maxconn=self._maxconn,
                    **self._config
                )
                return
            except Exception as e:
                last_err = e
                if i < attempts - 1:
                    time.sleep(2)
        raise RuntimeError(f"Failed to create DB pool after {attempts} attempts: {last_err}")

    def get_conn(self):
        if not self._pool: self.initialize_pool()
        return self._pool.getconn()

    def put_conn(self, conn, close: bool = False):
        if self._pool: self._pool.putconn(conn, close=close)

    def close(self):
        if self._pool:
            self._pool.closeall()
            self._pool = None


_default_db = None


def get_connection_pool(minconn: int = 1, maxconn: int = 10) -> pool.ThreadedConnectionPool:
    """Compatibility wrapper for the global shared pool."""
    global _default_db
    if _default_db is None:
        _default_db = Database(minconn=minconn, maxconn=maxconn)
    return _default_db._pool


def get_single_connection():
    """Create a single database connection (no pooling)."""
    return psycopg2.connect(**get_db_config())


def close_connection(conn) -> None:
    """Safely close a database connection."""
    try:
        if conn and not conn.closed:
            conn.close()
    except Exception:
        pass


def close_connection_pool() -> None:
    """Close the default database pool."""
    global _default_db
    if _default_db:
        _default_db.close()
        _default_db = None
