"""
Nexus Common Utilities

Shared utilities for database connections, logging, and constants.
"""

# Constants are always available (no external deps)
from .constants import (
    CATEGORY_MAP, REGION_MAP, CATEGORY_BASELINES, REGION_WEIGHTS,
    HOUR_FACTORS, DOW_FACTORS, DAY_OF_WEEK_FACTORS,
    get_hour_factor, get_dow_factor,
    PRODUCTS, REGIONS, PAYMENT_METHODS,
    FEATURE_COLUMNS, SEVERITY_THRESHOLDS, classify_severity,
)
from .config import load_config
from .logging_utils import get_logger

# DB utilities require psycopg2 — import gracefully for test environments
try:
    from .db_utils import get_connection_pool, get_single_connection, close_connection
except ImportError:
    get_connection_pool = None
    get_single_connection = None
    close_connection = None

__all__ = [
    'get_connection_pool',
    'get_single_connection',
    'close_connection',
    'CATEGORY_MAP',
    'REGION_MAP',
    'CATEGORY_BASELINES',
    'REGION_WEIGHTS',
    'HOUR_FACTORS',
    'DOW_FACTORS',
    'DAY_OF_WEEK_FACTORS',
    'get_hour_factor',
    'get_dow_factor',
    'PRODUCTS',
    'REGIONS',
    'PAYMENT_METHODS',
    'FEATURE_COLUMNS',
    'SEVERITY_THRESHOLDS',
    'classify_severity',
    'load_config',
    'get_logger',
]
