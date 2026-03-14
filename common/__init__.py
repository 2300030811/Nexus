# common/__init__.py — full replacement
"""
Nexus common utilities.

Import strategy:
- Constants and config: always available (no external deps)
- DB utilities: require psycopg2 — raise ImportError clearly if missing
- Logging: always available
- Metrics: require prometheus_client — degrade gracefully for test environments
"""

from .constants import (
    CATEGORY_MAP, REGION_MAP, CATEGORY_BASELINES, REGION_WEIGHTS,
    HOUR_FACTORS, DOW_FACTORS, DAY_OF_WEEK_FACTORS,
    get_hour_factor, get_dow_factor,
    PRODUCTS, REGIONS, PAYMENT_METHODS,
    FEATURE_COLUMNS, SEVERITY_THRESHOLDS, classify_severity,
)
from .config import load_config, get_db_dsn
from .logging_utils import get_logger

try:
    from .db_utils import (
        get_db_config, get_connection_pool, get_single_connection,
        close_connection, close_connection_pool,
    )
    _DB_AVAILABLE = True
except ImportError as e:
    _DB_AVAILABLE = False
    import warnings
    warnings.warn(
        f"psycopg2 not available — DB utilities disabled: {e}",
        ImportWarning,
        stacklevel=2,
    )
    # Provide stubs that fail loudly rather than silently
    def get_db_config(): raise ImportError("psycopg2 is not installed")
    def get_connection_pool(*a, **kw): raise ImportError("psycopg2 is not installed")
    def get_single_connection(): raise ImportError("psycopg2 is not installed")
    def close_connection(conn): pass
    def close_connection_pool(): pass

try:
    from .cache import TTLCache, _cache
    _CACHE_AVAILABLE = True
except ImportError:
    _CACHE_AVAILABLE = False

__all__ = [
    # Constants
    "CATEGORY_MAP", "REGION_MAP", "CATEGORY_BASELINES", "REGION_WEIGHTS",
    "HOUR_FACTORS", "DOW_FACTORS", "DAY_OF_WEEK_FACTORS",
    "get_hour_factor", "get_dow_factor",
    "PRODUCTS", "REGIONS", "PAYMENT_METHODS",
    "FEATURE_COLUMNS", "SEVERITY_THRESHOLDS", "classify_severity",
    # Config
    "load_config", "get_db_dsn",
    # Logging
    "get_logger",
    # DB
    "get_db_config", "get_connection_pool", "get_single_connection",
    "close_connection", "close_connection_pool",
]
