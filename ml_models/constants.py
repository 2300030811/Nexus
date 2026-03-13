"""
Backward-compatible constants module for ML pipeline.

All constants are now defined in common/constants.py.
This module re-exports them so existing imports continue to work.
"""

from common.constants import (  # noqa: F401
    CATEGORY_MAP,
    REGION_MAP,
    CATEGORY_BASELINES,
    REGION_WEIGHTS,
    HOUR_FACTORS,
    DAY_OF_WEEK_FACTORS,
    FEATURE_COLUMNS,
    SEVERITY_THRESHOLDS,
    PRODUCTS,
    REGIONS,
    PAYMENT_METHODS,
    get_hour_factor,
    get_dow_factor,
    classify_severity,
)
