"""
Centralized feature engineering definitions for Nexus.
Used by Spark streaming, ML scoring, and the AI Copilot.
"""

from datetime import datetime
from common.constants import (
    CATEGORY_BASELINES, REGION_WEIGHTS, get_hour_factor, get_dow_factor,
    classify_severity as _classify_severity
)

# ---------------------------------------------------------------------------
# Windowing & Streaming Constants
# ---------------------------------------------------------------------------
WINDOW_DURATION = "15 minutes"  # Standard feature window
WINDOW_STEP = "5 minutes"       # Sliding step
WATERMARK_DELAY = "2 minutes"

# ---------------------------------------------------------------------------
# Feature Logic
# ---------------------------------------------------------------------------

def compute_expected_revenue(category: str, region: str, timestamp_or_factors: datetime | tuple[int, int]) -> float:
    """
    Calculate the expected revenue for a given category, region, and time.
    Shared source of truth for ML scoring and anomaly detection.
    """
    if isinstance(timestamp_or_factors, datetime):
        hour = timestamp_or_factors.hour
        dow = timestamp_or_factors.weekday()
    else:
        hour, dow = timestamp_or_factors
    
    base = CATEGORY_BASELINES.get(category, 300.0)
    region_w = REGION_WEIGHTS.get(region, 0.15)
    hf = get_hour_factor(hour)
    df = get_dow_factor(dow)
    
    return round(base * region_w * hf * df, 2)


def calculate_revenue_ratio(actual: float, expected: float) -> float:
    """Calculate the ratio of actual to expected revenue."""
    if expected <= 0:
        return 0.0
    return round(actual / expected, 4)


def classify_severity(revenue_ratio: float) -> str:
    """Determine the severity of an anomaly based on the revenue ratio."""
    return _classify_severity(revenue_ratio)


def calculate_trend_pct(current_revenue: float, baseline_revenue: float) -> float:
    """
    Calculate the momentum trend.
    Typically: revenue_last_15m / (revenue_last_60m / 4.0)
    """
    if baseline_revenue <= 0:
        return 0.0
    return round(current_revenue / baseline_revenue, 4)

# Feature column names for ML models
FEATURE_COLUMNS = [
    "hour", "day_of_week", "category_enc", "region_enc",
    "order_count", "total_revenue", "avg_order_value",
    "expected_revenue", "revenue_ratio",
]
