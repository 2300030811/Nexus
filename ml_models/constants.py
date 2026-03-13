"""
Shared constants and lookups for ML pipeline.

Centralized definitions to avoid duplication across generate_training_data.py,
train_model.py, and detect_anomalies.py.
"""

# Product categories and their encoding
CATEGORY_MAP = {
    "Electronics": 0,
    "Footwear": 1,
    "Apparel": 2,
    "Home": 3,
    "Accessories": 4,
}

# Regional breakdown and their encoding
REGION_MAP = {
    "Delhi": 0,
    "Maharashtra": 1,
    "Karnataka": 2,
    "Tamil Nadu": 3,
    "West Bengal": 4,
}

# Expected revenue baselines per category (used in both training and production)
CATEGORY_BASELINES = {
    "Electronics": 2800.0,
    "Footwear": 400.0,
    "Apparel": 250.0,
    "Home": 600.0,
    "Accessories": 120.0,
}

# Regional market weights (proportion of total volume)
REGION_WEIGHTS = {
    "Delhi": 0.30,
    "Maharashtra": 0.25,
    "Karnataka": 0.15,
    "Tamil Nadu": 0.20,
    "West Bengal": 0.10,
}

# Hour-of-day factors (seasonality pattern)
HOUR_FACTORS = {
    # 0-6am: off-hours
    (0, 6): 0.3,
    # 6-9am: morning
    (6, 9): 0.7,
    # 9-12pm: AM peak
    (9, 12): 1.2,
    # 12-2pm: lunch
    (12, 14): 1.4,
    # 2-6pm: afternoon
    (14, 18): 1.1,
    # 6-9pm: evening peak
    (18, 21): 1.3,
    # 9pm-midnight: night
    (21, 24): 0.6,
}

# Day-of-week factors (weekday patterns)
DAY_OF_WEEK_FACTORS = [
    0.9,   # Monday
    0.85,  # Tuesday
    0.9,   # Wednesday
    1.0,   # Thursday
    1.15,  # Friday
    1.3,   # Saturday
    1.2,   # Sunday
]

# ML feature column names (must match training schema)
FEATURE_COLUMNS = [
    "hour", "day_of_week", "category_enc", "region_enc",
    "order_count", "total_revenue", "avg_order_value",
    "expected_revenue", "revenue_ratio",
]

# Anomaly severity thresholds (based on revenue_ratio)
SEVERITY_THRESHOLDS = {
    "critical": {"min_ratio": 0.0, "max_ratio": 0.2, "spiky_ratio": 4.0},
    "high": {"min_ratio": 0.0, "max_ratio": 0.4, "spiky_ratio": 2.5},
    "medium": {"min_ratio": 0.0, "max_ratio": float('inf'), "spiky_ratio": float('inf')},
}


def get_hour_factor(hour: int) -> float:
    """Get the hour-of-day multiplier for expected revenue."""
    for (start, end), factor in HOUR_FACTORS.items():
        if start <= hour < end:
            return factor
    return 0.6  # Default for off-hours


def get_dow_factor(day_of_week: int) -> float:
    """Get the day-of-week multiplier for expected revenue."""
    if 0 <= day_of_week <= 6:
        return DAY_OF_WEEK_FACTORS[day_of_week]
    return 1.0  # Default

