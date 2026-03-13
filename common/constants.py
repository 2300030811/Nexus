"""
Centralized constants for the entire Nexus platform.

Includes category/region mappings, baselines, product catalog, 
and temporal factors used across ML and data generation services.
"""

# ---------------------------------------------------------------------------
# Category and Region Mappings
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Revenue Baselines and Weights
# ---------------------------------------------------------------------------

# Expected revenue baselines per category (5-minute window)
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

# ---------------------------------------------------------------------------
# Temporal Seasonality Factors
# ---------------------------------------------------------------------------

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
DOW_FACTORS = [
    0.9,   # Monday
    0.85,  # Tuesday
    0.9,   # Wednesday
    1.0,   # Thursday
    1.15,  # Friday
    1.3,   # Saturday
    1.2,   # Sunday
]

# Backward-compatible alias (ml_models used this name)
DAY_OF_WEEK_FACTORS = DOW_FACTORS


def get_hour_factor(hour: int) -> float:
    """Get the hour-of-day multiplier for expected revenue."""
    for (start, end), factor in HOUR_FACTORS.items():
        if start <= hour < end:
            return factor
    return 0.6  # Default for off-hours


def get_dow_factor(day_of_week: int) -> float:
    """Get the day-of-week multiplier for expected revenue (0=Monday, 6=Sunday)."""
    if 0 <= day_of_week <= 6:
        return DOW_FACTORS[day_of_week]
    return 1.0  # Default

# ---------------------------------------------------------------------------
# Product Catalog (for Producer)
# ---------------------------------------------------------------------------

PRODUCTS = [
    # Electronics (8 products)
    {"product_id": "SKU-1001", "name": "iPhone 15 Pro",        "category": "Electronics", "base_price": 999.00},
    {"product_id": "SKU-1002", "name": "MacBook Air M3",       "category": "Electronics", "base_price": 1199.00},
    {"product_id": "SKU-1003", "name": "AirPods Pro",          "category": "Electronics", "base_price": 249.00},
    {"product_id": "SKU-1007", "name": "Kindle Paperwhite",    "category": "Electronics", "base_price": 139.99},
    {"product_id": "SKU-1010", "name": "Sony WH-1000XM5",      "category": "Electronics", "base_price": 348.00},
    {"product_id": "SKU-1011", "name": "Samsung Galaxy S24",    "category": "Electronics", "base_price": 899.00},
    {"product_id": "SKU-1012", "name": "iPad Air M2",           "category": "Electronics", "base_price": 599.00},
    {"product_id": "SKU-1013", "name": "JBL Flip 6 Speaker",   "category": "Electronics", "base_price": 129.99},
    # Footwear (5 products)
    {"product_id": "SKU-1004", "name": "Nike Air Max 90",      "category": "Footwear",    "base_price": 130.00},
    {"product_id": "SKU-1014", "name": "Adidas Ultraboost 23", "category": "Footwear",    "base_price": 190.00},
    {"product_id": "SKU-1015", "name": "Puma RS-X Sneakers",   "category": "Footwear",    "base_price": 110.00},
    {"product_id": "SKU-1016", "name": "New Balance 574",      "category": "Footwear",    "base_price": 89.99},
    {"product_id": "SKU-1017", "name": "Woodland Leather Boots", "category": "Footwear",  "base_price": 145.00},
    # Apparel (6 products)
    {"product_id": "SKU-1005", "name": "Levi's 501 Jeans",     "category": "Apparel",     "base_price": 69.50},
    {"product_id": "SKU-1018", "name": "Allen Solly Shirt",    "category": "Apparel",     "base_price": 45.00},
    {"product_id": "SKU-1019", "name": "Peter England Blazer", "category": "Apparel",     "base_price": 120.00},
    {"product_id": "SKU-1020", "name": "Van Heusen Chinos",    "category": "Apparel",     "base_price": 55.00},
    {"product_id": "SKU-1021", "name": "US Polo T-Shirt",      "category": "Apparel",     "base_price": 35.00},
    {"product_id": "SKU-1022", "name": "Raymond Formal Suit",  "category": "Apparel",     "base_price": 250.00},
    # Home (5 products)
    {"product_id": "SKU-1006", "name": "Instant Pot Duo 7-in-1", "category": "Home",      "base_price": 89.99},
    {"product_id": "SKU-1008", "name": "Dyson V15 Vacuum",     "category": "Home",        "base_price": 749.99},
    {"product_id": "SKU-1023", "name": "Philips Air Fryer",    "category": "Home",        "base_price": 119.99},
    {"product_id": "SKU-1024", "name": "Prestige Mixer Grinder", "category": "Home",      "base_price": 65.00},
    {"product_id": "SKU-1025", "name": "Havells Tower Fan",    "category": "Home",        "base_price": 55.00},
    # Accessories (6 products)
    {"product_id": "SKU-1009", "name": "Yeti Rambler 26oz",    "category": "Accessories", "base_price": 35.00},
    {"product_id": "SKU-1026", "name": "Fossil Leather Watch", "category": "Accessories", "base_price": 149.00},
    {"product_id": "SKU-1027", "name": "Ray-Ban Aviators",     "category": "Accessories", "base_price": 165.00},
    {"product_id": "SKU-1028", "name": "Skagen Crossbody Bag", "category": "Accessories", "base_price": 89.00},
    {"product_id": "SKU-1029", "name": "Titan Smart Band",     "category": "Accessories", "base_price": 45.00},
    {"product_id": "SKU-1030", "name": "Wildcraft Backpack",   "category": "Accessories", "base_price": 55.00},
]

REGIONS = ["Delhi", "Maharashtra", "Karnataka", "Tamil Nadu", "West Bengal"]

PAYMENT_METHODS = ["credit_card", "debit_card", "digital_wallet", "buy_now_pay_later"]

# ---------------------------------------------------------------------------
# ML Feature Columns
# ---------------------------------------------------------------------------

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


def classify_severity(revenue_ratio: float) -> str:
    """Classify anomaly severity based on revenue ratio. Single source of truth."""
    if revenue_ratio < 0.2 or revenue_ratio > 4.0:
        return "critical"
    elif revenue_ratio < 0.4 or revenue_ratio > 2.5:
        return "high"
    else:
        return "medium"
