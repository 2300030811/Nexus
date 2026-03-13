"""
Nexus – Synthetic Training Data Generator

Generates historical revenue_metrics rows that mimic realistic retail
patterns: hourly seasonality, day-of-week effects, category mix, and
injected anomalies.  Output is a CSV used to train the XGBoost model.

Usage:
    python generate_training_data.py          # writes to /app/data/training_data.csv
"""

import os
import random
import csv
import logging
import sys
from datetime import datetime, timedelta

from common.constants import (
    CATEGORY_MAP, REGION_MAP,
    CATEGORY_BASELINES, REGION_WEIGHTS,
    get_hour_factor, get_dow_factor
)

from common.logging_utils import get_logger

logger = get_logger("nexus.datagen")

CATEGORIES = list(CATEGORY_MAP.keys())
REGIONS = list(REGION_MAP.keys())


def generate_training_data(
    days: int = 30,
    anomaly_rate: float = 0.05,
    output_path: str = "/app/data/training_data.csv",
) -> str:
    """Generate synthetic revenue metric rows and write to CSV."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    rows = []
    start = datetime(2025, 1, 1)
    window_minutes = 5
    windows_per_day = (24 * 60) // window_minutes  # 288

    for day_offset in range(days):
        current_day = start + timedelta(days=day_offset)
        dow = current_day.weekday()

        for window_idx in range(windows_per_day):
            window_start = current_day + timedelta(minutes=window_idx * window_minutes)
            window_end = window_start + timedelta(minutes=window_minutes)
            hour = window_start.hour

            for category in CATEGORIES:
                for region in REGIONS:
                    base = CATEGORY_BASELINES[category]
                    region_share = REGION_WEIGHTS[region]

                    expected = (
                        base
                        * region_share
                        * get_hour_factor(hour)
                        * get_dow_factor(dow)
                    )

                    # Normal noise: +/-15%
                    noise = random.uniform(0.85, 1.15)
                    revenue = expected * noise

                    # Label: 0 = normal, 1 = anomaly
                    is_anomaly = 0

                    # Inject anomalies: sudden drops or spikes
                    if random.random() < anomaly_rate:
                        is_anomaly = 1
                        if random.random() < 0.7:
                            # revenue drop (more common failure mode)
                            revenue = expected * random.uniform(0.05, 0.35)
                        else:
                            # revenue spike (flash sale, duplicate charges, etc.)
                            revenue = expected * random.uniform(2.5, 5.0)

                    revenue = round(revenue, 2)
                    order_count = max(1, int(revenue / (base * 0.05)))
                    avg_order_value = round(revenue / order_count, 2)

                    rows.append({
                        "window_start": window_start.isoformat(),
                        "window_end": window_end.isoformat(),
                        "category": category,
                        "region": region,
                        "hour": hour,
                        "day_of_week": dow,
                        "order_count": order_count,
                        "total_revenue": revenue,
                        "avg_order_value": avg_order_value,
                        "expected_revenue": round(expected, 2),
                        "revenue_ratio": round(revenue / expected, 4) if expected > 0 else 0,
                        "is_anomaly": is_anomaly,
                    })

    # Shuffle to avoid temporal ordering bias during training
    random.shuffle(rows)

    fieldnames = [
        "window_start", "window_end", "category", "region",
        "hour", "day_of_week", "order_count", "total_revenue",
        "avg_order_value", "expected_revenue", "revenue_ratio", "is_anomaly",
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    anomaly_count = sum(1 for r in rows if r["is_anomaly"] == 1)
    logger.info("Generated %d rows (%d anomalies, %.1f%%) → %s",
                len(rows), anomaly_count, anomaly_count / len(rows) * 100, output_path)
    return output_path


if __name__ == "__main__":
    generate_training_data()
