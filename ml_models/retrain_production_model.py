"""
Nexus – Production Model Retraining Script

Trains the anomaly detection model on real production data from the database.
This script should be run periodically (e.g., weekly) to keep the model updated
with actual production patterns and labeled anomalies.

Usage:
    python retrain_production_model.py --days 30 --min-samples 1000
"""

import os
import sys
import argparse
import hashlib
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, precision_score, recall_score, f1_score
import psycopg2

from common.constants import (
    CATEGORY_MAP, REGION_MAP, CATEGORY_BASELINES, REGION_WEIGHTS,
    get_hour_factor, get_dow_factor, FEATURE_COLUMNS
)
from common.db_utils import get_single_connection, close_connection
from common.model_utils import save_versioned_model, load_metadata

from common.logging_utils import get_logger

logger = get_logger("nexus.retrain")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MODEL_DIR = Path("/app/model")
MODEL_PATH = MODEL_DIR / "model.json"




def extract_training_data(days: int = 30) -> pd.DataFrame:
    """
    Extract training data from production database.

    Combines revenue_metrics with confirmed anomalies to create labeled training data.

    WARNING – feedback loop risk: labels here are derived from the model's own prior
    predictions stored in the anomalies table.  To break the loop over time, operators
    should mark false positives with status='false_positive' via the dashboard or API.
    Those rows are excluded below so they cannot reinforce incorrect model behaviour.
    Ideally, a human-review workflow sets a 'confirmed' status before retraining.
    """
    conn = get_single_connection()

    # Only treat anomalies that have NOT been marked as false positives as positive labels.
    # This prevents confirmed mistakes from entrenching themselves in successive model versions.
    query = """
        WITH labeled_windows AS (
            SELECT 
                rm.window_start,
                rm.window_end,
                rm.category,
                rm.region,
                rm.order_count,
                rm.total_revenue,
                rm.avg_order_value,
                CASE WHEN a.id IS NOT NULL THEN 1 ELSE 0 END as is_anomaly
            FROM revenue_metrics rm
            LEFT JOIN anomalies a 
                ON rm.window_start = a.window_start 
                AND rm.category = a.category 
                AND rm.region = a.region
                AND a.status != 'false_positive'
            WHERE rm.window_start >= NOW() - (%s * INTERVAL '1 day')
        )
        SELECT * FROM labeled_windows
        ORDER BY window_start;
    """
    
    df = pd.read_sql(query, conn, params=(days,))
    close_connection(conn)
    
    logger.info("Extracted %d windows from last %d days", len(df), days)
    logger.info("Anomalies: %d (%.2f%%)", df['is_anomaly'].sum(), df['is_anomaly'].mean()*100)
    
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived features to match training schema.
    """
    # Temporal features
    df["hour"] = pd.to_datetime(df["window_start"]).dt.hour
    df["day_of_week"] = pd.to_datetime(df["window_start"]).dt.dayofweek
    
    # Encoding
    df["category_enc"] = df["category"].map(CATEGORY_MAP).fillna(-1).astype(int)
    df["region_enc"] = df["region"].map(REGION_MAP).fillna(-1).astype(int)
    
    # Expected revenue calculation (vectorized)
    df["base_revenue"] = df["category"].map(lambda c: CATEGORY_BASELINES.get(c, 300.0))
    df["region_weight"] = df["region"].map(lambda r: REGION_WEIGHTS.get(r, 0.15))
    df["hour_factor"] = df["hour"].apply(get_hour_factor)
    df["dow_factor"] = df["day_of_week"].apply(get_dow_factor)
    
    df["expected_revenue"] = (
        df["base_revenue"] * df["region_weight"] * df["hour_factor"] * df["dow_factor"]
    ).round(2)
    
    # Revenue ratio
    df["revenue_ratio"] = np.where(
        df["expected_revenue"] > 0,
        (df["total_revenue"] / df["expected_revenue"]).round(4),
        0
    )
    
    return df


def train_model(df: pd.DataFrame, test_size: float = 0.2) -> xgb.XGBClassifier:
    """
    Train XGBoost model on labeled production data.
    """
    feature_cols = [
        "hour", "day_of_week", "category_enc", "region_enc",
        "order_count", "total_revenue", "avg_order_value",
        "expected_revenue", "revenue_ratio"
    ]
    
    X = df[feature_cols]
    y = df["is_anomaly"]
    
    # Check for class imbalance
    class_ratio = y.value_counts()
    logger.info("Class distribution: Normal=%d, Anomaly=%d", class_ratio[0], class_ratio[1])
    
    if len(class_ratio) < 2:
        raise ValueError("Training data must contain both normal and anomaly samples")
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=42, stratify=y
    )
    
    # Calculate class weight
    scale_pos_weight = float(np.sum(y_train == 0)) / float(np.sum(y_train == 1))
    
    # Train model
    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.1,
        eval_metric="aucpr",
        scale_pos_weight=scale_pos_weight,
        random_state=42
    )
    
    logger.info("Training on %d samples", len(X_train))
    model.fit(X_train, y_train)
    
    # Evaluate
    y_pred = model.predict(X_test)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    
    logger.info("Test Set Performance: Precision=%.4f  Recall=%.4f  F1=%.4f", precision, recall, f1)
    logger.info("\n%s", classification_report(y_test, y_pred))
    
    # Feature importance
    importance = pd.DataFrame({
        "feature": feature_cols,
        "importance": model.feature_importances_
    }).sort_values("importance", ascending=False)
    logger.info("Top 5 important features:\n%s", importance.head().to_string(index=False))
    
    return model, {"precision": precision, "recall": recall, "f1": f1}


def save_model_with_gate(model, metrics, df):
    """Save model but only promote to production if it meets performance criteria."""
    current_meta = load_metadata(MODEL_DIR)
    current_f1 = (current_meta or {}).get("metrics", {}).get("f1", 0.0)
    
    new_f1 = metrics["f1"]
    
    metadata = {
        "num_samples":   len(df),
        "num_anomalies": int(df["is_anomaly"].sum()),
        "anomaly_rate":  float(df["is_anomaly"].mean()),
        "metrics":       metrics,
        "features":      FEATURE_COLUMNS,
        "category_map":  CATEGORY_MAP,
        "region_map":    REGION_MAP,
    }

    if new_f1 < current_f1:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.warning(
            "New model F1=%.4f is WORSE than current production F1=%.4f. "
            "Skipping production promotion. Saved as candidate only.",
            new_f1, current_f1
        )
        # Just save timestamped model/meta without overwriting 'model.json'
        model.save_model(str(MODEL_DIR / f"candidate_{timestamp}.json"))
        with open(MODEL_DIR / f"candidate_meta_{timestamp}.json", "w") as f:
            json.dump(metadata, f, indent=2)
        return

    logger.info("New model F1=%.4f >= current F1=%.4f. Promoting to production.", new_f1, current_f1)
    save_versioned_model(
        model,
        model_dir=MODEL_DIR,
        metadata=metadata,
        source_df=df,
    )


def main():
    parser = argparse.ArgumentParser(description="Retrain anomaly detection model on production data")
    parser.add_argument("--days", type=int, default=30, help="Number of days of history to use (default: 30)")
    parser.add_argument("--min-samples", type=int, default=1000, help="Minimum samples required (default: 1000)")
    parser.add_argument("--test-size", type=float, default=0.2, help="Test set proportion (default: 0.2)")
    
    args = parser.parse_args()
    
    logger.info("Production model retraining started")
    logger.info("Config: Days=%d, Min samples=%d", args.days, args.min_samples)
    
    # Extract data
    df = extract_training_data(days=args.days)
    
    if len(df) < args.min_samples:
        logger.error("Insufficient data: %d < %d. Retraining aborted.", len(df), args.min_samples)
        sys.exit(1)
    
    # Engineer features
    df = engineer_features(df)
    
    # Train model
    model, metrics = train_model(df, test_size=args.test_size)
    
    # Save model with gate
    save_model_with_gate(model, metrics, df)
    
    logger.info("Model retraining completed successfully")


if __name__ == "__main__":
    main()
