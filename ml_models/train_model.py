"""
Nexus – XGBoost Revenue Anomaly Detection Model

Trains a binary classifier to detect revenue anomalies from windowed
metrics features.  Persists the trained model to disk for the detection
service to load at runtime.

Usage:
    python train_model.py       # reads training_data.csv, writes model.json
"""

import os
import json
import hashlib
import logging
import sys
from datetime import datetime

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, precision_recall_fscore_support

from common.constants import CATEGORY_MAP, REGION_MAP, FEATURE_COLUMNS

from common.logging_utils import get_logger

logger = get_logger("nexus.trainer")

DATA_PATH = os.getenv("TRAINING_DATA_PATH", "/app/data/training_data.csv")
MODEL_PATH = os.getenv("MODEL_PATH", "/app/model/model.json")
METADATA_PATH = os.getenv("METADATA_PATH", "/app/model/metadata.json")


def load_and_prepare(path: str) -> tuple[pd.DataFrame, pd.Series]:
    """Load CSV and return feature matrix X and labels y."""
    df = pd.read_csv(path)

    # Encode categoricals
    df["category_enc"] = df["category"].map(CATEGORY_MAP)
    df["region_enc"] = df["region"].map(REGION_MAP)

    X = df[FEATURE_COLUMNS]
    y = df["is_anomaly"]
    return X, y


def train(X: pd.DataFrame, y: pd.Series) -> tuple[xgb.XGBClassifier, dict]:
    """Train an XGBoost classifier with class-weight handling for imbalance."""
    n_normal = (y == 0).sum()
    n_anomaly = (y == 1).sum()
    scale_pos_weight = n_normal / n_anomaly if n_anomaly > 0 else 1.0

    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.1,
        scale_pos_weight=scale_pos_weight,
        eval_metric="aucpr",
        random_state=42,
        use_label_encoder=False,
    )

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y,
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False,
    )

    # Evaluation
    y_pred = model.predict(X_test)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_test, y_pred, average="binary",
    )

    print("[EVAL] Test set results:")
    print(classification_report(y_test, y_pred, target_names=["normal", "anomaly"]))
    logger.info("Precision: %.4f  Recall: %.4f  F1: %.4f", precision, recall, f1)

    # Feature importance
    importances = dict(zip(X.columns, model.feature_importances_))
    logger.info("Feature importances:")
    for feat, imp in sorted(importances.items(), key=lambda x: -x[1]):
        logger.info("  %s %.4f", feat, imp)

    return model, {"precision": precision, "recall": recall, "f1": f1, "importances": importances}


def save_model(model: xgb.XGBClassifier, metrics: dict, training_data_path: str = DATA_PATH) -> None:
    """Persist model and metadata to disk with versioning support.
    
    Args:
        model: Trained XGBoost classifier
        metrics: Training metrics (precision, recall, f1, etc.)
        training_data_path: Path to training data CSV (for data hash)
    """
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    
    # Compute data hash for rollback purposes
    data_hash = "unknown"
    if os.path.exists(training_data_path):
        with open(training_data_path, "rb") as f:
            data_hash = hashlib.sha256(f.read()).hexdigest()[:8]
    
    # Create versioned model path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    versioned_model_path = MODEL_PATH.replace(".json", f"_v{timestamp}.json")
    
    # Save model to both current and versioned paths
    model.save_model(MODEL_PATH)
    model.save_model(versioned_model_path)
    logger.info("Model saved to %s", MODEL_PATH)
    logger.info("Versioned backup: %s", versioned_model_path)

    metadata = {
        "model_type": "XGBClassifier",
        "training_timestamp": timestamp,
        "data_hash": data_hash,
        "features": FEATURE_COLUMNS,
        "category_map": CATEGORY_MAP,
        "region_map": REGION_MAP,
        "metrics": {k: float(v) if isinstance(v, (np.floating, float)) else v
                    for k, v in metrics.items()},
    }

    # Convert numpy floats in importances
    if "importances" in metadata["metrics"]:
        metadata["metrics"]["importances"] = {
            k: float(v) for k, v in metadata["metrics"]["importances"].items()
        }

    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2)
    logger.info("Metadata saved to %s", METADATA_PATH)
    
    # Also save versioned metadata
    versioned_metadata_path = METADATA_PATH.replace(".json", f"_v{timestamp}.json")
    with open(versioned_metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    logger.info("Versioned metadata: %s", versioned_metadata_path)


def main() -> None:
    logger.info("Reading training data from %s", DATA_PATH)
    X, y = load_and_prepare(DATA_PATH)
    logger.info("%d samples, %d anomalies (%.1f%%)", len(X), y.sum(), y.mean() * 100)

    model, metrics = train(X, y)
    save_model(model, metrics)
    logger.info("Training complete")


if __name__ == "__main__":
    main()
