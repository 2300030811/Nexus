# ml_models/drift_monitor.py
"""
Monitors model prediction quality over time.
Detects: feature drift, prediction drift, label drift.
Writes drift scores to a new DB table for dashboard visibility.
"""

import os
import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from common.constants import (
    CATEGORY_MAP, REGION_MAP, CATEGORY_BASELINES,
    REGION_WEIGHTS, get_hour_factor, get_dow_factor,
)
from common.logging_utils import get_logger

logger = get_logger("nexus.drift_monitor")

PG_HOST     = os.getenv("PG_HOST", "postgres")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DB       = os.getenv("PG_DB", "nexus")
PG_USER     = os.getenv("PG_USER", "nexus")
PG_PASSWORD = os.getenv("PG_PASSWORD", "nexus_password")


def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD, connect_timeout=5,
    )




def population_stability_index(expected: np.ndarray, actual: np.ndarray,
                                 bins: int = 10) -> float:
    """
    PSI measures how much a distribution has shifted.
    PSI < 0.1   → no significant change
    PSI 0.1–0.2 → moderate change, monitor closely
    PSI > 0.2   → significant drift, consider retraining
    """
    # Build shared bin edges from both arrays combined
    combined = np.concatenate([expected, actual])
    bin_edges = np.percentile(combined, np.linspace(0, 100, bins + 1))
    bin_edges = np.unique(bin_edges)  # remove duplicates at tails

    def safe_pct(arr, edges):
        counts, _ = np.histogram(arr, bins=edges)
        pct = counts / len(arr)
        return np.clip(pct, 1e-6, None)  # avoid log(0)

    exp_pct = safe_pct(expected, bin_edges)
    act_pct = safe_pct(actual,   bin_edges)

    psi = np.sum((act_pct - exp_pct) * np.log(act_pct / exp_pct))
    return float(psi)


def load_baseline_distribution(metadata_path: str) -> dict | None:
    """Load training-time feature distributions from metadata."""
    path = Path(metadata_path)
    if not path.exists():
        return None
    with open(path) as f:
        meta = json.load(f)
    return meta.get("feature_distributions")


def compute_drift(conn, window_hours: int = 24) -> dict:
    """Compute drift metrics for the last N hours vs baseline."""
    cutoff = datetime.utcnow() - timedelta(hours=window_hours)

    # Fetch recent scored windows
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT rm.total_revenue, rm.order_count, rm.avg_order_value,
                   a.anomaly_score, a.severity
            FROM revenue_metrics rm
            LEFT JOIN anomalies a
                ON rm.window_start = a.window_start
                AND rm.category    = a.category
                AND rm.region      = a.region
            WHERE rm.window_start >= %s
        """, (cutoff,))
        rows = cur.fetchall()

    if len(rows) < 50:
        logger.info("Not enough windows (%d) for drift analysis (need 50+)", len(rows))
        return {}

    df = pd.DataFrame(rows)
    df["anomaly_score"] = df["anomaly_score"].fillna(0).astype(float)
    df["is_anomaly"]    = df["anomaly_score"] > 0.5

    anomaly_rate = float(df["is_anomaly"].mean())
    avg_score    = float(df["anomaly_score"].mean())
    score_std    = float(df["anomaly_score"].std())

    # Expected anomaly rate from training (approximately 5%)
    expected_anomaly_rate = 0.05

    # PSI on revenue distribution
    # Baseline: synthetic data with known distribution
    # We approximate it from the last 7 days as the "stable" reference
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT total_revenue, order_count
            FROM revenue_metrics
            WHERE window_start >= NOW() - INTERVAL '7 days'
              AND window_start <  NOW() - (%s * INTERVAL '1 hour')
            LIMIT 5000
        """, (window_hours,))
        baseline_rows = cur.fetchall()

    drift_flag = False
    psi_revenue     = None
    psi_order_count = None
    notes           = []

    if len(baseline_rows) >= 50:
        baseline_df = pd.DataFrame(baseline_rows)
        psi_revenue = population_stability_index(
            baseline_df["total_revenue"].astype(float).values,
            df["total_revenue"].astype(float).values,
        )
        psi_order_count = population_stability_index(
            baseline_df["order_count"].astype(float).values,
            df["order_count"].astype(float).values,
        )

        if psi_revenue > 0.2:
            drift_flag = True
            notes.append(f"Revenue distribution shift: PSI={psi_revenue:.3f}")
        if psi_order_count > 0.2:
            drift_flag = True
            notes.append(f"Order count distribution shift: PSI={psi_order_count:.3f}")

    # Anomaly rate drift
    if abs(anomaly_rate - expected_anomaly_rate) > 0.15:
        drift_flag = True
        notes.append(
            f"Anomaly rate {anomaly_rate:.1%} far from expected "
            f"{expected_anomaly_rate:.1%}"
        )

    # Score distribution collapse (model outputting all 0s or all 1s)
    if score_std < 0.01:
        drift_flag = True
        notes.append(f"Score distribution collapsed: std={score_std:.4f}")

    return {
        "anomaly_rate":    anomaly_rate,
        "avg_score":       avg_score,
        "score_std":       score_std,
        "psi_revenue":     psi_revenue,
        "psi_order_count": psi_order_count,
        "drift_flag":      drift_flag,
        "notes":           "; ".join(notes) if notes else None,
    }


def log_drift(conn, window_hours: int, metrics: dict):
    if not metrics:
        return
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO model_drift_log
                (window_hours, anomaly_rate, avg_score, score_std,
                 psi_revenue, psi_order_count, drift_flag, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            window_hours,
            metrics["anomaly_rate"],
            metrics["avg_score"],
            metrics["score_std"],
            metrics["psi_revenue"],
            metrics["psi_order_count"],
            metrics["drift_flag"],
            metrics["notes"],
        ))
    conn.commit()

    if metrics["drift_flag"]:
        logger.warning(
            "MODEL DRIFT DETECTED | anomaly_rate=%.3f | "
            "psi_revenue=%s | notes: %s",
            metrics["anomaly_rate"],
            f"{metrics['psi_revenue']:.3f}" if metrics["psi_revenue"] else "n/a",
            metrics["notes"],
        )
    else:
        logger.info(
            "Drift check passed | anomaly_rate=%.3f | avg_score=%.3f",
            metrics["anomaly_rate"], metrics["avg_score"],
        )


def run_once():
    conn = get_conn()
    metrics = compute_drift(conn, window_hours=24)
    log_drift(conn, 24, metrics)
    conn.close()
    return metrics


if __name__ == "__main__":
    run_once()
