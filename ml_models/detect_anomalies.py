"""
Nexus – Anomaly Detection Service

Runs on a loop, querying the latest revenue_metrics from PostgreSQL,
scoring each window with the trained XGBoost model, and writing detected
anomalies to an 'anomalies' table for the AI copilot and dashboard to consume.
"""

import os
import signal
import sys
import time
import requests
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import xgboost as xgb
import psycopg2
from psycopg2.extras import execute_values

from common.constants import (
    CATEGORY_MAP, REGION_MAP, CATEGORY_BASELINES, REGION_WEIGHTS,
    FEATURE_COLUMNS, get_hour_factor, get_dow_factor, classify_severity
)
from common.logging_utils import get_logger
from common.metrics import (
    SCANS_TOTAL, ANOMALIES_DETECTED, SCORING_LATENCY,
    WINDOWS_SCORED, DB_RECONNECTS, start_metrics_server,
)

# ---------------------------------------------------------------------------
# Structured logging (via shared utility)
# ---------------------------------------------------------------------------
logger = get_logger("nexus.anomaly_detector")

# ---------------------------------------------------------------------------
# Global state for graceful shutdown
# ---------------------------------------------------------------------------
shutdown_flag = False

def signal_handler(sig, frame):
    """Handle SIGTERM/SIGINT for graceful shutdown."""
    global shutdown_flag
    logger.info("Received signal %s, initiating graceful shutdown", sig)
    shutdown_flag = True

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_db_cfg = get_db_config()
PG_HOST = _db_cfg['host']
PG_PORT = _db_cfg['port']
PG_DB = _db_cfg['dbname']
PG_USER = _db_cfg['user']
PG_PASSWORD = _db_cfg['password']
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))  # seconds
MODEL_PATH = os.getenv("MODEL_PATH", "/app/model/model.json")
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")

# ---------------------------------------------------------------------------
# Database Functions
# ---------------------------------------------------------------------------


from common.db_utils import get_single_connection, close_connection



# Removed ensure_anomalies_table - handled by init.sql


def load_model(path: str) -> xgb.XGBClassifier:
    """Load the trained XGBoost model from disk."""
    model = xgb.XGBClassifier()
    model.load_model(path)
    logger.info("Loaded model from %s", path)
    return model


def compute_expected_revenue(category: str, region: str, hour: int, dow: int) -> float:
    """Recompute expected revenue using the same logic as training data gen."""
    base = CATEGORY_BASELINES.get(category, 300.0)
    region_w = REGION_WEIGHTS.get(region, 0.15)
    hf = get_hour_factor(hour)
    df = get_dow_factor(dow)
    return round(base * region_w * hf * df, 2)


def fetch_recent_metrics(conn, lookback_minutes: int = 10) -> pd.DataFrame:
    """Fetch revenue_metrics rows from the last N minutes that haven't been scored."""
    query = """
        SELECT rm.window_start, rm.window_end, rm.category, rm.region,
               rm.order_count, rm.total_revenue, rm.avg_order_value
        FROM revenue_metrics rm
        WHERE rm.window_start >= NOW() - (%s * INTERVAL '1 minute')
          AND NOT EXISTS (
              SELECT 1 FROM anomalies a
              WHERE a.window_start = rm.window_start
                AND a.category = rm.category
                AND a.region = rm.region
          )
        ORDER BY rm.window_start DESC
        LIMIT 500;
    """
    return pd.read_sql(query, conn, params=(lookback_minutes,))


def score_metrics(model: xgb.XGBClassifier, df: pd.DataFrame) -> pd.DataFrame:
    """Run the model on recent metrics and return rows flagged as anomalies."""
    if df.empty:
        return df

    # Compute derived features to match training schema (vectorized operations)
    df["hour"] = pd.to_datetime(df["window_start"]).dt.hour
    df["day_of_week"] = pd.to_datetime(df["window_start"]).dt.dayofweek
    df["category_enc"] = df["category"].map(CATEGORY_MAP).fillna(-1).astype(int)
    df["region_enc"] = df["region"].map(REGION_MAP).fillna(-1).astype(int)

    # Vectorized expected revenue computation
    df["base_revenue"] = df["category"].map(lambda c: CATEGORY_BASELINES.get(c, 300.0))
    df["region_weight"] = df["region"].map(lambda r: REGION_WEIGHTS.get(r, 0.15))
    df["hour_factor"] = df["hour"].apply(get_hour_factor)
    df["dow_factor"] = df["day_of_week"].apply(get_dow_factor)
    
    df["expected_revenue"] = (
        df["base_revenue"] * df["region_weight"] * df["hour_factor"] * df["dow_factor"]
    ).round(2)
    
    df["revenue_ratio"] = np.where(
        df["expected_revenue"] > 0,
        (df["total_revenue"] / df["expected_revenue"]).round(4),
        0,
    )

    feature_cols = [
        "hour", "day_of_week", "category_enc", "region_enc",
        "order_count", "total_revenue", "avg_order_value",
        "expected_revenue", "revenue_ratio",
    ]
    X = df[feature_cols]

    df["anomaly_pred"] = model.predict(X)
    df["anomaly_score"] = model.predict_proba(X)[:, 1].round(4)

    # Only keep predicted anomalies
    anomalies = df[df["anomaly_pred"] == 1].copy()

    # Use shared severity classification
    anomalies["severity"] = anomalies["revenue_ratio"].apply(classify_severity)
    return anomalies


def send_webhook_alert(row: pd.Series) -> None:
    """Send an alert to a webhook for critical anomalies."""
    if not ALERT_WEBHOOK_URL:
        return
    try:
        payload = {
            "text": f"🚨 *CRITICAL ANOMALY DETECTED*\n"
                    f"Category: {row['category']} | Region: {row['region']}\n"
                    f"Actual: ₹{row['total_revenue']:.2f} vs Expected: ₹{row['expected_revenue']:.2f}\n"
                    f"Score: {row['anomaly_score']:.3f}\n"
        }
        resp = requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=5)
        resp.raise_for_status()
        logger.info("Alert sent to webhook successfully.")
    except Exception as e:
        logger.error("Failed to send webhook alert: %s", e)


def write_anomalies(conn, anomalies: pd.DataFrame) -> int:
    """Insert detected anomalies into the anomalies table."""
    if anomalies.empty:
        return 0

    rows = [
        (
            row["window_start"], row["window_end"],
            row["category"], row["region"],
            float(row["total_revenue"]), float(row["expected_revenue"]),
            float(row["anomaly_score"]), row["severity"],
        )
        for _, row in anomalies.iterrows()
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO anomalies
                (window_start, window_end, category, region,
                 actual_revenue, expected_revenue, anomaly_score, severity)
            VALUES %s
            ON CONFLICT (window_start, category, region) DO NOTHING
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def main() -> None:
    logger.info("Anomaly detection service starting")

    # Start Prometheus metrics endpoint
    metrics_port = int(os.getenv("METRICS_PORT", "9091"))
    start_metrics_server(metrics_port)
    logger.info("Prometheus metrics available on port %d", metrics_port)

    # Wait for model to be available (training runs first)
    while not os.path.exists(MODEL_PATH):
        logger.warning("Model not found at %s, retrying in 10s", MODEL_PATH)
        time.sleep(10)

    model = load_model(MODEL_PATH)
    last_model_mtime = os.path.getmtime(MODEL_PATH)

    conn = get_single_connection()
    # ensure_anomalies_table(conn)
    logger.info("Scanning every %ds for anomalies", SCAN_INTERVAL)

    scan_count = 0
    reconnect_backoff = 1  # seconds, will grow exponentially
    max_backoff = 60  # cap at 60s
    
    while not shutdown_flag:
        try:
            # Hot model reloading
            try:
                current_mtime = os.path.getmtime(MODEL_PATH)
                if current_mtime > last_model_mtime:
                    logger.info("Model file changed on disk. Hot-reloading model...")
                    model = load_model(MODEL_PATH)
                    last_model_mtime = current_mtime
            except Exception as e:
                logger.error("Error during model hot-reload check: %s", e)

            scan_count += 1
            SCANS_TOTAL.inc()
            df = fetch_recent_metrics(conn, lookback_minutes=10)
            # Reset backoff on successful query
            reconnect_backoff = 1

            if df.empty:
                if scan_count % 5 == 0:
                    logger.info("Scan #%d: No new metrics to score", scan_count)
            else:
                with SCORING_LATENCY.time():
                    anomalies = score_metrics(model, df)
                WINDOWS_SCORED.inc(len(df))
                n = write_anomalies(conn, anomalies)

                if n > 0:
                    logger.warning("Scan #%d: detected %d anomalies!", scan_count, n)
                    for _, row in anomalies.iterrows():
                        ANOMALIES_DETECTED.labels(severity=row['severity']).inc()
                        logger.warning("%s | %s | %s | actual=₹%.2f vs expected=₹%.2f (score=%.3f)",
                                      row['severity'].upper(), row['category'],
                                      row['region'], row['total_revenue'],
                                      row['expected_revenue'], row['anomaly_score'])
                        if row["severity"] == "critical":
                            send_webhook_alert(row)
                else:
                    if scan_count % 5 == 0:
                        logger.info("Scan #%d: Scored %d windows – all normal", scan_count, len(df))

        except psycopg2.OperationalError as db_err:
            DB_RECONNECTS.labels(service="anomaly-detector").inc()
            logger.error("Lost DB connection, waiting %ds before retry: %s", reconnect_backoff, db_err)
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(reconnect_backoff)
            try:
                conn = get_single_connection()
                logger.info("Reconnected to database")
                reconnect_backoff = 1  # Reset on successful reconnect
            except psycopg2.OperationalError as reconnect_err:
                logger.error("Reconnection failed: %s", reconnect_err)
                # Exponential backoff with cap
                reconnect_backoff = min(reconnect_backoff * 2, max_backoff)
        except Exception as e:
            logger.error("Unexpected error: %s", e)

        time.sleep(SCAN_INTERVAL)
    
    # Cleanup on shutdown
    logger.info("Closing database connection")
    try:
        conn.close()
    except Exception:
        pass
    logger.info("Anomaly detection service stopped after %d scans", scan_count)


if __name__ == "__main__":
    main()
