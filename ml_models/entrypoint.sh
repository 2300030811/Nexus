#!/bin/bash
set -euo pipefail

echo "=== Nexus ML Pipeline ==="

# ── Wait for Postgres ──────────────────────────────────────────────────────
echo "[0/4] Waiting for database..."
until python -c "from common.db_utils import get_single_connection; c=get_single_connection(); c.close()" 2>/dev/null; do
    echo "  DB not ready, retrying in 5s..."
    sleep 5
done
echo "  Database is ready."

echo "[1/4] Generating training data ..."
python generate_training_data.py

echo "[2/4] Training anomaly detection model ..."
python train_model.py

echo "[3/4] Starting drift monitor in background ..."
# Capture PID so we can clean up on exit
_drift_pid=""
drift_loop() {
    while true; do
        echo "$(date) - Running drift monitor..."
        python drift_monitor.py >> /tmp/drift_monitor.log 2>&1 || \
            echo "$(date) - Drift monitor failed, see /tmp/drift_monitor.log"
        sleep 3600
    done
}
drift_loop &
_drift_pid=$!

# Ensure background process is cleaned up on exit
cleanup() {
    echo "Shutting down drift monitor (PID $_drift_pid)..."
    kill "$_drift_pid" 2>/dev/null || true
}
trap cleanup EXIT SIGTERM SIGINT

echo "[4/4] Starting anomaly detection service (foreground) ..."
exec python detect_anomalies.py
