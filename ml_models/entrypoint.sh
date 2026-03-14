#!/bin/bash
set -e

echo "=== Nexus ML Pipeline ==="

echo "[1/4] Generating training data ..."
python generate_training_data.py

echo "[2/4] Training anomaly detection model ..."
python train_model.py

echo "[3/4] Starting anomaly detection service ..."
# Run drift monitor hourly in background
while true; do
    echo "$(date) - Running drift monitor..."
    python drift_monitor.py >> /var/log/drift_monitor.log 2>&1 || echo "Drift monitor failed, see logs"
    sleep 3600
done &

python detect_anomalies.py
