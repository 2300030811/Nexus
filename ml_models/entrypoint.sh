#!/bin/bash
set -e

echo "=== Nexus ML Pipeline ==="

# Step 1: Generate synthetic training data
echo "[1/3] Generating training data ..."
python generate_training_data.py

# Step 2: Train the XGBoost model
echo "[2/3] Training anomaly detection model ..."
python train_model.py

# Step 3: Run the live anomaly detection loop
echo "[3/3] Starting anomaly detection service ..."
python detect_anomalies.py
