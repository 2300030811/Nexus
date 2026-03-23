#!/usr/bin/env python3
"""
Nexus - Full Incident Loop Demo
Simulates an outage, detects anomalies, generates a report, and guides the user 
through the resolution and ML governance steps.
"""

import time
import requests
import sys
import os

# Add parent directory to path for common imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.logging_utils import get_logger

logger = get_logger("simulate_incident")

API_URL = "http://localhost:8000/api/v1"
API_KEY = "dev_testing_key_123"
HEADERS = {"X-API-Key": API_KEY}

def print_step(title, desc):
    """Print step with logging."""
    logger.info("Demo step", extra={"step_title": title, "step_description": desc})
    print(f"\n{'='*60}")
    print(f"🚀 STEP: {title}")
    print(desc)
    print(f"{'='*60}\n")
    time.sleep(1)

def main():
    logger.info("Starting incident simulation demo")
    print_step("Incident Demo Start", "Initializing the full Nexus incident loop demo.")
    
    # 1. Simulate Outage
    print_step("Simulate Outage", "Triggering a massive stockout event in 'Electronics' category across 'North America'.")
    
    try:
        res = requests.post(f"{API_URL}/config/simulation_mode", headers=HEADERS, json={"simulate_stockout": True})
        if res.status_code != 200:
            logger.error("Failed to start simulation", extra={
                "status_code": res.status_code,
                "response": res.text
            })
            print(f"Error starting simulation: {res.text}")
            return
        
        logger.info("Simulation mode enabled successfully")
        print("✅ Simulation mode enabled. System is now generating anomalous data.")
        
    except Exception as e:
        logger.error("Exception starting simulation", extra={"error": str(e)})
        print(f"Error starting simulation: {e}")
        return
    
    # 2. Wait for Processing
    print_step("Metric Aggregation & Detection", "Waiting for Spark streaming to aggregate windows and ML model to flag the anomaly (approx. 30 seconds)...")
    
    for i in range(30, 0, -1):
        print(f"Wait... {i}s", end='\r')
        time.sleep(1)
    
    print("\n")  # New line after countdown
    
    # 3. Detect Anomaly
    print_step("Check Anomalies", "Querying API for newly detected anomalies...")
    
    try:
        res = requests.get(f"{API_URL}/anomalies?limit=5", headers=HEADERS)
        anomalies = res.json().get("items", [])
        
        logger.info("Checked for anomalies", extra={"anomaly_count": len(anomalies)})
        
        anomaly = None
        for a in anomalies:
            if a["status"] == "open":
                anomaly = a
                break
        
        if not anomaly:
            logger.warning("No anomalies detected", extra={"checked_count": len(anomalies)})
            print("❌ No anomalies detected yet. The batch might be delayed. Please check the dashboard manually.")
            return
        
        logger.info("Anomaly detected", extra={
            "anomaly_id": anomaly["id"],
            "category": anomaly["category"],
            "region": anomaly["region"],
            "severity": anomaly["severity"],
            "score": anomaly["anomaly_score"]
        })
        
        print(f"✅ Anomaly Detected (ID: {anomaly['id']} | {anomaly['category']} in {anomaly['region']})")
        print(f"   Severity: {anomaly['severity']} | Score: {anomaly['anomaly_score']:.3f}")
        
    except Exception as e:
        logger.error("Error checking anomalies", extra={"error": str(e)})
        print(f"Error checking anomalies: {e}")
        return
    
    # 4. Copilot Report
    print_step("AI Copilot Investigation", "Calling AI Copilot to investigate root cause...")
    time.sleep(3)
    
    try:
        res = requests.get(f"{API_URL}/reports?limit=5", headers=HEADERS)
        reports = res.json().get("items", [])
        
        logger.info("Checked for copilot reports", extra={"report_count": len(reports)})
        
        report = next((r for r in reports if r["anomaly_id"] == anomaly["id"]), None)
        if not report:
            logger.warning("No copilot report found", extra={"anomaly_id": anomaly["id"]})
            print("⏳ Copilot is still generating the report. You can view it on the dashboard soon.")
        else:
            logger.info("Copilot report ready", extra={
                "anomaly_id": anomaly["id"],
                "confidence": report["confidence"],
                "root_cause": report["root_cause"]
            })
            print(f"✅ Copilot Report Ready (Confidence: {report['confidence']:.2f})")
            print(f"   Root Cause: {report['root_cause']}")
            print(f"   Recommended Action: {report['recommended_action']}")
    
    except Exception as e:
        logger.error("Error checking copilot reports", extra={"error": str(e)})
        print(f"Error checking copilot reports: {e}")

    # 5. Review & Remediation
    print_step("Human Review & Remediation", "Disabling simulation mode and turning off the outage.")
    
    try:
        res = requests.post(f"{API_URL}/config/simulation_mode", headers=HEADERS, json={"simulate_stockout": False})
        if res.status_code == 200:
            logger.info("Simulation mode disabled successfully")
            print("✅ Outage resolved.")
        else:
            logger.warning("Failed to disable simulation", extra={"status_code": res.status_code})
            print("⚠️ Failed to disable simulation mode.")
    except Exception as e:
        logger.error("Error disabling simulation", extra={"error": str(e)})
        print(f"Error disabling simulation: {e}")
    
    logger.info("Incident simulation completed", extra={
        "anomaly_id": anomaly["id"] if anomaly else None,
        "next_steps": "dashboard_review"
    })
    
    print("\nNext steps in your workflow:")
    print("1. Go to http://localhost:8501 (Dashboard)")
    print(f"2. Locate Anomaly #{anomaly['id']}")
    print("3. Mark as 'Acknowledged' or 'Resolved' (Confirmed Anomaly)")
    print("4. Or mark as 'False Positive' to prevent future retraining on this pattern.")
    print("5. Run `python ml_models/retrain_production_model.py` to upgrade the model.")
    print("6. Run `python ml_models/drift_monitor.py` to check for data drift.")

if __name__ == "__main__":
    main()
