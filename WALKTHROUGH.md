# Nexus: 5-Minute Walkthrough

Welcome to Nexus, a real-time anomaly detection and operational intelligence platform! This walkthrough is designed for your portfolio or technical interview deep dives, demonstrating end-to-end incident management, MLOps, and architecture scaling.

## 1. Fast Startup
Assuming you have Docker and Docker Compose installed:
```bash
docker compose up --build -d
```
All 6 core services (API, Dashboard, Spark Processor, Kafka Producer, Anomaly Detector, AI Copilot) and infrastructure (Postgres, Prometheus, Grafana, Kafka) will spin up. 

Open the [Streamlit Dashboard](http://localhost:8501) and [Grafana](http://localhost:3000) (if configured) in your browser.

## 2. The Architecture at a Glance
- **Kafka & Spark Streaming:** High-throughput streaming data ingestion and aggregation to calculate running revenue metrics.
- **ML & AI-Copilot:** Real-time anomaly detection using XGBoost, coupled with an AI investigator that drafts root-cause reports instantly.
- **Observability:** Prometheus exposes RED metrics and deep business KPIs, with alerts defining our Operational SLOs.
- **Governance:** Production model retraining features explicit F1 gating, preventing degraded models from deploying. 

## 3. Experience the Incident Loop
A core feature of Nexus is identifying disruptions before customers complain. To demo this, run our incident simulator:

```bash
python scripts/simulate_incident.py
```
**What happens behind the scenes?**
1. **Outage:** The simulation flags the DB, dropping "Electronics" revenue in "North America".
2. **Detection:** The ML Model detects this deviation from the historical baselines.
3. **Copilot Analysis:** The AI Copilot kicks in, using LLMs to read the anomaly payload and drafting an actionable root cause report.
4. **Resolution:** The user clears the simulation and resolves the anomaly on the dashboard.

## 4. MLOps: Retraining & Governance
One of the hardest parts of MLOps is the feedback loop. 
- In Nexus, if you mark an anomaly as a **False Positive** on the Dashboard, our training pipeline excludes it. The model learns *not* to make that mistake again without degrading real anomaly signal.
- **Auto-Retraining:** Over time, data distribution drifts. We monitor this via `drift_monitor.py`. A cron job can automatically spawn retraining when drift is severe:
```bash
python ml_models/drift_monitor.py --auto-retrain
```
- **Promotion Gates:** The `retrain_production_model.py` compares the candidate model's F1-score against production. If it fails, it saves the candidate without promoting it, prioritizing stability.
- **Rollbacks:** Sometimes mistakes happen. A model rollback is a single command away:
```bash
python ml_models/rollback_model.py --timestamp <VERSION>
```

## 5. Deployment Readiness
Ready for Kubernetes or Cloud run? We have `docker-compose.prod.yml` and `docker-compose.staging.yml` demonstrating:
- **Replica Scaling:** The API and Kafka processors run with multiple isolated replicas.
- **Secret Management:** Database passwords and Grafana credentials are kept via Docker native `/run/secrets/`.

---

*Thank you for exploring Nexus! Want to see the code? Check out `docs/architecture/incident_flow.md` to see the Mermaid sequence diagrams mapping this experience.*
