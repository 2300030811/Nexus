# Nexus Architecture — System Design Deep Dive

Nexus is a real-time retail intelligence platform designed to detect revenue loss and autonomously propose operational fixes. This document outlines the technical design decisions and data flow.

## 1. High-Level Architecture

The system follows a microservices architecture coordinated via a dual-network Docker Compose setup (data, backend, frontend).

### Core Pipeline Flow
1. **Ingestion**: `kafka-producer` generates synthetic retail events (JSON) and pushes them to Kafka.
2. **Stream Processing**: `spark-processor` consumes from Kafka, writes raw events to PostgreSQL, and aggregates 5-minute revenue windows.
3. **Storage**: PostgreSQL 16 serves as the centralized data warehouse and feature store.
4. **Intelligence (ML)**: `anomaly-detector` runs periodically, scoring revenue windows using a pre-trained XGBoost model.
5. **Investigation (AI)**: `ai-copilot` (LangChain) monitors the anomalies table. When it finds a new anomaly, it gathers data using SQL tools and generates a report via Llama 3 (Ollama).
6. **Frontend**: `dashboard` (Streamlit) provides real-time visibility and control.

## 2. Key Engineering Decisions

### Real-Time Quality vs. Throughput
Spark Structured Streaming is configured with **Watermarking** (2-minute delay) to handle late-arriving events gracefully without losing state. We use **Structured Streaming** over traditional Spark Streaming for its robust SQL-like API and better checkpointing.

### Feature Store & ML Loop
To avoid cold-start issues, `ml_models/generate_training_data.py` creates a balanced synthetic history for initial training. The system includes a **Drift Monitor** that measures Population Stability Index (PSI) to detect when incoming data patterns diverge from training data — a critical feature for production ML systems.

### AI Investigation (Two-Phase ReAct)
Instead of a single "black box" prompt, the AI Copilot uses a two-phase approach:
1. **Gathering**: Parallel execution of SQL tools to get category trends, regional health, and product volume.
2. **Reasoning**: Feeding the gathered context into Llama 3 with strict output constraints for structured reports.

### Fault Tolerance & Scalability
- **Circuit Breakers**: Used in `ai-copilot` to prevent cascading failures if Ollama is overloaded.
- **Dead Letter Queue (DLQ)**: The Kafka producer implements a DLQ for events that fail to publish.
- **PostgreSQL Partitioning**: The `order_events` table is partitioned by month to ensure query performance remains stable as data grows to millions of rows.

## 3. Data Schema

| Table | Purpose | Strategy |
|-------|---------|----------|
| `order_events` | Raw event audit trail | Range-partitioned by month |
| `revenue_metrics` | Windowed aggregations | Indexed by window and dimensions |
| `feature_store` | Multi-window signals | High-frequency updates from Spark |
| `anomalies` | Detected deviations | Write-once, read-heavy for Copilot |
| `copilot_reports` | AI Reasoning logic | Structured TEXT for dashboard |

## 4. Latency Characteristics

- **Ingestion to DB**: ~2-4 seconds
- **Aggregation Window**: 5 minutes
- **ML Detection Loop**: 60 seconds
- **AI Investigation**: ~15-30 seconds (hardware dependent)
- **Dashboard Refresh**: 5 seconds

---

For setup instructions, see [README.md](../README.md).
