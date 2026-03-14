# Prerequisites & Requirements

Complete list of prerequisites, dependencies, and system requirements needed to build and run the Nexus platform.

---

## System Requirements

| Resource       | Minimum        | Recommended     |
|----------------|----------------|-----------------|
| RAM            | 8 GB           | 16 GB           |
| Disk Space     | 15 GB          | 25 GB           |
| CPU Cores      | 4              | 8               |
| OS             | Windows 10/11, macOS 12+, or Linux (Ubuntu 20.04+) |

> Ollama (Llama 3) and Spark are the most resource-intensive services. Allocate at least 8 GB to the Docker engine.

---

## Required Software

| Software         | Minimum Version | Purpose                              |
|------------------|-----------------|--------------------------------------|
| Docker Engine    | 20.10+          | Container runtime for all services   |
| Docker Compose   | 2.0+ (V2)      | Multi-container orchestration        |

No other software needs to be installed on the host. All languages, libraries, and tools run inside containers.

### Verify Installation

```bash
docker --version
docker compose version
```

---

## Network & Ports

The following host ports must be available (not in use by other applications):

| Port    | Service        | Protocol |
|---------|----------------|----------|
| 8501    | Streamlit Dashboard | HTTP |
| 9092    | Kafka (external listener) | TCP |
| 5432    | PostgreSQL     | TCP      |
| 11434   | Ollama API     | HTTP     |

> Internal service-to-service communication uses Docker's bridge network. Only the ports above are exposed to the host.

---

## Docker Images Pulled at Build Time

| Image                              | Size (approx.) | Service              |
|------------------------------------|-----------------|----------------------|
| `python:3.11-slim`                 | ~150 MB         | Producer, ML, Copilot, Dashboard |
| `bitnami/spark:3.5.0`             | ~1.2 GB         | Spark Structured Streaming |
| `confluentinc/cp-zookeeper:7.5.0` | ~800 MB         | Zookeeper            |
| `confluentinc/cp-kafka:7.5.0`     | ~800 MB         | Kafka broker + init  |
| `postgres:16-alpine`              | ~80 MB          | PostgreSQL data warehouse |
| `ollama/ollama:latest`            | ~1.5 GB         | Local LLM server     |

Additionally, Ollama pulls the **Llama 3** model (~4.7 GB) on first startup via the `ollama-init` service.

---

## Python Dependencies (per service)

All Python packages are installed inside their respective containers. Listed here for reference.

### Kafka Producer

| Package          | Version  |
|------------------|----------|
| kafka-python     | 2.0.2    |

### Spark Streaming

| Package          | Version  |
|------------------|----------|
| pyspark          | 3.5.0    |

Spark also downloads these JARs at runtime via `--packages`:
- `org.apache.spark:spark-sql-kafka` (Kafka connector)
- `org.postgresql:postgresql` (JDBC driver)

### Anomaly Detector (ML Models)

| Package          | Version  |
|------------------|----------|
| xgboost          | 2.0.3    |
| pandas           | 2.1.4    |
| numpy            | 1.26.3   |
| scikit-learn     | 1.4.0    |
| psycopg2-binary  | 2.9.9    |

### AI Copilot

| Package          | Version  |
|------------------|----------|
| langchain        | >= 0.3.0 |
| langchain-ollama | >= 0.2.0 |
| langchain-core   | >= 0.3.0 |
| langgraph        | >= 0.2.0 |
| psycopg2-binary  | 2.9.9    |

### Dashboard

| Package          | Version  |
|------------------|----------|
| streamlit        | 1.40.0   |
| pandas           | 2.1.4    |
| psycopg2-binary  | 2.9.9    |

---

## Infrastructure Services

| Service           | Technology               | Version  | Role                                      |
|-------------------|--------------------------|----------|--------------------------------------------|
| Zookeeper         | Confluent CP Zookeeper   | 7.5.0    | Kafka cluster coordination                 |
| Kafka             | Confluent CP Kafka       | 7.5.0    | Event streaming / message broker           |
| PostgreSQL        | PostgreSQL Alpine        | 16       | Data warehouse (5 tables, 9 indexes)       |
| Spark             | Bitnami Spark            | 3.5.0    | Stream processing (Kafka to PostgreSQL)    |
| Ollama            | Ollama                   | latest   | Local LLM inference server                 |

---

## External Services & API Keys

**None.** The platform is fully self-contained. The LLM (Llama 3) runs locally via Ollama. No cloud accounts, API keys, or external service subscriptions are required.

---

## Environment Variables

All services require a `.env` file to run properly (each has `env_file: ../.env` defined in `docker-compose.yml`). You MUST copy the provided `.env.example` to `.env` before starting the services.

| Variable            | Value            | Used By                    |
|---------------------|------------------|----------------------------|
| `POSTGRES_DB`       | `nexus`          | PostgreSQL                 |
| `POSTGRES_USER`     | `nexus`          | PostgreSQL                 |
| `POSTGRES_PASSWORD` | `nexus_password` | PostgreSQL                 |
| `KAFKA_BROKER`      | `kafka:29092`    | Producer, Spark            |
| `OLLAMA_HOST`       | `ollama:11434`   | AI Copilot                 |
| `SCAN_INTERVAL`     | `60`             | Anomaly Detector (seconds) |
| `COPILOT_INTERVAL`  | `90`             | AI Copilot (seconds)       |

---

## Quick Checklist

Before running `docker compose up --build`, confirm:

- [ ] Docker Engine is installed and running
- [ ] Docker Compose V2 is available (`docker compose version`)
- [ ] At least 8 GB RAM allocated to Docker (check Docker Desktop settings)
- [ ] Ports 8501, 9092, 5432, and 11434 are free
- [ ] Sufficient disk space (~15 GB for images + Llama 3 model)
- [ ] Stable internet connection (first build downloads images and the Llama 3 model)
