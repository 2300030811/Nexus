# Scaling Guide

This document describes how to scale the Nexus platform to handle higher event volumes, more anomalies, and larger ML datasets.

---

## Current Baseline

**Tested throughput:**
- ~2 events/second (120 events/min) from Kafka producer
- 5-minute windowed aggregation in Spark (288 windows/day per category-region pair)
- XGBoost model scoring every 60 seconds
- Copilot investigation every 90 seconds

**Resource Usage:**
- Producer: 100MB RAM, < 0.01 CPU
- Spark Streaming: 2-4 GB RAM (configured)
- PostgreSQL: 1-2 GB RAM (configured)
- Ollama LLM: 4-8 GB RAM (configured)
- Dashboard: 500MB RAM

---

## Scaling Strategies

### 1. **Scale Up: Event Ingestion (10x volume)**

**Goal:** Handle 20 events/second (1,200 events/min)

#### Producer Changes
```bash
# Increase EVENTS_PER_SECOND in .env
EVENTS_PER_SECOND=20

# Restart producer
docker compose restart producer

# Monitor:
docker compose logs -f producer | grep SENT
```

#### Kafka Changes
```yaml
# docker-compose.yml - increase partitions for parallelism
kafka-init command:
  kafka-topics --create --if-not-exists 
    --bootstrap-server kafka:29092 
    --partitions 6              # Increase from 3
    --replication-factor 1 
    --topic order_events
```

#### Producer Threading (advanced)
For multi-threaded producer (requires code changes):
```python
# In producer.py, use ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=10) as executor:
    for event in events:
        executor.submit(producer.send, KAFKA_TOPIC, value=event)
```

---

### 2. **Scale Out: Spark Cluster (Distributed Processing)**

**Goal:** Process 100+ events/second with horizontal scaling

#### Current Limitation
- `--master local[*]` runs on a single machine
- Single worker bottleneck at ~10 events/sec on typical hardware

#### Upgrade to Spark Cluster Mode

##### Option A: Docker Swarm
```bash
# Initialize swarm
docker swarm init

# Deploy Spark cluster manifest
docker stack deploy -c spark-compose.yml nexus-spark

# Scale Spark workers
docker service scale nexus-spark_spark-worker=3
```

##### Option B: Kubernetes (Production)
```bash
# Use Spark-on-Kubernetes operator
helm install spark spark-operator/spark-operator \
  --namespace spark-operator --create-namespace

# Submit job to cluster
spark-submit \
  --master k8s://https://k8s-cluster:6443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.namespace=spark-operator \
  stream_processor.py
```

##### Option C: Standalone Cluster
```bash
# Start master
docker run --rm -it -p 7077:7077 -p 8080:8080 \
  bitnami/spark:3.5.0 spark-class org.apache.spark.deploy.master.Master

# Start workers
docker run --rm -it -p 8081:8081 \
  bitnami/spark:3.5.0 spark-class org.apache.spark.deploy.worker.Worker \
  spark://master-ip:7077
```

#### Update stream_processor.py
```python
# Change from local to cluster
CMD ["spark-submit",
     "--master", "spark://spark-master:7077",  # Cluster URL
     "--deploy-mode", "client",
     "--executor-cores", "4",
     "--executor-memory", "2G",
     "--num-executors", "2",
     "stream_processor.py"]
```

---

### 3. **Scale Out: PostgreSQL (Database Replication)**

**Goal:** Handle 100k+ rows/second writes and high query load

#### Read Replicas (Recommended First Step)

##### Setup Primary-Replica Replication
```sql
-- On PRIMARY (production database)
CREATE USER replication_user REPLICATION ENCRYPTED PASSWORD 'repl_pass';

-- Configure postgresql.conf
wal_level = replica
max_wal_senders = 3
wal_keep_size = 1GB
```

##### Setup Replica
```bash
# Connect replica and perform base backup
docker exec postgres pg_basebackup -h primary_host -U replication_user \
  -D /var/lib/postgresql/data -P -v

# Start replica in recovery mode (reads only)
docker compose up -d postgres-replica
```

##### Route Reads to Replica
```python
# In Python services, open separate connections
read_conn = psycopg2.connect(host="postgres-replica", ...)  # Replica (read-only)
write_conn = psycopg2.connect(host="postgres", ...)  # Primary (read/write)
```

#### Sharding (For Extreme Scale: 1M+ TPS)

If replication isn't enough, shard by region:

```python
# In producer.py, route to region-specific database
def get_db_for_region(region):
    shards = {
        "North-India": "postgres-east",
        "South-India": "postgres-south",
        "West-India": "postgres-west",
        "East-India": "postgres-east2",
        "Central-India": "postgres-central",
    }
    return shards.get(region, "postgres")

# Write to region's database
conn = get_connection(host=get_db_for_region(event["region"]))
```

---

### 4. **Scale ML Pipeline (Faster Predictions)**

**Goal:** Score 1000+ anomalies/minute (from 60/min)

#### Batch Prediction
```python
# Current: Score 1 window at a time every 60s
# New: Batch score 100+ windows, run every 10s

def score_metrics_batch(model, df: pd.DataFrame) -> pd.DataFrame:
    """Vectorized scoring for all rows at once."""
    if df.empty:
        return df
    
    # Use model.predict_proba() on entire batch (vectorized)
    X = df[FEATURE_COLUMNS]
    df["anomaly_score"] = model.predict_proba(X)[:, 1]
    df["anomaly_pred"] = model.predict(X)
    
    # Return only anomalies
    return df[df["anomaly_pred"] == 1]

# In detect_anomalies.py:
# Change SCAN_INTERVAL from 60s to 10s for more frequent checks
```

#### GPU Acceleration (XGBoost)
```python
# Enable GPU for faster inference
model = xgb.XGBClassifier(
    n_estimators=200,
    tree_method="gpu_hist",  # GPU histogram computation
    gpu_id=0,
    max_depth=5,
    learning_rate=0.1,
    scale_pos_weight=scale_pos_weight,
)
```

#### Quantization (Faster Inference)
```python
# Quantize model for faster inference (tradeoff: slightly lower accuracy)
import onnx
import onnxruntime as rt

# Convert to ONNX format (portable, optimized)
onnx_model = convert_sklearn(model, initial_types=[('features', FloatTensorType([None, 9]))])

# Use ONNX Runtime (faster inference)
sess = rt.InferenceSession("model.onnx")
scores = sess.run(None, {"features": X.values.astype(np.float32)})
```

---

### 5. **Scale Copilot (Faster LLM Inference)**

**Goal:** Investigate 100+ anomalies/minute (from ~1 anomaly/1.5 min)

#### LLM Caching
```python
# Cache frequently asked query results
from functools import lru_cache

@lru_cache(maxsize=1000)
def get_revenue_by_category_cached(category, minutes):
    """Cache query - repeated anomalies for same category hit cache."""
    return get_revenue_by_category(category, minutes)
```

#### Parallel Investigation
```python
# Current: Investigate anomalies one-by-one
# New: Investigate 5-10 in parallel

from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [
        executor.submit(investigate_anomaly, agent, anomaly)
        for anomaly in anomalies
    ]
    for future in futures:
        report = future.result()
        save_report(conn, anomaly, report)
```

#### Model Quantization / Distillation
```bash
# Use a smaller, faster LLM model
# Instead of llama3 (13B params), use llama3-mini (7B)
# Or use Mistral-7B (faster, lighter)

# In .env
OLLAMA_MODEL=mistral:latest  # 6x faster than llama3
```

---

### 6. **Database Query Optimization**

**Bottleneck:** Copilot tools scan full tables every 90 seconds

#### Add Time-Windowed Indexes
```sql
-- In init.sql
CREATE INDEX idx_revenue_metrics_recent 
  ON revenue_metrics (window_start DESC) 
  WHERE window_start > NOW() - INTERVAL '1 day';

CREATE INDEX idx_order_events_recent 
  ON order_events (event_timestamp DESC) 
  WHERE event_timestamp > NOW() - INTERVAL '8 hours';

CREATE INDEX idx_anomalies_recent 
  ON anomalies (detected_at DESC) 
  WHERE detected_at > NOW() - INTERVAL '24 hours';
```

#### Materialized Views (Cache Computations)
```sql
-- Pre-compute expensive aggregations
CREATE MATERIALIZED VIEW mv_category_revenue_last_hour AS
SELECT 
  category, 
  SUM(total_revenue) as revenue_1h,
  COUNT(*) as order_count_1h,
  AVG(total_amount) as avg_order_value
FROM order_events
WHERE event_timestamp > NOW() - INTERVAL '1 hour'
GROUP BY category;

CREATE INDEX idx_mv_category ON mv_category_revenue_last_hour (category);

-- Refresh periodically (every 5 minutes)
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_category_revenue_last_hour;
```

---

### 7. **Resource Scaling**

#### Increase Container Resources
```yaml
# docker-compose.yml
deploy:
  resources:
    limits:
      cpus: "8"           # Increase from 4
      memory: 8G          # Increase from 4G
    reservations:
      cpus: "4"
      memory: 4G
```

#### Host Machine Scaling
- **Current minimum:** 8 GB RAM, 4 CPU cores
- **For 10x volume:** 32 GB RAM, 16 CPU cores, SSD storage
- **For 100x volume:** 64 GB RAM, 32 CPU cores, NVMe SSD

---

## Performance Benchmarking

### Before Scaling
```bash
# Measure baseline throughput
docker compose logs producer | grep SENT | tail -20
# Expected: ~2 events/sec

# Check Spark latency
docker compose logs spark-processor | grep "Batch processing"
# Expected: Batch completes in < 10 seconds
```

### After Scaling
```bash
# Monitor throughput
watch -n 1 'docker compose logs producer | tail -5'

# Monitor database query time
docker compose exec postgres psql -U nexus -d nexus -c "
  SELECT 
    mean_exec_time / 1000 as avg_ms,
    calls,
    query
  FROM pg_stat_statements
  ORDER BY mean_exec_time DESC
  LIMIT 10;
"

# Check Spark executors
docker compose exec spark-processor curl http://localhost:4040/api/v1/executors
```

---

## Scaling Checklist

| Component | 10x | 100x | 1000x |
|-----------|-----|------|-------|
| **Kafka** | 6 partitions | Multi-broker | Multi-cluster |
| **Spark** | Standalone cluster | Kubernetes | Managed service |
| **PostgreSQL** | Read replicas | Sharding | Cloud DB (RDS) |
| **ML** | Batch scoring | GPU | Distributed inference |
| **Copilot** | Parallel threads | Model distillation | Specialized LLM |
| **Host RAM** | 16 GB | 32 GB | 64+ GB |
| **Host CPU** | 8 cores | 16 cores | 32+ cores |

---

## Recommended Path Forward

1. **Phase 1 (10x):** Increase Kafka partitions, add Spark read replicas, optimize queries
2. **Phase 2 (100x):** Deploy Spark cluster, implement postgres sharding, use GPU for ML
3. **Phase 3 (1000x):** Kubernetes, multi-region deployment, specialized backend services

