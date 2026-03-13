# Disaster Recovery Guide

This document outlines procedures to recover from various failure scenarios in the Nexus platform.

---

## Table of Contents

1. [PostgreSQL Database Backup & Restore](#postgresql-backup--restore)
2. [Spark Checkpoint Recovery](#spark-checkpoint-recovery)
3. [ML Model Rollback](#ml-model-rollback)
4. [Multi-Service Outage Recovery](#multi-service-outage-recovery)
5. [Data Validation After Recovery](#data-validation-after-recovery)

---

## PostgreSQL Backup & Restore

### Regular Backups (Recommended Daily)

#### Create a backup:
```bash
# Backup the full database
docker compose exec postgres pg_dump -U nexus -d nexus > backup_$(date +%Y%m%d_%H%M%S).sql

# Or use compressed format (recommended)
docker compose exec postgres pg_dump -U nexus -d nexus | gzip > backup_$(date +%Y%m%d_%H%M%S).sql.gz

# List available backups
ls -lh backup_*.sql*
```

#### Restore from backup:
```bash
# Stop services that depend on the database
docker compose stop spark-processor anomaly-detector ai-copilot

# Restore from SQL dump
docker compose exec -T postgres psql -U nexus -d nexus < backup_20250308_120000.sql

# Or from compressed dump
gunzip -c backup_20250308_120000.sql.gz | docker compose exec -T postgres psql -U nexus -d nexus

# Restart services
docker compose up -d spark-processor anomaly-detector ai-copilot

# Verify restored data
docker compose exec postgres psql -U nexus -d nexus -c "SELECT COUNT(*) FROM order_events; SELECT COUNT(*) FROM anomalies;"
```

### Volume-Based Backup

The `postgres_data` volume contains the database files. You can back it up directly:

```bash
# Backup PostgreSQL data volume
docker run --rm -v nexus_postgres_data:/data -v $(pwd):/backup alpine tar czf /backup/postgres_data_backup.tar.gz /data

# Restore from volume backup
docker compose stop postgres
docker run --rm -v nexus_postgres_data:/data -v $(pwd):/backup alpine tar xzf /backup/postgres_data_backup.tar.gz -C /
docker compose up -d postgres
```

### Point-in-Time Recovery (PITR)

For production systems, enable WAL archiving:

1. Create a WAL archive directory:
```bash
mkdir -p /var/lib/nexus/wal_archive
chmod 700 /var/lib/nexus/wal_archive
```

2. Update PostgreSQL configuration to archive WALs:
```
# In docker-compose.yml, add volume:
- /var/lib/nexus/wal_archive:/var/lib/postgresql/wal_archive

# Then configure archive_command in postgres init scripts
```

3. To recover to a specific point in time:
```bash
# Restore up to timestamp
docker compose exec postgres psql -U nexus -d nexus -c "
  SELECT pg_wal_replay_pause();
  -- Manually specify recovery_target_time
"
```

---

## Spark Checkpoint Recovery

### Understanding Checkpoints

Spark maintains checkpoints in `/opt/spark-checkpoints` for exactly-once processing semantics:
- `raw_events/` — Raw event writes offset tracking
- `revenue_metrics/` — Aggregated metrics state
- `feature_store/` — Feature computation state

### Automatic Recovery

If Spark restarts, it automatically reads checkpoints:
```bash
# Logs will show:
# [INFO] Attempting to recover state from checkpoint...
# [INFO] Applying committed batch 1234
```

### Manual Checkpoint Management

#### Inspect checkpoint state:
```bash
# List checkpoint directories
docker compose exec spark-processor ls -la /opt/spark-checkpoints/

# Check current Kafka offset
docker compose exec spark-processor cat /opt/spark-checkpoints/raw_events/commits/0
```

#### Reset checkpoints (restart from beginning):
```bash
# WARNING: This will re-process all historical data!
docker compose stop spark-processor
docker volume rm nexus_spark_checkpoints  # or delete files manually
docker compose up -d spark-processor
```

#### Verify data consistency:
```bash
# Check that ingested events match expected count
docker compose exec postgres psql -U nexus -d nexus -c "
  SELECT COUNT(*) as total_events FROM order_events;
  SELECT MAX(window_start) as latest_window FROM revenue_metrics;
"
```

---

## ML Model Rollback

### Model Versioning

Models are saved with timestamps:
- `model.json` — Current (active) model
- `model_v20250308_120000.json` — Timestamped backup
- `metadata.json` — Feature encoding and metrics
- `metadata_v20250308_120000.json` — Timestamped metadata

### Rollback Procedure

If anomaly detection starts producing false positives:

```bash
# 1. Stop anomaly detector
docker compose stop anomaly-detector

# 2. Restore previous model version
docker compose exec -T ml_models bash -c "
  cp /app/model/model_v20250307_180000.json /app/model/model.json && \
  cp /app/model/metadata_v20250307_180000.json /app/model/metadata.json
"

# 3. Restart anomaly detector
docker compose up -d anomaly-detector

# 4. Monitor logs
docker compose logs -f anomaly-detector
```

### Retraining After Rollback

If you identified a problem with the training data:

```bash
# 1. Stop all ML services
docker compose stop anomaly-detector ai-copilot

# 2. Delete corrupted model
docker volume rm nexus_ml_models  # or manually delete model directory

# 3. Regenerate training data (optional filtering)
docker compose exec ml_models python generate_training_data.py

# 4. Retrain model
docker compose exec ml_models python train_model.py

# 5. Restart services
docker compose up -d anomaly-detector ai-copilot
```

---

## Multi-Service Outage Recovery

### Full Stack Recovery (All Services Down)

#### 1. Start infrastructure only:
```bash
docker compose up -d zookeeper kafka postgres postgres-init
docker compose logs -f postgres  # Wait for "database system is ready"
```

#### 2. Verify data integrity:
```bash
docker compose exec postgres psql -U nexus -d nexus -c "
  SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;
"
```

#### 3. Start streaming services:
```bash
docker compose up -d producer spark-processor
docker compose logs -f spark-processor  # Monitor startup
```

#### 4. Start ML & Copilot:
```bash
docker compose up -d ollama ollama-init
docker compose logs -f ollama-init  # Wait for "Pulling..."
sleep 30  # Give Ollama time to fully start
docker compose up -d anomaly-detector ai-copilot
```

#### 5. Start dashboard:
```bash
docker compose up -d dashboard
# Access at http://localhost:8501
```

### Database Corruption Recovery

If you suspect database corruption:

```bash
# 1. Check database integrity
docker compose exec postgres psql -U nexus -d nexus -c "
  REINDEX DATABASE nexus;
"

# 2. Vacuum and analyze
docker compose exec postgres psql -U nexus -d nexus -c "
  VACUUM ANALYZE;
"

# 3. If still broken, restore from backup (see PostgreSQL section above)
```

### Kafka Message Loss Recovery

If Kafka topic data was lost:

```bash
# Check offsets
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group nexus --describe

# Reset to earliest (replay all history)
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group nexus --reset-offsets --to-earliest --execute

# Then restart Spark streaming
docker compose restart spark-processor
```

---

## Data Validation After Recovery

### Quick Health Check
```bash
#!/bin/bash
docker compose exec postgres psycopg2 -U nexus -d nexus << EOF
-- Check row counts
SELECT 'order_events' as table_name, COUNT(*) as rows FROM order_events
UNION ALL
SELECT 'revenue_metrics', COUNT(*) FROM revenue_metrics
UNION ALL
SELECT 'anomalies', COUNT(*) FROM anomalies
UNION ALL
SELECT 'copilot_reports', COUNT(*) FROM copilot_reports;

-- Check for recent data
SELECT 'Latest order event' as check_type, MAX(event_timestamp)::TEXT FROM order_events
UNION ALL
SELECT 'Latest anomaly', MAX(detected_at)::TEXT FROM anomalies
UNION ALL
SELECT 'Latest report', MAX(created_at)::TEXT FROM copilot_reports;

-- Check for orphaned rows (data integrity)
SELECT COUNT(*) as orphaned_reports FROM copilot_reports
WHERE anomaly_id NOT IN (SELECT id FROM anomalies);
EOF
```

### Detailed Validation

```bash
# Check for data gaps (missing hours)
docker compose exec postgres psql -U nexus -d nexus -c "
  SELECT DATE_TRUNC('hour', window_start) as hour, COUNT(*) as windows
  FROM revenue_metrics
  GROUP BY 1
  ORDER BY 1 DESC
  LIMIT 24;
"

# Check anomaly detection freshness
docker compose exec postgres psql -U nexus -d nexus -c "
  SELECT 
    COUNT(*) as total_anomalies,
    COUNT(CASE WHEN status = 'open' THEN 1 END) as open,
    COUNT(CASE WHEN status = 'acknowledged' THEN 1 END) as acknowledged,
    MAX(detected_at) as latest_detection
  FROM anomalies
  WHERE detected_at > NOW() - INTERVAL '24 hours';
"
```

---

## Runbook Summary

| Scenario | Steps | ETA |
|----------|-------|-----|
| **DB disk full** | Delete old order_events → VACUUM | 5 min |
| **Spark lag** | Check CHECKPOINT_DIR permissions, restart | 2 min |
| **Model bad** | Rollback to previous version → restart | 1 min  |
| **All down** | UP infrastructure → UP streaming → UP ML → UP UI | 10 min |
| **Data lost** | Restore from backup → Validate → Restart | 15 min |

---

## Prevention Checklist

- [ ] Daily automated backups to external storage
- [ ] Monitoring for disk space on postgres_data volume
- [ ] Alerts for Spark lag (lag > 5 minutes)
- [ ] Model performance tracking (F1 score < 0.85)
- [ ] Regular dry-run disaster recovery exercises
- [ ] Documentation updates after each incident

