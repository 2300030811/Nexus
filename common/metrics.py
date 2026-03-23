"""
Prometheus metrics utilities for the Nexus platform.

Provides a lightweight HTTP server to expose /metrics endpoint
and pre-defined metric collectors for each service.
"""

import threading
from prometheus_client import (
    Counter, Histogram, Gauge, Info,
    start_http_server, CollectorRegistry, REGISTRY,
)

# Default metrics port (each service overrides via env var)
DEFAULT_METRICS_PORT = 9090


def start_metrics_server(port: int = DEFAULT_METRICS_PORT) -> None:
    """Start the Prometheus metrics HTTP server in a daemon thread."""
    start_http_server(port)


# ---------------------------------------------------------------------------
# Producer Metrics
# ---------------------------------------------------------------------------

EVENTS_PRODUCED = Counter(
    "nexus_events_produced_total",
    "Total number of events produced to Kafka",
    ["topic"],
)

PRODUCE_ERRORS = Counter(
    "nexus_produce_errors_total",
    "Total number of Kafka produce errors",
)

PRODUCE_LATENCY = Histogram(
    "nexus_produce_latency_seconds",
    "Time to produce a single event to Kafka",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
)

SIMULATION_ACTIVE = Gauge(
    "nexus_simulation_mode",
    "Whether stockout simulation is active (1=active, 0=off)",
)

# ---------------------------------------------------------------------------
# Anomaly Detector Metrics
# ---------------------------------------------------------------------------

SCANS_TOTAL = Counter(
    "nexus_anomaly_scans_total",
    "Total number of anomaly detection scans",
)

ANOMALIES_DETECTED = Counter(
    "nexus_anomalies_detected_total",
    "Total anomalies detected",
    ["severity"],
)

SCORING_LATENCY = Histogram(
    "nexus_scoring_latency_seconds",
    "Time to score a batch of metrics",
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)

WINDOWS_SCORED = Counter(
    "nexus_windows_scored_total",
    "Total metric windows scored by the ML model",
)

MODEL_DRIFT_DETECTED_FLAG = Gauge(
    "nexus_model_drift_detected_flag",
    "Whether model drift is currently detected (1=yes, 0=no)",
)

# ---------------------------------------------------------------------------
# AI Copilot Metrics
# ---------------------------------------------------------------------------

INVESTIGATIONS_TOTAL = Counter(
    "nexus_investigations_total",
    "Total anomaly investigations by AI copilot",
)

INVESTIGATION_ERRORS = Counter(
    "nexus_investigation_errors_total",
    "Total failed investigations",
)

LLM_RESPONSE_TIME = Histogram(
    "nexus_llm_response_seconds",
    "Time for LLM to generate investigation report",
    buckets=[1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)

REPORTS_SAVED = Counter(
    "nexus_reports_saved_total",
    "Total copilot reports saved",
)

# ---------------------------------------------------------------------------
# Dead Letter Queue Metrics
# ---------------------------------------------------------------------------

DLQ_PENDING = Gauge(
    "nexus_dlq_pending_events",
    "Number of unresolved events in the dead letter queue",
)

DLQ_EXHAUSTED = Counter(
    "nexus_dlq_exhausted_total",
    "Total events that exceeded max retries in DLQ",
)

# ---------------------------------------------------------------------------
# Spark Streaming Metrics
# ---------------------------------------------------------------------------

SPARK_BATCH_DURATION = Histogram(
    "nexus_spark_batch_processing_seconds",
    "Time to process a foreachBatch in Spark",
    ["sink"],
    buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0],
)

SPARK_RECORDS_PROCESSED = Counter(
    "nexus_spark_records_processed_total",
    "Total records written to sinks by Spark",
    ["sink"],
)

SPARK_PROCESSING_LAG = Gauge(
    "nexus_spark_processing_lag_seconds",
    "Lag between event generation and processing completion",
)

DATA_FRESHNESS = Gauge(
    "nexus_data_freshness_seconds",
    "Time since the last event was processed",
)

# ---------------------------------------------------------------------------
# Shared DB Metrics
# ---------------------------------------------------------------------------

DB_RECONNECTS = Counter(
    "nexus_db_reconnects_total",
    "Total database reconnection attempts",
    ["service"],
)

# ---------------------------------------------------------------------------
# API Service Metrics
# ---------------------------------------------------------------------------

API_REQUESTS_TOTAL = Counter(
    "nexus_api_requests_total",
    "Total API requests received",
    ["method", "endpoint", "status_code"],
)

API_REQUEST_LATENCY = Histogram(
    "nexus_api_request_duration_seconds",
    "API request processing time",
    ["method", "endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
)

API_ACTIVE_CONNECTIONS = Gauge(
    "nexus_api_active_connections",
    "Number of active API connections",
)

API_CACHE_HITS = Counter(
    "nexus_api_cache_hits_total",
    "Total cache hits",
    ["cache_type"],
)

API_CACHE_MISSES = Counter(
    "nexus_api_cache_misses_total",
    "Total cache misses",
    ["cache_type"],
)

# ---------------------------------------------------------------------------
# Authentication Metrics
# ---------------------------------------------------------------------------

AUTH_LOGIN_ATTEMPTS = Counter(
    "nexus_auth_login_attempts_total",
    "Total login attempts",
    ["result"],  # success, failure
)

AUTH_TOKEN_ISSUED = Counter(
    "nexus_auth_tokens_issued_total",
    "Total JWT tokens issued",
    ["user_role"],
)

AUTH_TOKEN_VALIDATION = Counter(
    "nexus_auth_token_validations_total",
    "Total token validations",
    ["result"],  # success, expired, invalid
)

AUTH_API_KEY_USAGE = Counter(
    "nexus_auth_api_key_usage_total",
    "Total API key authentications",
    ["result"],
)

# ---------------------------------------------------------------------------
# System Health Metrics
# ---------------------------------------------------------------------------

SYSTEM_UPTIME = Gauge(
    "nexus_system_uptime_seconds",
    "System uptime in seconds",
    ["service"],
)

MEMORY_USAGE = Gauge(
    "nexus_memory_usage_bytes",
    "Memory usage in bytes",
    ["service", "type"],  # type: rss, vms, shared
)

CPU_USAGE = Gauge(
    "nexus_cpu_usage_percent",
    "CPU usage percentage",
    ["service"],
)

DISK_USAGE = Gauge(
    "nexus_disk_usage_bytes",
    "Disk usage in bytes",
    ["service", "mount_point"],
)

# ---------------------------------------------------------------------------
# Business Metrics
# ---------------------------------------------------------------------------

BUSINESS_REVENUE_TOTAL = Gauge(
    "nexus_business_revenue_total",
    "Total revenue processed",
    ["category", "region"],
)

BUSINESS_ORDERS_TOTAL = Counter(
    "nexus_business_orders_total",
    "Total orders processed",
    ["category", "region"],
)

BUSINESS_ANOMALY_RATE = Gauge(
    "nexus_business_anomaly_rate",
    "Current anomaly detection rate",
    ["severity"],
)

BUSINESS_ALERTS_TOTAL = Counter(
    "nexus_business_alerts_total",
    "Total business alerts generated",
    ["alert_type", "severity"],
)
