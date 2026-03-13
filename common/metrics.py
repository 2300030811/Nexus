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

SIMULATION_MODE = Gauge(
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
# Shared DB Metrics
# ---------------------------------------------------------------------------

DB_RECONNECTS = Counter(
    "nexus_db_reconnects_total",
    "Total database reconnection attempts",
    ["service"],
)
