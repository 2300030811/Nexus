"""
Nexus – AI Copilot Agent

A LangChain ReAct agent powered by Llama 3 (via Ollama) that:
  1. Monitors the anomalies table for new 'open' anomalies
  2. Investigates each anomaly using SQL tools
  3. Generates a root-cause analysis and recommended action
  4. Writes the report to the copilot_reports table for the dashboard
"""

import os
import re
import signal
import sys
import time
import json
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
from psycopg2.extras import RealDictCursor

from langchain_ollama import ChatOllama
from circuit_breaker import CircuitBreaker

from tools import (
    query_revenue_by_category, query_revenue_by_region,
    query_product_order_volume, query_feature_snapshot,
    query_recent_order_trend, query_payment_method_breakdown,
)
from common.logging_utils import get_logger
from common.db_utils import get_connection_pool
from common.metrics import (
    INVESTIGATIONS_TOTAL, INVESTIGATION_ERRORS, LLM_RESPONSE_TIME,
    REPORTS_SAVED, DB_RECONNECTS, start_metrics_server,
)

# ---------------------------------------------------------------------------
# Structured logging (via shared utility)
# ---------------------------------------------------------------------------
logger = get_logger("nexus.copilot")

# ---------------------------------------------------------------------------
# Global state for graceful shutdown
# ---------------------------------------------------------------------------
shutdown_flag = False

def signal_handler(sig, frame):
    """Handle SIGTERM/SIGINT for graceful shutdown."""
    global shutdown_flag
    logger.info("Received signal %s, initiating graceful shutdown", sig)
    shutdown_flag = True

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "nexus")
PG_USER = os.getenv("PG_USER", "nexus")
PG_PASSWORD = os.getenv("PG_PASSWORD", "nexus_password")

SCAN_INTERVAL = int(os.getenv("COPILOT_INTERVAL", "90"))  # seconds

SYSTEM_PROMPT = """You are Nexus AI Copilot, an operations intelligence agent for a retail platform.

You will receive data about a revenue anomaly along with investigation data already gathered from the database. Your job is to analyze this data and produce a structured report.

RULES:
- Calculate Estimated Loss = Expected Revenue minus Actual Revenue. If actual > expected (a spike), loss is 0.
- Confidence is a decimal between 0.0 and 1.0 (e.g. 0.75). Higher means stronger evidence.
- Always provide a root cause or your top 3 hypotheses ranked by likelihood.
- Always provide a specific, actionable recommendation.
- Be concise. Do not repeat the raw data.

You MUST respond with EXACTLY this format and nothing else:

ANOMALY_REPORT_START
Anomaly ID: <id>
Severity: <severity>
Category: <category>
Region: <region>
Estimated Loss: <number only, no currency symbol>
Confidence: <decimal between 0.0 and 1.0>
Root Cause: <one paragraph>
Recommended Action: <specific actions>
ANOMALY_REPORT_END"""



# Removed ensure_reports_table - handled by init.sql


def fetch_uninvestigated_anomalies(conn) -> list[dict]:
    """Find open anomalies that the copilot hasn't reported on yet."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT a.*
            FROM anomalies a
            WHERE a.status = 'open'
              AND NOT EXISTS (
                  SELECT 1 FROM copilot_reports r WHERE r.anomaly_id = a.id
              )
            ORDER BY
                CASE a.severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 ELSE 3 END,
                a.detected_at DESC
            LIMIT 5;
        """)
        return [dict(row) for row in cur.fetchall()]


def parse_report(report_text: str, anomaly: dict | None = None) -> dict:
    """Extract structured fields from the agent's report using flexible regex.

    Falls back to anomaly data for estimated_loss and uses a heuristic for
    confidence when the LLM output doesn't match exactly.
    """
    root_cause = ""
    action = ""
    confidence = 0.0
    estimated_loss = 0.0
    confidence_found = False
    loss_found = False

    # --- Try structured block first (ANOMALY_REPORT_START ... END) ---
    block_match = re.search(
        r"ANOMALY_REPORT_START(.*?)ANOMALY_REPORT_END", report_text, re.DOTALL
    )
    text = block_match.group(1) if block_match else report_text

    # --- Confidence: flexible regex ---
    conf_m = re.search(
        r"[Cc]onfidence\s*[:=]\s*([\d]*\.?[\d]+)", text
    )
    if conf_m:
        confidence = float(conf_m.group(1))
        # Handle cases where LLM writes "75" instead of "0.75"
        if confidence > 1.0:
            confidence = confidence / 100.0
        confidence = max(0.0, min(confidence, 1.0))
        confidence_found = True

    # --- Estimated Loss: flexible regex ---
    loss_m = re.search(
        r"[Ee]stimated\s*[Ll]oss\s*[:=]\s*[₹$Rs.\s]*([-\d,]*\.?\d+)", text
    )
    if loss_m:
        try:
            estimated_loss = float(loss_m.group(1).replace(",", ""))
            loss_found = True
        except ValueError:
            pass

    # --- Root Cause: try several heading variants ---
    cause_m = re.search(
        r"[Rr]oot\s*[Cc]ause(?:\s*[Aa]nalysis)?\s*[:=]\s*(.+?)(?:\n\s*(?:[Rr]ecommended|ANOMALY_REPORT_END)|$)",
        text, re.DOTALL,
    )
    if cause_m:
        root_cause = re.sub(r"\s+", " ", cause_m.group(1)).strip()

    # --- Recommended Action ---
    action_m = re.search(
        r"[Rr]ecommended\s*[Aa]ction\s*[:=]\s*(.+?)(?:\n\s*ANOMALY_REPORT_END|$)",
        text, re.DOTALL,
    )
    if action_m:
        action = re.sub(r"\s+", " ", action_m.group(1)).strip()

    # --- Fallback: broad text extraction if structured sections not found ---
    if not root_cause:
        # Grab any substantial paragraph from the LLM response as the analysis
        paragraphs = [p.strip() for p in report_text.split("\n\n") if len(p.strip()) > 40]
        if paragraphs:
            root_cause = paragraphs[-1][:500]  # last substantial paragraph

    # --- Fallback from anomaly data ---
    if anomaly and not loss_found:
        actual = float(anomaly.get("actual_revenue", 0))
        expected = float(anomaly.get("expected_revenue", 0))
        estimated_loss = max(expected - actual, 0.0)
        loss_found = True
        logger.info("Estimated loss computed from anomaly data: %.2f", estimated_loss)

    if not confidence_found:
        # Heuristic: if the agent produced a non-trivial analysis, give baseline confidence
        if root_cause and len(root_cause) > 50:
            confidence = 0.55
        elif root_cause:
            confidence = 0.35
        else:
            confidence = 0.15
        logger.info("Confidence inferred heuristically: %.2f", confidence)

    return {
        "root_cause": root_cause.strip() or "Unable to determine root cause.",
        "recommended_action": action.strip() or "Manual investigation recommended.",
        "confidence": confidence,
        "estimated_loss": estimated_loss,
    }


def save_report(conn, anomaly: dict, report_text: str, parsed: dict) -> None:
    """Save the copilot's report and mark the anomaly as acknowledged."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO copilot_reports
                (anomaly_id, severity, category, region,
                 actual_revenue, expected_revenue,
                 confidence, estimated_loss,
                 root_cause, recommended_action, full_report)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            anomaly["id"], anomaly["severity"], anomaly["category"],
            anomaly["region"], float(anomaly["actual_revenue"]),
            float(anomaly["expected_revenue"]),
            parsed["confidence"], parsed["estimated_loss"],
            parsed["root_cause"], parsed["recommended_action"], report_text,
        ))

        cur.execute("""
            UPDATE anomalies SET status = 'acknowledged' WHERE id = %s;
        """, (anomaly["id"],))

    conn.commit()


def create_llm():
    """Build a ChatOllama LLM instance."""
    return ChatOllama(
        base_url=f"http://{OLLAMA_HOST}",
        model=OLLAMA_MODEL,
        temperature=0.1,
    )


def gather_investigation_data(anomaly: dict) -> str:
    """Phase 1: Gather all relevant data from the database in parallel."""
    category = anomaly["category"]
    region = anomaly["region"]
    
    # Define investigation tasks
    tasks = [
        ("Revenue by Category",  lambda: query_revenue_by_category(category)),
        ("Revenue by Region",    lambda: query_revenue_by_region(region)),
        ("Product Order Volume", lambda: query_product_order_volume(category)),
        ("Feature Snapshot",     lambda: query_feature_snapshot(category, region)),
        ("Recent Order Trend",   lambda: query_recent_order_trend(15)),
        ("Payment Breakdown",    lambda: query_payment_method_breakdown()),
    ]

    sections = []
    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        future_to_name = {executor.submit(func): name for name, func in tasks}
        # results are returned as they complete
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result = future.result()
                sections.append(f"### {name}\n{result}")
            except Exception as e:
                logger.error("Investigation task '%s' failed: %s", name, e)
                sections.append(f"### {name}\nData unavailable: {e}")

    return "\n\n".join(sections)


def investigate_anomaly(llm, breaker, anomaly: dict) -> str:
    """Two-phase investigation: gather data, then ask LLM to analyze."""
    actual = float(anomaly['actual_revenue'])
    expected = float(anomaly['expected_revenue'])
    loss = max(expected - actual, 0.0)
    pct = ((actual / expected) - 1) * 100 if expected > 0 else 0
    direction = "DROP" if pct < 0 else "SPIKE"

    # Phase 1: gather data
    investigation_data = gather_investigation_data(anomaly)
    logger.info("Gathered %d chars of investigation data for anomaly #%d",
                len(investigation_data), anomaly['id'])

    # Phase 2: ask LLM to analyze
    user_msg = (
        f"Analyze this anomaly and produce the ANOMALY_REPORT_START/END block.\n\n"
        f"ANOMALY DETAILS:\n"
        f"  Anomaly ID: {anomaly['id']}\n"
        f"  Severity: {anomaly['severity']}\n"
        f"  Category: {anomaly['category']}\n"
        f"  Region: {anomaly['region']}\n"
        f"  Actual Revenue: {actual:,.2f}\n"
        f"  Expected Revenue: {expected:,.2f}\n"
        f"  Direction: {direction} {abs(pct):.1f}%\n"
        f"  Pre-calculated Loss: {loss:,.2f}\n"
        f"  Anomaly Score: {float(anomaly['anomaly_score']):.3f}\n"
        f"  Detected At: {anomaly['detected_at']}\n\n"
        f"INVESTIGATION DATA:\n{investigation_data}\n\n"
        f"Now write the report. Remember: Estimated Loss and Confidence must be plain numbers."
    )

    try:
        response = breaker.call(llm.invoke, [
            ("system", SYSTEM_PROMPT),
            ("human", user_msg),
        ])
    except RuntimeError as breaker_open:
        logger.error("LLM unavailable: %s", breaker_open)
        return ""  # Skip this anomaly, retry next scan

    content = response.content if hasattr(response, "content") else str(response)
    if isinstance(content, list):
        content = "\n".join(str(c) for c in content)

    logger.info("Raw LLM output for anomaly #%d (len=%d): %.500s",
                anomaly["id"], len(content), content)
    return content


def wait_for_ollama() -> None:
    """Wait for Ollama to be ready and the model to be available."""
    url = f"http://{OLLAMA_HOST}/api/tags"
    for attempt in range(1, 31):
        try:
            resp = urllib.request.urlopen(url, timeout=5)
            data = json.loads(resp.read())
            models = [m["name"] for m in data.get("models", [])]
            if any(OLLAMA_MODEL in m for m in models):
                logger.info("Ollama ready, model '%s' available", OLLAMA_MODEL)
                return
            else:
                logger.warning("Ollama up but model '%s' not found (have: %s), retrying", OLLAMA_MODEL, models)
        except Exception:
            logger.warning("Ollama not ready (attempt %d/30), retrying in 10s", attempt)
        time.sleep(10)
    raise RuntimeError(f"Ollama not available at {url} after 30 attempts")


def main() -> None:
    logger.info("AI Copilot starting")

    # Start Prometheus metrics endpoint
    metrics_port = int(os.getenv("METRICS_PORT", "9093"))
    start_metrics_server(metrics_port)
    logger.info("Prometheus metrics available on port %d", metrics_port)

    wait_for_ollama()

    llm = create_llm()
    llm_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=300, name="ollama")
    logger.info("Copilot scanning for anomalies every %ds", SCAN_INTERVAL)

    scan_count = 0
    reconnect_backoff = 1  # seconds for exponential backoff
    max_backoff = 60
    pool = get_connection_pool(minconn=1, maxconn=5)
    
    while not shutdown_flag:
        try:
            scan_count += 1
            conn = pool.getconn()
            try:
                anomalies = fetch_uninvestigated_anomalies(conn)
            finally:
                pool.putconn(conn)
            
            reconnect_backoff = 1  # Reset on successful query

            if not anomalies:
                if scan_count % 5 == 0:
                    logger.info("Scan #%d: No new anomalies to investigate", scan_count)
            else:
                logger.info("Scan #%d: Found %d anomalies to investigate", scan_count, len(anomalies))

                def process_anomaly(anomaly):
                    worker_conn = pool.getconn()
                    try:
                        INVESTIGATIONS_TOTAL.inc()
                        with LLM_RESPONSE_TIME.time():
                            report = investigate_anomaly(llm, llm_breaker, anomaly)
                        if not report:
                            return
                        parsed = parse_report(report, anomaly)

                        save_report(worker_conn, anomaly, report, parsed)
                        REPORTS_SAVED.inc()
                        logger.info("Anomaly #%d – report saved | cause: %s | confidence: %.2f | loss: ₹%.2f",
                                   anomaly['id'], parsed['root_cause'][:120],
                                   parsed['confidence'], parsed['estimated_loss'])
                    except Exception as e:
                        INVESTIGATION_ERRORS.inc()
                        logger.error("Failed to investigate anomaly #%d: %s", anomaly['id'], e)
                    finally:
                        pool.putconn(worker_conn)

                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = [executor.submit(process_anomaly, anomaly) for anomaly in anomalies]
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            logger.error("Parallel investigation worker failed: %s", e)

        except psycopg2.pool.PoolError as pool_err:
            logger.error("Database pool exhausted: %s", pool_err)
        except psycopg2.OperationalError as db_err:
            DB_RECONNECTS.labels(service="ai-copilot").inc()
            logger.error("Database error, pool will handle reconnect: %s", db_err)
            time.sleep(reconnect_backoff)
        except Exception as e:
            logger.error("Unexpected error: %s", e)

        time.sleep(SCAN_INTERVAL)
    
    # Cleanup on shutdown
    logger.info("Closing database pool")
    from common.db_utils import close_connection_pool
    close_connection_pool()
    logger.info("AI Copilot stopped after %d scans", scan_count)


if __name__ == "__main__":
    main()
