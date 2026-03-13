from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from typing import List, Optional

from common.logging_utils import get_logger
from common.db_utils import get_db_config

logger = get_logger("nexus.api")

app = FastAPI(
    title="Nexus Platform API",
    description="API for the Nexus Autonomous Retail Monitoring Platform",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def _get_connection():
    try:
        cfg = get_db_config()
        return psycopg2.connect(**cfg)
    except psycopg2.Error as e:
        logger.error("API DB connection error: %s", e)
        raise HTTPException(status_code=503, detail="Database unavailable")

@app.get("/health")
def health_check():
    """Verify API and Database health."""
    try:
        conn = _get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail="Unhealthy")

@app.get("/api/anomalies")
def get_anomalies(limit: int = 10, status: Optional[str] = None):
    """Fetch recent anomalies."""
    conn = _get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = "SELECT * FROM anomalies"
            params = []
            if status:
                query += " WHERE status = %s"
                params.append(status)
            query += " ORDER BY detected_at DESC LIMIT %s"
            params.append(limit)
            cur.execute(query, tuple(params))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

@app.get("/api/reports")
def get_reports(limit: int = 10):
    """Fetch recent AI Copilot reports."""
    conn = _get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM copilot_reports ORDER BY created_at DESC LIMIT %s", (limit,))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

@app.get("/api/kpis")
def get_kpis(minutes: int = Query(30, description="Lookback window in minutes")):
    """Get high-level KPIs."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*), COALESCE(SUM(total_amount), 0) FROM order_events WHERE event_timestamp >= NOW() - INTERVAL '%s minutes'",
                (minutes,)
            )
            orders, revenue = cur.fetchone()
            
            cur.execute("SELECT COUNT(*) FROM anomalies WHERE status = 'open'")
            open_anom = cur.fetchone()[0]
            
            return {
                "lookback_minutes": minutes,
                "orders": orders,
                "revenue": float(revenue),
                "open_anomalies": open_anom
            }
    finally:
        conn.close()
