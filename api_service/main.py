from contextlib import asynccontextmanager
from typing import Optional
import os

import psycopg2
import psycopg2.pool
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.extras import RealDictCursor

from common.logging_utils import get_logger
from common.db_utils import get_db_config
from rate_limit import RateLimitMiddleware

logger = get_logger("nexus.api")

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage connection pool lifecycle."""
    global _pool
    cfg = get_db_config()
    _pool = psycopg2.pool.ThreadedConnectionPool(minconn=2, maxconn=20, **cfg)
    logger.info("Database pool created (min=2, max=20)")
    yield
    if _pool:
        _pool.closeall()
        logger.info("Database pool closed")

app = FastAPI(
    title="Nexus Platform API",
    version="1.0.0",
    lifespan=lifespan,
)

CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:8501").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "Authorization"],
)
app.add_middleware(RateLimitMiddleware, requests_per_minute=60)


def get_conn():
    """Dependency: borrow a connection from the pool."""
    if not _pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn)


@app.get("/health")
def health_check(conn=Depends(get_conn)):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return {"status": "ok", "database": "reachable"}
    except Exception as e:
        logger.error("Health check DB error: %s", e)
        raise HTTPException(status_code=503, detail="Database unreachable")


@app.get("/api/anomalies")
def get_anomalies(
    limit: int = Query(default=10, ge=1, le=100),
    status: Optional[str] = Query(default=None, pattern="^(open|acknowledged)$"),
    conn=Depends(get_conn),
):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if status:
                cur.execute(
                    "SELECT * FROM anomalies WHERE status = %s "
                    "ORDER BY detected_at DESC LIMIT %s",
                    (status, limit),
                )
            else:
                cur.execute(
                    "SELECT * FROM anomalies ORDER BY detected_at DESC LIMIT %s",
                    (limit,),
                )
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.error("get_anomalies error: %s", e)
        raise HTTPException(status_code=500, detail="Query failed")


@app.get("/api/reports")
def get_reports(
    limit: int = Query(default=10, ge=1, le=100),
    conn=Depends(get_conn),
):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM copilot_reports ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.error("get_reports error: %s", e)
        raise HTTPException(status_code=500, detail="Query failed")


@app.get("/api/kpis")
def get_kpis(
    minutes: int = Query(default=30, ge=1, le=1440),
    conn=Depends(get_conn),
):
    try:
        with conn.cursor() as cur:
            # OPTIMIZATION: Query pre-aggregated revenue_metrics instead of raw order_events
            cur.execute("""
                SELECT SUM(order_count), SUM(total_revenue)
                FROM revenue_metrics
                WHERE window_end >= NOW() - INTERVAL '%s minutes'
            """, (minutes,))
            orders, revenue = cur.fetchone()
            
            # Fallback if metrics are empty or interval is too short for any micro-batch to have finished
            orders = orders or 0
            revenue = float(revenue or 0.0)

            cur.execute("SELECT COUNT(*) FROM anomalies WHERE status = 'open'")
            open_anom = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM copilot_reports")
            total_reports = cur.fetchone()[0]

        return {
            "lookback_minutes": minutes,
            "orders": orders,
            "revenue": float(revenue),
            "open_anomalies": open_anom,
            "total_reports": total_reports,
        }
    except Exception as e:
        logger.error("get_kpis error: %s", e)
        raise HTTPException(status_code=500, detail="Query failed")


@app.get("/api/metrics/summary")
def get_metrics_summary(conn=Depends(get_conn)):
    """Aggregated summary for the dashboard header cards."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE severity = 'critical') AS critical_count,
                    COUNT(*) FILTER (WHERE severity = 'high')     AS high_count,
                    COUNT(*) FILTER (WHERE status  = 'open')      AS open_count,
                    MAX(detected_at)                               AS last_detected
                FROM anomalies
                WHERE detected_at >= NOW() - INTERVAL '24 hours'
            """)
            row = cur.fetchone()
        return {
            "critical_24h":  row[0],
            "high_24h":      row[1],
            "open_count":    row[2],
            "last_detected": row[3].isoformat() if row[3] else None,
        }
    except Exception as e:
        logger.error("get_metrics_summary error: %s", e)
        raise HTTPException(status_code=500, detail="Query failed")
