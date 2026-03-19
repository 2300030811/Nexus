import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import psycopg2
import psycopg2.pool
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, ORJSONResponse
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel
from cachetools import TTLCache

try:
    from api_service.rate_limit import RateLimitMiddleware
    from api_service.auth import verify_api_key
except ImportError:
    from rate_limit import RateLimitMiddleware
    from auth import verify_api_key

from common.db_utils import close_connection_pool, get_connection_pool, get_db_config
from common.logging_utils import get_logger

logger = get_logger("nexus.api")

# In-memory caches with 30s TTL to reduce DB pressure on dashboard refresh
kpi_cache = TTLCache(maxsize=10, ttl=30)
metrics_cache = TTLCache(maxsize=10, ttl=30)

# ---------------------------------------------------------------------------
# Pydantic Schemas (Service Contracts)
# ---------------------------------------------------------------------------
class AnomalyResponse(BaseModel):
    id: int
    detected_at: datetime
    window_start: datetime
    window_end: datetime
    category: str
    region: str
    actual_revenue: float
    expected_revenue: float
    anomaly_score: float
    severity: str
    status: str

class ReportResponse(BaseModel):
    id: int
    anomaly_id: int
    created_at: datetime
    severity: str
    category: str
    region: str
    actual_revenue: float | None
    expected_revenue: float | None
    confidence: float
    estimated_loss: float
    root_cause: str
    recommended_action: str

class PaginatedResponse(BaseModel):
    items: list[Any]
    total: int
    limit: int
    offset: int

class KPIResponse(BaseModel):
    lookback_minutes: int
    orders: int
    revenue: float
    open_anomalies: int
    total_reports: int

class MetricsSummaryResponse(BaseModel):
    critical_24h: int
    high_24h: int
    open_count: int
    last_detected: datetime | None

# ---------------------------------------------------------------------------
# Database & Lifespan
# ---------------------------------------------------------------------------
_pool: psycopg2.pool.ThreadedConnectionPool | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage connection pool lifecycle."""
    global _pool
    env = os.getenv("ENV", "development").strip().lower()
    if env not in {"development", "local", "test"} and not os.getenv("API_KEY", "").strip():
        raise RuntimeError("API_KEY must be configured when ENV is not development/local/test")
    _pool = get_connection_pool(minconn=2, maxconn=20)
    logger.info("Database pool initialized via common utilities")
    yield
    close_connection_pool()
    logger.info("Database pool closed")

app = FastAPI(
    title="Nexus Platform API",
    description="Real-time retail intelligence API providing anomalies, AI reports, and business KPIs.",
    version="1.0.0",
    lifespan=lifespan,
    default_response_class=ORJSONResponse,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ---------------------------------------------------------------------------
# API Versioning
# ---------------------------------------------------------------------------
v1_router = APIRouter(prefix="/api/v1")
# Temporary compatibility router until all clients migrate to /api/v1.
legacy_router = APIRouter(prefix="/api")

# ---------------------------------------------------------------------------
# Middleware (Production Readiness)
# ---------------------------------------------------------------------------
@app.middleware("http")
async def add_correlation_id(request: Request, call_next):
    """Assign a unique ID to every request for distributed tracing."""
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    response: Response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    return response


@app.middleware("http")
async def legacy_route_deprecation(request: Request, call_next):
    """Warn callers on the unversioned /api/* path to migrate to /api/v1/*."""
    response: Response = await call_next(request)
    path = request.url.path
    if path.startswith("/api/") and not path.startswith("/api/v1/"):
        versioned = path.replace("/api/", "/api/v1/", 1)
        response.headers["Deprecation"] = "true"
        response.headers["Link"] = f'<{versioned}>; rel="successor-version"'
        response.headers["Sunset"] = "Sat, 01 Aug 2026 00:00:00 GMT"
    return response

CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:8501").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "Authorization", "X-Correlation-ID"],
)
app.add_middleware(RateLimitMiddleware, requests_per_minute=60)


def get_conn():
    """Dependency: borrow a connection from the pool, always return it clean."""
    if not _pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    conn = _pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


@app.get("/health", tags=["System"])
def health_check(conn=Depends(get_conn)):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return {"status": "ok", "database": "reachable"}
    except Exception as e:
        logger.error("Health check DB error: %s", e)
        raise HTTPException(status_code=503, detail="Database unreachable") from e


@v1_router.get("/anomalies", response_model=PaginatedResponse, tags=["Core"])
@legacy_router.get("/anomalies", response_model=PaginatedResponse, tags=["Core"])
def get_anomalies(
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    status: str | None = Query(default=None, pattern="^(open|acknowledged)$"),
    _: None = Depends(verify_api_key),
    conn=Depends(get_conn),
):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get total count
            count_query = "SELECT COUNT(*) FROM anomalies"
            if status:
                count_query += " WHERE status = %s"
                cur.execute(count_query, (status,))
            else:
                cur.execute(count_query)
            total = cur.fetchone()["count"]

            # Get paginated items
            if status:
                cur.execute(
                    "SELECT * FROM anomalies WHERE status = %s "
                    "ORDER BY detected_at DESC LIMIT %s OFFSET %s",
                    (status, limit, offset),
                )
            else:
                cur.execute(
                    "SELECT * FROM anomalies ORDER BY detected_at DESC LIMIT %s OFFSET %s",
                    (limit, offset),
                )
            items = [dict(r) for r in cur.fetchall()]

            return {
                "items": items,
                "total": total,
                "limit": limit,
                "offset": offset
            }
    except Exception as e:
        logger.error("get_anomalies error: %s", e)
        raise HTTPException(status_code=500, detail="Query failed") from e


@v1_router.get("/reports", response_model=PaginatedResponse, tags=["Core"])
@legacy_router.get("/reports", response_model=PaginatedResponse, tags=["Core"])
def get_reports(
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    _: None = Depends(verify_api_key),
    conn=Depends(get_conn),
):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) FROM copilot_reports")
            total = cur.fetchone()["count"]

            cur.execute(
                "SELECT * FROM copilot_reports ORDER BY created_at DESC LIMIT %s OFFSET %s",
                (limit, offset),
            )
            items = [dict(r) for r in cur.fetchall()]

            return {
                "items": items,
                "total": total,
                "limit": limit,
                "offset": offset
            }
    except Exception as e:
        logger.error("get_reports error: %s", e)
        raise HTTPException(status_code=500, detail="Query failed") from e


@v1_router.get("/kpis", response_model=KPIResponse, tags=["Business"])
@legacy_router.get("/kpis", response_model=KPIResponse, tags=["Business"])
def get_kpis(
    minutes: int = Query(default=30, ge=1, le=1440),
    _: None = Depends(verify_api_key),
    conn=Depends(get_conn),
):
    memo_key = f"kpis_{minutes}"
    if memo_key in kpi_cache:
        return kpi_cache[memo_key]

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    (SELECT COALESCE(SUM(order_count), 0)  FROM revenue_metrics
                     WHERE window_end >= NOW() - (%s * INTERVAL '1 minute')) AS orders,
                    (SELECT COALESCE(SUM(total_revenue), 0) FROM revenue_metrics
                     WHERE window_end >= NOW() - (%s * INTERVAL '1 minute')) AS revenue,
                    (SELECT COUNT(*) FROM anomalies WHERE status = 'open')   AS open_anomalies,
                    (SELECT COUNT(*) FROM copilot_reports)                   AS total_reports
            """, (minutes, minutes))
            row = cur.fetchone()
            orders, revenue, open_anom, total_reports = row

        res = {
            "lookback_minutes": minutes,
            "orders": int(orders),
            "revenue": float(revenue),
            "open_anomalies": int(open_anom),
            "total_reports": int(total_reports),
        }
        kpi_cache[memo_key] = res
        return res
    except Exception as e:
        logger.error("get_kpis error: %s", e)
        raise HTTPException(status_code=500, detail="Query failed") from e


@v1_router.get("/metrics/summary", response_model=MetricsSummaryResponse, tags=["Business"])
@legacy_router.get("/metrics/summary", response_model=MetricsSummaryResponse, tags=["Business"])
def get_metrics_summary(_: None = Depends(verify_api_key), conn=Depends(get_conn)):
    """Aggregated summary for the dashboard header cards."""
    if "metrics_summary" in metrics_cache:
        return metrics_cache["metrics_summary"]

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

        res = {
            "critical_24h":  row[0] or 0,
            "high_24h":      row[1] or 0,
            "open_count":    row[2] or 0,
            "last_detected": row[3] if row[3] else None,
        }
        metrics_cache["metrics_summary"] = res
        return res
    except Exception as e:
        logger.error("get_metrics_summary error: %s", e)
        raise HTTPException(status_code=500, detail="Query failed") from e


@v1_router.get("/anomalies/stream", tags=["Core"])
@legacy_router.get("/anomalies/stream", tags=["Core"])
async def stream_anomalies(request: Request, _: None = Depends(verify_api_key)):
    """
    Real-time anomaly stream via Server-Sent Events (SSE).
    Differentiator: Demonstrates knowledge of real-time push architectures.
    """
    async def event_generator():
        last_id = 0
        while True:
            if await request.is_disconnected():
                break
            try:
                loop = asyncio.get_event_loop()
                new_anomalies = await loop.run_in_executor(
                    None, _fetch_new_anomalies, last_id
                )
                for anom in new_anomalies:
                    last_id = anom["id"]
                    for k, v in anom.items():
                        if isinstance(v, datetime):
                            anom[k] = v.isoformat()
                    yield f"event: anomaly\nid: {anom['id']}\ndata: {json.dumps(dict(anom))}\n\n"
            except Exception as e:
                logger.error("Stream error: %s", e)
            await asyncio.sleep(5)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


def _fetch_new_anomalies(last_id: int) -> list[dict]:
    """Sync DB fetch using the pool — runs in executor."""
    if not _pool:
        return []
    conn = _pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM anomalies WHERE id > %s ORDER BY id ASC LIMIT 50",
                (last_id,),
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        _pool.putconn(conn)


app.include_router(v1_router)
app.include_router(legacy_router)


