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
    from common.auth import get_current_user, RequireRead, RequireWrite, RequireAdmin, verify_api_key_legacy, TokenData
except ImportError:
    from rate_limit import RateLimitMiddleware
    try:
        from common.auth import get_current_user, RequireRead, RequireWrite, RequireAdmin, verify_api_key_legacy, TokenData
    except ImportError:
        import sys
        sys.path.append('..')
        from common.auth import get_current_user, RequireRead, RequireWrite, RequireAdmin, verify_api_key_legacy, TokenData

from common.db_utils import close_connection_pool, get_connection_pool, get_db_config
from common.logging_utils import get_logger
from common.metrics import (
    start_metrics_server, API_REQUESTS_TOTAL, API_REQUEST_LATENCY,
    AUTH_LOGIN_ATTEMPTS, AUTH_TOKEN_ISSUED, AUTH_TOKEN_VALIDATION,
    SYSTEM_UPTIME, API_ACTIVE_CONNECTIONS
)
from common.optimizations import (
    timed, smart_cache, OptimizedDatabasePool, QueryOptimizer,
    PerformanceMiddleware, get_performance_report
)

logger = get_logger("nexus.api")

# In-memory caches with 30s TTL to reduce DB pressure on dashboard refresh
kpi_cache = TTLCache(maxsize=10, ttl=30)
metrics_cache = TTLCache(maxsize=10, ttl=30)

# Pydantic Schemas
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

class SystemHealthResponse(BaseModel):
    epm: float
    latency: float
    last_pred: datetime | None
    kafka_connected: bool
    kafka_stalled: bool

class TrendEntry(BaseModel):
    window_start: datetime
    total_rev: float

class CategoryRevenueEntry(BaseModel):
    category: str
    rev: float

class RegionalRevenueEntry(BaseModel):
    region: str
    rev: float

class SimulationModeResponse(BaseModel):
    simulate_stockout: bool

class ModelHealthResponse(BaseModel):
    available: bool
    measured_at: datetime | None = None
    anomaly_rate: float | None = None
    avg_score: float | None = None
    psi_revenue: float | None = None
    drift_flag: bool | None = None
    notes: str | None = None

class DLQStatsResponse(BaseModel):
    total: int = 0
    pending: int = 0
    exhausted: int = 0
    oldest: datetime | None = None

class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str
    expires_in: int
    user_info: dict

class RefreshTokenRequest(BaseModel):
    refresh_token: str

class UserCreateRequest(BaseModel):
    username: str
    role: str = "viewer"

class UserInfo(BaseModel):
    id: int
    username: str
    role: str
    permissions: list[str]
    active: bool
    created_at: datetime

# Database & Connection Pooling
_pool: OptimizedDatabasePool | None = None

def get_optimized_conn():
    global _pool
    if _pool is None:
        db_config = get_db_config()
        _pool = OptimizedDatabasePool(minconn=5, maxconn=25, **db_config)
    return _pool.get_connection()

def return_optimized_conn(conn):
    global _pool
    if _pool:
        _pool.return_connection(conn)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    import time
    
    start_time = time.time()
    
    env = os.getenv("ENV", "development").strip().lower()
    if env not in {"development", "local", "test"} and not os.getenv("API_KEY", "").strip():
        raise RuntimeError("API_KEY must be configured when ENV is not development/local/test")
    
    db_config = get_db_config()
    _pool = OptimizedDatabasePool(minconn=5, maxconn=25, **db_config)
    logger.info("Optimized database pool initialized")
    
    metrics_port = int(os.getenv("METRICS_PORT", "9090"))
    start_metrics_server(metrics_port)
    logger.info("Metrics server started", extra={"port": metrics_port})
    
    SYSTEM_UPTIME.labels(service="api").set(0)
    
    yield
    
    if _pool:
        _pool = None
    close_connection_pool()
    logger.info("Optimized database pool closed")

app = FastAPI(
    title="Nexus Platform API",
    description="Real-time retail intelligence API providing anomalies, AI reports, and business KPIs.",
    version="1.0.0",
    lifespan=lifespan,
    default_response_class=ORJSONResponse,
    docs_url="/docs",
    redoc_url="/redoc",
)

# API Versioning
v1_router = APIRouter(prefix="/api/v1")
legacy_router = APIRouter(prefix="/api")

# Middleware
@app.middleware("http")
async def add_correlation_id(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    response: Response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    return response

@app.middleware("http")
async def collect_metrics(request: Request, call_next):
    import time
    
    start_time = time.time()
    method = request.method
    path = request.url.path
    
    API_ACTIVE_CONNECTIONS.inc()
    
    try:
        response: Response = await call_next(request)
        
        status_code = str(response.status_code)
        duration = time.time() - start_time
        
        endpoint = path
        for segment in path.split('/'):
            if segment.isdigit():
                endpoint = endpoint.replace(segment, "{id}")
        
        API_REQUESTS_TOTAL.labels(
            method=method,
            endpoint=endpoint,
            status_code=status_code
        ).inc()
        
        API_REQUEST_LATENCY.labels(
            method=method,
            endpoint=endpoint
        ).observe(duration)
        
        return response
        
    except Exception as e:
        API_REQUESTS_TOTAL.labels(
            method=method,
            endpoint=path,
            status_code="500"
        ).inc()
        
        duration = time.time() - start_time
        API_REQUEST_LATENCY.labels(
            method=method,
            endpoint=path
        ).observe(duration)
        
        raise
        
    finally:
        API_ACTIVE_CONNECTIONS.dec()

@app.middleware("http")
async def legacy_route_deprecation(request: Request, call_next):
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
    allow_headers=["Content-Type", "Authorization", "X-Correlation-ID", "X-API-Key"],
)
app.add_middleware(RateLimitMiddleware, requests_per_minute=60)
app.add_middleware(PerformanceMiddleware)


def get_conn():
    if not _pool:
        raise HTTPException(status_code=500, detail="Database pool not initialized")
    conn = _pool.get_connection()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.return_connection(conn)


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
    status: str | None = Query(default=None, pattern="^(open|acknowledged|false_positive|resolved)$"),
    current_user: TokenData = Depends(RequireRead),
    conn=Depends(get_conn),
):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            count_query = "SELECT COUNT(*) FROM anomalies"
            if status:
                count_query += " WHERE status = %s"
                cur.execute(count_query, (status,))
            else:
                cur.execute(count_query)
            total = cur.fetchone()["count"]

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
    current_user: TokenData = Depends(RequireRead),
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
    current_user: TokenData = Depends(RequireRead),
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


@v1_router.get("/metrics/health", response_model=SystemHealthResponse, tags=["Business"])
def get_system_health(conn=Depends(get_conn), current_user: TokenData = Depends(RequireRead)):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) as count,
                    MAX(ingested_at) as last_ingested
                FROM order_events
                WHERE ingested_at >= NOW() - INTERVAL '5 minutes'
            """)
            epm_check = cur.fetchone()
            event_count = int(epm_check["count"]) if epm_check else 0
            epm = round(event_count / 5.0, 1)
            last_ingested = epm_check["last_ingested"] if epm_check else None

            cur.execute("""
                SELECT AVG(EXTRACT(EPOCH FROM (ingested_at - event_timestamp))) as lat 
                FROM (SELECT ingested_at, event_timestamp FROM order_events ORDER BY ingested_at DESC LIMIT 100) s
            """)
            lat_row = cur.fetchone()
            latency = round(float(lat_row["lat"]) if lat_row and lat_row["lat"] else 0.0, 2)

            cur.execute("SELECT MAX(detected_at) as last FROM anomalies")
            last_pred_row = cur.fetchone()
            last_pred = last_pred_row["last"] if last_pred_row else None

            kafka_connected = event_count > 0
            kafka_stalled = False
            if last_ingested is not None:
                age_seconds = (datetime.now() - last_ingested.replace(tzinfo=None)).total_seconds()
                kafka_stalled = age_seconds > 120
                kafka_connected = age_seconds <= 600

            return {
                "epm": epm,
                "latency": latency,
                "last_pred": last_pred,
                "kafka_connected": kafka_connected,
                "kafka_stalled": kafka_stalled,
            }
    except Exception as e:
        logger.error("get_system_health error: %s", e)
        raise HTTPException(status_code=500, detail="Health check failed")


@v1_router.get("/metrics/revenue_trend", response_model=list[TrendEntry], tags=["Business"])
def get_revenue_trend(minutes: int = Query(default=30, ge=1, le=1440), conn=Depends(get_conn), current_user: TokenData = Depends(RequireRead)):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT window_start, SUM(total_revenue) as total_rev
                FROM revenue_metrics
                WHERE window_start >= NOW() - (%s * INTERVAL '1 minute')
                GROUP BY window_start
                ORDER BY window_start
            """, (minutes,))
            rows = cur.fetchall()
            if rows:
                return [dict(r) for r in rows]

            cur.execute("""
                SELECT
                    date_bin(INTERVAL '5 minutes', event_timestamp, TIMESTAMP '2001-01-01') as window_start,
                    SUM(total_amount) as total_rev
                FROM order_events
                WHERE event_timestamp >= NOW() - (%s * INTERVAL '1 minute')
                GROUP BY 1
                ORDER BY 1
            """, (minutes,))
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.error("get_revenue_trend error: %s", e)
        raise HTTPException(status_code=500, detail="Trend query failed")


@v1_router.get("/metrics/category_revenue", response_model=list[CategoryRevenueEntry], tags=["Business"])
def get_category_revenue(minutes: int = Query(default=30, ge=1, le=1440), conn=Depends(get_conn), current_user: TokenData = Depends(RequireRead)):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT category, SUM(total_revenue) as rev 
                FROM revenue_metrics 
                WHERE window_start >= NOW() - (%s * INTERVAL '1 minute') 
                GROUP BY category ORDER BY rev DESC
            """, (minutes,))
            rows = cur.fetchall()
            if rows:
                return [dict(r) for r in rows]
            
            cur.execute("""
                SELECT category, SUM(total_amount) as rev 
                FROM order_events 
                WHERE event_timestamp >= NOW() - (%s * INTERVAL '1 minute') 
                GROUP BY category ORDER BY rev DESC
            """, (minutes,))
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.error("get_category_revenue error: %s", e)
        raise HTTPException(status_code=500, detail="Category query failed")


@v1_router.get("/metrics/regional_revenue", response_model=list[RegionalRevenueEntry], tags=["Business"])
def get_regional_revenue(minutes: int = Query(default=30, ge=1, le=1440), conn=Depends(get_conn), current_user: TokenData = Depends(RequireRead)):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT region, SUM(total_revenue) as rev 
                FROM revenue_metrics 
                WHERE window_start >= NOW() - (%s * INTERVAL '1 minute') 
                GROUP BY region ORDER BY rev DESC
            """, (minutes,))
            rows = cur.fetchall()
            if rows:
                return [dict(r) for r in rows]
            
            cur.execute("""
                SELECT region, SUM(total_amount) as rev 
                FROM order_events 
                WHERE event_timestamp >= NOW() - (%s * INTERVAL '1 minute') 
                GROUP BY region ORDER BY rev DESC
            """, (minutes,))
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.error("get_regional_revenue error: %s", e)
        raise HTTPException(status_code=500, detail="Regional query failed")


# Include routers
app.include_router(v1_router)
app.include_router(legacy_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
