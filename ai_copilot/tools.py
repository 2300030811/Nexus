"""
Nexus – SQL Investigation Tools

Custom LangChain tools that the AI copilot agent uses to query the
PostgreSQL data warehouse.  Each tool is a focused, read-only query
that returns structured data the LLM can reason over.
"""

import os

import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor
from langchain_core.tools import tool

from common.logging_utils import get_logger
from common.db_utils import get_db_config
from common.cache import _cache

logger = get_logger("nexus.copilot.tools")

# ---------------------------------------------------------------------------
# Connection pool (thread-safe, enables concurrent queries)
# ---------------------------------------------------------------------------
_pool = None


def _get_pool():
    """Get or create a module-level connection pool."""
    global _pool
    if _pool is None:
        db_cfg = get_db_config()
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=5, **db_cfg
        )
        logger.info("Connection pool created (min=1, max=5)")
    return _pool


def _query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a read-only query using a pooled connection."""
    pool = _get_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = True
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
    except psycopg2.OperationalError:
        # Connection died — discard it, get a fresh one, and retry once
        pool.putconn(conn, close=True)
        conn = pool.getconn()
        conn.autocommit = True
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
    finally:
        try:
            pool.putconn(conn)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Tools exposed to the LangChain agent
# ---------------------------------------------------------------------------

@tool
def get_open_anomalies() -> str:
    """Retrieve all anomalies with status='open', ordered by severity and detection time.
    Use this tool FIRST to understand what anomalies need investigation."""
    rows = _query("""
        SELECT id, detected_at, window_start, window_end,
               category, region, actual_revenue, expected_revenue,
               anomaly_score, severity, status
        FROM anomalies
        WHERE status = 'open'
        ORDER BY
            CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 ELSE 3 END,
            detected_at DESC
        LIMIT 20;
    """)
    if not rows:
        return "No open anomalies found."
    lines = []
    for r in rows:
        pct = ((float(r["actual_revenue"]) / float(r["expected_revenue"])) - 1) * 100 if float(r["expected_revenue"]) > 0 else 0
        direction = "DROP" if pct < 0 else "SPIKE"
        lines.append(
            f"ID={r['id']} | {r['severity'].upper()} | {r['category']} / {r['region']} | "
            f"actual=₹{float(r['actual_revenue']):,.2f} vs expected=₹{float(r['expected_revenue']):,.2f} "
            f"({direction} {abs(pct):.1f}%) | score={float(r['anomaly_score']):.3f} | {r['detected_at']}"
        )
    return "\n".join(lines)


@tool
def get_revenue_by_category(category: str) -> str:
    """Get recent revenue metrics for a specific product category over the last 30 minutes.
    Use this to investigate whether a category's revenue is trending down."""
    rows = _query("""
        SELECT window_start, region, order_count, total_revenue, avg_order_value
        FROM revenue_metrics
        WHERE category = %s
          AND window_start >= NOW() - INTERVAL '30 minutes'
        ORDER BY window_start DESC;
    """, (category,))
    if not rows:
        return f"No recent revenue data found for category '{category}'."
    lines = [f"Revenue for '{category}' (last 30 min):"]
    for r in rows:
        lines.append(
            f"  {r['window_start']} | {r['region']} | "
            f"orders={r['order_count']} | revenue=₹{float(r['total_revenue']):,.2f} | "
            f"avg=₹{float(r['avg_order_value']):,.2f}"
        )
    return "\n".join(lines)


@tool
def get_revenue_by_region(region: str) -> str:
    """Get recent revenue metrics for a specific region over the last 30 minutes.
    Use this to check if a revenue drop is region-specific or global."""
    rows = _query("""
        SELECT window_start, category, order_count, total_revenue, avg_order_value
        FROM revenue_metrics
        WHERE region = %s
          AND window_start >= NOW() - INTERVAL '30 minutes'
        ORDER BY window_start DESC;
    """, (region,))
    if not rows:
        return f"No recent revenue data found for region '{region}'."
    lines = [f"Revenue for '{region}' (last 30 min):"]
    for r in rows:
        lines.append(
            f"  {r['window_start']} | {r['category']} | "
            f"orders={r['order_count']} | revenue=₹{float(r['total_revenue']):,.2f} | "
            f"avg=₹{float(r['avg_order_value']):,.2f}"
        )
    return "\n".join(lines)


@tool
def get_product_order_volume(category: str) -> str:
    """Get order counts per product in a category over the last 30 minutes.
    Use this to identify if a specific product has stopped selling (potential stockout)."""
    rows = _query("""
        SELECT product_id, product_name,
               COUNT(*) as order_count,
               SUM(quantity) as total_units,
               SUM(total_amount) as total_revenue
        FROM order_events
        WHERE category = %s
          AND event_timestamp >= NOW() - INTERVAL '30 minutes'
        GROUP BY product_id, product_name
        ORDER BY total_revenue DESC;
    """, (category,))
    if not rows:
        return f"No recent orders found for category '{category}'. Possible complete stockout or system issue."
    lines = [f"Product breakdown for '{category}' (last 30 min):"]
    for r in rows:
        lines.append(
            f"  {r['product_id']} {r['product_name']} | "
            f"orders={r['order_count']} units={r['total_units']} | "
            f"revenue=₹{float(r['total_revenue']):,.2f}"
        )
    return "\n".join(lines)


@tool
def get_recent_order_trend(minutes: int = 15) -> str:
    """Get total order count and revenue per minute for the last N minutes.
    Use this to see if order volume is declining, stable, or spiking."""
    if minutes < 1:
        minutes = 1
    if minutes > 60:
        minutes = 60
    rows = _query("""
        SELECT date_trunc('minute', event_timestamp) as minute,
               COUNT(*) as orders,
               SUM(total_amount) as revenue
        FROM order_events
        WHERE event_timestamp >= NOW() - (%s * INTERVAL '1 minute')
        GROUP BY minute
        ORDER BY minute DESC;
    """, (minutes,))
    if not rows:
        return f"No orders found in the last {minutes} minutes."
    lines = [f"Order trend (last {minutes} min):"]
    for r in rows:
        lines.append(
            f"  {r['minute']} | orders={r['orders']} | revenue=₹{float(r['revenue']):,.2f}"
        )
    return "\n".join(lines)


@tool
def get_payment_method_breakdown() -> str:
    """Get order counts grouped by payment method for the last 30 minutes.
    Use this to check if a specific payment processor might be failing."""
    rows = _query("""
        SELECT payment_method,
               COUNT(*) as order_count,
               SUM(total_amount) as total_revenue
        FROM order_events
        WHERE event_timestamp >= NOW() - INTERVAL '30 minutes'
        GROUP BY payment_method
        ORDER BY total_revenue DESC;
    """)
    if not rows:
        return "No recent orders found."
    lines = ["Payment method breakdown (last 30 min):"]
    for r in rows:
        lines.append(
            f"  {r['payment_method']:25s} | orders={r['order_count']} | "
            f"revenue=₹{float(r['total_revenue']):,.2f}"
        )
    return "\n".join(lines)


@tool
def get_feature_snapshot(category: str, region: str) -> str:
    """Get the latest computed features for a category/region pair from the feature store.
    Use this to see multi-window revenue trends (5m, 15m, 60m) and momentum."""
    rows = _query("""
        SELECT computed_at, revenue_last_5m, revenue_last_15m, revenue_last_60m,
               orders_last_5m, orders_last_15m, orders_last_60m,
               avg_order_value_last_15m, revenue_trend_pct
        FROM feature_store
        WHERE category = %s AND region = %s
        ORDER BY computed_at DESC
        LIMIT 3;
    """, (category, region))
    if not rows:
        return f"No features found for {category}/{region}."
    lines = [f"Feature snapshot for '{category}' / '{region}':"]
    for r in rows:
        trend = float(r["revenue_trend_pct"])
        trend_label = "DECLINING" if trend < 0.6 else "STABLE" if trend < 1.4 else "SURGING"
        lines.append(
            f"  {r['computed_at']} | "
            f"rev 5m=₹{float(r['revenue_last_5m']):,.2f} "
            f"15m=₹{float(r['revenue_last_15m']):,.2f} "
            f"60m=₹{float(r['revenue_last_60m']):,.2f} | "
            f"orders 5m={r['orders_last_5m']} 15m={r['orders_last_15m']} 60m={r['orders_last_60m']} | "
            f"avg=₹{float(r['avg_order_value_last_15m']):,.2f} | "
            f"trend={trend:.2f} ({trend_label})"
        )
    return "\n".join(lines)


ALL_TOOLS = [
    get_open_anomalies,
    get_revenue_by_category,
    get_revenue_by_region,
    get_product_order_volume,
    get_recent_order_trend,
    get_payment_method_breakdown,
    get_feature_snapshot,
]


# ---------------------------------------------------------------------------
# Direct query helpers (no @tool decorator) for the two-phase approach.
# These call the same SQL but return the formatted strings directly.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Direct query helpers (Optimized with LRU-TTL Cache decorator)
# ---------------------------------------------------------------------------

@_cache.cached(key_fn=lambda category: f"rev_cat:{category}")
def query_revenue_by_category(category: str) -> str:
    return get_revenue_by_category.invoke(category)

@_cache.cached(key_fn=lambda region: f"rev_reg:{region}")
def query_revenue_by_region(region: str) -> str:
    return get_revenue_by_region.invoke(region)

@_cache.cached(key_fn=lambda category: f"prod_vol:{category}")
def query_product_order_volume(category: str) -> str:
    return get_product_order_volume.invoke(category)

@_cache.cached(key_fn=lambda category, region: f"feat:{category}:{region}")
def query_feature_snapshot(category: str, region: str) -> str:
    return get_feature_snapshot.invoke({"category": category, "region": region})

@_cache.cached(key_fn=lambda minutes=15: f"trend:{minutes}")
def query_recent_order_trend(minutes: int = 15) -> str:
    return get_recent_order_trend.invoke(minutes)

@_cache.cached(key_fn=lambda: "pay_breakdown")
def query_payment_method_breakdown() -> str:
    return get_payment_method_breakdown.invoke({})
