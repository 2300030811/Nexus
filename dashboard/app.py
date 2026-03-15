import os
from html import escape
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.pool
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
import plotly.graph_objects as go

from common.db_utils import get_db_config, get_connection_pool

st.set_page_config(page_title="Nexus Ops Dashboard", layout="wide", initial_sidebar_state="expanded")

# ---------------------------------------------------------------------------
# Authentication Gate (Phase 1.3)
# ---------------------------------------------------------------------------
def check_password():
    """Returns True if the user had the correct password."""
    if "password_correct" not in st.session_state:
        st.session_state.password_correct = False

    if not st.session_state.password_correct:
        # Password not correct, show input + instructions.
        st.warning("⚠️ Dashboard access restricted. Please enter password.")
        password = st.text_input("Password", type="password", key="password_input")
        dashboard_password = os.getenv("DASHBOARD_PASSWORD", "nexus_secure_pass_123")
        if password == dashboard_password:
            st.session_state.password_correct = True
            st.rerun()
        elif password:
            st.error("❌ Password incorrect.")
        return False
    return True

# ---------------------------------------------------------------------------
# Configuration (via shared db_utils)
# ---------------------------------------------------------------------------

@st.cache_resource
def get_pool():
    return get_connection_pool(minconn=1, maxconn=10)

def run_query(query: str, params: tuple = ()) -> pd.DataFrame:
    """Execute a SELECT query and return results as DataFrame."""
    try:
        pool = get_pool()
        conn = pool.getconn()
        try:
            return pd.read_sql(query, conn, params=params)
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            pool.putconn(conn)
    except Exception as e:
        st.error(f"Database query error: {str(e)}")
        # If pool is broken, this might help on next run
        return pd.DataFrame()

def run_update(query: str, params: tuple = ()):
    """Execute an INSERT/UPDATE/DELETE query."""
    try:
        pool = get_pool()
        conn = pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
            conn.commit()
        finally:
            pool.putconn(conn)
    except Exception as e:
        st.error(f"Database update error: {str(e)}")

# ---------------------------------------------------------------------------
# Data functions
# ---------------------------------------------------------------------------
@st.cache_data(ttl=5)
def load_kpis(minutes: int) -> dict:
    """Load KPIs using pre-aggregated metrics for performance."""
    try:
        query = """
            SELECT 
                (SELECT SUM(order_count) FROM revenue_metrics WHERE window_end >= NOW() - (%s * INTERVAL '1 minute')) as total_orders,
                (SELECT SUM(total_revenue) FROM revenue_metrics WHERE window_end >= NOW() - (%s * INTERVAL '1 minute')) as total_revenue,
                (SELECT COUNT(*) FROM anomalies WHERE status = 'open') as open_anomalies,
                (SELECT COUNT(*) FROM copilot_reports) as total_reports
        """
        row = run_query(query, (minutes, minutes))
        
        row_data = row.iloc[0]
        orders = int(row_data["total_orders"]) if not pd.isna(row_data["total_orders"]) else 0
        revenue = float(row_data["total_revenue"]) if not pd.isna(row_data["total_revenue"]) else 0.0
        open_anom = int(row_data["open_anomalies"]) if not pd.isna(row_data["open_anomalies"]) else 0
        total_reps = int(row_data["total_reports"]) if not pd.isna(row_data["total_reports"]) else 0
        
        return {
            "orders": orders,
            "revenue": revenue,
            "open_anomalies": open_anom,
            "total_reports": total_reps
        }
    except Exception as e:
        st.warning(f"KPI load error: {str(e)}")
        return {"orders": 0, "revenue": 0.0, "open_anomalies": 0, "total_reports": 0}

@st.cache_data(ttl=10)
def load_system_health() -> dict:
    """Load system health metrics with performance optimizations."""
    try:
        # Estimate throughput (5-minute rolling average)
        # We can't use revenue_metrics for near-instant ingestion status, 
        # so we scan a small window of order_events. 
        # But we limit it to just the count.
        epm_check = run_query("SELECT COUNT(*) as count FROM order_events WHERE ingested_at >= NOW() - INTERVAL '5 minutes'")
        event_count = int(epm_check["count"].iloc[0]) if not epm_check.empty else 0
        epm = round(event_count / 5.0, 1)
        
        # Estimate latency using a small sample
        latency_df = run_query("""
            SELECT AVG(EXTRACT(EPOCH FROM (ingested_at - event_timestamp))) as lat 
            FROM (SELECT ingested_at, event_timestamp FROM order_events ORDER BY ingested_at DESC LIMIT 100) s
        """)
        latency = round(float(latency_df["lat"].iloc[0]) if not latency_df.empty and latency_df["lat"].iloc[0] else 0.0, 2)
        
        last_pred_df = run_query("SELECT MAX(detected_at) as last FROM anomalies")
        last_pred = last_pred_df["last"].iloc[0] if not last_pred_df.empty and last_pred_df["last"].iloc[0] else None
        
        kafka_connected = event_count > 0 # Based on 5m window
        
        return {
            "epm": epm,
            "latency": latency,
            "last_pred": last_pred,
            "kafka_connected": kafka_connected
        }
    except Exception as e:
        st.warning(f"Health load error: {str(e)}")
        return {"epm": 0, "latency": 0.0, "last_pred": None, "kafka_connected": False}

def get_simulation_mode() -> bool:
    res = run_query("SELECT value FROM app_config WHERE key = 'simulate_stockout'")
    return res.iloc[0]['value'].lower() == 'true' if not res.empty else False

def toggle_simulation(target: bool):
    run_update("UPDATE app_config SET value = %s WHERE key = 'simulate_stockout'", (str(target).lower(),))


def load_model_health() -> dict:
    """Load latest drift metrics."""
    try:
        df = run_query("""
            SELECT measured_at, anomaly_rate, avg_score, score_std,
                   psi_revenue, drift_flag, notes
            FROM model_drift_log
            ORDER BY measured_at DESC
            LIMIT 1
        """)
        if df.empty:
            return {"available": False}
        row = df.iloc[0]
        return {
            "available":    True,
            "measured_at":  row["measured_at"],
            "anomaly_rate": float(row["anomaly_rate"]),
            "avg_score":    float(row["avg_score"]),
            "psi_revenue":  float(row["psi_revenue"]) if row["psi_revenue"] else None,
            "drift_flag":   bool(row["drift_flag"]),
            "notes":        row["notes"],
        }
    except Exception:
        return {"available": False}


def load_dlq_stats() -> dict:
    """Load dead letter queue depth."""
    try:
        df = run_query("""
            SELECT
                COUNT(*)                                          AS total,
                COUNT(*) FILTER (WHERE resolved = FALSE)         AS pending,
                COUNT(*) FILTER (WHERE retry_count >= 5
                                   AND resolved = FALSE)         AS exhausted,
                MAX(first_failed)                                AS oldest
            FROM dlq_events
        """)
        if df.empty:
            return {"pending": 0, "exhausted": 0}
        row = df.iloc[0]
        return {
            "total":     int(row["total"]),
            "pending":   int(row["pending"]),
            "exhausted": int(row["exhausted"]),
            "oldest":    row["oldest"],
        }
    except Exception:
        return {"pending": 0, "exhausted": 0}

# ---------------------------------------------------------------------------
# UI Components
# ---------------------------------------------------------------------------
# Check authentication first
if not check_password():
    st.stop()



# Non-blocking auto-refresh every 5s for near real-time updates
st_autorefresh(interval=5000, key="datarefresh")

# Header with last updated timestamp
col_title, col_time = st.columns([3, 1])
with col_title:
    st.title("Nexus – Autonomous Retail Monitoring")
with col_time:
    st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')} (5s polling)")

# Sidebar
st.sidebar.header("Demo Controls")
sim_mode = get_simulation_mode()
if st.sidebar.button("🔴 Force iPhone Stockout" if not sim_mode else "🟢 Restore Normal Operations"):
    toggle_simulation(not sim_mode)
    st.rerun()

if sim_mode:
    st.sidebar.warning("SIMULATION ACTIVE: Revenue drop expected in North-India Electronics")

lookback = st.sidebar.selectbox("Lookback", [15, 30, 60, 120], index=1, format_func=lambda x: f"{x}m")

# Layout
kpis = load_kpis(lookback)
health = load_system_health()

st.header("System Health & Throughput")
h1, h2, h3, h4 = st.columns(4)
h1.metric("Events Per Minute", f"{health['epm']}")
h2.metric("Pipeline Latency", f"{health['latency']}s")

# Last ML Scan with health warning
if health['last_pred']:
    time_since_scan = (datetime.now() - health['last_pred'].replace(tzinfo=None)).total_seconds()
    if time_since_scan > 300:  # 5 minutes
        h3.metric("Last ML Scan", health['last_pred'].strftime("%H:%M:%S"), delta="⚠️ Stale")
    else:
        h3.metric("Last ML Scan", health['last_pred'].strftime("%H:%M:%S"))
else:
    h3.metric("Last ML Scan", "N/A", help="ML anomaly detection hasn't produced results yet")

# Kafka Status with actual health check
kafka_status = "🟢 Connected" if health['kafka_connected'] else "🔴 Disconnected"
h4.metric("Kafka Status", kafka_status)

st.header("ML Model Health")
model_health = load_model_health()
dlq          = load_dlq_stats()

mh1, mh2, mh3, mh4 = st.columns(4)

if model_health["available"]:
    drift_delta = "DRIFT DETECTED" if model_health["drift_flag"] else "Normal"
    mh1.metric("Anomaly Rate", f"{model_health['anomaly_rate']:.1%}", delta=drift_delta)
    mh2.metric("Avg Score",    f"{model_health['avg_score']:.3f}")
    psi = model_health["psi_revenue"]
    psi_label = f"{psi:.3f}" if psi is not None else "n/a"
    psi_delta = "High drift" if psi and psi > 0.2 else ("Monitor" if psi and psi > 0.1 else None)
    mh3.metric("Revenue PSI", psi_label, delta=psi_delta)
    if model_health["drift_flag"] and model_health["notes"]:
        st.warning(f"Model drift alert: {model_health['notes']}")
else:
    mh1.metric("Model Health", "No data yet")

mh4.metric("DLQ Depth", dlq["pending"],
           delta=f"{dlq['exhausted']} exhausted" if dlq["exhausted"] > 0 else None)
if dlq.get("exhausted", 0) > 0:
    st.error(
        f"{dlq['exhausted']} events in the dead letter queue have exceeded "
        f"max retries and will not be retried automatically."
    )

st.header("Business Performance")
b1, b2, b3, b4 = st.columns(4)
b1.metric("Revenue", f"₹{kpis['revenue']:,.2f}")
b2.metric("Orders", f"{kpis['orders']:,}")
b3.metric("Critical Alerts", kpis['open_anomalies'])
b4.metric("AI Reports", kpis['total_reports'])

# Charts
st.header("Revenue Insights")

# Revenue Trend Over Time
st.subheader("Revenue Trend (5-Min Windows)")
df_trend = run_query("""
    SELECT window_start, SUM(total_revenue) as total_rev 
    FROM revenue_metrics 
    WHERE window_start >= NOW() - (%s * INTERVAL '1 minute') 
    GROUP BY window_start 
    ORDER BY window_start
""", (lookback,))
if not df_trend.empty:
    fig_trend = px.line(df_trend, x='window_start', y='total_rev', 
                        labels={'window_start': 'Time', 'total_rev': 'Revenue (₹)'},
                        title='Total Revenue Over Time')
    fig_trend.update_traces(line_color='#1f77b4', line_width=2)
    fig_trend.update_layout(height=300, margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("No revenue data available for selected timeframe")

c1, c2 = st.columns(2)
with c1:
    st.subheader("Category Revenue")
    df_cat = run_query("SELECT category, SUM(total_revenue) as rev FROM revenue_metrics WHERE window_start >= NOW() - (%s * INTERVAL '1 minute') GROUP BY category ORDER BY rev DESC", (lookback,))
    if not df_cat.empty:
        fig_cat = px.bar(df_cat, x='category', y='rev', 
                         labels={'category': 'Category', 'rev': 'Revenue (₹)'},
                         text='rev')
        fig_cat.update_traces(texttemplate='₹%{text:,.0f}', textposition='outside')
        fig_cat.update_layout(height=400, margin=dict(l=0, r=0, t=20, b=0), showlegend=False)
        st.plotly_chart(fig_cat, use_container_width=True)
    else:
        st.info("No category data")
with c2:
    st.subheader("Regional Revenue")
    df_reg = run_query("SELECT region, SUM(total_revenue) as rev FROM revenue_metrics WHERE window_start >= NOW() - (%s * INTERVAL '1 minute') GROUP BY region ORDER BY rev DESC", (lookback,))
    if not df_reg.empty:
        fig_reg = px.bar(df_reg, x='region', y='rev',
                         labels={'region': 'Region', 'rev': 'Revenue (₹)'},
                         text='rev')
        fig_reg.update_traces(texttemplate='₹%{text:,.0f}', textposition='outside')
        fig_reg.update_layout(height=400, margin=dict(l=0, r=0, t=20, b=0), showlegend=False)
        st.plotly_chart(fig_reg, use_container_width=True)
    else:
        st.info("No regional data")

# Anomalies
st.header("Anomalies & Alerts Feed")
anoms = run_query("SELECT * FROM anomalies ORDER BY detected_at DESC LIMIT 5")
if not anoms.empty:
    for _, row in anoms.iterrows():
        color = "#FF4B4B" if row['severity'] == "critical" else "#FFA500" if row['severity'] == "high" else "#FFD700"
        icon = "🚨" if row['severity'] == "critical" else "⚠️" if row['severity'] == "high" else "👀"
        
        expected = float(row['expected_revenue'])
        actual = float(row['actual_revenue'])
        drop_pct = ((expected - actual) / expected) * 100 if expected > 0 else 0
        drop_text = f"<span style='color:{color}; font-weight:bold;'>▼ {drop_pct:.1f}% DROP</span>" if actual < expected else f"<span style='color:#00FF00; font-weight:bold;'>▲ {abs(drop_pct):.1f}% SPIKE</span>"

        cat = escape(str(row['category']))
        reg = escape(str(row['region']))
        sev = escape(str(row['severity'])).upper()
        det = escape(row['detected_at'].strftime("%H:%M:%S"))
        act = escape(f"₹{actual:,.2f}")
        exp = escape(f"₹{expected:,.2f}")
        scr = escape(f"{row['anomaly_score']:.2f}")
        
        st.markdown(f"""
            <div style="border-left: 5px solid {color}; padding: 15px; background: rgba(30, 30, 30, 0.6); margin-bottom: 15px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom: 8px;">
                    <div style="font-size: 1.1em;">{icon} <b style="color:{color}">{sev}</b> Anomaly detected in <b>{cat}</b> (Region: {reg})</div>
                    <div style="color: #888; font-size: 0.9em;">{det}</div>
                </div>
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <div>
                        <span style="color:#aaa;">Actual:</span> <span style="font-weight:bold;">{act}</span> &nbsp;|&nbsp; 
                        <span style="color:#aaa;">Expected:</span> <span>{exp}</span>
                    </div>
                    <div>{drop_text}</div>
                    <div style="color:#aaa; font-style:italic;">Score: {scr}</div>
                </div>
            </div>
        """, unsafe_allow_html=True)
else:
    # Context-aware message during simulation
    if sim_mode:
        if health['last_pred'] is None:
            st.warning("⏳ Simulation is active but anomaly detection hasn't started yet. Check if the ML service is running.")
        else:
            time_since = (datetime.now() - health['last_pred'].replace(tzinfo=None)).total_seconds()
            if time_since > 300:
                st.warning("⚠️ Simulation is active but no recent anomaly scans. Last scan was over 5 minutes ago.")
            else:
                st.info("🔍 Simulation active — waiting for anomaly detection to identify revenue changes...")
    else:
        st.success("✅ No anomalies detected in recent windows. Everything is running smoothly.")

# Copilot Reports
st.header("🤖 AI Copilot Diagnostics")
reports = run_query("SELECT * FROM copilot_reports ORDER BY created_at DESC LIMIT 10")
if not reports.empty:
    for _, row in reports.iterrows():
        conf = float(row['confidence'])
        conf_color = "red" if conf < 0.4 else "orange" if conf < 0.7 else "green"
        conf_icon = "🟢" if conf >= 0.7 else "🟠" if conf >= 0.4 else "🔴"
        
        with st.expander(f"{conf_icon} Report #{row['id']} | {row['category']} in {row['region']} | Confidence: {conf:.0%} ({row['created_at'].strftime('%H:%M:%S')})"):
            c1, c2 = st.columns([2, 1])
            with c1:
                st.markdown("#### 🔍 Root Cause Analysis")
                st.write(row['root_cause'])
                st.markdown("#### ⚡ Recommended Action")
                st.info(row['recommended_action'])
            with c2:
                st.markdown("#### 📊 Impact Metrics")
                st.metric("Estimated Revenue Loss", f"₹{float(row['estimated_loss']):,.2f}")
                
                # Show percentage of actual vs expected
                expected = float(row['expected_revenue'])
                if expected > 0:
                    pct = (float(row['estimated_loss']) / expected) * 100
                    st.caption(f" Represents {pct:.1f}% of expected revenue")

                st.markdown("#### 🎯 AI Confidence")
                st.progress(conf, text=f"Evidence Strength: {int(conf*100)}%")
                
                # Metadata
                st.caption(f"Severity: **{row['severity'].upper()}**")
else:
    st.info("⏳ Waiting for AI Copilot to generate reports... Usually takes 1-2 minutes after an anomaly.")
