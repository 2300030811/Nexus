import pytest
from ai_copilot.copilot import parse_report
from ai_copilot import tools

class TestCopilotParseReport:
    """Test the robust regex parsing of LLM reports."""

    def test_parse_perfect_format(self):
        report = """
ANOMALY_REPORT_START
Anomaly ID: 123
Severity: critical
Category: Electronics
Region: Delhi
Estimated Loss: 1540.50
Confidence: 0.85
Root Cause: There appears to be a sudden drop in Electronics sales in Delhi.
Recommended Action: Check inventory levels for stockouts.
ANOMALY_REPORT_END
        """
        parsed = parse_report(report)
        assert parsed["estimated_loss"] == 1540.50
        assert parsed["confidence"] == 0.85
        assert parsed["root_cause"] == "There appears to be a sudden drop in Electronics sales in Delhi."
        assert parsed["recommended_action"] == "Check inventory levels for stockouts."

    def test_parse_with_currency_symbols(self):
        report = """
Estimated Loss: ₹ 1,540.50
Confidence: 85
Root Cause: A drop.
Recommended Action: Fix it.
        """
        parsed = parse_report(report)
        assert parsed["estimated_loss"] == 1540.50
        assert parsed["confidence"] == 0.85  # Should handle > 1.0 logic

    def test_parse_fallback_to_anomaly_data(self):
        report = """
Confidence: 0.90
Root Cause: We matched the pattern.
Recommended Action: Inspect further.
        """
        anomaly = {"actual_revenue": 500, "expected_revenue": 2500}
        parsed = parse_report(report, anomaly)
        assert parsed["estimated_loss"] == 2000.0
        assert parsed["confidence"] == 0.90

    def test_parse_heuristic_confidence_no_confidence_in_text(self):
        report = """
Root Cause Analysis:
This is a very long and detailed analysis of the anomaly. The numbers dropped significantly in the Electronics category. 
This suggests a potential stockout or pricing error in the system. We should definitely investigate the supply chain logs.
Recommended Action: Review the latest warehouse inventory counts.
        """
        parsed = parse_report(report)
        assert parsed["confidence"] == 0.55  # because root cause > 50 chars

    def test_parse_garbage_text(self):
        report = "Bad"
        parsed = parse_report(report)
        assert parsed["confidence"] == 0.15
        assert parsed["root_cause"] == "Unable to determine root cause."
        assert parsed["recommended_action"] == "Manual investigation recommended."


class TestCopilotTools:
    def test_query_restores_connection_autocommit(self, monkeypatch):
        class DummyCursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql, params):
                self.sql = sql
                self.params = params

            def fetchall(self):
                return [{"id": 1}]

        class DummyConn:
            def __init__(self):
                self.autocommit = False

            def cursor(self, cursor_factory=None):
                return DummyCursor()

        class DummyPool:
            def __init__(self, conn):
                self.conn = conn
                self.put_calls = []

            def getconn(self):
                return self.conn

            def putconn(self, conn, close=False):
                self.put_calls.append((conn, close))

        conn = DummyConn()
        pool = DummyPool(conn)
        monkeypatch.setattr(tools, "_get_pool", lambda: pool)

        result = tools._query("SELECT 1")

        assert result == [{"id": 1}]
        assert conn.autocommit is False
        assert pool.put_calls == [(conn, False)]

    def test_derive_revenue_trend_falls_back_to_recent_windows(self):
        trend = tools._derive_revenue_trend(300.0, 1200.0, 0.0)
        assert trend == pytest.approx(1.0)

    def test_derive_revenue_trend_prefers_stored_value(self):
        trend = tools._derive_revenue_trend(300.0, 1200.0, 1.25)
        assert trend == pytest.approx(1.25)

    def test_query_pool_safety_on_error(self, monkeypatch):
        """Ensure connection is returned to pool even if query fails."""
        class ErrorConn:
            def cursor(self, **kwargs):
                raise RuntimeError("DB Explosion")
            def rollback(self):
                pass
            @property
            def autocommit(self): return False
            @autocommit.setter
            def autocommit(self, val): pass

        class DummyPool:
            def __init__(self):
                self.conn = ErrorConn()
                self.put_calls = []
            def getconn(self):
                return self.conn
            def putconn(self, conn, close=False):
                self.put_calls.append((conn, close))

        pool = DummyPool()
        monkeypatch.setattr(tools, "_get_pool", lambda: pool)

        with pytest.raises(RuntimeError):
            tools._query("SELECT 1")
        
        # Ensure connection was returned despite error
        assert len(pool.put_calls) == 1
        assert pool.put_calls[0][0] == pool.conn

    def test_feature_store_contract_validation(self, monkeypatch):
        """Test get_feature_snapshot handles DB rows correctly."""
        mock_data = [{
            "computed_at": "2026-03-14T10:00:00Z",
            "revenue_last_5m": 100.0,
            "revenue_last_15m": 300.0,
            "revenue_last_60m": 1200.0,
            "orders_last_5m": 1,
            "orders_last_15m": 3,
            "orders_last_60m": 12,
            "avg_order_value_last_15m": 100.0,
            "revenue_trend_pct": 1.0
        }]
        monkeypatch.setattr(tools, "_query", lambda sql, params: mock_data)
        
        result = tools.get_feature_snapshot.invoke({"category": "test", "region": "test"})
        assert "Snapshot" in result
        assert "rev 5m=₹100.00" in result
        assert "trend=1.00 (STABLE)" in result
