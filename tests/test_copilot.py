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
