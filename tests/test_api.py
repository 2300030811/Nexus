# Mock standard Nexus imports before importing the app
import sys
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

sys.modules["psycopg2"] = MagicMock()
sys.modules["psycopg2.extras"] = MagicMock()
sys.modules["psycopg2.pool"] = MagicMock()

from api_service.main import HTTPException, app, get_conn  # noqa: E402

client = TestClient(app)

class TestNexusAPI:
    def test_health_check_success(self):
        mock_conn = MagicMock()
        app.dependency_overrides[get_conn] = lambda: mock_conn

        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
        app.dependency_overrides.clear()

    def test_health_check_failure(self):
        def mock_get_conn():
            raise HTTPException(status_code=503, detail="Database unreachable")

        app.dependency_overrides[get_conn] = mock_get_conn

        response = client.get("/health")
        assert response.status_code == 503
        app.dependency_overrides.clear()

    def test_get_anomalies(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        # Mocking fetchone for the count query and fetchall for items
        mock_cur.fetchone.return_value = {"count": 1}
        mock_cur.fetchall.return_value = [
            {"id": 1, "category": "Electronics", "severity": "critical"}
        ]

        app.dependency_overrides[get_conn] = lambda: mock_conn

        response = client.get("/api/anomalies?limit=1")
        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert len(data["items"]) == 1
        assert data["items"][0]["category"] == "Electronics"
        assert data["total"] == 1
        app.dependency_overrides.clear()

    def test_get_kpis(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        # New implementation fetchone() returns the tuple in a single call
        mock_cur.fetchone.return_value = (100, 5000.0, 3, 5)

        app.dependency_overrides[get_conn] = lambda: mock_conn

        response = client.get("/api/kpis?minutes=30")
        assert response.status_code == 200
        data = response.json()
        assert data["orders"] == 100
        assert data["revenue"] == 5000.0
        assert data["open_anomalies"] == 3
        app.dependency_overrides.clear()
