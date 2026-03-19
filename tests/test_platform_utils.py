import pytest
import time
from cachetools import TTLCache
from api_service.rate_limit import RateLimitMiddleware
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

class TestPlatformUtils:
    def test_ttl_cache_expiry(self):
        """Verify that the cache correctly expires items."""
        cache = TTLCache(maxsize=1, ttl=0.1)
        cache["key"] = "value"
        assert cache["key"] == "value"
        
        time.sleep(0.15)
        with pytest.raises(KeyError):
            _ = cache["key"]

    def test_rate_limiter_blocking(self):
        """Verify that the rate limiter blocks excessive requests."""
        app = FastAPI()
        # 2 requests per second limit for testing
        app.add_middleware(RateLimitMiddleware, calls_per_second=2)

        @app.get("/")
        def index():
            return {"ok": True}

        client = TestClient(app)
        
        # First 2 should pass
        assert client.get("/").status_code == 200
        assert client.get("/").status_code == 200
        
        # 3rd should be rate limited
        response = client.get("/")
        assert response.status_code == 429
        assert "Rate limit exceeded" in response.json()["detail"]

    def test_rate_limiter_recovery(self):
        """Verify that the rate limiter allows requests again after the window."""
        app = FastAPI()
        app.add_middleware(RateLimitMiddleware, calls_per_second=10)

        @app.get("/")
        def index():
            return {"ok": True}

        client = TestClient(app)
        
        # Burst
        for _ in range(10):
            client.get("/")
        
        assert client.get("/").status_code == 429
        
        # Wait for window to reset
        time.sleep(1.1)
        assert client.get("/").status_code == 200
