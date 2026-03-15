import time
import threading
from collections import defaultdict, deque
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

class RateLimitMiddleware(BaseHTTPMiddleware):
    """Memory-efficient sliding-window rate limiter."""

    def __init__(self, app, requests_per_minute: int = 100):
        super().__init__(app)
        self.rpm = requests_per_minute
        self._windows: dict[str, deque] = {}
        self._last_cleanup = time.time()
        self._lock = threading.Lock()

    async def dispatch(self, request: Request, call_next):
        # Skip health checks
        if request.url.path == "/health":
            return await call_next(request)

        # Try to get client IP from X-Forwarded-For (for proxies)
        client = request.headers.get("X-Forwarded-For")
        if client:
            client = client.split(",")[0].strip()
        else:
            client = request.client.host if request.client else "unknown"
        now = time.time()
        window_start = now - 60.0

        with self._lock:
            # Periodic cleanup of stagnant entries every 5 minutes
            if now - self._last_cleanup > 300:
                self._cleanup(now)
            
            if client not in self._windows:
                self._windows[client] = deque()
            
            dq = self._windows[client]
            while dq and dq[0] < window_start:
                dq.popleft()
                
            if len(dq) >= self.rpm:
                from starlette.responses import JSONResponse
                return JSONResponse(
                    status_code=429,
                    content={"detail": f"Rate limit exceeded: {self.rpm} RPM"},
                    headers={"Retry-After": "60"},
                )
            dq.append(now)

        return await call_next(request)

    def _cleanup(self, now: float):
        """Purge entries that haven't been active in over a minute."""
        expired_ips = [
            ip for ip, dq in self._windows.items() 
            if not dq or dq[-1] < (now - 60.0)
        ]
        for ip in expired_ips:
            del self._windows[ip]
        self._last_cleanup = now
