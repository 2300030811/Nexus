import time
import threading
from collections import defaultdict, deque
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

class RateLimitMiddleware(BaseHTTPMiddleware):
    """Memory-efficient sliding-window rate limiter.

    Accepts either ``requests_per_minute`` (60-second window, default) or
    ``calls_per_second`` (1-second window) to keep unit tests fast.
    """

    def __init__(
        self,
        app,
        requests_per_minute: int = 100,
        calls_per_second: int | None = None,
    ):
        super().__init__(app)
        if calls_per_second is not None:
            self.rpm = calls_per_second
            self._window_seconds = 1.0
        else:
            self.rpm = requests_per_minute
            self._window_seconds = 60.0
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
        window_start = now - self._window_seconds

        with self._lock:
            # Periodic cleanup every 5x the window size
            if now - self._last_cleanup > max(self._window_seconds * 5, 1.0):
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
            if not dq or dq[-1] < (now - self._window_seconds)
        ]
        for ip in expired_ips:
            del self._windows[ip]
        self._last_cleanup = now
