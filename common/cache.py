import time
import threading
from collections import OrderedDict
from typing import Any, Callable

class TTLCache:
    """Thread-safe in-memory cache with per-key TTL and LRU eviction."""

    def __init__(self, default_ttl: int = 300, max_size: int = 1000):
        self._store: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._ttl = default_ttl
        self._max_size = max_size
        self._lock = threading.Lock()

    def get(self, key: str) -> Any | None:
        with self._lock:
            if key in self._store:
                value, expires_at = self._store[key]
                if time.monotonic() < expires_at:
                    # Move to end to mark as most recently used
                    self._store.move_to_end(key)
                    return value
                del self._store[key]
        return None

    def set(self, key: str, value: Any, ttl: int | None = None):
        expires_at = time.monotonic() + (ttl or self._ttl)
        with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
            self._store[key] = (value, expires_at)
            
            # LRU Eviction: remove oldest items if over limit
            if len(self._store) > self._max_size:
                self._store.popitem(last=False)

    def cached(self, key_fn: Callable, ttl: int | None = None):
        """Decorator factory: cache the result keyed by key_fn(*args, **kwargs)."""
        def decorator(fn: Callable):
            def wrapper(*args, **kwargs):
                key = key_fn(*args, **kwargs)
                hit = self.get(key)
                if hit is not None:
                    return hit
                result = fn(*args, **kwargs)
                self.set(key, result, ttl)
                return result
            return wrapper
        return decorator

# Global cache shared across copilot tools. Limited to 1000 items to prevent leaks.
_cache = TTLCache(default_ttl=60, max_size=1000)
