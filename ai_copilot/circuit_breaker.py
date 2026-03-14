import time
import threading
from enum import Enum

class State(Enum):
    CLOSED = "closed"       # normal — calls pass through
    OPEN   = "open"         # tripped — calls blocked
    HALF   = "half_open"    # testing — one call allowed

class CircuitBreaker:
    def __init__(self, failure_threshold=3, recovery_timeout=300, name="breaker"):
        self.name = name
        self.threshold = failure_threshold
        self.timeout = recovery_timeout
        self._state = State.CLOSED
        self._failures = 0
        self._opened_at: float | None = None
        self._probe_allowed = False
        self._lock = threading.Lock()

    @property
    def state(self) -> State:
        with self._lock:
            if self._state == State.OPEN:
                if self._opened_at is not None and time.monotonic() - self._opened_at >= self.timeout:
                    self._state = State.HALF
                    self._probe_allowed = True
            return self._state

    def call(self, fn, *args, **kwargs):
        with self._lock:
            s = self.state
            if s == State.OPEN:
                remaining = int(self.timeout - (time.monotonic() - (self._opened_at or 0)))
                raise RuntimeError(
                    f"Circuit breaker [{self.name}] is OPEN. "
                    f"Retrying in {remaining}s"
                )
            if s == State.HALF:
                if not self._probe_allowed:
                    raise RuntimeError("Circuit half-open, probe already in flight")
                self._probe_allowed = False
        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except Exception as exc:
            self._on_failure()
            raise

    def _on_success(self):
        with self._lock:
            self._failures = 0
            self._state = State.CLOSED

    def _on_failure(self):
        with self._lock:
            self._failures += 1
            if self._failures >= self.threshold:
                self._state = State.OPEN
                self._opened_at = time.monotonic()
