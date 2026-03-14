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
        self._lock = threading.Lock()

    @property
    def state(self) -> State:
        with self._lock:
            if self._state == State.OPEN:
                if time.monotonic() - self._opened_at >= self.timeout:
                    self._state = State.HALF
            return self._state

    def call(self, fn, *args, **kwargs):
        s = self.state
        if s == State.OPEN:
            raise RuntimeError(
                f"Circuit breaker [{self.name}] is OPEN. "
                f"Retrying in {int(self.timeout - (time.monotonic() - self._opened_at))}s"
            )
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
