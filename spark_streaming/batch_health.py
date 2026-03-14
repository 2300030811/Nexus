# spark_streaming/batch_health.py
"""Tracks consecutive batch failures and exposes health state."""

import threading
import time
from dataclasses import dataclass, field
from common.logging_utils import get_logger

logger = get_logger("nexus.batch_health")


@dataclass
class BatchHealth:
    name: str
    failure_threshold: int = 5
    _consecutive_failures: int = field(default=0, init=False, repr=False)
    _total_failures: int = field(default=0, init=False, repr=False)
    _total_batches: int = field(default=0, init=False, repr=False)
    _last_failure: float | None = field(default=None, init=False, repr=False)
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )

    def record_success(self):
        with self._lock:
            self._consecutive_failures = 0
            self._total_batches += 1

    def record_failure(self, error: Exception):
        with self._lock:
            self._consecutive_failures += 1
            self._total_failures += 1
            self._total_batches += 1
            self._last_failure = time.monotonic()
            if self._consecutive_failures >= self.failure_threshold:
                logger.critical(
                    "BATCH SINK [%s] — %d consecutive failures. "
                    "Last error: %s. Manual investigation required.",
                    self.name, self._consecutive_failures, error,
                )

    @property
    def is_healthy(self) -> bool:
        with self._lock:
            return self._consecutive_failures < self.failure_threshold

    @property
    def stats(self) -> dict:
        with self._lock:
            rate = (
                self._total_failures / self._total_batches
                if self._total_batches > 0 else 0
            )
            return {
                "name": self.name,
                "consecutive_failures": self._consecutive_failures,
                "total_failures": self._total_failures,
                "total_batches": self._total_batches,
                "failure_rate": round(rate, 4),
                "healthy": self.is_healthy,
            }
