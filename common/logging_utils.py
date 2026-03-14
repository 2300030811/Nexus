"""
Structured logging utilities for the Nexus platform.

Provides a consistent JSON-formatted logger for all services.
"""

import logging
import json
import sys
from datetime import datetime, timezone


import re

# Regex to find potential secrets in log strings
SECRETS_REGEX = re.compile(r"(password|secret|key|token|auth)['\"]?\s*[:=]\s*['\"]?([^'\"\s,]+)['\"]?", re.IGNORECASE)

class JSONFormatter(logging.Formatter):
    """Format log records as JSON with automated secret masking."""

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        # Mask common secret patterns
        masked_msg = SECRETS_REGEX.sub(r"\1: [REDACTED]", msg)

        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": record.name,
            "message": masked_msg,
        }
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        # Include extra fields if provided
        for key in ("event_count", "scan_count", "anomaly_id", "batch_id", "extra"):
            if hasattr(record, key):
                log_entry[key] = getattr(record, key)
        return json.dumps(log_entry)


def get_logger(service_name: str, level: str = "INFO") -> logging.Logger:
    """
    Create a structured JSON logger for a service.
    
    Args:
        service_name: Name of the service (e.g., 'producer', 'anomaly-detector')
        level: Logging level (default: INFO)
    """
    logger = logging.getLogger(service_name)
    
    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger
