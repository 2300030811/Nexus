"""
Configuration management utilities.

Centralized configuration loading with validation and defaults.
"""

import os
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def get_env_var(key: str, default: Any = None, required: bool = False) -> Any:
    """
    Get environment variable or default value.
    If required is True and key is missing, raises ValueError.
    """
    value = os.getenv(key)
    if value is None:
        if required:
            error_msg = f"MISSING REQUIRED CONFIG: Environment variable '{key}' must be set."
            logger.error(error_msg)
            raise ValueError(error_msg)
        return default
    return value


def load_config() -> Dict[str, Any]:
    """
    Load configuration from environment variables.
    Returns a dictionary with all configuration values.
    """
    # Environment mode (default to development)
    env_mode = get_env_var("ENV", "development").lower()

    # Core required fields
    # In production, we should be more strict
    is_prod = env_mode == "production"

    config = {
        'env': env_mode,

        # Database Configuration
        'database': {
            'host': get_env_var("PG_HOST", "postgres"),
            'port': int(get_env_var("PG_PORT", "5432")),
            'dbname': get_env_var("PG_DB", "nexus"),
            'user': get_env_var("PG_USER", "nexus"),
            'password': get_env_var("PG_PASSWORD", required=is_prod, default="nexus_password"),
        },

        # Kafka Configuration
        'kafka': {
            'broker': get_env_var("KAFKA_BROKER", "kafka:29092,localhost:9092"),
            'topic': get_env_var("KAFKA_TOPIC", "order_events"),
            'events_per_second': float(get_env_var("EVENTS_PER_SECOND", "2.0")),
        },

        # Ollama / LLM Configuration
        'ollama': {
            'host': get_env_var("OLLAMA_HOST", "ollama:11434"),
            'model': get_env_var("OLLAMA_MODEL", "llama3"),
        },

        # Service Intervals
        'intervals': {
            'scan_interval': int(get_env_var("SCAN_INTERVAL", "60")),
            'copilot_interval': int(get_env_var("COPILOT_INTERVAL", "90")),
        },

        # Spark Configuration
        'spark': {
            'checkpoint_dir': get_env_var("CHECKPOINT_DIR", "/opt/spark-checkpoints"),
        },

        # Dashboard Configuration
        'dashboard': {
            'password': get_env_var("DASHBOARD_PASSWORD", required=is_prod, default="CHANGE_ME_IN_PROD"),
        },
    }

    # Additional validation if needed
    if is_prod:
        # Check if secrets still have 'CHANGE_ME' values
        for section, values in config.items():
            if isinstance(values, dict):
                for k, v in values.items():
                    if isinstance(v, str) and "CHANGE_ME" in v:
                        error_msg = f"INSECURE CONFIG: '{k}' in '{section}' still contains 'CHANGE_ME' in production."
                        logger.error(error_msg)
                        raise ValueError(error_msg)

    return config


def get_db_dsn() -> str:
    """Get PostgreSQL DSN string for connection."""
    config = load_config()
    db = config['database']
    return f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['dbname']}?connect_timeout=5"
