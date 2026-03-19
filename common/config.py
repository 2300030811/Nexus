"""
Configuration management utilities.

Centralized configuration loading with validation and defaults.
"""

import os
from typing import Any, Dict


def load_config() -> Dict[str, Any]:
    """
    Load configuration from environment variables.
    Returns a dictionary with all configuration values.
    """
    return {
        # Database Configuration
        'database': {
            'host': os.getenv("PG_HOST", "postgres"),
            'port': int(os.getenv("PG_PORT", "5432")),
            'dbname': os.getenv("PG_DB", "nexus"),
            'user': os.getenv("PG_USER", "nexus"),
            'password': os.getenv("PG_PASSWORD", "nexus_password"),
        },
        
        # Kafka Configuration
        'kafka': {
            'broker': os.getenv("KAFKA_BROKER", "kafka:29092,localhost:9092"),
            'topic': os.getenv("KAFKA_TOPIC", "order_events"),
            'events_per_second': float(os.getenv("EVENTS_PER_SECOND", "2.0")),
        },
        
        # Ollama / LLM Configuration
        'ollama': {
            'host': os.getenv("OLLAMA_HOST", "ollama:11434"),
            'model': os.getenv("OLLAMA_MODEL", "llama3"),
        },
        
        # Service Intervals
        'intervals': {
            'scan_interval': int(os.getenv("SCAN_INTERVAL", "60")),
            'copilot_interval': int(os.getenv("COPILOT_INTERVAL", "90")),
        },
        
        # Spark Configuration
        'spark': {
            'checkpoint_dir': os.getenv("CHECKPOINT_DIR", "/opt/spark-checkpoints"),
        },
        
        # Dashboard Configuration
        'dashboard': {
            'password': os.getenv("DASHBOARD_PASSWORD", "CHANGE_ME_IN_PROD"),
        },
    }


def get_db_dsn() -> str:
    """Get PostgreSQL DSN string for connection."""
    config = load_config()
    db = config['database']
    return f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['dbname']}?connect_timeout=5"
