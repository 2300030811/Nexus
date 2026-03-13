"""
Pytest configuration and fixtures for Nexus tests.
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def sample_order_event():
    """Fixture providing a sample order event."""
    return {
        "event_id": "test-event-123",
        "event_type": "order_placed",
        "timestamp": "2026-03-09T12:00:00Z",
        "order_id": "ORD-TEST123",
        "product_id": "SKU-1001",
        "product_name": "iPhone 15 Pro",
        "category": "Electronics",
        "quantity": 2,
        "unit_price": 999.00,
        "total_amount": 1998.00,
        "region": "Delhi",
        "payment_method": "credit_card",
    }


@pytest.fixture
def sample_revenue_metrics():
    """Fixture providing sample revenue metrics data."""
    import pandas as pd
    from datetime import datetime

    return pd.DataFrame(
        [
            {
                "window_start": datetime(2026, 3, 9, 12, 0),
                "window_end": datetime(2026, 3, 9, 12, 5),
                "category": "Electronics",
                "region": "Delhi",
                "order_count": 10,
                "total_revenue": 5000.0,
                "avg_order_value": 500.0,
            },
            {
                "window_start": datetime(2026, 3, 9, 12, 5),
                "window_end": datetime(2026, 3, 9, 12, 10),
                "category": "Electronics",
                "region": "Delhi",
                "order_count": 15,
                "total_revenue": 7500.0,
                "avg_order_value": 500.0,
            },
        ]
    )


@pytest.fixture
def sample_anomaly():
    """Fixture providing a sample anomaly record."""
    return {
        "id": 1,
        "window_start": "2026-03-09T12:00:00",
        "window_end": "2026-03-09T12:05:00",
        "category": "Electronics",
        "region": "Delhi",
        "actual_revenue": 500.0,
        "expected_revenue": 2800.0,
        "anomaly_score": 0.95,
        "severity": "critical",
        "status": "open",
    }
