"""
Unit tests for ML pipeline components.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class TestExpectedRevenueCalculation:
    """Test expected revenue calculation logic."""

    def test_expected_revenue_formula(self):
        """Test that expected revenue formula matches specification."""
        from common.constants import (
            CATEGORY_BASELINES,
            REGION_WEIGHTS,
            get_hour_factor,
            get_dow_factor,
        )

        category = "Electronics"
        region = "Delhi"
        hour = 12  # Lunch time (1.4x)
        dow = 4  # Friday (1.15x)

        base = CATEGORY_BASELINES[category]  # 2800
        region_w = REGION_WEIGHTS[region]  # 0.30
        hf = get_hour_factor(hour)  # 1.4
        df = get_dow_factor(dow)  # 1.15

        expected = base * region_w * hf * df
        # 2800 * 0.30 * 1.4 * 1.15 = 1357.2
        assert expected == pytest.approx(1357.2, rel=1e-2)

    def test_low_revenue_period(self):
        """Test expected revenue during off-hours."""
        from common.constants import (
            CATEGORY_BASELINES,
            REGION_WEIGHTS,
            get_hour_factor,
            get_dow_factor,
        )

        # Accessories in West Bengal at 2am on Tuesday
        category = "Accessories"
        region = "West Bengal"
        hour = 2  # Off-hours (0.3x)
        dow = 1  # Tuesday (0.85x)

        base = CATEGORY_BASELINES[category]  # 120
        region_w = REGION_WEIGHTS[region]  # 0.10
        hf = get_hour_factor(hour)  # 0.3
        df = get_dow_factor(dow)  # 0.85

        expected = base * region_w * hf * df
        # 120 * 0.10 * 0.3 * 0.85 = 3.06
        assert expected == pytest.approx(3.06, rel=1e-2)


class TestEventGeneration:
    """Test event generation for data quality."""

    def test_event_schema(self):
        """Test that generated events have correct schema."""
        from kafka_producer.producer import generate_order_event

        event = generate_order_event(simulate_stockout=False)

        required_fields = [
            "event_id",
            "event_type",
            "timestamp",
            "order_id",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price",
            "total_amount",
            "region",
            "payment_method",
        ]

        for field in required_fields:
            assert field in event, f"Event missing required field: {field}"

    def test_total_amount_calculation(self):
        """Test that total_amount = unit_price * quantity."""
        from kafka_producer.producer import generate_order_event

        for _ in range(10):
            event = generate_order_event(simulate_stockout=False)
            expected_total = round(event["unit_price"] * event["quantity"], 2)
            assert event["total_amount"] == expected_total

    def test_stockout_simulation(self):
        """Test that stockout simulation blocks Electronics in Delhi."""
        from kafka_producer.producer import generate_order_event

        # Generate 100 events with stockout active
        delhi_electronics_count = 0
        for _ in range(100):
            event = generate_order_event(simulate_stockout=True)
            if event["region"] == "Delhi" and event["category"] == "Electronics":
                delhi_electronics_count += 1

        # With stockout active, there should be ZERO Electronics orders from Delhi
        assert delhi_electronics_count == 0, "Stockout should block Delhi Electronics"


class TestAnomalyDetection:
    """Test anomaly detection logic using the shared classify_severity function."""

    def test_severity_assignment_critical_drop(self):
        """Test that severe drops are marked as critical."""
        from common.constants import classify_severity
        assert classify_severity(0.15) == "critical"  # 85% drop

    def test_severity_assignment_high_drop(self):
        """Test that moderate drops are marked as high."""
        from common.constants import classify_severity
        assert classify_severity(0.35) == "high"  # 65% drop

    def test_severity_assignment_medium(self):
        """Test that mild deviations are marked as medium."""
        from common.constants import classify_severity
        assert classify_severity(0.5) == "medium"
        assert classify_severity(1.0) == "medium"
        assert classify_severity(2.0) == "medium"

    def test_severity_assignment_spike_critical(self):
        """Test that extreme revenue spikes are marked as critical."""
        from common.constants import classify_severity
        assert classify_severity(5.0) == "critical"  # 5x spike

    def test_severity_assignment_spike_high(self):
        """Test that moderate revenue spikes are marked as high."""
        from common.constants import classify_severity
        assert classify_severity(3.0) == "high"  # 3x spike

    def test_severity_boundary_critical_low(self):
        """Test critical boundary at 0.2."""
        from common.constants import classify_severity
        assert classify_severity(0.19) == "critical"
        assert classify_severity(0.2) == "high"  # 0.2 is < 0.4 → high

    def test_severity_boundary_high_low(self):
        """Test high boundary at 0.4."""
        from common.constants import classify_severity
        assert classify_severity(0.39) == "high"
        assert classify_severity(0.4) == "medium"  # 0.4 is not < 0.4 → medium

    def test_severity_boundary_critical_high(self):
        """Test critical boundary at 4.0."""
        from common.constants import classify_severity
        assert classify_severity(4.1) == "critical"
        assert classify_severity(4.0) == "high"  # 4.0 is not > 4.0 → check > 2.5 → high
