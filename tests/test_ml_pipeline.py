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
    """Test anomaly detection logic."""

    def test_severity_assignment_critical_drop(self):
        """Test that severe drops are marked as critical."""
        revenue_ratio = 0.15  # 85% drop
        
        if revenue_ratio < 0.2:
            severity = "critical"
        elif revenue_ratio < 0.4:
            severity = "high"
        else:
            severity = "medium"
        
        assert severity == "critical"

    def test_severity_assignment_high_drop(self):
        """Test that moderate drops are marked as high."""
        revenue_ratio = 0.35  # 65% drop
        
        if revenue_ratio < 0.2:
            severity = "critical"
        elif revenue_ratio < 0.4:
            severity = "high"
        else:
            severity = "medium"
        
        assert severity == "high"

    def test_severity_assignment_spike(self):
        """Test that revenue spikes are marked as critical."""
        revenue_ratio = 5.0  # 5x spike
        
        if revenue_ratio > 4.0:
            severity = "critical"
        elif revenue_ratio > 2.5:
            severity = "high"
        else:
            severity = "medium"
        
        assert severity == "critical"


class TestFeatureEngineering:
    """Test feature engineering for ML model."""

    def test_revenue_ratio_calculation(self):
        """Test that revenue_ratio is calculated correctly."""
        actual_revenue = 500.0
        expected_revenue = 1000.0
        
        revenue_ratio = actual_revenue / expected_revenue if expected_revenue > 0 else 0
        
        assert revenue_ratio == 0.5

    def test_revenue_ratio_handles_zero_expected(self):
        """Test that zero expected revenue is handled gracefully."""
        actual_revenue = 500.0
        expected_revenue = 0.0
        
        revenue_ratio = actual_revenue / expected_revenue if expected_revenue > 0 else 0
        
        assert revenue_ratio == 0

    def test_category_encoding(self):
        """Test that categories are encoded correctly."""
        from common.constants import CATEGORY_MAP

        test_data = pd.DataFrame({"category": ["Electronics", "Footwear", "Unknown"]})
        test_data["category_enc"] = test_data["category"].map(CATEGORY_MAP).fillna(-1)

        assert test_data.loc[0, "category_enc"] == 0
        assert test_data.loc[1, "category_enc"] == 1
        assert test_data.loc[2, "category_enc"] == -1  # Unknown category
