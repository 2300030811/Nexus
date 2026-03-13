"""
Unit tests for common utilities.
"""

import pytest
from common.constants import (
    get_hour_factor,
    get_dow_factor,
    CATEGORY_MAP,
    REGION_MAP,
    CATEGORY_BASELINES,
    REGION_WEIGHTS,
)


class TestHourFactors:
    """Test hour-of-day factor calculations."""

    def test_off_hours(self):
        """Test off-hours (0-6am) factor."""
        assert get_hour_factor(0) == 0.3
        assert get_hour_factor(3) == 0.3
        assert get_hour_factor(5) == 0.3

    def test_morning(self):
        """Test morning (6-9am) factor."""
        assert get_hour_factor(6) == 0.7
        assert get_hour_factor(8) == 0.7

    def test_am_peak(self):
        """Test AM peak (9am-12pm) factor."""
        assert get_hour_factor(9) == 1.2
        assert get_hour_factor(11) == 1.2

    def test_lunch(self):
        """Test lunch (12-2pm) factor."""
        assert get_hour_factor(12) == 1.4
        assert get_hour_factor(13) == 1.4

    def test_evening_peak(self):
        """Test evening peak (6-9pm) factor."""
        assert get_hour_factor(18) == 1.3
        assert get_hour_factor(20) == 1.3

    def test_night(self):
        """Test night (9pm-midnight) factor."""
        assert get_hour_factor(21) == 0.6
        assert get_hour_factor(23) == 0.6


class TestDayOfWeekFactors:
    """Test day-of-week factor calculations."""

    def test_monday(self):
        assert get_dow_factor(0) == 0.9

    def test_tuesday(self):
        assert get_dow_factor(1) == 0.85

    def test_thursday(self):
        assert get_dow_factor(3) == 1.0

    def test_friday(self):
        assert get_dow_factor(4) == 1.15

    def test_saturday(self):
        assert get_dow_factor(5) == 1.3

    def test_sunday(self):
        assert get_dow_factor(6) == 1.2

    def test_invalid_dow(self):
        """Test invalid day of week returns default."""
        assert get_dow_factor(-1) == 1.0
        assert get_dow_factor(7) == 1.0


class TestCategoryMappings:
    """Test category and region mappings."""

    def test_category_map_completeness(self):
        """Ensure all expected categories are mapped."""
        expected_categories = ["Electronics", "Footwear", "Apparel", "Home", "Accessories"]
        for cat in expected_categories:
            assert cat in CATEGORY_MAP
            assert isinstance(CATEGORY_MAP[cat], int)

    def test_category_encodings_unique(self):
        """Ensure category encodings are unique."""
        encodings = list(CATEGORY_MAP.values())
        assert len(encodings) == len(set(encodings))

    def test_region_map_completeness(self):
        """Ensure all expected regions are mapped."""
        expected_regions = ["Delhi", "Maharashtra", "Karnataka", "Tamil Nadu", "West Bengal"]
        for region in expected_regions:
            assert region in REGION_MAP
            assert isinstance(REGION_MAP[region], int)

    def test_region_encodings_unique(self):
        """Ensure region encodings are unique."""
        encodings = list(REGION_MAP.values())
        assert len(encodings) == len(set(encodings))


class TestBaselinesAndWeights:
    """Test revenue baselines and regional weights."""

    def test_category_baselines_positive(self):
        """All category baselines should be positive."""
        for category, baseline in CATEGORY_BASELINES.items():
            assert baseline > 0, f"{category} baseline should be positive"

    def test_region_weights_sum_to_one(self):
        """Regional weights should approximately sum to 1.0."""
        total_weight = sum(REGION_WEIGHTS.values())
        assert abs(total_weight - 1.0) < 0.01, "Regional weights should sum to 1.0"

    def test_region_weights_positive(self):
        """All region weights should be positive."""
        for region, weight in REGION_WEIGHTS.items():
            assert weight > 0, f"{region} weight should be positive"

    def test_delhi_has_highest_weight(self):
        """Delhi should have the highest regional weight."""
        delhi_weight = REGION_WEIGHTS["Delhi"]
        for region, weight in REGION_WEIGHTS.items():
            if region != "Delhi":
                assert delhi_weight >= weight, "Delhi should have highest weight"
