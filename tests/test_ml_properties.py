"""Property-based tests using hypothesis."""
from hypothesis import given, settings, assume
from hypothesis import strategies as st
import pytest

from common.constants import (
    get_hour_factor, get_dow_factor,
    CATEGORY_BASELINES, REGION_WEIGHTS,
)


@given(st.integers(min_value=0, max_value=23))
def test_hour_factor_always_positive(hour):
    """Hour factor must always be positive — zero would suppress all revenue."""
    assert get_hour_factor(hour) > 0


@given(st.integers(min_value=0, max_value=6))
def test_dow_factor_always_positive(dow):
    assert get_dow_factor(dow) > 0


@given(
    st.sampled_from(list(CATEGORY_BASELINES.keys())),
    st.sampled_from(list(REGION_WEIGHTS.keys())),
    st.integers(min_value=0, max_value=23),
    st.integers(min_value=0, max_value=6),
)
def test_expected_revenue_always_positive(category, region, hour, dow):
    """Expected revenue must always be strictly positive for valid inputs."""
    base     = CATEGORY_BASELINES[category]
    region_w = REGION_WEIGHTS[region]
    hf       = get_hour_factor(hour)
    df       = get_dow_factor(dow)
    expected = base * region_w * hf * df
    assert expected > 0


@given(st.integers(min_value=0, max_value=23))
def test_peak_hours_beat_off_hours(hour):
    """Evening and lunch peaks must be higher than off-hours."""
    # 3am is defined as (0,6) which has factor 0.3
    off_hour_factor = get_hour_factor(3)  # 3am = off hours
    # Lunch and evening: (12,14)->1.4, (18,21)->1.3
    peak_hours = [12, 13, 18, 19, 20]
    peak_factors = [get_hour_factor(h) for h in peak_hours]
    for peak in peak_factors:
        assert peak > off_hour_factor


@given(st.floats(min_value=0.0, max_value=10.0))
def test_severity_classification_is_exhaustive(ratio):
    """Every revenue ratio must produce a severity — no unclassified case."""
    assume(ratio >= 0)
    from common.constants import classify_severity
    severity = classify_severity(ratio)
    assert severity in ("critical", "high", "medium")
