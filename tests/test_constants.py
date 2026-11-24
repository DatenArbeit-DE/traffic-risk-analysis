"""Business vocabulary invariants.

These tests protect the two places where an off-by-one silently breaks every
downstream number: the time-of-day buckets and the severity scale.
"""

from __future__ import annotations

import pytest

from accident_risk.constants import (
    ALLOWED_PSEUDONYM_COLUMNS,
    BUCKET_HOURS,
    HOUR_TO_BUCKET,
    PII_DENYLIST_TOKENS,
    RISK_WEIGHTS,
    SEVERE_THRESHOLD,
    SEVERITY_CLASS,
    TIME_BUCKET_ORDER,
    bucket_for_hour,
)


def test_every_hour_maps_to_exactly_one_bucket() -> None:
    assert sorted(HOUR_TO_BUCKET) == list(range(24))
    assert set(HOUR_TO_BUCKET.values()) == set(TIME_BUCKET_ORDER)


def test_declared_bucket_sizes_match_the_mapping() -> None:
    for bucket, hours in BUCKET_HOURS.items():
        assert hours == sum(1 for value in HOUR_TO_BUCKET.values() if value == bucket), bucket
    assert sum(BUCKET_HOURS.values()) == 24


@pytest.mark.parametrize(
    ("hour", "expected"),
    [
        (0, "night"),
        (5, "night"),
        (22, "night"),
        (23, "night"),
        (6, "am_peak"),
        (9, "am_peak"),
        (10, "midday"),
        (15, "midday"),
        (16, "pm_peak"),
        (19, "pm_peak"),
        (20, "evening"),
        (21, "evening"),
    ],
)
def test_bucket_boundaries(hour: int, expected: str) -> None:
    assert bucket_for_hour(hour) == expected


def test_missing_and_out_of_range_hours_are_not_bucketed() -> None:
    assert bucket_for_hour(None) is None
    assert bucket_for_hour(24) is None
    assert bucket_for_hour(-1) is None


def test_severity_scale_is_inverted_from_the_source() -> None:
    # Unfallatlas: 1 = killed, 2 = serious, 3 = slight -> internally higher = worse
    assert SEVERITY_CLASS == {"1": 3, "2": 2, "3": 1}
    assert SEVERITY_CLASS["1"] > SEVERITY_CLASS["2"] > SEVERITY_CLASS["3"]
    assert SEVERE_THRESHOLD == 2


def test_risk_weights_are_a_convex_combination() -> None:
    assert set(RISK_WEIGHTS) == {"severity", "density", "rain"}
    assert sum(RISK_WEIGHTS.values()) == pytest.approx(1.0)
    assert all(0 < weight < 1 for weight in RISK_WEIGHTS.values())


def test_pseudonym_columns_are_the_only_denylist_escape_hatch() -> None:
    assert ALLOWED_PSEUDONYM_COLUMNS == frozenset({"plate_pseudonym", "plate_redacted_at"})
    for column in ALLOWED_PSEUDONYM_COLUMNS:
        assert any(token in column for token in PII_DENYLIST_TOKENS)
