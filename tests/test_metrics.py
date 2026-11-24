"""Task 4 - metric definitions.

The SQL is the source of truth for the numbers, so the tests here bind the two
risks that would silently change results: drift between the Python constants and
the SQL parameters, and a wrong analysis window. Behavioural checks on real data
live in `test_pipeline.py`.
"""

from __future__ import annotations

import re
from datetime import date, timedelta

import pytest

from accident_risk import metrics
from accident_risk.config import Settings
from accident_risk.constants import BUCKET_HOURS, RISK_WEIGHTS

PLACEHOLDER = re.compile(r"%\((\w+)\)s")


def test_window_start_without_a_limit_uses_all_data(settings: Settings) -> None:
    assert metrics.window_start(settings, date(2024, 3, 1)) is None


def test_window_start_subtracts_the_configured_years(settings: Settings) -> None:
    from dataclasses import replace

    three_years = replace(settings, analysis_window_years=3)
    assert metrics.window_start(three_years, date(2024, 3, 1)) == date(2021, 3, 1)


def test_window_start_handles_the_leap_day(settings: Settings) -> None:
    from dataclasses import replace

    one_year = replace(settings, analysis_window_years=1)
    assert metrics.window_start(one_year, date(2024, 2, 29)) == date(2023, 2, 28)


def test_metric_params_bind_every_constant(settings: Settings) -> None:
    params = metrics.metric_params(settings, date(2024, 3, 1))

    assert params["bucket_labels"] == list(BUCKET_HOURS)
    assert sum(params["bucket_hours"]) == 24.0
    assert params["min_segment_accidents"] == settings.min_segment_accidents
    assert params["retention_years"] == settings.clean_retention_years
    assert params["w_severity"] == pytest.approx(RISK_WEIGHTS["severity"])
    assert params["w_density"] == pytest.approx(RISK_WEIGHTS["density"])
    assert params["w_rain"] == pytest.approx(RISK_WEIGHTS["rain"])


def test_every_sql_placeholder_is_bound(settings: Settings) -> None:
    params = metrics.metric_params(settings, date(2024, 3, 1))
    placeholders = set(PLACEHOLDER.findall(metrics.METRIC_SQL))

    assert placeholders, "METRIC_SQL should be parameterized"
    assert placeholders <= set(params), f"unbound placeholders: {placeholders - set(params)}"


def test_scoring_uses_the_documented_weights(settings: Settings) -> None:
    # Guards against an accidental hard-coded weight appearing in the SQL.
    weights = re.findall(r"%\(w_(\w+)\)s", metrics.METRIC_SQL)
    assert set(weights) == {"severity", "density", "rain"}
    assert "100 *" in metrics.METRIC_SQL


def test_segment_floor_and_all_row_are_enforced_in_sql() -> None:
    assert "HAVING count(*) >= %(min_segment_accidents)s" in metrics.METRIC_SQL
    assert "'all'::text AS time_bucket" in metrics.METRIC_SQL
    assert "FROM clean.accidents" in metrics.METRIC_SQL


def test_analysis_window_filters_by_occurrence_not_ingestion() -> None:
    assert "occurred_at >= %(window_start)s::date" in metrics.METRIC_SQL


def test_retention_until_is_derived_from_the_logical_date() -> None:
    assert "make_interval(years => %(retention_years)s)" in metrics.METRIC_SQL


def test_risk_ratio_is_per_hour_relative_to_the_segment_average() -> None:
    # accidents/hour in the bucket divided by the segment's accidents/hour overall
    assert "bc.accidents / bh.hours" in metrics.METRIC_SQL
    assert "n.accidents / 24.0" in metrics.METRIC_SQL


@pytest.mark.parametrize("sql", [metrics.OVERALL_SQL, metrics.TOP_SEGMENTS_SQL, metrics.BUCKET_SQL])
def test_read_models_query_the_analytics_layer(sql: str) -> None:
    assert "analytics.risk_metrics" in sql


def test_top_segments_query_is_parameterized() -> None:
    assert "%(limit)s" in metrics.TOP_SEGMENTS_SQL


def test_metric_defaults_match_the_schema_contract(settings: Settings) -> None:
    assert settings.min_segment_accidents >= 1
    assert settings.top_segments_heatmap >= 1
    assert timedelta(days=1) < timedelta(days=settings.raw_retention_days) <= timedelta(days=120)
