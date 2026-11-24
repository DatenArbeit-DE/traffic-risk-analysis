"""End-to-end pipeline behaviour against PostgreSQL.

Everything here runs against the disposable `traffic_test` database that
`conftest.warehouse` rebuilt from the shipped SQL. The suite proves the three
properties a reviewer cares about: the numbers are right, reruns are safe, and
no personal data can survive anywhere in the warehouse.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from accident_risk import anonymize, db, extract, load, metrics, viz
from accident_risk.config import Settings

pytestmark = pytest.mark.integration

LOGICAL_DATE = date(2024, 3, 1)
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def scalar(settings: Settings, sql: str, params: dict | None = None):
    with db.transaction(settings) as conn:
        row = db.fetch_one(conn, sql, params) or {}
    return next(iter(row.values()))


# --- task 1: extract ----------------------------------------------------------


def test_extract_lands_the_release_into_the_raw_zone(pipeline: dict, warehouse: Settings) -> None:
    extract_summary = pipeline["extract"]

    assert extract_summary["raw_rows_loaded"] == 90_000
    assert extract_summary["logical_date"] == LOGICAL_DATE.isoformat()
    assert set(extract_summary["dropped_source_columns"]) == {
        "Name",
        "Strasse",
        "Halter",
        "Unfallort_Freitext",
    }
    assert scalar(warehouse, "SELECT count(*) FROM raw.accidents_raw WHERE logical_date = %(d)s", {"d": LOGICAL_DATE}) == 90_000


def test_raw_payload_never_contains_pii_columns(warehouse: Settings) -> None:
    keys = scalar(
        warehouse,
        """
        SELECT coalesce(array_agg(DISTINCT key), '{}')
        FROM raw.accidents_raw, jsonb_object_keys(payload) AS key
        """,
    )
    forbidden = {key for key in keys if key.casefold() in {"name", "strasse", "halter", "unfallort_freitext"}}
    assert forbidden == set()
    assert "plate_raw" not in keys  # redacted by task 3 in the same run


def test_extract_is_partition_replacing(pipeline: dict, warehouse: Settings) -> None:
    extract.run(warehouse, LOGICAL_DATE)
    assert scalar(warehouse, "SELECT count(*) FROM raw.accidents_raw") == 90_000

    # Restore the post-load state (plates redacted) for the later assertions.
    anonymize.run(warehouse, LOGICAL_DATE)
    load.run(warehouse, LOGICAL_DATE)
    assert scalar(warehouse, "SELECT count(*) FROM raw.accidents_raw") == 90_000


# --- task 2 + 3: anonymize and load ------------------------------------------


def test_anonymize_counts_every_dropped_record(pipeline: dict) -> None:
    stats = pipeline["anonymize"]

    assert stats["rows_in"] == 90_000
    assert stats["rows_out"] < stats["rows_in"]
    assert stats["invalid_time_removed"] > 0  # injected: impossible months
    assert stats["invalid_severity_removed"] > 0  # injected: unknown Kategorie
    assert stats["duplicates_removed"] > 0  # injected: repeated accidents
    assert stats["invalid_plate_removed"] > 0  # injected: malformed plates
    assert stats["plate_pseudonyms"] > 0
    assert stats["missing_hour"] > 0  # injected: hour 25
    assert stats["rows_in"] == stats["rows_out"] + sum(
        stats[key]
        for key in ("invalid_time_removed", "invalid_severity_removed", "duplicates_removed")
    )


def test_staging_artifact_round_trips(pipeline: dict, warehouse: Settings) -> None:
    path = warehouse.staging_file(LOGICAL_DATE)
    assert path.exists()

    frame = load.read_staging(path)
    assert len(frame) == pipeline["anonymize"]["rows_out"]
    assert frame["is_anonymized"].eq(1).all()
    assert "plate_raw" not in frame.columns
    assert frame["accident_key"].is_unique


def test_load_matches_the_staging_rows(pipeline: dict, warehouse: Settings) -> None:
    load_summary = pipeline["load"]

    assert load_summary["staging_rows"] == pipeline["anonymize"]["rows_out"]
    assert load_summary["inserted_rows"] == load_summary["staging_rows"]
    assert load_summary["skipped_duplicates"] == 0
    assert scalar(warehouse, "SELECT count(*) FROM clean.accidents") == load_summary["inserted_rows"]


def test_load_is_idempotent(pipeline: dict, warehouse: Settings) -> None:
    before = scalar(warehouse, "SELECT count(*) FROM clean.accidents")
    again = load.run(warehouse, LOGICAL_DATE)

    assert again["inserted_rows"] == 0
    assert again["skipped_duplicates"] == again["staging_rows"]
    assert scalar(warehouse, "SELECT count(*) FROM clean.accidents") == before


def test_clear_text_plates_are_redacted_in_the_raw_zone(warehouse: Settings) -> None:
    remaining = scalar(
        warehouse,
        "SELECT count(*) FROM raw.accidents_raw WHERE payload ? 'plate_raw'",
    )
    assert remaining == 0

    redacted = scalar(
        warehouse,
        "SELECT count(*) FROM raw.accidents_raw WHERE plate_redacted_at IS NOT NULL",
    )
    assert redacted > 0


def test_clean_layer_refuses_non_anonymized_rows(warehouse: Settings) -> None:
    with pytest.raises(Exception):
        with db.transaction(warehouse) as conn:
            db.execute(
                conn,
                """
                INSERT INTO clean.accidents (
                    accident_key, logical_date, occurred_at, year, month, hour_of_day,
                    time_bucket, district_code, segment_key, severity_class, is_severe,
                    road_condition, is_rain, is_anonymized, source, retention_until
                )
                VALUES (
                    repeat('a', 64), %(d)s, %(d)s::date + interval '7 hours', 2022, 7, 7,
                    'am_peak', '05315', '05315|4', 1, false,
                    'dry', false, false, 'test', %(d)s::date + interval '5 years'
                )
                """,
                {"d": LOGICAL_DATE},
            )


# --- task 4: analysis ---------------------------------------------------------


def test_metric_layer_has_one_all_row_per_segment(warehouse: Settings) -> None:
    with db.transaction(warehouse) as conn:
        row = db.fetch_one(
            conn,
            """
            SELECT count(DISTINCT segment_key) AS segments,
                   count(*) FILTER (WHERE time_bucket = 'all') AS all_rows,
                   count(*) FILTER (WHERE time_bucket <> 'all') AS bucket_rows
            FROM analytics.risk_metrics
            """,
        )
    assert row["all_rows"] == row["segments"]
    assert row["bucket_rows"] <= row["segments"] * 5


def test_analytics_metrics_are_bounded_and_ranked(pipeline: dict, warehouse: Settings) -> None:
    report = pipeline["analyze"]

    assert report["segments"] > 0
    assert report["rows_written"] == report["segments"] * 6  # 1 'all' + 5 buckets per segment
    assert report["analysed_accidents"] > 0
    assert 0 <= report["max_high_risk_score"] <= 100
    assert 0 <= report["avg_severity_ratio"] <= 1
    assert 0 <= report["avg_rain_share"] <= 1

    with db.transaction(warehouse) as conn:
        checks = db.fetch_one(
            conn,
            """
            SELECT count(*) FILTER (WHERE high_risk_score NOT BETWEEN 0 AND 100) AS bad_scores,
                   count(*) FILTER (WHERE severity_ratio NOT BETWEEN 0 AND 1) AS bad_ratios,
                   count(*) FILTER (WHERE time_bucket = 'all' AND time_of_day_risk_ratio IS NOT NULL) AS all_with_ratio,
                   count(*) FILTER (WHERE time_bucket <> 'all' AND time_of_day_risk_ratio IS NULL) AS buckets_without_ratio
            FROM analytics.risk_metrics
            """,
        )
    assert checks["bad_scores"] == 0
    assert checks["bad_ratios"] == 0
    assert checks["all_with_ratio"] == 0
    assert checks["buckets_without_ratio"] == 0


def test_bucket_counts_sum_to_the_segment_total(pipeline: dict, warehouse: Settings) -> None:
    with db.transaction(warehouse) as conn:
        mismatches = db.fetch_all(
            conn,
            """
            WITH totals AS (
                SELECT segment_key, accident_count AS total FROM analytics.risk_metrics WHERE time_bucket = 'all'
            ), buckets AS (
                SELECT segment_key, sum(accident_count) AS bucketed
                FROM analytics.risk_metrics WHERE time_bucket <> 'all'
                GROUP BY segment_key
            )
            SELECT t.segment_key, t.total, b.bucketed
            FROM totals t JOIN buckets b USING (segment_key)
            WHERE t.total <> b.bucketed
            """,
        )
    assert mismatches == []


def test_top_segment_is_the_highest_scoring_one(pipeline: dict, warehouse: Settings) -> None:
    top = pipeline["analyze"]["top_segments"][0]
    best = scalar(
        warehouse,
        "SELECT max(high_risk_score) FROM analytics.risk_metrics WHERE time_bucket = 'all'",
    )
    assert abs(float(top["high_risk_score"]) - float(best)) < 0.01
    assert top["accident_count"] >= pipeline["analyze"]["min_segment_accidents"]


def test_time_of_day_answers_question_two(pipeline: dict) -> None:
    buckets = {
        row["time_bucket"]: float(row["avg_time_of_day_risk_ratio"])
        for row in pipeline["analyze"]["time_of_day"]
    }

    assert set(buckets) == {"night", "am_peak", "midday", "pm_peak", "evening"}
    assert all(row["accident_count"] > 0 for row in pipeline["analyze"]["time_of_day"])
    # Ratios are normalised per segment, so a bucket average near 1.0 means
    # "as expected". Nights must be under-represented and the evening rush over.
    assert buckets["night"] < 0.8
    assert buckets["pm_peak"] > 1.2
    assert buckets["pm_peak"] > buckets["night"]


def test_analysis_is_a_full_refresh(pipeline: dict, warehouse: Settings) -> None:
    before = scalar(warehouse, "SELECT count(*) FROM analytics.risk_metrics")
    scores_before = scalar(
        warehouse, "SELECT round(sum(high_risk_score), 4) FROM analytics.risk_metrics"
    )

    again = metrics.run(warehouse, LOGICAL_DATE)

    assert again["rows_written"] == before
    assert scalar(warehouse, "SELECT count(*) FROM analytics.risk_metrics") == before
    assert scalar(
        warehouse, "SELECT round(sum(high_risk_score), 4) FROM analytics.risk_metrics"
    ) == scores_before


# --- task 5: visualizations ---------------------------------------------------


def test_three_charts_and_their_data_exports_exist(pipeline: dict, warehouse: Settings) -> None:
    report = pipeline["visualize"]
    output_dir = warehouse.output_dir(LOGICAL_DATE)

    assert len(report["charts"]) == 3
    for name in report["charts"]:
        png = output_dir / f"{name}.png"
        assert png.exists(), png
        assert png.read_bytes()[:8] == PNG_MAGIC
        assert (output_dir / f"{name}.csv").exists(), name

    for name in report["charts"]:
        assert (warehouse.docs_img_dir / f"{name}.png").exists()


def test_governance_report_passes(pipeline: dict, warehouse: Settings) -> None:
    from accident_risk.cli import verify

    report = verify(warehouse)

    assert report["passed"] is True, report["checks"]
    assert report["pii_named_columns"] == {}
    assert report["checks"]["no_unanonymized_rows"]
    assert report["checks"]["no_plates_left_in_raw"]
    assert report["counts"]["clean_rows"] == pipeline["load"]["inserted_rows"]


def test_staging_reader_rejects_uanonymized_rows(pipeline: dict, warehouse: Settings, tmp_path) -> None:
    import pandas as pd

    frame = pd.read_csv(warehouse.staging_file(LOGICAL_DATE), dtype=str, nrows=5)
    frame.loc[0, "is_anonymized"] = "0"
    bad = tmp_path / "bad.csv"
    frame.to_csv(bad, index=False)

    with pytest.raises(RuntimeError):
        load.read_staging(bad)

