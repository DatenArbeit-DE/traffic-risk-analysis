"""Task 2 - anonymization and cleaning rules.

The tests are split into the cryptographic primitives (plate normalization,
HMAC pseudonyms, participant tokenization) and the cleaning pipeline, because
the first group is what a reviewer audits for GDPR compliance and the second
group is what protects the metrics from garbage input.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from accident_risk import anonymize
from accident_risk.config import Settings

LOGICAL_DATE = date(2024, 3, 1)


def payload(**overrides: str | None) -> dict[str, str | None]:
    """A row exactly as `extract.project_row` would emit it."""
    base = {
        "year": "2022",
        "month": "7",
        "hour": "17",
        "state_code": "05",
        "district_code": "05315",
        "accident_type_code": "4",
        "road_condition_code": "2",
        "light_code": "0",
        "severity_code": "3",
        "is_car": "1",
        "is_bicycle": "0",
        "is_pedestrian": "0",
        "is_motorcycle": "0",
        "is_hgv": "0",
        "is_other": "0",
        "lon": "6.977",
        "lat": "50.902",
    }
    base.update(overrides)
    return base


# --- plate handling -----------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (" M-AB 123 ", "MAB123"),
        ("b am 1234", "BAM1234"),
        ("HH-XY-9999", "HHXY9999"),
        ("M AB 1234 E", "MAB1234E"),
        ("M AB 1234 H", "MAB1234H"),
        ("BMW1234", "BMW1234"),
    ],
)
def test_normalize_plate_accepts_german_formats(raw: str, expected: str) -> None:
    assert anonymize.normalize_plate(raw) == expected


@pytest.mark.parametrize(
    "raw", ["", "   ", None, pd.NA, "1234", "M AB", "M AB 12345", "M AB 1234 X", "???"]
)
def test_normalize_plate_rejects_invalid_values(raw) -> None:
    assert anonymize.normalize_plate(raw) is None


def test_pseudonym_is_keyed_and_deterministic() -> None:
    first = anonymize.pseudonymize("MAB123", "pepper-a")
    again = anonymize.pseudonymize("MAB123", "pepper-a")
    other_pepper = anonymize.pseudonymize("MAB123", "pepper-b")

    assert first == again
    assert first != other_pepper
    assert len(first) == 64 and all(character in "0123456789abcdef" for character in first)


def test_pseudonym_is_not_an_unkeyed_hash() -> None:
    import hashlib

    plate = "MAB123"
    assert anonymize.pseudonymize(plate, "pepper") != hashlib.sha256(plate.encode()).hexdigest()


def test_participant_flags_collapse_into_a_mix() -> None:
    assert anonymize.participant_mix({"is_car": "1", "is_bicycle": "0"}) == "car"
    assert anonymize.participant_mix({"is_car": "1", "is_hgv": "1"}) == "car+hgv"
    assert (
        anonymize.participant_mix({"is_bicycle": "true", "is_pedestrian": "ja"})
        == "bicycle+pedestrian"
    )
    assert anonymize.participant_mix({}) == "unspecified"
    assert anonymize.participant_mix({"is_car": "0"}) == "unspecified"


def test_pii_guard_rejects_denylisted_column_names() -> None:
    with pytest.raises(RuntimeError):
        anonymize.assert_no_pii_columns(["accident_key", "plate"])
    anonymize.assert_no_pii_columns(["accident_key", "plate_pseudonym"])


# --- cleaning pipeline --------------------------------------------------------


def test_clean_records_maps_columns_and_severity(settings: Settings) -> None:
    frame, stats = anonymize.clean_records([payload()], settings, LOGICAL_DATE)

    assert list(frame.columns) == list(anonymize.STAGING_COLUMNS)
    assert stats["rows_in"] == 1 and stats["rows_out"] == 1 and stats["duplicates_removed"] == 0

    row = frame.iloc[0]
    assert row["severity_class"] == 1  # Kategorie 3 = slight -> internal 1
    assert row["is_severe"] == 0
    assert row["time_bucket"] == "pm_peak"
    assert row["road_condition"] == "wet" and row["is_rain"] == 1
    assert row["light_condition"] == "daylight"
    assert row["participant_mix"] == "car"
    assert row["is_anonymized"] == 1
    assert row["logical_date"] == LOGICAL_DATE.isoformat()
    assert row["source"] == settings.extract_source
    assert row["retention_until"] == date(2032, 7, 1)
    assert len(row["accident_key"]) == 64
    assert row["segment_key"] == "05315|4"
    assert row["district_label"] == "Cologne"


@pytest.mark.parametrize(
    ("code", "severity_class", "is_severe"),
    [("1", 3, 1), ("2", 2, 1), ("3", 1, 0)],
)
def test_severity_codes_are_inverted(
    settings: Settings, code: str, severity_class: int, is_severe: int
) -> None:
    frame, _ = anonymize.clean_records([payload(severity_code=code)], settings, LOGICAL_DATE)
    assert int(frame.iloc[0]["severity_class"]) == severity_class
    assert int(frame.iloc[0]["is_severe"]) == is_severe


def test_rain_is_derived_from_road_condition_only(settings: Settings) -> None:
    frame, stats = anonymize.clean_records(
        [
            payload(road_condition_code="1", accident_type_code="1"),
            payload(road_condition_code="2", accident_type_code="2"),
            payload(road_condition_code="3", accident_type_code="3"),
            payload(road_condition_code="9", accident_type_code="4"),
        ],
        settings,
        LOGICAL_DATE,
    )
    conditions = dict(zip(frame["accident_type_code"], frame["road_condition"]))
    assert conditions == {"1": "dry", "2": "wet", "3": "snow", "4": "unknown"}
    assert int(frame["is_rain"].sum()) == 1
    assert stats["unknown_road_condition"] == 1


def test_invalid_records_are_dropped_and_counted(settings: Settings) -> None:
    frame, stats = anonymize.clean_records(
        [
            payload(),
            payload(month="13"),  # impossible calendar
            payload(year="1899"),  # outside the accepted window
            payload(severity_code="9"),  # unknown Kategorie
        ],
        settings,
        LOGICAL_DATE,
    )
    assert stats["invalid_time_removed"] == 2
    assert stats["invalid_severity_removed"] == 1
    assert stats["rows_out"] == 1
    assert len(frame) == 1



def test_missing_hour_is_flagged_not_dropped(settings: Settings) -> None:
    frame, stats = anonymize.clean_records(
        # Different district so the two rows cannot collapse into one dedupe key:
        # a missing hour removes the only field that told them apart.
        [payload(hour=""), payload(hour=None, district_code="05316")],
        settings,
        LOGICAL_DATE,
    )

    assert len(frame) == 2  # the accident still happened; only the time is unusable
    assert frame["hour_of_day"].isna().all()
    assert frame["time_bucket"].isna().all()
    assert set(frame["data_quality_flag"]) == {"missing_hour"}
    assert stats["missing_hour"] == 2


def test_duplicates_on_the_natural_key_are_removed(settings: Settings) -> None:
    frame, stats = anonymize.clean_records(
        [payload(), payload(), payload(hour="18")], settings, LOGICAL_DATE
    )

    assert stats["duplicates_removed"] == 1
    assert len(frame) == 2
    assert frame["accident_key"].is_unique


def test_plate_pseudonyms_are_derived_and_invalid_plates_are_counted(settings: Settings) -> None:
    frame, stats = anonymize.clean_records(
        [
            payload(plate_raw="M-AB 123", hour="8"),
            payload(plate_raw="M AB 123", hour="9"),  # same plate, different accident
            payload(plate_raw="not-a-plate", hour="10"),
        ],
        settings,
        LOGICAL_DATE,
    )

    assert stats["plate_pseudonyms"] == 2
    assert stats["invalid_plate_removed"] == 1
    pseudonyms = frame["plate_pseudonym"].tolist()
    assert pseudonyms[0] == pseudonyms[1]  # same plate -> same pseudonym
    assert pd.isna(pseudonyms[2])  # invalid plate -> no identifier at all
    assert "plate_raw" not in frame.columns
    flattened = " ".join(str(value) for value in frame.astype(str).values.ravel()).lower()
    assert "m-ab" not in flattened and "m ab" not in flattened


def test_empty_input_returns_the_staging_contract(settings: Settings) -> None:
    frame, stats = anonymize.clean_records([], settings, LOGICAL_DATE)
    assert list(frame.columns) == list(anonymize.STAGING_COLUMNS)
    assert frame.empty
    assert stats["rows_in"] == 0 and stats["rows_out"] == 0


def test_retention_horizon_follows_configuration(settings: Settings) -> None:
    from dataclasses import replace

    five_years = replace(settings, clean_retention_years=5, pseudonymization_pepper="other-pepper")
    frame, _ = anonymize.clean_records([payload(year="2020", month="2")], five_years, LOGICAL_DATE)
    assert frame.iloc[0]["retention_until"] == date(2025, 2, 1)

    # A different pepper must produce a different accident key for the same row.
    baseline, _ = anonymize.clean_records([payload(year="2020", month="2")], settings, LOGICAL_DATE)
    assert frame.iloc[0]["accident_key"] != baseline.iloc[0]["accident_key"]

