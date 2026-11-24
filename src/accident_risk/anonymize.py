"""Task 2: anonymize_and_clean.

Defensive GDPR layer. The Unfallatlas is published pre-anonymized, yet this step
stays mandatory, for four reasons documented in docs/decisions.md:

1. **Minimization is enforced here, not trusted upstream.** Only allowlisted
   fields are used; everything else that reached the raw zone is discarded.
2. **Vehicle participants are tokenized.** Per-vehicle identifiers collapse into
   a coarse participation mix; plates become HMAC-SHA256 pseudonyms.
3. **Data quality is made explicit.** Every rule that drops or flags a record is
   counted, so silent data loss is impossible.
4. **Retention is executed here** (raw zone purge) so storage limitation is part
   of the pipeline rather than an operational afterthought.

`is_anonymized` is set to true only after all of the above succeeded; the
database rejects anything else (see sql/01_schema.sql).
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import re
from datetime import date

import pandas as pd

from . import db, reference
from .config import Settings
from .constants import (
    ACCIDENT_TYPE_LABEL,
    ALLOWED_PSEUDONYM_COLUMNS,
    LIGHT_CONDITION,
    PARTICIPANT_COLUMNS,
    PII_DENYLIST_TOKENS,
    RAIN_CONDITIONS,
    ROAD_CONDITION,
    SEVERE_THRESHOLD,
    SEVERITY_CLASS,
    bucket_for_hour,
)

log = logging.getLogger(__name__)

# Canonical payload keys expected from extract (missing ones become empty).
PAYLOAD_COLUMNS: tuple[str, ...] = (
    "year",
    "month",
    "hour",
    "state_code",
    "district_code",
    "accident_type_code",
    "road_condition_code",
    "light_code",
    "severity_code",
    "is_car",
    "is_bicycle",
    "is_pedestrian",
    "is_motorcycle",
    "is_hgv",
    "is_other",
    "lon",
    "lat",
    "plate_raw",
)

# Column order of the staging artifact consumed by load_to_postgres.
STAGING_COLUMNS: tuple[str, ...] = (
    "accident_key",
    "logical_date",
    "occurred_at",
    "year",
    "month",
    "hour_of_day",
    "time_bucket",
    "state_code",
    "district_code",
    "district_label",
    "accident_type_code",
    "accident_type_label",
    "segment_key",
    "segment_label",
    "severity_class",
    "is_severe",
    "road_condition",
    "is_rain",
    "light_condition",
    "participant_mix",
    "plate_pseudonym",
    "is_anonymized",
    "data_quality_flag",
    "grid_lat",
    "grid_lon",
    "source",
    "retention_until",
)

# German plates: 1-3 letters for the city, 1-2 letters, 1-4 digits (with optional
# separator characters). Used only to validate input, never to store it.
_PLATE_PATTERN = re.compile(r"^[A-Z]{1,3}[A-Z]{1,2}[0-9]{1,4}[EH]?$")
_TRUTHY = frozenset({"1", "1.0", "true", "ja", "yes"})


def normalize_plate(value: str | None) -> str | None:
    """Normalize a plate for hashing; return None when it is not a valid plate."""
    if value is None or pd.isna(value):
        return None
    candidate = re.sub(r"[\s\-\.]", "", str(value)).upper()
    if not candidate or not _PLATE_PATTERN.match(candidate):
        return None
    return candidate


def pseudonymize(value: str, pepper: str) -> str:
    """HMAC-SHA256 pseudonym.

    Plain sha256(plate) is brute-forceable because the German plate space is
    only ~10^7 keys; keying the hash with a secret pepper removes that attack.
    """
    return hmac.new(pepper.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()


def participant_mix(row: dict) -> str:
    """Collapse participation flags into a coarse, non-identifying mix string."""
    labels = [
        label
        for column, label in PARTICIPANT_COLUMNS.items()
        if str(row.get(column) or "").strip().lower() in _TRUTHY
    ]
    return "+".join(labels) if labels else "unspecified"


def assert_no_pii_columns(columns: list[str] | tuple[str, ...]) -> None:
    """Fail loudly if a channel name looks like personal data."""
    offenders = [
        column
        for column in columns
        if column not in ALLOWED_PSEUDONYM_COLUMNS
        and any(token in column.casefold() for token in PII_DENYLIST_TOKENS)
    ]
    if offenders:
        raise RuntimeError(f"PII-shaped columns detected in the clean layer: {offenders}")


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _text(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().replace({"": None})



def clean_records(
    records: list[dict],
    settings: Settings,
    logical_date: date,
) -> tuple[pd.DataFrame, dict]:
    """Apply the anonymize + clean rules and return the staging frame plus counts."""
    pepper = settings.pseudonymization_pepper
    stats: dict = {
        "rows_in": len(records),
        "rows_out": 0,
        "invalid_time_removed": 0,
        "invalid_severity_removed": 0,
        "duplicates_removed": 0,
        "invalid_plate_removed": 0,
        "plate_pseudonyms": 0,
        "missing_hour": 0,
        "unknown_road_condition": 0,
    }
    if not records:
        return pd.DataFrame(columns=list(STAGING_COLUMNS)), stats

    df = pd.DataFrame(records).reset_index(drop=True)
    for column in PAYLOAD_COLUMNS:
        if column not in df.columns:
            df[column] = None

    # --- calendar fields -----------------------------------------------------
    df["year_value"] = _numeric(df["year"])
    df["month_value"] = _numeric(df["month"])
    df["hour_value"] = _numeric(df["hour"])
    valid_calendar = df["year_value"].between(1990, 2100) & df["month_value"].between(1, 12)
    valid_calendar = valid_calendar.fillna(False)
    stats["invalid_time_removed"] = int((~valid_calendar).sum())
    df = df[valid_calendar].reset_index(drop=True)

    # Hours outside 00-23 cannot be trusted; the record is kept but flagged so
    # that time-based metrics stay honest instead of silently shifting mass.
    df["hour_of_day"] = df["hour_value"].where(df["hour_value"].between(0, 23))
    df["occurred_at"] = pd.to_datetime(
        {
            "year": df["year_value"].astype(int),
            "month": df["month_value"].astype(int),
            "day": 1,
        }
    ) + pd.to_timedelta(df["hour_of_day"].fillna(0), unit="h")

    # --- severity, conditions, labels ---------------------------------------
    df["severity_class"] = _text(df["severity_code"]).map(SEVERITY_CLASS)
    invalid_severity = df["severity_class"].isna()
    stats["invalid_severity_removed"] = int(invalid_severity.sum())
    df = df[~invalid_severity].reset_index(drop=True)
    df["severity_class"] = df["severity_class"].astype(int)

    road = _text(df["road_condition_code"]).map(ROAD_CONDITION)
    stats["unknown_road_condition"] = int(road.isna().sum())
    df["road_condition"] = road.fillna("unknown")
    df["is_rain"] = df["road_condition"].isin(RAIN_CONDITIONS)
    df["light_condition"] = _text(df["light_code"]).map(LIGHT_CONDITION).fillna("unknown")

    df["district_code"] = _text(df["district_code"]).fillna("unknown")
    df["state_code"] = _text(df["state_code"])
    df["accident_type_code"] = _text(df["accident_type_code"]).fillna("unknown")
    df["accident_type_label"] = (
        df["accident_type_code"].map(ACCIDENT_TYPE_LABEL).fillna("Unknown accident type")
    )
    labels = reference.load_district_labels(settings.reference_dir / "kreise.csv")
    df["district_label"] = [reference.district_label(code, labels) for code in df["district_code"]]
    df["segment_key"] = df["district_code"] + "|" + df["accident_type_code"]
    df["segment_label"] = df["district_label"] + " - " + df["accident_type_label"]

    # --- participant tokenization -------------------------------------------
    df["participant_mix"] = [
        participant_mix(row) for row in df[list(PARTICIPANT_COLUMNS)].to_dict("records")
    ]

    # --- location (already 100 m gridded upstream; kept coarse) -------------
    latitude = _numeric(df["lat"]).where(_numeric(df["lat"]).between(-90, 90))
    df["grid_lat"] = latitude.round(5)
    df["grid_lon"] = _numeric(df["lon"]).where(_numeric(df["lon"]).between(-180, 180)).round(5)

    # --- pseudonyms ----------------------------------------------------------
    natural_key = (
        df["occurred_at"].dt.strftime("%Y-%m-%dT%H")
        + "|"
        + df["district_code"]
        + "|"
        + df["accident_type_code"]
        + "|"
        + df["grid_lat"].round(3).astype("string").fillna("na")
        + "|"
        + df["grid_lon"].round(3).astype("string").fillna("na")
    )
    df["accident_key"] = [pseudonymize(value, pepper) for value in natural_key]

    raw_plates = _text(df["plate_raw"])
    normalized_plates = [normalize_plate(value) for value in raw_plates]
    stats["invalid_plate_removed"] = int(
        sum(1 for raw, norm in zip(raw_plates, normalized_plates) if pd.notna(raw) and norm is None)
    )
    df["plate_pseudonym"] = [
        pseudonymize(norm, pepper) if norm else None for norm in normalized_plates
    ]
    stats["plate_pseudonyms"] = int(sum(1 for norm in normalized_plates if norm))

    before = len(df)
    df = df.drop_duplicates(subset=["accident_key"], keep="first").reset_index(drop=True)
    stats["duplicates_removed"] = before - len(df)

    # --- derived / governance fields ----------------------------------------
    df["is_severe"] = df["severity_class"] >= SEVERE_THRESHOLD
    df["time_bucket"] = [
        bucket_for_hour(int(hour)) if pd.notna(hour) else None for hour in df["hour_of_day"]
    ]
    df["data_quality_flag"] = ["missing_hour" if pd.isna(hour) else None for hour in df["hour_of_day"]]
    stats["missing_hour"] = int(df["data_quality_flag"].notna().sum())
    df["logical_date"] = logical_date.isoformat()
    df["source"] = settings.extract_source
    df["retention_until"] = (
        df["occurred_at"] + pd.DateOffset(years=settings.clean_retention_years)
    ).dt.date
    df["is_anonymized"] = 1
    df["is_severe"] = df["is_severe"].astype(int)
    df["is_rain"] = df["is_rain"].astype(int)
    df["year"] = df["year_value"].astype(int)
    df["month"] = df["month_value"].astype(int)
    df["hour_of_day"] = df["hour_of_day"].astype("Int64")
    df["occurred_at"] = df["occurred_at"].dt.strftime("%Y-%m-%d %H:%M:%S")

    staging = df[list(STAGING_COLUMNS)].copy()
    assert_no_pii_columns(list(staging.columns))
    stats["rows_out"] = len(staging)
    return staging, stats



def run(settings: Settings, logical_date: date) -> dict:
    """Anonymize + clean one logical date and write the staging artifact."""
    staging_path = settings.staging_file(logical_date)
    staging_path.parent.mkdir(parents=True, exist_ok=True)

    with db.transaction(settings) as conn:
        rows = db.fetch_all(
            conn,
            "SELECT payload FROM raw.accidents_raw WHERE logical_date = %(logical_date)s ORDER BY ingest_id",
            {"logical_date": logical_date},
        )
        staging, stats = clean_records([row["payload"] for row in rows], settings, logical_date)
        staging.to_csv(staging_path, index=False, compression="gzip", na_rep="")
        purged = db.purge_expired(conn, settings)

    summary = {
        "logical_date": logical_date.isoformat(),
        "staging_file": str(staging_path),
        "clean_retention_years": settings.clean_retention_years,
        "purged_rows": purged,
        **stats,
    }
    log.info("anonymize_and_clean finished: %s", summary)
    return summary
