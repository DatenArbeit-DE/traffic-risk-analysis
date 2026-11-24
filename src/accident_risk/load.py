"""Task 3: load_to_postgres.

Loads the staging artifact into `clean.accidents` and finishes the GDPR promise
of the run by redacting any plate value that is still sitting in the raw zone.

Implementation notes
--------------------
* Staging artifact -> temporary table -> `INSERT ... ON CONFLICT DO NOTHING`.
  The temp table exists only inside the loading transaction; it is an
  implementation detail of an ELT load, not a fourth entity in the data model.
* Natural-key deduplication makes reruns idempotent: re-processing the same
  release inserts zero rows.
* Plate redaction happens in the same transaction as the load, so a failed load
  can never leave clear-text plates behind.
"""

from __future__ import annotations

import logging
import math
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from . import db
from .anonymize import STAGING_COLUMNS
from .config import Settings

log = logging.getLogger(__name__)

LOAD_COLUMNS: tuple[str, ...] = STAGING_COLUMNS

# Explicit dtypes: the staging file is a contract between two tasks, so it is
# parsed with the same strictness as an external source.
STAGING_DTYPES: dict[str, str] = {
    "accident_key": "string",
    "logical_date": "string",
    "occurred_at": "string",
    "year": "int64",
    "month": "int64",
    "hour_of_day": "Int64",
    "time_bucket": "string",
    "state_code": "string",
    "district_code": "string",
    "district_label": "string",
    "accident_type_code": "string",
    "accident_type_label": "string",
    "segment_key": "string",
    "segment_label": "string",
    "severity_class": "int64",
    "is_severe": "int64",
    "road_condition": "string",
    "is_rain": "int64",
    "light_condition": "string",
    "participant_mix": "string",
    "plate_pseudonym": "string",
    "is_anonymized": "int64",
    "data_quality_flag": "string",
    "grid_lat": "float64",
    "grid_lon": "float64",
    "source": "string",
    "retention_until": "string",
}


def read_staging(path: Path) -> pd.DataFrame:
    """Read the staging artifact with explicit dtypes."""
    frame = pd.read_csv(path, dtype=STAGING_DTYPES)
    missing = [column for column in LOAD_COLUMNS if column not in frame.columns]
    if missing:
        raise RuntimeError(f"Staging file {path} is missing columns: {missing}")
    if not frame.empty and not bool(frame["is_anonymized"].eq(1).all()):
        raise RuntimeError("Staging file contains records that are not marked as anonymized")
    return frame


def _pg_value(value):
    """Convert pandas/numpy scalars into values COPY understands."""
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    return str(value) if isinstance(value, (pd.Timestamp, date)) else value


def run(settings: Settings, logical_date: date) -> dict:
    """Load one logical date into the clean layer and redact raw plates."""
    staging_path = settings.staging_file(logical_date)
    if not staging_path.exists():
        raise FileNotFoundError(
            f"Staging artifact {staging_path} not found; run anonymize_and_clean first."
        )

    frame = read_staging(staging_path)
    rows = [
        tuple(_pg_value(value) for value in record)
        for record in frame[list(LOAD_COLUMNS)].itertuples(index=False, name=None)
    ]

    column_list = ", ".join(LOAD_COLUMNS)
    with db.transaction(settings) as conn:
        db.execute(
            conn,
            # INCLUDING DEFAULTS keeps ingested_at working, INCLUDING CONSTRAINTS
            # makes the staging table reject invalid rows at COPY time instead of
            # at the final INSERT.
            "CREATE TEMP TABLE staging_accidents "
            "(LIKE clean.accidents INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP",
        )
        loaded = db.copy_rows(conn, "staging_accidents", LOAD_COLUMNS, rows)
        inserted = db.execute(
            conn,
            f"""
            INSERT INTO clean.accidents ({column_list})
            SELECT {column_list} FROM staging_accidents
            ON CONFLICT (accident_key) DO NOTHING
            """,
        )
        # GDPR completion step: the clear-text plate value must not survive.
        redacted = db.execute(
            conn,
            """
            UPDATE raw.accidents_raw
               SET payload = payload - 'plate_raw',
                   plate_redacted_at = now()
             WHERE logical_date = %(logical_date)s
               AND payload ? 'plate_raw'
               AND plate_redacted_at IS NULL
            """,
            {"logical_date": logical_date},
        )

    summary = {
        "logical_date": logical_date.isoformat(),
        "staging_rows": loaded,
        "inserted_rows": inserted,
        "skipped_duplicates": loaded - inserted,
        "raw_rows_plate_redacted": redacted,
    }
    log.info("load_to_postgres finished: %s", summary)
    return summary
