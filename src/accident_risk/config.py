"""Environment-driven configuration.

The same settings object is usable in the Airflow container and in the
processing container: both receive identical environment variables and mount
the repository at the same paths (see docker-compose.yml).

Secrets (`PSEUDONYMIZATION_PEPPER`, database DSN) are read from the environment
only - never from files committed to git - and fail fast when missing, because a
pipeline that silently runs without a pseudonymization key would violate the
GDPR design.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

VALID_SOURCES = ("fixture", "unfallatlas")


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(
            f"Required environment variable {name!r} is missing or empty. "
            "Copy .env.example to .env and fill in the values."
        )
    return value


def _optional_int(name: str, default: int | None = None) -> int | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - configuration error path
        raise RuntimeError(f"Environment variable {name!r} must be an integer, got {raw!r}") from exc


def _path(name: str, default: Path) -> Path:
    raw = os.environ.get(name, "").strip()
    return Path(raw) if raw else default


@dataclass(frozen=True)
class Settings:
    """Immutable view of the runtime configuration."""

    dsn: str
    data_dir: Path
    raw_dir: Path
    staging_dir: Path
    reference_dir: Path
    fixture_path: Path
    outputs_dir: Path
    docs_img_dir: Path
    sql_dir: Path
    extract_source: str
    unfallatlas_urls: tuple[str, ...]
    pseudonymization_pepper: str
    raw_retention_days: int
    clean_retention_years: int
    analysis_window_years: int | None
    min_segment_accidents: int
    top_segments_heatmap: int

    def staging_file(self, logical_date: date) -> Path:
        """Deterministic handoff artifact between anonymize and load."""
        return self.staging_dir / logical_date.isoformat() / "clean_accidents.csv.gz"

    def output_dir(self, logical_date: date) -> Path:
        return self.outputs_dir / logical_date.isoformat()


def _build_dsn() -> str:
    explicit = os.environ.get("TRAFFIC_DB_DSN", "").strip()
    if explicit:
        return explicit
    user = os.environ.get("POSTGRES_USER", "traffic")
    password = _require("POSTGRES_PASSWORD")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    database = os.environ.get("POSTGRES_DB", "traffic")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Build (and cache) the settings from the environment."""
    data_dir = _path("DATA_DIR", PROJECT_ROOT / "data")
    urls = tuple(
        url.strip()
        for url in os.environ.get("UNFALLATLAS_URLS", "").split(",")
        if url.strip()
    )
    source = os.environ.get("EXTRACT_SOURCE", "fixture").strip().lower() or "fixture"
    if source not in VALID_SOURCES:
        raise RuntimeError(f"EXTRACT_SOURCE must be one of {VALID_SOURCES}, got {source!r}")
    if source == "unfallatlas" and not urls:
        raise RuntimeError(
            "EXTRACT_SOURCE=unfallatlas requires UNFALLATLAS_URLS to list at least one "
            "release URL (see README, section 'Data source')."
        )

    settings = Settings(
        dsn=_build_dsn(),
        data_dir=data_dir,
        raw_dir=_path("RAW_DIR", data_dir / "raw"),
        staging_dir=_path("STAGING_DIR", data_dir / "staging"),
        reference_dir=_path("REFERENCE_DIR", data_dir / "reference"),
        fixture_path=_path("FIXTURE_PATH", data_dir / "sample" / "unfallatlas_sample.csv.gz"),
        outputs_dir=_path("OUTPUTS_DIR", PROJECT_ROOT / "outputs"),
        docs_img_dir=_path("DOCS_IMG_DIR", PROJECT_ROOT / "docs" / "img"),
        sql_dir=_path("SQL_DIR", PROJECT_ROOT / "sql"),
        extract_source=source,
        unfallatlas_urls=urls,
        pseudonymization_pepper=_require("PSEUDONYMIZATION_PEPPER"),
        raw_retention_days=_optional_int("RAW_RETENTION_DAYS", 90) or 90,
        clean_retention_years=_optional_int("CLEAN_RETENTION_YEARS", 10) or 10,
        analysis_window_years=_optional_int("ANALYSIS_WINDOW_YEARS", None),
        min_segment_accidents=_optional_int("MIN_SEGMENT_ACCIDENTS", 20) or 20,
        top_segments_heatmap=_optional_int("TOP_SEGMENTS_HEATMAP", 12) or 12,
    )

    if not 5 <= settings.clean_retention_years <= 10:
        raise RuntimeError(
            "CLEAN_RETENTION_YEARS must stay within the agreed GDPR window of 5-10 years, "
            f"got {settings.clean_retention_years}."
        )
    if not 1 <= settings.raw_retention_days <= 120:
        raise RuntimeError(
            "RAW_RETENTION_DAYS must stay within 1-120 days (schema enforces 120 as the "
            f"hard ceiling), got {settings.raw_retention_days}."
        )
    return settings


def resolve_logical_date(value: date | datetime | str | None = None) -> date:
    """Normalize the Airflow logical date / CLI argument into a plain date."""
    if value is None:
        return date.today()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()
