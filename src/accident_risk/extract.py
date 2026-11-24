"""Task 1: extract_load_raw.

Reads the configured release (offline fixture by default, Unfallatlas release
URLs when enabled), projects every record through a **strict allowlist** and
lands the result in `raw.accidents_raw`.

Design decisions
----------------
* Allowlist, not denylist: only fields with a documented downstream use are
  persisted. Unknown columns are dropped and reported, which keeps the raw zone
  free of names, addresses, free text and anything a future release adds.
* Plate-like columns are the single exception. They are carried into the raw
  zone under `plate_raw` only so that the next task can derive a pseudonym, and
  are removed again by `load_to_postgres` in the same run.
* Idempotency: the raw partition for a logical date is replaced, so re-running
  the task cannot duplicate rows.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import logging
import shutil
import urllib.request
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator

from . import db
from .config import Settings

log = logging.getLogger(__name__)

USER_AGENT = "urban-traffic-accident-risk/1.0 (portfolio project)"

# Source column -> canonical payload key. Extend deliberately: every entry here
# is a field we are willing to store for a limited time.
SOURCE_FIELD_ALLOWLIST: dict[str, str] = {
    "UJAHR": "year",
    "UMONAT": "month",
    "USTUNDE": "hour",
    "ULAND": "state_code",
    "UKREIS": "district_code",
    "UART": "accident_type_code",
    "UPOART": "road_condition_code",
    "ULICHT": "light_code",
    "Kategorie": "severity_code",
    "IstPKW": "is_car",
    "IstRad": "is_bicycle",
    "IstFuss": "is_pedestrian",
    "IstKrad": "is_motorcycle",
    "IstGueter": "is_hgv",
    "IstGüter": "is_hgv",
    "IstSonstige": "is_other",
    "XGCSWGS84": "lon",
    "YGCSWGS84": "lat",
    # Plate-like identifiers: transient, pseudonymized in the next task.
    "KENNZEICHEN": "plate_raw",
    "plate": "plate_raw",
    "vehicle_plate": "plate_raw",
}

# Case-insensitive lookup so minor header variations between releases are safe.
NORMALIZED_ALLOWLIST: dict[str, str] = {
    source.casefold(): key for source, key in SOURCE_FIELD_ALLOWLIST.items()
}

RAW_COPY_COLUMNS = ("ingest_id", "source", "source_file", "logical_date", "payload", "retention_until")


def project_row(row: dict[str, str], mapping: dict[str, str] | None = None) -> dict[str, str]:
    """Keep allowlisted fields only and normalize values to trimmed strings."""
    mapping = mapping if mapping is not None else NORMALIZED_ALLOWLIST
    projected: dict[str, str] = {}
    for source_column, value in row.items():
        if not source_column:
            continue
        key = mapping.get(source_column.strip().casefold())
        if key is None:
            continue
        if isinstance(value, str):
            value = value.strip()
        if value in ("", None):
            continue
        projected[key] = value
    return projected



def _detect_delimiter(path: Path) -> str:
    opener = gzip.open if path.suffix.lower() == ".gz" else open
    with opener(path, "rt", encoding="utf-8-sig", newline="") as handle:  # type: ignore[operator]
        sample = handle.read(8192)
    try:
        return csv.Sniffer().sniff(sample, delimiters=";,").delimiter
    except csv.Error:
        # Unfallatlas releases are semicolon separated; be explicit about the default.
        return ";"


def iter_source_rows(path: Path) -> Iterator[dict[str, str]]:
    """Stream records from a CSV/CSV.GZ file without loading it into memory."""
    opener = gzip.open if path.suffix.lower() == ".gz" else open
    delimiter = _detect_delimiter(path)
    with opener(path, "rt", encoding="utf-8-sig", newline="") as handle:  # type: ignore[operator]
        reader = csv.DictReader(handle, delimiter=delimiter)
        for row in reader:
            yield row


def read_source_fieldnames(path: Path) -> list[str]:
    """Header of a release file, used for the minimization audit trail."""
    opener = gzip.open if path.suffix.lower() == ".gz" else open
    with opener(path, "rt", encoding="utf-8-sig", newline="") as handle:  # type: ignore[operator]
        header = handle.readline()
    delimiter = _detect_delimiter(path)
    return [name.strip() for name in header.strip().split(delimiter) if name.strip()]


def download_release(url: str, target_dir: Path, force: bool = False) -> list[Path]:
    """Download one release URL (CSV, CSV.GZ or ZIP) and return usable CSV paths."""
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = Path(url.split("?")[0]).name or "release.csv"
    target = target_dir / filename

    if force or not target.exists() or target.stat().st_size == 0:
        log.info("downloading release %s", url)
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(request, timeout=120) as response, target.open("wb") as handle:
            shutil.copyfileobj(response, handle)

    if target.suffix.lower() == ".zip":
        extracted: list[Path] = []
        with zipfile.ZipFile(target) as archive:
            for member in archive.infolist():
                if member.is_dir() or not member.filename.lower().endswith((".csv", ".csv.gz")):
                    continue
                # basename only: never trust archive paths
                member_target = target_dir / Path(member.filename).name
                with archive.open(member) as source, member_target.open("wb") as destination:
                    shutil.copyfileobj(source, destination)
                extracted.append(member_target)
        if not extracted:
            raise RuntimeError(f"ZIP release {url} contained no CSV files")
        return extracted

    return [target]


def resolve_source_files(
    settings: Settings, source: str | None = None, force_download: bool = False
) -> list[Path]:
    """Resolve the input files for the configured source."""
    source = (source or settings.extract_source).lower()
    if source == "fixture":
        if not settings.fixture_path.exists():
            raise FileNotFoundError(
                f"Offline fixture not found at {settings.fixture_path}. "
                "Run 'python tools/make_fixture.py' or set EXTRACT_SOURCE=unfallatlas."
            )
        return [settings.fixture_path]
    if source == "unfallatlas":
        files: list[Path] = []
        for url in settings.unfallatlas_urls:
            files.extend(download_release(url, settings.raw_dir, force=force_download))
        if not files:
            raise RuntimeError("No source files resolved; check UNFALLATLAS_URLS")
        return files
    raise ValueError(f"Unsupported source {source!r}")


def dropped_columns(fieldnames: list[str], mapping: dict[str, str] | None = None) -> list[str]:
    """Columns present in the source but deliberately not persisted."""
    mapping = mapping if mapping is not None else NORMALIZED_ALLOWLIST
    return [name for name in fieldnames if name and name.strip().casefold() not in mapping]


def _iter_copy_rows(
    files: list[Path],
    settings: Settings,
    logical_date: date,
    stats: dict,
) -> Iterator[tuple]:
    """Yield COPY tuples for the raw zone (streaming, constant memory)."""
    # Retention is anchored on the day the data actually arrives, not on the
    # logical date: a backfill for a two-year-old month must not be deleted by
    # the very next retention sweep. For forward-dated runs the logical date is
    # the later of the two, so the stricter window wins.
    retention_until = max(logical_date, date.today()) + timedelta(days=settings.raw_retention_days)
    row_index = 0
    for path in files:
        for row in iter_source_rows(path):
            row_index += 1
            payload = project_row(row)
            if not payload:
                stats["empty_rows"] += 1
                continue
            if "plate_raw" in payload:
                stats["rows_with_plate"] += 1
            # Deterministic id: same logical date + source row -> same id, which
            # makes reruns and lineage tracing trivial.
            ingest_id = hashlib.sha256(
                f"{settings.extract_source}|{path.name}|{row_index}|{logical_date.isoformat()}".encode()
            ).hexdigest()[:32]
            yield (
                ingest_id,
                settings.extract_source,
                path.name,
                logical_date,
                json.dumps(payload, ensure_ascii=False),
                retention_until,
            )
            stats["rows_landed"] += 1


def run(
    settings: Settings,
    logical_date: date,
    source: str | None = None,
    force_download: bool = False,
) -> dict:
    """Land one release into raw.accidents_raw (partition replace for the date)."""
    files = resolve_source_files(settings, source=source, force_download=force_download)
    stats: dict = {"rows_landed": 0, "rows_with_plate": 0, "empty_rows": 0}

    with db.transaction(settings) as conn:
        replaced = db.execute(
            conn,
            "DELETE FROM raw.accidents_raw WHERE logical_date = %(logical_date)s",
            {"logical_date": logical_date},
        )
        loaded = db.copy_rows(
            conn,
            "raw.accidents_raw",
            RAW_COPY_COLUMNS,
            _iter_copy_rows(files, settings, logical_date, stats),
        )

    dropped = sorted(
        {
            name
            for path in files
            for name in dropped_columns(read_source_fieldnames(path))
        }
    )

    summary = {
        "source": settings.extract_source,
        "source_files": [path.name for path in files],
        "logical_date": logical_date.isoformat(),
        "raw_rows_replaced": replaced,
        "raw_rows_loaded": loaded,
        "rows_with_plate_in_raw": stats["rows_with_plate"],
        "dropped_source_columns": dropped,
        "raw_retention_days": settings.raw_retention_days,
    }
    log.info("extract_load_raw finished: %s", summary)
    return summary

