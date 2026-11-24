"""Task 1 - allowlist projection.

The raw zone is only GDPR-defensible if it is impossible to persist a column
nobody explicitly allowed, so these tests focus on what is *dropped*.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from accident_risk import extract
from accident_risk.config import Settings
from accident_risk.constants import PII_DENYLIST_TOKENS

PII_SHAPED_SOURCE_COLUMNS = ("Name", "Strasse", "Halter", "Unfallort_Freitext")


def test_unknown_and_pii_columns_are_dropped() -> None:
    row = {
        "UJAHR": "2022",
        "UMONAT": "7",
        "Name": "TESTPERSON B",
        "Strasse": "TESTWEG 2",
        "Halter": "TESTPERSON C",
        "Unfallort_Freitext": "some free text",
        "Unfallort": "anything new a release adds",
    }
    projected = extract.project_row(row)

    assert projected == {"year": "2022", "month": "7"}
    for column in PII_SHAPED_SOURCE_COLUMNS:
        assert column not in projected


def test_allowlist_mapping_is_case_insensitive_and_trims_values() -> None:
    projected = extract.project_row({"ujahr": " 2021 ", "Kategorie": "1", "istgueter": "1"})
    assert projected == {"year": "2021", "severity_code": "1", "is_hgv": "1"}


def test_umlaut_alias_is_supported() -> None:
    assert extract.project_row({"IstGüter": "1"}) == {"is_hgv": "1"}


def test_empty_values_are_not_persisted() -> None:
    assert extract.project_row({"UJAHR": "2022", "KENNZEICHEN": "   ", "ULAND": ""}) == {
        "year": "2022"
    }


def test_dropped_columns_reports_the_minimization_audit_trail() -> None:
    header = list(PII_SHAPED_SOURCE_COLUMNS) + ["UJAHR", "UKREIS"]
    assert extract.dropped_columns(header) == list(PII_SHAPED_SOURCE_COLUMNS)


def test_no_allowlisted_field_looks_like_personal_data() -> None:
    persisted = set(extract.SOURCE_FIELD_ALLOWLIST.values()) - {"plate_raw"}
    offenders = {
        key
        for key in persisted
        if any(token in key.casefold() for token in PII_DENYLIST_TOKENS)
    }
    # plate_raw is the documented, transient exception handled in task 3.
    assert offenders == set()


def test_fixture_is_readable_and_contains_the_expected_defects() -> None:
    path = Path(__file__).resolve().parents[1] / "data" / "sample" / "unfallatlas_sample.csv.gz"
    if not path.exists():  # pragma: no cover - fixture is committed, this is a guard
        pytest.skip("fixture not generated; run python tools/make_fixture.py")

    header = extract.read_source_fieldnames(path)
    assert "UJAHR" in header and "KENNZEICHEN" in header
    assert "Name" in header  # defect column: must never reach the database

    rows = extract.iter_source_rows(path)
    first = next(rows)
    assert extract.project_row(first).keys() <= set(extract.SOURCE_FIELD_ALLOWLIST.values())


def test_resolve_source_files_requires_the_fixture(settings: Settings, tmp_path: Path) -> None:
    missing = replace(settings, fixture_path=tmp_path / "does-not-exist.csv.gz", extract_source="fixture")
    with pytest.raises(FileNotFoundError):
        extract.resolve_source_files(missing)


def test_unfallatlas_source_requires_urls(settings: Settings) -> None:
    without_urls = replace(settings, extract_source="unfallatlas", unfallatlas_urls=())
    with pytest.raises(RuntimeError):
        extract.resolve_source_files(without_urls)
