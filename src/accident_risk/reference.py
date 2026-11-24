"""Readable district labels for charts.

The Unfallatlas only carries numeric district codes (UKREIS). Charts labelled
"11000" are useless in a management review, so a small curated reference file
maps the codes used by the fixture/analysis to city names. The file contains no
personal data and is intentionally tiny (a lookup, not another data model).
"""

from __future__ import annotations

import csv
import logging
from functools import lru_cache
from pathlib import Path

log = logging.getLogger(__name__)


@lru_cache(maxsize=4)
def load_district_labels(path: Path) -> dict[str, str]:
    """Return {district_code: district_name}; empty mapping when the file is absent."""
    if not path.exists():
        log.warning("district label reference not found at %s; falling back to codes", path)
        return {}

    labels: dict[str, str] = {}
    with path.open("r", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            code = (row.get("district_code") or "").strip()
            name = (row.get("district_name") or "").strip()
            if code and name:
                labels[code] = name
    return labels


def district_label(code: str | None, labels: dict[str, str]) -> str:
    """Human-readable district label with a stable fallback for unknown codes."""
    if not code:
        return "unknown district"
    return labels.get(code, f"District {code}")
