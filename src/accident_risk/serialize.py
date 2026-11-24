"""JSON normalization for task return values.

Airflow pushes task return values into XCom, which must be JSON serializable.
psycopg returns `Decimal`/`date`/`datetime` objects, so every `run()` passes its
summary through `jsonable()` before returning it. Keeping this in one helper
means the DAG never has to know which driver types appear where.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def jsonable(value: Any) -> Any:
    """Recursively convert driver types into JSON-friendly primitives."""
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def as_json(value: Any) -> str:
    """Serialized form, handy for log lines and XCom payload sizes."""
    return json.dumps(jsonable(value), ensure_ascii=False, sort_keys=True)
