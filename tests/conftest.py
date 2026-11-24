"""Shared fixtures for the test suite.

Two groups of tests:

* pure tests (`test_constants`, `test_extract`, `test_anonymize`) - no database
* integration tests (`test_pipeline`, marked `integration`) - need PostgreSQL

Integration tests target `traffic_test`, never the demo warehouse `traffic`, so a
test run can truncate and rebuild schemas without destroying local results.
`docker compose run --rm processing pytest -q` provides everything they need.
"""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import date
from urllib.parse import urlsplit, urlunsplit

import pytest

from accident_risk import config, db

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"

LOGICAL_DATE = date(2024, 3, 1)


def _test_dsn() -> str:
    """DSN of the disposable test database (`traffic_test`)."""
    explicit = os.environ.get("TEST_DB_DSN", "").strip()
    if explicit:
        return explicit
    dsn = os.environ.get("TRAFFIC_DB_DSN", "").strip()
    if dsn:
        parts = urlsplit(dsn)
        return urlunsplit(parts._replace(path="/traffic_test"))
    user = os.environ.get("POSTGRES_USER", "traffic")
    password = os.environ.get("POSTGRES_PASSWORD", "")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    return f"postgresql://{user}:{password}@{host}:{port}/traffic_test"


@pytest.fixture(scope="session")
def settings(tmp_path_factory: pytest.TempPathFactory) -> config.Settings:
    """Runtime settings pointed at the test database and a throwaway output dir."""
    os.environ.setdefault("PSEUDONYMIZATION_PEPPER", "test-pepper-not-a-real-secret")
    os.environ.setdefault("TRAFFIC_DB_DSN", _test_dsn())

    base = config.get_settings()
    scratch = tmp_path_factory.mktemp("utar")
    return replace(
        base,
        dsn=_test_dsn(),
        staging_dir=scratch / "staging",
        outputs_dir=scratch / "outputs",
        docs_img_dir=scratch / "docs" / "img",
    )


def _statements(path: Path) -> list[str]:
    return db._split_sql(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def warehouse(settings: config.Settings) -> config.Settings:
    """Rebuild raw/clean/analytics in the test database from the shipped SQL."""
    try:
        reachable = db.ping(settings)
    except Exception as exc:  # pragma: no cover - environment problem, not a bug
        pytest.skip(f"PostgreSQL not reachable ({exc.__class__.__name__}); integration tests skipped")
    if not reachable:  # pragma: no cover
        pytest.skip("PostgreSQL not reachable; integration tests skipped")

    with db.transaction(settings) as conn:
        db.execute(conn, "DROP SCHEMA IF EXISTS raw, clean, analytics CASCADE")
        for name in ("01_schema.sql", "02_indexes.sql"):
            for statement in _statements(settings.sql_dir / name):
                db.execute(conn, statement)
    return settings


@pytest.fixture(scope="session")
def pipeline(warehouse: config.Settings) -> dict[str, dict]:
    """Run all five pipeline steps once; integration tests assert on the result."""
    from accident_risk import anonymize, extract, load, metrics, viz

    summaries = {
        "extract": extract.run(warehouse, LOGICAL_DATE),
        "anonymize": anonymize.run(warehouse, LOGICAL_DATE),
        "load": load.run(warehouse, LOGICAL_DATE),
        "analyze": metrics.run(warehouse, LOGICAL_DATE),
        "visualize": viz.run(warehouse, LOGICAL_DATE),
    }
    return summaries
