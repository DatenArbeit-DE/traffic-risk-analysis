"""Command line entry point: `python -m accident_risk.cli <command>`.

The CLI exists so the pipeline can be exercised without Airflow (tests,
backfills, debugging) and so the governance checks (`ping`, `verify`) are
runnable by a reviewer in one line. Each command maps 1:1 to a DAG task.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date

from . import anonymize, config, db, extract, load, metrics, viz
from .constants import ALLOWED_PSEUDONYM_COLUMNS, PII_DENYLIST_TOKENS
from .serialize import as_json, jsonable

log = logging.getLogger("accident_risk.cli")

STEPS = ("extract", "anonymize", "load", "analyze", "visualize")


def run_all(settings: config.Settings, logical_date: date, source: str | None = None) -> dict:
    """Run the full pipeline in-process (the DAG runs the same steps as tasks)."""
    results: dict[str, dict] = {}
    results["extract"] = extract.run(settings, logical_date, source=source)
    results["anonymize"] = anonymize.run(settings, logical_date)
    results["load"] = load.run(settings, logical_date)
    results["analyze"] = metrics.run(settings, logical_date)
    results["visualize"] = viz.run(settings, logical_date)
    return results


def verify(settings: config.Settings) -> dict:
    """Governance and integrity checks; returns a report with a `passed` flag."""
    with db.transaction(settings) as conn:
        counts = db.fetch_one(
            conn,
            """
            SELECT (SELECT count(*) FROM raw.accidents_raw)::int   AS raw_rows,
                   (SELECT count(*) FROM clean.accidents)::int     AS clean_rows,
                   (SELECT count(*) FROM analytics.risk_metrics)::int AS metric_rows,
                   (SELECT count(*) FROM clean.accidents
                     WHERE is_anonymized IS NOT TRUE)::int          AS not_anonymized,
                   (SELECT count(*) FROM raw.accidents_raw
                     WHERE payload ? 'plate_raw')::int             AS raw_plates_remaining,
                   (SELECT count(*) FROM raw.accidents_raw
                     WHERE retention_until < CURRENT_DATE)::int    AS raw_past_retention,
                   (SELECT count(*) FROM clean.accidents
                     WHERE retention_until < CURRENT_DATE)::int    AS clean_past_retention
            """,
        ) or {}

        column_hits: dict[str, list[str]] = {}
        for schema, table in (
            ("raw", "accidents_raw"),
            ("clean", "accidents"),
            ("analytics", "risk_metrics"),
        ):
            columns = [row["column_name"] for row in db.table_columns(conn, schema, table)]
            hits = [
                column
                for column in columns
                if column not in ALLOWED_PSEUDONYM_COLUMNS
                and any(token in column.casefold() for token in PII_DENYLIST_TOKENS)
            ]
            if hits:
                column_hits[f"{schema}.{table}"] = hits

    checks = {
        "clean_layer_has_rows": counts.get("clean_rows", 0) > 0,
        "analytics_layer_has_rows": counts.get("metric_rows", 0) > 0,
        "no_unanonymized_rows": counts.get("not_anonymized", 1) == 0,
        "no_plates_left_in_raw": counts.get("raw_plates_remaining", 1) == 0,
        "raw_retention_respected": counts.get("raw_past_retention", 1) == 0,
        "clean_retention_respected": counts.get("clean_past_retention", 1) == 0,
        "no_pii_named_columns": not column_hits,
    }
    return jsonable(
        {
            "counts": counts,
            "pii_named_columns": column_hits,
            "checks": checks,
            "passed": all(checks.values()),
        }
    )



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="accident-risk",
        description="Urban traffic accident risk analysis - pipeline commands",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name, help_text in (
        ("extract", "Task 1: land the source release into raw.accidents_raw"),
        ("anonymize", "Task 2: anonymize + clean the raw partition"),
        ("load", "Task 3: load the staging artifact into clean.accidents"),
        ("analyze", "Task 4: recompute the three decision metrics"),
        ("visualize", "Task 5: render the three charts"),
        ("run", "Run all five steps in order"),
    ):
        sub = subparsers.add_parser(name, help=help_text)
        sub.add_argument("--date", default=None, help="logical date (ISO 8601), default: today")
        if name in ("extract", "run"):
            sub.add_argument(
                "--source",
                default=None,
                choices=list(config.VALID_SOURCES),
                help="override EXTRACT_SOURCE",
            )
            sub.add_argument("--force-download", action="store_true", help="ignore cached files")

    subparsers.add_parser("ping", help="check database connectivity")
    subparsers.add_parser("verify", help="run the governance and integrity checks")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s"
    )
    args = build_parser().parse_args(argv)
    settings = config.get_settings()

    if args.command == "ping":
        reachable = db.ping(settings)
        print(json.dumps({"database": "ok" if reachable else "unreachable"}))
        return 0 if reachable else 1

    if args.command == "verify":
        report = verify(settings)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["passed"] else 1

    logical_date = config.resolve_logical_date(args.date)
    source = getattr(args, "source", None)
    force_download = getattr(args, "force_download", False)

    if args.command == "extract":
        print(
            as_json(
                extract.run(
                    settings, logical_date, source=source, force_download=force_download
                )
            )
        )
    elif args.command == "anonymize":
        print(as_json(anonymize.run(settings, logical_date)))
    elif args.command == "load":
        print(as_json(load.run(settings, logical_date)))
    elif args.command == "analyze":
        print(as_json(metrics.run(settings, logical_date)))
    elif args.command == "visualize":
        print(as_json(viz.run(settings, logical_date)))
    elif args.command == "run":
        print(as_json(run_all(settings, logical_date, source=source)))
    else:  # pragma: no cover - argparse enforces the choices
        raise SystemExit(f"unknown command {args.command!r}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
