"""Urban traffic accident risk - Airflow 3 DAG with five tasks.

The DAG is deliberately thin: every task delegates to one `run()` function of
the Airflow-agnostic `accident_risk` package, so the exact same code path can be
executed by `docker compose run --rm processing python -m accident_risk.cli run`.

Task graph
----------
extract_load_raw -> anonymize_and_clean -> load_to_postgres -> run_analysis
                 -> build_visualizations

Data flow between tasks is XCom-only (JSON summaries); the actual payload hand-
off is the staging artifact on the shared `data/staging/<date>` volume, which
keeps XCom small and lets a failed load be re-run from disk.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

try:  # Airflow 3 public DAG-authoring interface
    from airflow.sdk import DeadlineAlert, DeadlineReference, SyncCallback, dag, task
except ImportError:  # pragma: no cover - Airflow 2 compatibility
    from airflow.decorators import dag, task

    DeadlineAlert = None  # type: ignore[assignment]
    DeadlineReference = None  # type: ignore[assignment]
    SyncCallback = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "email_on_failure": False,
}

DOC_MD = """
# Urban traffic accident risk analysis

Monthly refresh of the urban accident risk model.

| # | Task | Purpose |
|---|------|---------|
| 1 | `extract_load_raw` | land the release into `raw.accidents_raw` (allowlist only) |
| 2 | `anonymize_and_clean` | HMAC pseudonyms, quality rules, staging artifact |
| 3 | `load_to_postgres` | idempotent load into `clean.accidents`, redact raw plates |
| 4 | `run_analysis` | High-Risk Score, Time-of-Day Risk Ratio, Severity Ratio |
| 5 | `build_visualizations` | heatmap, top segments, rain vs. dry charts |

GDPR: the clean layer refuses non-anonymized rows (`CHECK (is_anonymized)`),
plates are redacted inside the load transaction and every zone carries a
`retention_until` date that `03_retention.sql` enforces.
"""


def _logical_date(context: dict) -> date:
    """Resolve the run's logical date (Airflow 3: `logical_date`, else `ds`)."""
    from accident_risk.config import resolve_logical_date

    value = context.get("logical_date") or context.get("ds")
    return resolve_logical_date(value)


def _notify_analysis_sla_missed(context: dict, **kwargs) -> None:
    """Log when the monthly run exceeds its one-hour SLA.

    Airflow 3 removed the task-level ``sla=`` kwarg; Deadline Alerts are the
    replacement. This is a synchronous callback (no triggerer needed), which
    matches this demo's single-container setup.
    """
    dag_run = context.get("dag_run")
    log.warning(
        "SLA missed: monthly run exceeded 1 hour (run_id=%s, logical_date=%s)",
        getattr(dag_run, "run_id", None),
        context.get("logical_date") or context.get("ds"),
    )


# Airflow 3 removed task SLAs; a one-hour Deadline Alert measured from when the
# run is queued is the closest equivalent and demonstrates the same
# "analysis took too long -> alert" behaviour the old ``sla=`` provided.
DEADLINE = (
    DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(hours=1),
        callback=SyncCallback(_notify_analysis_sla_missed),
        name="analysis_one_hour_sla",
    )
    if DeadlineAlert is not None
    else None
)


@dag(
    dag_id="urban_accident_risk",
    description="Urban traffic accident risk analysis (5 tasks, GDPR-safe)",
    schedule="0 3 1 * *",  # 03:00 on the first of every month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    deadline=DEADLINE,
    default_args=DEFAULT_ARGS,
    tags=["traffic", "gdpr", "analytics"],
    doc_md=DOC_MD,
)
def urban_accident_risk() -> None:
    @task(task_id="extract_load_raw")
    def extract_load_raw(**context) -> dict:
        from accident_risk import extract
        from accident_risk.config import get_settings

        settings = get_settings()
        summary = extract.run(settings, _logical_date(context))
        log.info("extract summary: %s", summary)
        return summary

    @task(task_id="anonymize_and_clean")
    def anonymize_and_clean(extract_summary: dict, **context) -> dict:
        from accident_risk import anonymize
        from accident_risk.config import get_settings

        settings = get_settings()
        summary = anonymize.run(settings, _logical_date(context))
        if extract_summary.get("rows_with_plate_in_raw"):
            log.info(
                "%s raw rows carried a plate value; %s valid pseudonyms were derived",
                extract_summary["rows_with_plate_in_raw"],
                summary.get("plate_pseudonyms"),
            )
        if summary.get("rows_in") and not summary.get("rows_out"):
            raise ValueError("anonymize_and_clean produced zero rows; refusing to load")
        return summary

    @task(task_id="load_to_postgres")
    def load_to_postgres(clean_summary: dict, **context) -> dict:
        from accident_risk import load
        from accident_risk.config import get_settings

        settings = get_settings()
        summary = load.run(settings, _logical_date(context))
        log.info(
            "loaded %s of %s staging rows (%s duplicates skipped) from %s",
            summary["inserted_rows"],
            summary["staging_rows"],
            summary["skipped_duplicates"],
            clean_summary.get("staging_file"),
        )
        return summary

    @task(task_id="run_analysis")
    def run_analysis(load_summary: dict, **context) -> dict:
        from accident_risk import metrics
        from accident_risk.config import get_settings

        settings = get_settings()
        summary = metrics.run(settings, _logical_date(context))
        log.info(
            "%s segments scored over %s accidents, max High-Risk Score %s",
            summary["segments"],
            summary["analysed_accidents"],
            summary["max_high_risk_score"],
        )
        return summary

    @task(task_id="build_visualizations")
    def build_visualizations(analysis_summary: dict, **context) -> dict:
        from accident_risk import viz
        from accident_risk.config import get_settings

        settings = get_settings()
        summary = viz.run(settings, _logical_date(context))
        log.info("wrote charts %s to %s", summary["charts"], summary["output_dir"])
        return summary

    extract_summary = extract_load_raw()
    clean_summary = anonymize_and_clean(extract_summary)
    load_summary = load_to_postgres(clean_summary)
    analysis_summary = run_analysis(load_summary)
    build_visualizations(analysis_summary)


urban_accident_risk()
