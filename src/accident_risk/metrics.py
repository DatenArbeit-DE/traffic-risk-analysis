"""Task 4: run_analysis - the three decision metrics.

Definitions (identical in SQL, README and docs/decisions.md):

* **High-Risk Score (0-100)** = 100 * (0.45 * severity component
  + 0.30 * exposure/density component + 0.25 * rain component), each component
  min-max normalized across segments. It answers "where should we act first?".
* **Time-of-Day Risk Ratio** = accidents per hour in a bucket divided by the
  segment's own average accidents per hour. 1.0 means "average for this
  segment", which stops busy roads from looking risky just for being busy.
* **Severity Ratio** = share of accidents with serious or fatal outcome.

The whole analytics layer is a full refresh: the table holds one consistent
snapshot, so reruns converge instead of accumulating run-to-run noise.
"""

from __future__ import annotations

import logging
from datetime import date

from . import db
from .config import Settings
from .constants import BUCKET_HOURS, RISK_WEIGHTS
from .serialize import as_json, jsonable

log = logging.getLogger(__name__)

METRIC_SQL = """
WITH bucket_hours AS (
    SELECT label, hours
    FROM unnest(%(bucket_labels)s::text[], %(bucket_hours)s::numeric[]) AS t(label, hours)
),
filtered AS (
    SELECT segment_key, segment_label, time_bucket, severity_class, is_rain, is_severe
    FROM clean.accidents
    WHERE hour_of_day IS NOT NULL
      AND (%(window_start)s::date IS NULL OR occurred_at >= %(window_start)s::date)
),
totals AS (
    SELECT count(*)::numeric AS accidents_total,
           count(*) FILTER (WHERE is_rain)::numeric AS rain_total
    FROM filtered
),
seg AS (
    SELECT segment_key,
           min(segment_label) AS segment_label,
           count(*)::numeric AS accidents,
           count(*) FILTER (WHERE is_rain)::numeric AS rain_accidents,
           count(*) FILTER (WHERE is_severe)::numeric AS severe_accidents,
           sum(CASE severity_class WHEN 3 THEN 3.0 WHEN 2 THEN 2.0 ELSE 1.0 END) AS severity_weight
    FROM filtered
    GROUP BY segment_key
    HAVING count(*) >= %(min_segment_accidents)s
),
scored AS (
    SELECT s.segment_key,
           s.segment_label,
           s.accidents,
           s.severe_accidents / NULLIF(s.accidents, 0) AS severity_ratio,
           s.severity_weight / NULLIF(s.accidents, 0) AS severity_index,
           s.rain_accidents / NULLIF(s.accidents, 0) AS rain_share,
           (s.rain_accidents / NULLIF(s.accidents, 0))
               / NULLIF(t.rain_total / NULLIF(t.accidents_total, 0), 0) AS rain_uplift,
           s.accidents / NULLIF(t.accidents_total, 0) AS density_share
    FROM seg s
    CROSS JOIN totals t
),
bounds AS (
    SELECT min(severity_index) AS min_sev, max(severity_index) AS max_sev,
           min(density_share) AS min_den, max(density_share) AS max_den,
           min(rain_uplift) AS min_rain, max(rain_uplift) AS max_rain
    FROM scored
),
normalized AS (
    SELECT sc.*,
           CASE WHEN b.max_sev > b.min_sev
                THEN (sc.severity_index - b.min_sev) / (b.max_sev - b.min_sev) ELSE 0.5 END AS sev_norm,
           CASE WHEN b.max_den > b.min_den
                THEN (sc.density_share - b.min_den) / (b.max_den - b.min_den) ELSE 0.5 END AS den_norm,
           CASE WHEN b.max_rain > b.min_rain
                THEN (sc.rain_uplift - b.min_rain) / (b.max_rain - b.min_rain) ELSE 0.5 END AS rain_norm
    FROM scored sc
    CROSS JOIN bounds b
),
segment_rows AS (
    SELECT n.segment_key,
           'all'::text AS time_bucket,
           n.segment_label,
           n.accidents::bigint AS accident_count,
           round(n.severity_ratio, 4) AS severity_ratio,
           round(n.severity_index, 4) AS severity_index,
           round(n.rain_share, 4) AS rain_share,
           round(n.rain_uplift, 4) AS rain_uplift,
           round((100 * (%(w_severity)s * n.sev_norm
                        + %(w_density)s * n.den_norm
                        + %(w_rain)s * n.rain_norm))::numeric, 2) AS high_risk_score,
           NULL::numeric AS time_of_day_risk_ratio
    FROM normalized n
),
bucket_counts AS (
    SELECT segment_key, time_bucket, count(*)::numeric AS accidents
    FROM filtered
    WHERE time_bucket IS NOT NULL
    GROUP BY segment_key, time_bucket
),
bucket_rows AS (
    SELECT bc.segment_key,
           bc.time_bucket,
           n.segment_label,
           bc.accidents::bigint AS accident_count,
           round(n.severity_ratio, 4) AS severity_ratio,
           round(n.severity_index, 4) AS severity_index,
           round(n.rain_share, 4) AS rain_share,
           round(n.rain_uplift, 4) AS rain_uplift,
           round((100 * (%(w_severity)s * n.sev_norm
                        + %(w_density)s * n.den_norm
                        + %(w_rain)s * n.rain_norm))::numeric, 2) AS high_risk_score,
           round((bc.accidents / bh.hours) / NULLIF(n.accidents / 24.0, 0), 3) AS time_of_day_risk_ratio
    FROM bucket_counts bc
    JOIN normalized n USING (segment_key)
    JOIN bucket_hours bh ON bh.label = bc.time_bucket
),
combined AS (
    SELECT * FROM segment_rows
    UNION ALL
    SELECT * FROM bucket_rows
)
INSERT INTO analytics.risk_metrics (
    segment_key, time_bucket, segment_label, logical_date, accident_count,
    severity_ratio, severity_index, rain_share, rain_uplift, high_risk_score,
    time_of_day_risk_ratio, retention_until
)
SELECT segment_key,
       time_bucket,
       segment_label,
       %(logical_date)s::date,
       accident_count,
       severity_ratio,
       severity_index,
       rain_share,
       rain_uplift,
       high_risk_score,
       time_of_day_risk_ratio,
       (%(logical_date)s::date + make_interval(years => %(retention_years)s))::date
FROM combined
"""



OVERALL_SQL = """
SELECT count(*) FILTER (WHERE time_bucket = 'all')::int AS segments,
       coalesce(sum(accident_count) FILTER (WHERE time_bucket = 'all'), 0)::int AS analysed_accidents,
       round(avg(severity_ratio) FILTER (WHERE time_bucket = 'all'), 4) AS avg_severity_ratio,
       round(avg(rain_share) FILTER (WHERE time_bucket = 'all'), 4) AS avg_rain_share,
       round(max(high_risk_score) FILTER (WHERE time_bucket = 'all'), 2) AS max_high_risk_score
FROM analytics.risk_metrics
"""

TOP_SEGMENTS_SQL = """
SELECT segment_key, segment_label, accident_count, severity_ratio, severity_index,
       rain_share, rain_uplift, high_risk_score
FROM analytics.risk_metrics
WHERE time_bucket = 'all'
ORDER BY high_risk_score DESC NULLS LAST, accident_count DESC
LIMIT %(limit)s
"""

BUCKET_SQL = """
SELECT time_bucket,
       sum(accident_count)::int AS accident_count,
       round(avg(time_of_day_risk_ratio), 3) AS avg_time_of_day_risk_ratio
FROM analytics.risk_metrics
WHERE time_bucket <> 'all'
GROUP BY time_bucket
ORDER BY avg_time_of_day_risk_ratio DESC NULLS LAST
"""


def window_start(settings: Settings, logical_date: date) -> date | None:
    """First day of the analysis window (None = use everything available)."""
    years = settings.analysis_window_years
    if not years or years <= 0:
        return None
    try:
        return logical_date.replace(year=logical_date.year - years)
    except ValueError:  # 29 February edge case
        return logical_date.replace(year=logical_date.year - years, day=28)


def metric_params(settings: Settings, logical_date: date) -> dict:
    """Bind the constants from constants.py so SQL and Python cannot drift apart."""
    return {
        "logical_date": logical_date,
        "window_start": window_start(settings, logical_date),
        "min_segment_accidents": settings.min_segment_accidents,
        "retention_years": settings.clean_retention_years,
        "bucket_labels": list(BUCKET_HOURS),
        "bucket_hours": [float(hours) for hours in BUCKET_HOURS.values()],
        "w_severity": float(RISK_WEIGHTS["severity"]),
        "w_density": float(RISK_WEIGHTS["density"]),
        "w_rain": float(RISK_WEIGHTS["rain"]),
    }


def run(settings: Settings, logical_date: date) -> dict:
    """Full-refresh the analytics layer and return the headline numbers."""
    params = metric_params(settings, logical_date)
    with db.transaction(settings) as conn:
        db.execute(conn, "DELETE FROM analytics.risk_metrics")
        rows_written = db.execute(conn, METRIC_SQL, params)
        overall = db.fetch_one(conn, OVERALL_SQL) or {}
        top_segments = db.fetch_all(conn, TOP_SEGMENTS_SQL, {"limit": settings.top_segments_heatmap})
        buckets = db.fetch_all(conn, BUCKET_SQL)

    summary = {
        "logical_date": logical_date.isoformat(),
        "window_start": params["window_start"].isoformat() if params["window_start"] else None,
        "rows_written": rows_written,
        "segments": overall.get("segments", 0),
        "analysed_accidents": overall.get("analysed_accidents", 0),
        "avg_severity_ratio": overall.get("avg_severity_ratio"),
        "avg_rain_share": overall.get("avg_rain_share"),
        "max_high_risk_score": overall.get("max_high_risk_score"),
        "top_segments": top_segments,
        "time_of_day": buckets,
        "weights": dict(RISK_WEIGHTS),
        "min_segment_accidents": settings.min_segment_accidents,
    }
    log.info("run_analysis finished: %s", as_json(summary))
    return jsonable(summary)
