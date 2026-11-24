"""Task 5: build_visualizations.

Produces three charts from the decision layer plus their data as CSV, so every
picture in the README can be regenerated and audited:

1. `01_segment_bucket_heatmap` - Time-of-Day Risk Ratio for the highest-scoring
   segments (where *and when* to act).
2. `02_top_segments_high_risk_score` - ranked bar chart of the High-Risk Score
   (what to act on first).
3. `03_rain_vs_dry_severity` - outcome comparison of wet vs. dry road surface
   (whether the rain component earns its weight).

Charts read `analytics.risk_metrics`; only the rain comparison touches
`clean.accidents`, because the metric table aggregates rain away. Both are
aggregated, non-personal views.
"""

from __future__ import annotations

import logging
import shutil
from datetime import date
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless: no display server inside the container

import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from . import db  # noqa: E402
from .config import Settings  # noqa: E402
from .constants import TIME_BUCKET_ORDER  # noqa: E402
from .serialize import jsonable  # noqa: E402

log = logging.getLogger(__name__)

plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams.update({"figure.autolayout": True, "savefig.bbox": "tight"})

HEATMAP_CSV = "01_segment_bucket_heatmap.csv"
TOP10_CSV = "02_top_segments_high_risk_score.csv"
RAIN_CSV = "03_rain_vs_dry_severity.csv"

TOP_SEGMENTS_SQL = """
SELECT segment_key, segment_label, high_risk_score, accident_count
FROM analytics.risk_metrics
WHERE time_bucket = 'all'
ORDER BY high_risk_score DESC NULLS LAST, accident_count DESC
LIMIT %(limit)s
"""

BUCKET_RATIOS_SQL = """
SELECT segment_key, time_bucket, accident_count, time_of_day_risk_ratio
FROM analytics.risk_metrics
WHERE time_bucket <> 'all' AND segment_key = ANY(%(segment_keys)s)
"""

TOP10_SQL = """
SELECT segment_label, accident_count, high_risk_score, severity_ratio, rain_share
FROM analytics.risk_metrics
WHERE time_bucket = 'all'
ORDER BY high_risk_score DESC NULLS LAST, accident_count DESC
LIMIT %(limit)s
"""

RAIN_SQL = """
SELECT CASE WHEN is_rain THEN 'wet (rain)' ELSE 'dry / other' END AS road_condition,
       count(*)::int AS accident_count,
       round(avg(severity_class)::numeric, 3) AS avg_severity_class,
       round((count(*) FILTER (WHERE is_severe))::numeric / count(*), 4) AS severe_share,
       round((count(*) FILTER (WHERE severity_class = 3))::numeric / count(*), 4) AS fatal_share
FROM clean.accidents
WHERE hour_of_day IS NOT NULL
GROUP BY 1
ORDER BY 1
"""


def _figure_paths(settings: Settings, logical_date: date, name: str) -> tuple[Path, Path]:
    """Return (output PNG, docs/img PNG) for a chart basename."""
    out_dir = settings.output_dir(logical_date)
    out_dir.mkdir(parents=True, exist_ok=True)
    docs_dir = settings.docs_img_dir
    docs_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{name}.png", docs_dir / f"{name}.png"



def _cell_label_box() -> dict:
    """Translucent chip so a number stays readable on any heatmap colour."""
    return {"boxstyle": "round,pad=0.18", "facecolor": "white", "alpha": 0.62, "linewidth": 0}


def _bucket_windows() -> dict[str, str]:
    """Human-readable hour span per bucket, derived from the constants module."""
    from .constants import BUCKET_HOURS, HOUR_TO_BUCKET

    windows: dict[str, str] = {}
    for bucket in TIME_BUCKET_ORDER:
        hours = {hour for hour, name in HOUR_TO_BUCKET.items() if name == bucket}
        start = next(hour for hour in sorted(hours) if (hour - 1) % 24 not in hours)
        end = (start + BUCKET_HOURS[bucket] - 1) % 24
        windows[bucket] = f"{bucket.replace('_', ' ')}\n{start:02d}-{end:02d}"
    return windows


def plot_segment_bucket_heatmap(
    top_segments: pd.DataFrame,
    ratios: pd.DataFrame,
    png_path: Path,
    csv_path: Path,
) -> pd.DataFrame:
    """Heatmap: Time-of-Day Risk Ratio per segment x time bucket."""
    labels = dict(zip(top_segments["segment_key"], top_segments["segment_label"]))
    pivot = (
        ratios.pivot_table(
            index="segment_key",
            columns="time_bucket",
            values="time_of_day_risk_ratio",
            aggfunc="first",
        )
        .reindex(index=list(top_segments["segment_key"]), columns=list(TIME_BUCKET_ORDER))
        .astype(float)
    )
    export = pivot.rename(index=labels)
    export.index.name = "segment"
    export.to_csv(csv_path)

    fig, ax = plt.subplots(figsize=(9, 0.52 * max(len(pivot), 3) + 2.0))
    values = pivot.to_numpy(dtype=float)
    finite = values[~pd.isna(values)]
    vmax = max(2.0, float(finite.max()) * 0.9) if finite.size else 2.5
    # Anchor the low end at 0.5 instead of 0: every ratio sits between ~0.3 and
    # 2.0, so a 0-based scale would wash out the differences that matter.
    vmin = min(0.5, float(finite.min())) if finite.size else 0.0
    cmap = plt.get_cmap("YlOrRd")
    image = ax.imshow(values, cmap=cmap, aspect="auto", vmin=vmin, vmax=vmax)

    ax.set_xticks(range(len(TIME_BUCKET_ORDER)))
    windows = _bucket_windows()
    ax.set_xticklabels([windows[bucket] for bucket in TIME_BUCKET_ORDER], fontsize=9)
    ax.set_yticks(range(len(pivot)))
    ax.set_yticklabels([labels[key] for key in pivot.index], fontsize=9)

    for row in range(values.shape[0]):
        for column in range(values.shape[1]):
            value = values[row, column]
            text = "n/a" if pd.isna(value) else f"{value:.2f}"
            ax.text(
                column,
                row,
                text,
                ha="center",
                va="center",
                fontsize=8,
                color="#1a202c",
                bbox=_cell_label_box(),
            )

    colorbar = fig.colorbar(image, ax=ax, fraction=0.035, pad=0.02)
    colorbar.set_label("accidents per hour / segment average (1.0 = average)", fontsize=8)
    ax.set_title(
        "When do the highest-risk segments peak?\nTime-of-Day Risk Ratio", fontsize=11
    )
    ax.set_xlabel("time of day")
    fig.savefig(png_path, dpi=150)
    plt.close(fig)
    return export


def plot_top_segments(df: pd.DataFrame, png_path: Path, csv_path: Path) -> pd.DataFrame:
    """Ranked bar chart of the High-Risk Score."""
    export = df.copy()
    export.to_csv(csv_path, index=False)

    data = df.iloc[::-1].reset_index(drop=True)
    scores = data["high_risk_score"].astype(float).fillna(0.0)
    fig, ax = plt.subplots(figsize=(9.5, 0.48 * max(len(data), 3) + 2.0))
    # Normalise the palette to the *shown* score range: the top 12 segments all
    # sit within a few points of each other, so a 0-100 scale would paint every
    # bar the same dark red and hide the ranking.
    span = max(float(scores.max()) - float(scores.min()), 1e-9)
    scaled = (scores - float(scores.min())) / span
    colours = plt.cm.YlOrRd(0.35 + scaled.clip(0, 1) * 0.6)
    bars = ax.barh(data["segment_label"], scores, color=colours, edgecolor="#4a5568")

    for bar, score, count, severity in zip(
        bars,
        scores,
        data["accident_count"],
        data["severity_ratio"].astype(float).fillna(0.0),
    ):
        ax.text(
            bar.get_width() + 0.6,
            bar.get_y() + bar.get_height() / 2,
            f"{score:.1f}  (n={int(count)}, severe {severity * 100:.0f}%)",
            va="center",
            fontsize=8,
        )

    ax.set_xlim(0, max(scores.max(), 1.0) * 1.35)
    ax.set_xlabel("High-Risk Score (0-100)")
    ax.set_title(
        "Where to act first: High-Risk Score by segment\n"
        "0.45 severity + 0.30 exposure + 0.25 rain",
        fontsize=11,
    )
    fig.savefig(png_path, dpi=150)
    plt.close(fig)
    return export


def plot_rain_vs_dry(df: pd.DataFrame, png_path: Path, csv_path: Path) -> pd.DataFrame:
    """Wet vs. dry outcome comparison (does the rain component earn its weight?)."""
    export = df.copy()
    export.to_csv(csv_path, index=False)

    labels = df["road_condition"].tolist()
    counts = df["accident_count"].astype(int).tolist()
    severe = (df["severe_share"].astype(float) * 100).tolist()
    colours = ["#7a9e7e", "#2b6cb0"][: len(labels)]

    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.2))
    axes[0].bar(labels, counts, color=colours, edgecolor="#4a5568")
    axes[0].set_title("Accidents by road surface", fontsize=10)
    axes[0].set_ylabel("accidents")
    for index, value in enumerate(counts):
        axes[0].text(index, value, f"{value:,}", ha="center", va="bottom", fontsize=9)

    axes[1].bar(labels, severe, color=colours, edgecolor="#4a5568")
    axes[1].set_title("Severe outcome share (serious or fatal)", fontsize=10)
    axes[1].set_ylabel("% of accidents")
    axes[1].set_ylim(0, max(severe) * 1.3 if severe else 1.0)
    for index, value in enumerate(severe):
        axes[1].text(index, value, f"{value:.1f}%", ha="center", va="bottom", fontsize=9)

    if len(severe) == 2:
        delta = severe[1] - severe[0]
        fig.suptitle(
            f"Rain vs. dry: severe outcome share differs by {delta:+.1f} percentage points",
            fontsize=11,
        )
    fig.savefig(png_path, dpi=150)
    plt.close(fig)
    return export



def run(settings: Settings, logical_date: date) -> dict:
    """Render the three decision charts for one logical date."""
    with db.transaction(settings) as conn:
        top_segments = db.fetch_all(
            conn, TOP_SEGMENTS_SQL, {"limit": settings.top_segments_heatmap}
        )
        top_ranked = db.fetch_all(conn, TOP10_SQL, {"limit": 10})
        rain_rows = db.fetch_all(conn, RAIN_SQL)
        segment_keys = [row["segment_key"] for row in top_segments]
        bucket_rows = (
            db.fetch_all(conn, BUCKET_RATIOS_SQL, {"segment_keys": segment_keys})
            if segment_keys
            else []
        )

    if not top_segments or not top_ranked:
        raise RuntimeError(
            "analytics.risk_metrics is empty; run run_analysis before build_visualizations"
        )

    out_dir = settings.output_dir(logical_date)
    charts: list[str] = []

    heatmap = plot_segment_bucket_heatmap(
        pd.DataFrame(top_segments),
        pd.DataFrame(bucket_rows),
        _figure_paths(settings, logical_date, "01_segment_bucket_heatmap")[0],
        out_dir / HEATMAP_CSV,
    )
    charts.append("01_segment_bucket_heatmap")

    ranked = plot_top_segments(
        pd.DataFrame(top_ranked),
        _figure_paths(settings, logical_date, "02_top_segments_high_risk_score")[0],
        out_dir / TOP10_CSV,
    )
    charts.append("02_top_segments_high_risk_score")

    rain_compare = plot_rain_vs_dry(
        pd.DataFrame(rain_rows),
        _figure_paths(settings, logical_date, "03_rain_vs_dry_severity")[0],
        out_dir / RAIN_CSV,
    )
    charts.append("03_rain_vs_dry_severity")

    # The docs copies keep the README images reproducible from a single run.
    for name in charts:
        _, docs_png = _figure_paths(settings, logical_date, name)
        shutil.copy2(out_dir / f"{name}.png", docs_png)

    summary = {
        "logical_date": logical_date.isoformat(),
        "charts": charts,
        "output_dir": str(out_dir),
        "docs_img_dir": str(settings.docs_img_dir),
        "heatmap_rows": len(heatmap),
        "top_segment": ranked.iloc[0].to_dict() if not ranked.empty else None,
        "rain_vs_dry": rain_compare.to_dict(orient="records"),
    }
    log.info("build_visualizations finished: %s", jsonable(summary))
    return jsonable(summary)
