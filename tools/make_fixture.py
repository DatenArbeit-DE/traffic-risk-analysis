"""Generate the offline fixture for the pipeline.

The fixture is *synthetic* and schema-accurate: same column names, codings and
value ranges as a real Unfallatlas release, 90,000 rows (2019-2023), gzipped.
That keeps the repo self-contained (no 100 MB download, no portal session) while
still exercising every cleaning rule.

Deliberate imperfections, so the defensive layer has something to do:

* 25 rows with `USTUNDE=99`        -> kept, flagged as `missing_hour`
* 20 rows with `UMONAT=13`         -> dropped (invalid calendar)
* 15 rows with `Kategorie=9`       -> dropped (invalid severity code)
* ~500 exact duplicate rows        -> dropped by natural-key deduplication
* ~3% malformed `KENNZEICHEN`      -> counted, not pseudonymized
* PII-shaped columns (`Name`, `Strasse`, `Halter`, `Unfallort_Freitext`) carry
  obvious placeholder values; they exist only to prove that the extract
  allowlist drops them. They are never persisted anywhere.

Usage:
    python tools/make_fixture.py                   # 90k rows, default target
    python tools/make_fixture.py --rows 5000 --seed 7 --output /tmp/x.csv.gz
"""

from __future__ import annotations

import argparse
import csv
import gzip
import random
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "sample" / "unfallatlas_sample.csv.gz"
DEFAULT_ROWS = 90_000
DEFAULT_SEED = 20240301

HEADER = (
    "UJAHR",
    "UMONAT",
    "USTUNDE",
    "ULAND",
    "UKREIS",
    "UART",
    "UPOART",
    "ULICHT",
    "Kategorie",
    "IstPKW",
    "IstRad",
    "IstFuss",
    "IstKrad",
    "IstGueter",
    "IstSonstige",
    "XGCSWGS84",
    "YGCSWGS84",
    "KENNZEICHEN",
    "Name",
    "Strasse",
    "Halter",
    "Unfallort_Freitext",
)

# code, name, state code, lat, lon, plate city letters, relative volume
DISTRICTS: tuple[tuple[str, str, str, float, float, str, float], ...] = (
    ("11000", "Berlin", "11", 52.5200, 13.4050, "B", 8.0),
    ("09162", "Munich", "09", 48.1372, 11.5756, "M", 6.5),
    ("05315", "Cologne", "05", 50.9375, 6.9603, "K", 5.0),
    ("06412", "Frankfurt am Main", "06", 50.1109, 8.6821, "F", 4.5),
    ("02000", "Hamburg", "02", 53.5511, 9.9937, "HH", 5.5),
    ("04011", "Bremen", "04", 53.0793, 8.8017, "HB", 2.4),
    ("08111", "Stuttgart", "08", 48.7758, 9.1829, "S", 3.4),
    ("05111", "Duesseldorf", "05", 51.2277, 6.7735, "D", 3.2),
    ("14713", "Leipzig", "14", 51.3397, 12.3731, "L", 2.8),
    ("14612", "Dresden", "14", 51.0504, 13.7373, "DD", 2.6),
    ("03241", "Hanover", "03", 52.3759, 9.7320, "H", 2.5),
    ("09564", "Nuremberg", "09", 49.4521, 11.0767, "N", 2.4),
    ("05113", "Essen", "05", 51.4556, 6.9997, "E", 2.3),
    ("05913", "Dortmund", "05", 51.5136, 7.4653, "DO", 2.2),
    ("05314", "Bonn", "05", 50.7374, 7.0982, "BN", 2.0),
    ("05515", "Muenster", "05", 51.9607, 7.6261, "MS", 1.8),
    ("08212", "Karlsruhe", "08", 49.0069, 8.4037, "KA", 1.8),
    ("08311", "Freiburg im Breisgau", "08", 47.9990, 7.8421, "FR", 1.5),
    ("08222", "Mannheim", "08", 49.4875, 8.4660, "MA", 1.5),
    ("09761", "Augsburg", "09", 48.3705, 10.8978, "A", 1.4),
    ("09362", "Regensburg", "09", 49.0134, 12.1016, "R", 1.2),
    ("09663", "Wuerzburg", "09", 49.7913, 9.9534, "WU", 1.1),
    ("01002", "Kiel", "01", 54.3233, 10.1228, "KI", 1.0),
    ("12054", "Potsdam", "12", 52.3906, 13.0645, "P", 1.0),
    ("10041", "Saarbruecken", "10", 49.2402, 6.9969, "SB", 1.0),
    ("13003", "Rostock", "13", 54.0924, 12.0991, "HRO", 0.9),
)

HOUR_WEIGHTS = (
    1.2, 0.9, 0.7, 0.5, 0.4, 0.7, 1.6, 3.4, 3.6, 2.6, 2.0, 2.1,
    2.3, 2.5, 2.7, 2.9, 3.5, 3.7, 3.0, 2.2, 1.8, 1.6, 1.4, 1.3,
)
MONTH_WEIGHTS = (1.05, 0.95, 1.00, 0.95, 1.05, 1.05, 1.00, 0.95, 1.05, 1.10, 1.15, 1.10)
YEAR_WEIGHTS = ((2019, 1.05), (2020, 0.85), (2021, 0.95), (2022, 1.05), (2023, 1.10))
TYPE_WEIGHTS = (26.0, 12.0, 14.0, 9.0, 9.0, 21.0, 9.0)  # UART 1..7
TYPES = tuple(range(1, 8))
NIGHT_HOURS = frozenset(range(21, 24)) | frozenset(range(0, 6))
TWILIGHT_HOURS = frozenset({6, 7, 19, 20})
PLACEHOLDER_NAMES = ("TESTPERSON A", "TESTPERSON B", "TESTPERSON C")
PLACEHOLDER_STREETS = ("TESTSTRASSE 1", "TESTWEG 2", "TESTALLEE 3")



def pick_district(rng: random.Random):
    """Weighted district choice: bigger cities contribute more accidents."""
    return rng.choices(DISTRICTS, weights=[district[6] for district in DISTRICTS], k=1)[0]


def pick_road_condition(rng: random.Random, month: int) -> int:
    """UPOART: 1 dry, 2 wet, 3 snow/ice - wet and snow are winter-biased."""
    winter = month in (11, 12, 1, 2, 3)
    wet = 0.22 if winter else 0.13
    snow = 0.09 if winter else 0.01
    draw = rng.random()
    if draw < snow:
        return 3
    if draw < snow + wet:
        return 2
    return 1


def pick_light(hour: int) -> int:
    """ULICHT: 0 daylight, 1 twilight, 2 darkness."""
    if hour in NIGHT_HOURS:
        return 2
    if hour in TWILIGHT_HOURS:
        return 1
    return 0


def pick_participants(rng: random.Random, type_code: int) -> dict[str, int]:
    """Participation flags, biased by accident type."""
    pedestrian_bias = 0.35 if type_code in (3, 4) else 0.10
    bicycle_bias = 0.30 if type_code in (3, 5) else 0.18
    return {
        "IstPKW": int(rng.random() < 0.68),
        "IstRad": int(rng.random() < bicycle_bias),
        "IstFuss": int(rng.random() < pedestrian_bias),
        "IstKrad": int(rng.random() < 0.12),
        "IstGueter": int(rng.random() < 0.08),
        "IstSonstige": int(rng.random() < 0.10),
    }


def pick_kategorie(
    rng: random.Random,
    hour: int,
    type_code: int,
    road_condition: int,
    participants: dict[str, int],
) -> int:
    """Kategorie: 1 killed, 2 seriously injured, 3 slightly injured."""
    night = hour in NIGHT_HOURS
    wet = road_condition == 2
    snow = road_condition == 3
    pedestrian = participants["IstFuss"] == 1
    bicycle = participants["IstRad"] == 1
    motorcycle = participants["IstKrad"] == 1
    hgv = participants["IstGueter"] == 1
    crossing = type_code in (3, 4)

    serious = (
        0.10
        + 0.05 * night
        + 0.035 * wet
        + 0.05 * snow
        + 0.09 * pedestrian
        + 0.05 * bicycle
        + 0.04 * motorcycle
        + 0.03 * crossing
    )
    fatal = 0.005 + 0.005 * night + 0.004 * wet + 0.006 * snow + 0.012 * pedestrian + 0.004 * hgv

    draw = rng.random()
    if draw < fatal:
        return 1
    if draw < fatal + serious:
        return 2
    return 3



def plate_for(rng: random.Random, city_letters: str) -> str:
    """Synthetic plate; ~3% are malformed to exercise the validation branch."""
    if rng.random() < 0.03:
        return f"{city_letters}{rng.choice(('AB', 'CD', 'EF'))}{rng.randint(10_000, 99_999)}"
    letters = rng.choice(("A", "B", "C", "D", "E", "F", "G", "H", "K", "L", "M", "N"))
    extra = rng.choice(("", rng.choice(("A", "B", "C", "D", "E"))))
    digits = rng.randint(1, 9999)
    suffix = rng.choice(("", "", "", "", "E", "H"))
    return f"{city_letters}{letters}{extra}{digits}{suffix}"


def build_row(rng: random.Random) -> dict:
    """One synthetic accident record in Unfallatlas column names."""
    code, _name, state, lat0, lon0, city_letters, _weight = pick_district(rng)
    year = rng.choices([y for y, _ in YEAR_WEIGHTS], weights=[w for _, w in YEAR_WEIGHTS])[0]
    month = rng.choices(range(1, 13), weights=MONTH_WEIGHTS)[0]
    hour = rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
    type_code = rng.choices(TYPES, weights=TYPE_WEIGHTS)[0]
    road_condition = pick_road_condition(rng, month)
    participants = pick_participants(rng, type_code)
    kategorie = pick_kategorie(rng, hour, type_code, road_condition, participants)

    return {
        "UJAHR": year,
        "UMONAT": month,
        "USTUNDE": hour,
        "ULAND": state,
        "UKREIS": code,
        "UART": type_code,
        "UPOART": road_condition,
        "ULICHT": pick_light(hour),
        "Kategorie": kategorie,
        **participants,
        # 100 m grid: three decimals are ~110 m, the published resolution
        "XGCSWGS84": round(rng.gauss(lon0, 0.045), 3),
        "YGCSWGS84": round(rng.gauss(lat0, 0.030), 3),
        "KENNZEICHEN": plate_for(rng, city_letters) if rng.random() < 0.15 else "",
        # PII-shaped columns: dropped by the extract allowlist, never persisted.
        "Name": rng.choice(PLACEHOLDER_NAMES),
        "Strasse": rng.choice(PLACEHOLDER_STREETS),
        "Halter": rng.choice(PLACEHOLDER_NAMES),
        "Unfallort_Freitext": "synthetic fixture row - no real location text",
    }


def generate_rows(rng: random.Random, rows: int) -> tuple[list[dict], dict]:
    """Build the fixture rows, including the deliberate data-quality defects."""
    duplicates = max(1, rows // 180)
    base_count = rows - duplicates
    base: list[dict] = []
    anomalies = {"bad_hour": 0, "bad_month": 0, "bad_kategorie": 0, "duplicates": duplicates}

    for index in range(base_count):
        row = build_row(rng)
        if index < 25:
            row["USTUNDE"] = 99
            anomalies["bad_hour"] += 1
        elif index < 45:
            row["UMONAT"] = 13
            anomalies["bad_month"] += 1
        elif index < 60:
            row["Kategorie"] = 9
            anomalies["bad_kategorie"] += 1
        base.append(row)

    sample = [dict(row) for row in rng.sample(base, min(duplicates, len(base)))]
    all_rows = base + sample
    rng.shuffle(all_rows)
    return all_rows, anomalies


def write_fixture(rows: list[dict], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output, "wt", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(HEADER), delimiter=";", lineterminator="\n"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate the synthetic Unfallatlas fixture")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS, help="total rows to write")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="random seed")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="target .csv.gz path")
    args = parser.parse_args(argv)

    rng = random.Random(args.seed)
    rows, anomalies = generate_rows(rng, args.rows)
    write_fixture(rows, args.output)

    print(
        f"wrote {len(rows):,} rows to {args.output} "
        f"(seed={args.seed}, generated={date.today().isoformat()})"
    )
    print(f"deliberate defects: {anomalies}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

