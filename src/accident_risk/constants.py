"""Business constants: one place for the vocabulary shared by code and SQL.

Field mapping is based on the official Unfallatlas field catalogue. Two mapping
details matter for correctness and are easy to get wrong:

* `Kategorie` is 1 = killed, 2 = seriously injured, 3 = slightly injured. The
  Unfallatlas only contains accidents with personal injury, so there are no
  property-damage-only records. Internally we invert the scale so that a higher
  number means a worse outcome: 3 = fatal, 2 = serious injury, 1 = slight injury.
* `UPOART` is the road-surface condition: 1 = dry, 2 = wet, 3 = snow/ice.
"""

from __future__ import annotations

# --- Time of day -------------------------------------------------------------
# Buckets are disjoint and cover the full day; tests assert both properties.
TIME_BUCKET_ORDER: tuple[str, ...] = ("night", "am_peak", "midday", "pm_peak", "evening")

BUCKET_HOURS: dict[str, int] = {
    "night": 8,     # 22:00-05:59
    "am_peak": 4,   # 06:00-09:59
    "midday": 6,    # 10:00-15:59
    "pm_peak": 4,   # 16:00-19:59
    "evening": 2,   # 20:00-21:59
}

_BUCKET_RANGES: dict[str, range] = {
    "night": range(22, 24),  # continued below for 0-5
    "am_peak": range(6, 10),
    "midday": range(10, 16),
    "pm_peak": range(16, 20),
    "evening": range(20, 22),
}

HOUR_TO_BUCKET: dict[int, str] = {hour: "night" for hour in range(0, 6)}
for _bucket, _hours in _BUCKET_RANGES.items():
    for _hour in _hours:
        HOUR_TO_BUCKET[_hour] = _bucket


def bucket_for_hour(hour: int | None) -> str | None:
    """Return the time bucket for an hour of day, or None for missing hours."""
    if hour is None:
        return None
    return HOUR_TO_BUCKET.get(int(hour))


# --- Severity ----------------------------------------------------------------
# Unfallatlas Kategorie -> internal severity class (higher = worse outcome)
SEVERITY_CLASS: dict[str, int] = {"1": 3, "2": 2, "3": 1}
SEVERITY_LABEL: dict[int, str] = {3: "fatal", 2: "serious injury", 1: "slight injury"}
# Accidents at or above this class count as "severe" for the Severity Ratio.
SEVERE_THRESHOLD = 2

# --- Road condition ----------------------------------------------------------
ROAD_CONDITION: dict[str, str] = {"1": "dry", "2": "wet", "3": "snow"}
RAIN_CONDITIONS: frozenset[str] = frozenset({"wet"})

# --- Light condition ---------------------------------------------------------
LIGHT_CONDITION: dict[str, str] = {"0": "daylight", "1": "twilight", "2": "darkness"}

# --- Accident type -----------------------------------------------------------
ACCIDENT_TYPE_LABEL: dict[str, str] = {
    "1": "Driving accident",
    "2": "Turning accident",
    "3": "Crossing accident",
    "4": "Pedestrian crossing",
    "5": "Stationary traffic",
    "6": "Rear-end collision",
    "7": "Other accident",
}

# --- Participants ------------------------------------------------------------
# Raw participation flags are aggregated into a coarse mix string. This is the
# "tokenization" of vehicle participants: no per-vehicle identifier survives.
PARTICIPANT_COLUMNS: dict[str, str] = {
    "is_car": "car",
    "is_bicycle": "bicycle",
    "is_pedestrian": "pedestrian",
    "is_motorcycle": "motorcycle",
    "is_hgv": "hgv",
    "is_other": "other",
}

# --- Metric weights ----------------------------------------------------------
# High-Risk Score = 100 * (w_severity * severity + w_density * density + w_rain * rain)
RISK_WEIGHTS: dict[str, float] = {"severity": 0.45, "density": 0.30, "rain": 0.25}

# --- Governance --------------------------------------------------------------
# Used by tests and by the extract allowlist audit: a column or JSON key matching
# any of these tokens is treated as personal data and must never be persisted.
PII_DENYLIST_TOKENS: tuple[str, ...] = (
    "name",
    "vorname",
    "nachname",
    "adresse",
    "address",
    "street",
    "strasse",
    "email",
    "telefon",
    "phone",
    "geburts",
    "birth",
    "iban",
    "konto",
    "halter",
    "owner",
    "kennzeichen",
    "plate",
    "personalausweis",
    "pass_number",
)

# Columns that may match the denylist on purpose: the pseudonym is the token we
# keep, and `plate_redacted_at` is the audit timestamp proving the raw plate was
# removed - neither of them holds a plate value.
ALLOWED_PSEUDONYM_COLUMNS: frozenset[str] = frozenset({"plate_pseudonym", "plate_redacted_at"})
