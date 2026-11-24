-- Data model: three tables, one per governance zone.
--   raw.accidents_raw      -> replayable landing zone, short-lived, allowlisted
--   clean.accidents        -> pseudonymized fact table (is_anonymized enforced)
--   analytics.risk_metrics -> decision layer, aggregated, no personal reference
--
-- GDPR design notes are in docs/data_governance.md. Schema-level guarantees are
-- intentional: the database itself refuses non-anonymized records and refuses
-- retention windows beyond the agreed policy.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------------
-- Zone 1: raw. Only allowlisted fields arrive here (see accident_risk/extract.py).
-- A vehicle plate may exist for minutes until load_to_postgres redacts it.
-- ---------------------------------------------------------------------------
CREATE TABLE raw.accidents_raw (
    ingest_id         text        PRIMARY KEY,
    source            text        NOT NULL,
    source_file       text        NOT NULL,
    logical_date      date        NOT NULL,
    payload           jsonb       NOT NULL,
    loaded_at         timestamptz NOT NULL DEFAULT now(),
    retention_until   date        NOT NULL,
    plate_redacted_at timestamptz,
    CONSTRAINT raw_payload_is_object CHECK (jsonb_typeof(payload) = 'object'),
    -- Storage limitation is a schema rule, not a code comment: the raw zone may
    -- never be configured to keep records longer than 120 days after arrival
    -- (or after the logical date, if that is later).
    CONSTRAINT raw_retention_policy CHECK (
        retention_until <= GREATEST(logical_date, (loaded_at AT TIME ZONE 'UTC')::date) + 120
    ),
    CONSTRAINT raw_has_no_plate_after_redaction CHECK (
        plate_redacted_at IS NULL OR NOT (payload ? 'plate_raw')
    )
);

COMMENT ON TABLE raw.accidents_raw IS 'Short-lived landing zone; purged by retention policy (default 90 days)';
COMMENT ON COLUMN raw.accidents_raw.payload IS 'Allowlisted source fields only - no names, addresses, free text';

-- ---------------------------------------------------------------------------
-- Zone 2: clean. One row per accident, pseudonymized, no direct identifiers.
-- ---------------------------------------------------------------------------
CREATE TABLE clean.accidents (
    accident_key        char(64)    PRIMARY KEY,
    logical_date        date        NOT NULL,
    occurred_at         timestamp   NOT NULL,
    year                smallint    NOT NULL,
    month               smallint    NOT NULL CHECK (month BETWEEN 1 AND 12),
    hour_of_day         smallint    CHECK (hour_of_day BETWEEN 0 AND 23),
    time_bucket         text        CHECK (time_bucket IN ('night', 'am_peak', 'midday', 'pm_peak', 'evening')),
    state_code          text,
    district_code       text        NOT NULL,
    district_label      text,
    accident_type_code  text,
    accident_type_label text,
    segment_key         text        NOT NULL,
    segment_label       text,
    severity_class      smallint    NOT NULL CHECK (severity_class BETWEEN 1 AND 3),
    is_severe           boolean     NOT NULL,
    road_condition      text        NOT NULL CHECK (road_condition IN ('dry', 'wet', 'snow', 'unknown')),
    is_rain             boolean     NOT NULL,
    light_condition     text        CHECK (light_condition IN ('daylight', 'twilight', 'darkness', 'unknown')),
    participant_mix     text,
    plate_pseudonym     char(64),
    is_anonymized       boolean     NOT NULL DEFAULT false,
    data_quality_flag   text,
    grid_lat            numeric(8, 5),
    grid_lon            numeric(8, 5),
    source              text        NOT NULL,
    ingested_at         timestamptz NOT NULL DEFAULT now(),
    retention_until     date        NOT NULL,
    -- Defense in depth: the fact layer can only ever hold anonymized records.
    CONSTRAINT clean_only_anonymized CHECK (is_anonymized),
    CONSTRAINT clean_pseudonym_format CHECK (plate_pseudonym IS NULL OR plate_pseudonym ~ '^[0-9a-f]{64}$'),
    CONSTRAINT clean_time_or_quality_flag CHECK (hour_of_day IS NOT NULL OR data_quality_flag IS NOT NULL),
    CONSTRAINT clean_retention_policy CHECK (retention_until <= occurred_at::date + INTERVAL '10 years')
);

COMMENT ON TABLE clean.accidents IS 'Pseudonymized accident facts; plate_pseudonym is HMAC-SHA256(pepper, plate)';
COMMENT ON COLUMN clean.accidents.accident_key IS 'HMAC-SHA256 natural key - stable across reruns, not reversible to a location';
COMMENT ON COLUMN clean.accidents.is_anonymized IS 'Set true only after pseudonymization and PII removal succeeded';

-- ---------------------------------------------------------------------------
-- Zone 3: analytics. Aggregated metrics, no personal reference possible.
-- Grain: (segment_key, time_bucket) where 'all' holds segment-level KPIs.
-- ---------------------------------------------------------------------------
CREATE TABLE analytics.risk_metrics (
    segment_key            text        NOT NULL,
    time_bucket            text        NOT NULL CHECK (time_bucket IN ('all', 'night', 'am_peak', 'midday', 'pm_peak', 'evening')),
    segment_label          text,
    logical_date           date        NOT NULL,
    accident_count         bigint      NOT NULL CHECK (accident_count >= 0),
    severity_ratio         numeric(6, 4) CHECK (severity_ratio BETWEEN 0 AND 1),
    severity_index         numeric(8, 4),
    rain_share             numeric(6, 4),
    rain_uplift            numeric(8, 4),
    high_risk_score        numeric(6, 2) CHECK (high_risk_score BETWEEN 0 AND 100),
    time_of_day_risk_ratio numeric(8, 3),
    computed_at            timestamptz NOT NULL DEFAULT now(),
    retention_until        date        NOT NULL,
    PRIMARY KEY (segment_key, time_bucket),
    CONSTRAINT risk_metrics_retention_policy CHECK (retention_until <= logical_date + INTERVAL '10 years')
);

COMMENT ON TABLE analytics.risk_metrics IS 'Full-refresh decision layer: High-Risk Score, Time-of-Day Risk Ratio, Severity Ratio';
