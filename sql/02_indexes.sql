-- Indexes follow the three real access patterns:
--   1. retention purges (retention_until)
--   2. segment x hour aggregation for the heatmap
--   3. ranking segments by risk score for the top-10 chart

CREATE INDEX IF NOT EXISTS raw_accidents_logical_date_idx
    ON raw.accidents_raw (logical_date);
CREATE INDEX IF NOT EXISTS raw_accidents_retention_idx
    ON raw.accidents_raw (retention_until);
CREATE INDEX IF NOT EXISTS raw_accidents_unredacted_idx
    ON raw.accidents_raw (logical_date)
    WHERE plate_redacted_at IS NULL;

CREATE INDEX IF NOT EXISTS accidents_segment_hour_idx
    ON clean.accidents (segment_key, hour_of_day)
    WHERE hour_of_day IS NOT NULL;
CREATE INDEX IF NOT EXISTS accidents_occurred_at_idx
    ON clean.accidents (occurred_at);
CREATE INDEX IF NOT EXISTS accidents_logical_date_idx
    ON clean.accidents (logical_date);
CREATE INDEX IF NOT EXISTS accidents_retention_idx
    ON clean.accidents (retention_until);

CREATE INDEX IF NOT EXISTS risk_metrics_score_idx
    ON analytics.risk_metrics (high_risk_score DESC)
    WHERE time_bucket = 'all';
CREATE INDEX IF NOT EXISTS risk_metrics_ratio_idx
    ON analytics.risk_metrics (time_of_day_risk_ratio DESC)
    WHERE time_bucket <> 'all';
