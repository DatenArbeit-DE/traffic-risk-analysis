-- Least-privilege access model.
--
-- Group roles only (NOLOGIN): no credentials live in SQL. In a real deployment
-- each service gets its own login role that is granted one of these groups.
--   traffic_etl     -> read/write raw, clean, analytics
--   traffic_analyst -> read clean + analytics only (the raw zone is invisible)
--
-- Runs after 01_schema.sql / 02_indexes.sql (docker-entrypoint-initdb.d order).

CREATE ROLE traffic_etl NOLOGIN;
CREATE ROLE traffic_analyst NOLOGIN;

GRANT USAGE ON SCHEMA raw, clean, analytics TO traffic_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw, clean, analytics TO traffic_etl;

GRANT USAGE ON SCHEMA clean, analytics TO traffic_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA clean, analytics TO traffic_analyst;

-- The application role used by Airflow and the processing container.
GRANT traffic_etl TO traffic;
GRANT traffic_analyst TO traffic;

COMMENT ON ROLE traffic_analyst IS 'Read-only reporting role: no access to the raw landing zone by design';
