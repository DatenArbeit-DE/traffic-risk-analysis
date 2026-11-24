-- Databases hosted by the single PostgreSQL instance.
-- Rationale: the 3-container constraint means Airflow's metadata database and
-- the analytics warehouse share an instance but stay in separate databases, so
-- DAG bookkeeping can never touch business data.

-- Airflow metadata (owned by the same role, separate database).
CREATE DATABASE airflow;

-- Integration-test database: lets `pytest` truncate and rebuild schemas
-- without touching the local demo warehouse.
CREATE DATABASE traffic_test;

COMMENT ON DATABASE airflow IS 'Apache Airflow metadata (managed by airflow db migrate)';
COMMENT ON DATABASE traffic_test IS 'Ephemeral target for integration tests';
