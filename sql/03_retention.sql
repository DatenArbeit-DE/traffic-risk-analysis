-- Retention (GDPR Art. 5(1)(e), storage limitation).
--
-- Single source of truth: accident_risk.db.purge_expired() reads this file and
-- executes the statements one by one, logging the number of purged rows. The
-- file is deliberately parameter-free so an auditor can run it with psql:
--     docker compose exec -T postgres psql -U traffic -d traffic -f - < sql/03_retention.sql
--
-- Windows (see .env): raw 90 days, pseudonymized facts 10 years, aggregates
-- 10 years. Rows are only ever deleted here - never re-identified or archived.

-- Statement 1: drop raw landing rows past their short retention window.
DELETE FROM raw.accidents_raw WHERE retention_until < CURRENT_DATE;

-- Statement 2: drop pseudonymized accident facts past the 10-year window.
DELETE FROM clean.accidents WHERE retention_until < CURRENT_DATE;

-- Statement 3: drop aggregated metrics past the 10-year window.
DELETE FROM analytics.risk_metrics WHERE retention_until < CURRENT_DATE;
