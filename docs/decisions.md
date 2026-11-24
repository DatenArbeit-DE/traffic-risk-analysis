# Architecture decision records

Short records of the decisions that shaped this project, why they were taken,
and what the alternatives were. Format: context -> decision -> consequences.

---

## ADR-001: Keyed HMAC-SHA256 for every identifier, pepper from the environment

**Context.** Two identifiers are produced: `accident_key` (a stable natural key
so `load` can be idempotent) and `plate_pseudonym` (a token for "how often did
this vehicle appear"). A plain `sha256` is the default choice in most tutorials.

**Decision.** Both are `HMAC-SHA256(PSEUDONYMIZATION_PEPPER, value)`. The pepper is
a mandatory setting, read from `.env`, validated at startup, and never persisted
by the pipeline.

**Why not a plain hash.** The German licence-plate space is on the order of
10^7 - an unsalted SHA-256 falls to a rainbow table in minutes. A keyed hash
raises the attack cost from "offline dictionary" to "steal the key".
`accident_key` gets the same treatment because its pre-image
(`occurred_at|district|type|lat|lon`) is small enough to enumerate for a known
accident.

**Consequences.** The pepper becomes a secret to manage (rotation invalidates old
pseudonyms - intentional), and the pseudonyms are unlinkable across environments
with different peppers, which is exactly what you want between prod and dev.

---

## ADR-002: Allowlist the raw zone, and carry the plate only as a transient key

**Context.** The published Unfallatlas release is already aggregated and
PII-free. The "anonymous data needs no governance" shortcut would be tempting.

**Decision.** `extract` projects an explicit 18-field allowlist and reports every
dropped source column in the run summary. A `plate_raw` key may pass through the
raw zone (that is what makes the vehicle tokenization demonstrable), and `load`
deletes it inside the same transaction that inserts the clean rows, recording
`plate_redacted_at`.

**Why.** Upstream schema changes are the normal case, not the exception: a new
free-text or plate field must not silently become "our" data. The allowlist makes
the default action *drop*, and the transient-plate protocol keeps the single
sensitive field visible-but-audited for the shortest possible window.

**Consequences.** Adding a source field is an explicit code change plus a test
update - deliberate friction. The raw zone additionally cannot be trusted as a
long-term archive (see ADR-006).

---

## ADR-003: Five explicit tasks, not one monolithic "process" task

**Context.** A single Python entry point (`cli run`) could do everything in one
Airflow task.

**Decision.** The DAG has five tasks - `extract`, `anonymize`, `load`, `analyze`,
`visualize` - each delegating to the same function the CLI uses. Each task
returns a small JSON summary (no DataFrames, no row payloads) that is pushed to
XCom; the row payload itself moves through the filesystem.

**Why.** The boundaries are the interesting part of the problem: landing,
de-identification, idempotent loading, aggregation, presentation. Separate tasks
make failures attributable, allow retries from the failed step, and keep XCom
small (Airflow's metadata DB is not a data channel).

**Consequences.** Each step must be re-runnable on its own; that constraint is
what produced the partition-replace and upsert logic, and it is covered by
`tests/test_pipeline.py` (the partition-replace and idempotency tests).

---

## ADR-004: A file-based staging artifact between `anonymize` and `load`

**Context.** `anonymize` reads the raw partition, cleans it and produces the
pseudonymized fact rows; `load` inserts them into `clean.accidents`. The obvious
shortcut is to return the rows through XCom or keep everything in one task.

**Decision.** `anonymize` writes `data/staging/<date>/clean_accidents.csv.gz` and
returns only its path plus quality counters. `load.read_staging` re-reads it with
an explicit dtype map and refuses any row where `is_anonymized` is not true.

**Why.** XCom is serialized into the Airflow metadata database - pushing ~90k
rows through it would bloat the scheduler DB and make retries expensive. A file
artifact is inspectable (`zcat` + head), reprocessable without re-running the
transform, and gives the load step a real contract to validate.

**Consequences.** The staging directory needs the same protection as the clean
zone (it contains pseudonyms) and is deleted on `make clean`. The reader is the
natural place to enforce "no unanonymized row reaches the warehouse", which is
covered by a test.

---

## ADR-005: Full refresh of the analytics layer instead of incremental upserts

**Context.** The metrics table is ~10k rows (segments x 6 rows). Incremental
maintenance would mean diffing segment aggregates against a previous snapshot.

**Decision.** `analyze` computes everything for the window in a single SQL pass
and replaces the table content inside one transaction (`DELETE` + `INSERT ...
SELECT`). The whole run is idempotent by construction.

**Why.** The dataset is small enough that a full refresh costs a couple of
seconds, while incremental logic would add a second, untested code path whose
failure mode is silently stale numbers. Correctness beats micro-optimisation at
this size, and the query is the interesting artefact to review.

**Consequences.** If the fact table ever reaches tens of millions of rows, this
becomes a real cost and ADR-005 must be revisited (partition by month and
refresh the affected months only). `tests/test_pipeline.py` guards idempotency
in the meantime.

---

## ADR-006: Retention windows are anchored on arrival, and enforced in schema

**Context.** The first end-to-end run used `retention_until = logical_date + 90
days` for the raw zone. With a backdated demo (`--date 2024-03-01`) the very next
`anonymize` step purged all 90k raw rows, because 2024-05-30 is in the past.

**Decision.** The raw retention timestamp is anchored on
`max(logical_date, date.today())` - the day the data actually arrives - and the
schema constraint was changed to
`retention_until <= GREATEST(logical_date, loaded_at::date) + 120`. The 120-day
ceiling is a hard limit in `CHECK`, mirrored by a validated upper bound in
`config.get_settings`.

**Why.** A retention policy that deletes a legitimate backfill is a bug, not a
policy. Anchoring on arrival is also what the GDPR asks for ("no longer than
necessary" *after collection*), while the schema constraint guarantees that no
future configuration change can silently widen the window.

**Consequences.** Raw retention depends on wall-clock time, so the same logical
date yields different timestamps on different days - intentional, and asserted
both in the governance report (`raw_retention_respected`) and in
`tests/test_pipeline.py`.

---

## ADR-007: A "segment" is district x accident type, time buckets come from the hour

**Context.** The data has to support decisions like "where do we put a crossing?"
or "when is rain worst?". Any grouping is a modelling choice.

**Decision.** The analysis grain is `segment_key = district_code|accident_type`
(26 districts x 7 types = 182 segments in the fixture), with a 5-way time dimension:
`night` (22-05), `am_peak` (06-09), `midday` (10-15), `pm_peak` (16-19),
`evening` (20-21), plus an `all` row per segment.

**Why.** District x type is the coarsest grain that still yields an actionable
statement ("pedestrian crossings in Berlin"), and it is stable over time - unlike
street names, which change with every rename and would leak location detail.
Hour-of-day buckets are the finest temporal resolution that is still
non-identifying at 100 m grid level.

**Consequences.** `time_bucket = 'all'` duplicates totals by design: it is the
grain dashboards filter on, while the five bucket rows carry the
`time_of_day_risk_ratio`. A test asserts the bucket counts sum to the total.

---

## ADR-008: A transparent composite score (0.45 severity / 0.30 density / 0.25 rain)

**Context.** "High risk segment" needs a single number, but severity, exposure and
weather are not comparable units.

**Decision.** Each axis is min-max normalised across segments (0-1), then combined
as `100 * (0.45 * severity_index + 0.30 * accident_density + 0.25 * rain_uplift)`.
The weights live in `.env` (`RISK_WEIGHT_*`, validated to sum to 1) and the
component values are stored next to the score.

**Why.** A weighted sum is auditable: a planner can see *why* a segment scored
61.4 and can re-weight exposure without touching SQL. Severity dominates because
the cost of a severe accident is disproportionate; density stands in for
exposure; rain uplift is the only actionable environmental factor available in
this source. Normalising per axis prevents "big district wins" - a frequent
but mild segment cannot outrank a smaller, deadlier one on volume alone.
Unfallatlas provides no traffic-volume or segment-length data, so true
flow-normalized exposure cannot be computed from this source; accident share is
used as the exposure proxy.

**Consequences.** The score is relative to the window (min-max), so it is not
comparable across runs with different windows; that is documented and the raw
components are kept for anyone who needs absolute values. `tests/test_metrics.py`
locks the weights and the normalisation SQL shape.

---

## ADR-009: Airflow 3 runs as a single container for the demo

**Context.** Airflow 3 splits scheduler, API server, dag-processor, triggerer and
workers. The honest production layout is five containers plus Redis.

**Decision.** This demo runs all Airflow 3 components - `api-server`, `scheduler`
and `dag-processor` - inside one container, supervised by
`docker/airflow/entrypoint.sh`, with a Postgres-backed metadata database (the
`airflow` DB on the same server). No triggerer: the DAG has no deferrable
operators. The DAG uses TaskFlow + the same Python package the processing
container mounts, and its dependencies are installed under Airflow's own
constraint file (`constraints-3.4.0/constraints-3.11.txt`), so pandas/numpy match
what the scheduler was tested with. `requirements/processing.txt` therefore uses
ranges, not pins, and both images land on pandas 3.x.

**Why.** The project is about the data pipeline, not about proving I can copy five
`airflow` service definitions into a compose file. One container keeps the demo
startable in under a minute on a laptop, and everything the DAG needs
(`/src`, `/sql`, `/data`) is mounted identically in both containers - so the
production split would only be a compose change.

**Consequences.** `docker compose up` starts a scheduler that is not
highly available, and DAG parsing shares CPU with the API server. Documented in
the README as the first thing to change for anything beyond a demo. Because the
two images resolve their dependency ranges independently (the Airflow one under a
constraint file), `make versions` prints python/pandas/numpy/matplotlib from both
containers - today pandas 3.1.1 vs 3.1.0 - so a drift after an image rebuild is
visible instead of silent.

---

## ADR-010: Ship a schema-accurate fixture and make it the default source

**Context.** The real Unfallatlas releases are published per year through
open-data portals without a stable API, and they are large (~1 GB CSV per year).

**Decision.** `tools/make_fixture.py` generates a deterministic (seeded) 90k-row
fixture with the source's column names, plausible German districts, accident
types, hours, coordinates and - deliberately - PII-shaped columns, plus ~7% of
rows carrying a licence plate. `EXTRACT_SOURCE=unfallatlas` switches to the real
download URLs from `.env`.

**Why.** Reviewers must be able to run the whole thing offline, and the
governance story needs data that *actually* contains plates and names to prove
the anonymization works. A fixture that omitted them would let a broken allowlist
pass unnoticed.

**Consequences.** The fixture is synthetic: absolute numbers are meaningless, only
the pipeline behaviour is. The README states this explicitly and attributes the
source. Regenerate with `make fixture`.

---

## ADR-011: Deadline Alerts replace the removed task SLA; `tasks test` for validation

**Context.** Airflow 3.0 removed the task-level `sla=` kwarg (it now emits a
deprecation warning and is silently ignored). The replacement is the Deadline
Alert, configured on the DAG via `deadline=DeadlineAlert(...)`.

**Decision.** The one-hour budget that would have been a task SLA is expressed as a
Deadline Alert: `DeadlineReference.DAGRUN_QUEUED_AT`, `interval=timedelta(hours=1)`,
and a `SyncCallback` that logs the miss. `sla=` is not used anywhere.

**Why.** Deadline Alerts are the only Airflow 3.x mechanism that demonstrates
"the run took too long -> alert". A synchronous callback matches this demo's
single-container setup (no triggerer).

**Consequences.** Known issue: `airflow dags test` fails on Airflow 3.4.0 when a
Deadline Alert is configured - `dagrun.update_state` passes the alert object where
a database id is expected, raising a `ProgrammingError`. The DAG parses and
schedules normally; only the `dags test` runner trips. Per-task validation
therefore uses `airflow tasks test <dag> <task> <date>` instead.
