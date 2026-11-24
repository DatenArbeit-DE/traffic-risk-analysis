# Data governance and privacy design

This document explains how personal data is handled in the pipeline, which
technical and organisational measures are implemented, and what the residual
risks are. It is written as if the pipeline processed a real, personal-data-
bearing feed, because the measures exist to make that safe - not to satisfy a
checkbox for an already-anonymous dataset.

## 1. Roles and lawful basis

| Question | Answer |
|---|---|
| Controller | The road-safety authority publishing the statistics (in a real deployment: the city/state office) |
| Processor | Whoever operates this stack |
| Data subjects | Accident participants (drivers, cyclists, pedestrians) |
| Source data | [Unfallatlas](https://unfallatlas.statistikportal.de/) - accidents with personal injury, published aggregated and geocoded, licence DL-DE-BY-2.0 |
| Special categories (Art. 9) | Not processed. No health data is ingested; only a coarse injury class (slight / serious / fatal) that the source already publishes |
| Lawful basis (in production) | Art. 6(1)(e) - performance of a task in the public interest (road-safety planning), supported by a DPIA |

The repository ships a **synthetic fixture**, not real records. The fixture
contains deliberately PII-shaped columns (`Name`, `Strasse`, `Halter`) so the
governance controls can be demonstrated end to end.

## 2. Data flow and zones

```
release file ──extract──► raw.accidents_raw ──anonymize──► staging CSV ──load──► clean.accidents ──analyze──► analytics.risk_metrics
   (external)              allowlisted JSON     HMAC pseudonyms   no plate     pseudonymized facts   aggregated only
                           ≤ 90 days (≤120)     + quality rules   column       ≤ 10 years            ≤ 10 years
```

| Zone | Contents | Identifiability | Retention | Who can read it |
|---|---|---|---|---|
| `raw` | allowlisted JSONB payload, transient `plate_raw` key | pseudonymizable, not linkable after load | 90 days (schema ceiling 120) | `traffic_etl` only |
| `clean` | one row per accident, `accident_key`, `plate_pseudonym` | pseudonymous (Art. 4(5)) | 10 years | `traffic_etl`, `traffic_analyst` |
| `analytics` | segment × time bucket KPIs | anonymous (aggregates ≥ 20 accidents) | 10 years | `traffic_etl`, `traffic_analyst` |

Retention is enforced by `sql/03_retention.sql`, executed by every `anonymize`
run and parameter-free so an auditor can run it manually.

## 3. Minimization

* **Allowlist projection** in `extract.project_row`: 18 source fields are mapped
  by name; every other column is dropped and reported in the run summary
  (`dropped_source_columns`). A new column in a future release is therefore
  ignored by default.
* **Free text is never stored.** Names, street names, owner fields and free-text
  location descriptions are dropped before anything is persisted.
* **Location is coarse.** The source is gridded to 100 m; the pipeline keeps the
  coordinates rounded to 5 decimals and never joins them to an address.
* **Participant flags are aggregated** into a single `participant_mix` string
  (`car+hgv`, `bicycle+pedestrian`, ...). No per-vehicle row exists anywhere.


## 4. Pseudonymization

* `accident_key = HMAC-SHA256(pepper, occurred_at|district|accident_type|lat|lon)`.
  The natural key makes the pipeline idempotent while the HMAC hides the
  pre-image; the pepper is required from the environment (`PSEUDONYMIZATION_PEPPER`).
* `plate_pseudonym = HMAC-SHA256(pepper, normalized_plate)`.
  See `docs/decisions.md` (ADR-001) for why a keyed hash is mandatory here: the
  German plate space is only ~10^7 keys, so `sha256(plate)` is brute-forceable
  in minutes. Invalid or unparseable plates are discarded, never hashed.
* The pepper is never written to disk by the pipeline, never logged, and losing
  it makes existing pseudonyms unlinkable - which is the intended effect.

## 5. Defensive rationale (why anonymize an already-anonymous source?)

1. Minimization must be **ours**, not assumed: the pipeline does not trust the
   upstream release to stay PII-free.
2. **Future releases may add fields** (free-text location, plate). The allowlist
   plus the transient-plate protocol means such a field cannot silently persist.
3. **Vehicle-level identifiers** are a re-identification risk even in aggregate
   data; tokenization converts them into something that supports "how often did
   this vehicle appear" without exposing the plate.
4. **Storage limitation** must be executable, not aspirational.

## 6. Controls, and where they are verified

| Control | Implementation | Verified by |
|---|---|---|
| Only allowlisted fields persisted | `extract.SOURCE_FIELD_ALLOWLIST` | `tests/test_extract.py`, `verify` (`no_pii_named_columns`) |
| No unanonymized row can be stored | `CHECK (is_anonymized)` in `sql/01_schema.sql` | `tests/test_pipeline.py` (CHECK constraint rejection test) |
| No clear-text plate survives the run | `load.run` removes `payload->'plate_raw'` inside the loading transaction | `verify` (`no_plates_left_in_raw`), `plate_redacted_at` audit column |
| Retention executed | `sql/03_retention.sql`, `db.purge_expired` | `verify` (`raw_retention_respected`, `clean_retention_respected`) |
| Retention window cannot be widened silently | schema `CHECK` constraints + `config.get_settings` bounds | `tests/test_metrics.py`, `tests/test_pipeline.py` |
| Small groups cannot be published | `HAVING count(*) >= min_segment_accidents` (default 20) | `tests/test_pipeline.py` |
| Least privilege | `traffic_analyst` has no `USAGE` on `raw` | `sql/04_roles.sql` |
| Credentials not in git | `.env` git-ignored, pepper from env, `.env.example` has placeholders | manual review |

`make verify` (or `python -m accident_risk.cli verify`) prints the full report and
exits non-zero if any check fails, so it can be used as a CI gate or an
operational health check.


## 7. Data subject rights

* **Access / portability**: data is pseudonymous and the pepper is the only
  re-identification path. A request can only be answered if the controller can
  map a subject to an `accident_key`, which requires additional information held
  outside this system (e.g. a police report number). This is a documented
  limitation of pseudonymization, not an accident.
* **Erasure**: delete by `accident_key` in `clean.accidents`; the analytics layer
  is derived and is fully rebuilt on the next `analyze` run, so aggregates can
  never retain a deleted record for longer than one refresh cycle.
* **Rectification**: source corrections are handled by re-running the partition
  (`extract` replaces the logical-date partition, `load` is idempotent).

## 8. Residual risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Pepper leak permits plate brute force | low | secret manager, rotation, never logged; rotation invalidates old pseudonyms |
| Re-identification through rare grid cells or timestamps | medium | 100 m grid, month granularity, hour-of-day buckets, minimum segment size |
| Rare accident-type x district segments | medium | `MIN_SEGMENT_ACCIDENTS` floor removes them from the analytics layer |
| Backups outlive the retention window | medium (operational) | backup retention aligned with `retention_until`; tracked as a runbook item |
| Raw zone readable by an over-broad role | low | `traffic_analyst` cannot see `raw`; no `PUBLIC` grants were added |

## 9. Out of scope

This project does not implement consent management, a record of processing
activities, a DPIA or a data-processing agreement; those are organisational
artefacts. It implements the technical measures such documents would reference.
