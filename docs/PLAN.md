# Nimbus — Plan

> Source of truth for requirements: [`docs/PROJECT_BRIEF.md`](PROJECT_BRIEF.md).
> This file tracks phase-by-phase progress with checkboxes. Full architecture, Kafka design, data model, volume estimates, risks, and external-API verification notes from planning session 1 live in `docs/decisions/0001-initial-architecture-verification.md`.

## Ground rules (every phase)

- No cost: every data source, service, and library used is free/open-source and self-hosted, except the Anthropic API, which stays opt-in (`LLM_ENABLED=false` by default) and is never enabled without the user explicitly adding a key.
- `make lint`, `make typecheck`, `make test` must pass before a phase is called done.
- Every significant design decision gets an ADR in `docs/decisions/`.
- Each phase ends with a summary + talking points appended to `docs/interview-notes.md`, then stops for a fresh session.

## Phase 0 — Plan and foundation

- [x] Interview user, verify external APIs/libraries against current docs (session 1)
- [x] Approve plan, save `docs/PLAN.md`
- [x] Write `CLAUDE.md`
- [x] Git repo init + first commit
- [x] Repo scaffold (`src/nimbus/...`, `config/`, `dashboard/`, `migrations/`, `orchestration/airflow/`, `tests/`, `evals/`, `docs/`)
- [x] `uv` project (`pyproject.toml`) with pinned dependencies (SQLAlchemy 2.0.x, httpx <1.0, curated ruff ruleset — see ADR 0001)
- [x] `.env.example`
- [x] `docker-compose.yml`: Kafka (KRaft), Kafbat UI, Postgres, each with health checks
- [x] Alembic migration scaffold (`silver`, `gold`, `ops` schemas) — `alembic heads`/`alembic history` verified; not yet run against a live Postgres (no Docker yet)
- [x] `Makefile`: `up`, `down`, `logs`, `demo`, `backfill`, `test`, `test-integration`, `lint`, `typecheck`, `eval`, `trace`, `replay` (`demo`/`backfill`/`eval`/`trace`/`replay` call CLI entry points that land in later phases)
- [x] pre-commit config (ruff, mypy) — installed and run clean against all files
- [x] GitHub Actions CI (lint, typecheck, unit test, integration test)
- [x] `common/settings.py` (pydantic-settings) + `common/logging.py` (structured JSON logs)
- [x] Smoke test (Testcontainers: produce a Kafka message, consume it, write a row to Postgres) — **run against real Docker, passing**
- [x] README skeleton
- [x] Docker Desktop installed; `make up` verified healthy (Kafka, Kafbat UI, Postgres all `healthy`); Alembic migration applied to the live DB; smoke test (`make test-integration`) run and passing
- [x] Phase 0 summary + talking points in `docs/interview-notes.md`

*Acceptance (brief §15): `make up` is healthy from a fresh clone, and `make test` and CI pass.*

## Phase 1 — Forecast ingestion slice ✅

- [x] Forecast producer (5 locations × 3 models, Single Runs API) — `nimbus.ingestion.forecast_producer`
- [x] Bronze sink consumer (Parquet lake) — `nimbus.streaming.bronze_sink`
- [x] Silver consumer: validation, pandas transforms, DLQ, idempotent upserts — `nimbus.streaming.forecast_silver`
- [x] `config/locations.yaml` (5 starter locations, contrasting climates) — done in Phase 0
- [x] `config/models.yaml` (3 of 4 planned models for this phase)
- [x] Idempotent Kafka topic provisioning (`nimbus.jobs.init_topics`, wired into `make up`)
- [x] `silver.forecast` table (migration 0002)
- [x] ADR 0002: bronze/silver design, DLQ shape, the chunked-upsert bug found via manual verification

*Acceptance: forecasts queryable in silver (verified: 10,080 real rows from 15 real Open-Meteo events); zero duplicates on re-run (verified twice — automated integration test + a real-run duplicate-run edge case caught live); malformed messages land in DLQ without stopping the consumer (verified); kill/restart mid-batch loses nothing (verified via a dedicated restart-safety integration test).*

## Phase 2 — Observations and backfill

- [x] METAR producer (aviationweather.gov) + silver consumer + `silver.observation`
- [x] Corrected/late report handling (COR flag; correction outranks original even in one batch)
- [x] Expand to all 25 locations (every station verified live)
- [x] Backfill: Open-Meteo Previous Runs (forecasts) + IEM ASOS (observations), same topics, rate-limit aware and resumable — ADR 0004
- [x] `--drain` consumers (lag-based) so `make demo` / `make backfill` run unattended
- [x] `replay bronze` (rebuild from the lake) and `replay offsets` (reset a group)
- [x] `make reconcile` — rebuild-equivalence check, results in `ops.reconciliation_results`
- [x] `docs/runbook.md`
- [x] `make demo` (30 days) run against the live providers from a clean checkout on a GitHub runner (`live-demo.yml`): 1,512,000 forecast + 108,376 observation rows, reconcile `MATCH`, 0 NaN — ADR 0004
- [x] Four months (120 days, 2026-05-22 to 2026-09-18) for all 25 locations and stations, from a clean checkout on real providers: 6,048,000 forecast + 435,832 observation rows, reconcile `MATCH`, 0 NaN (ADR 0004)
- [ ] *(optional)* full `make backfill` since 2024-01-01 — **not run**; projected ~50M forecast rows, ~3.2h drain, ~15k API calls over two days

*Acceptance:*
- *months of history per location* — **MET for four months** (120 days, all 25 locations and stations, reconciled `MATCH`), from a clean checkout on real providers, after fixing three things the real runs found (truncated responses, a slow observation transform, IEM 503/429s). The full history since 2024-01-01 has not been run.
- *reconciliation counts match* — covered by `tests/integration/test_backfill_and_rebuild.py` (poison messages accounted for; missing/extra rows detected).
- *silver rebuilds from bronze via the runbook* — covered by the same test: truncate both tables, rebuild from a real lake, every row identical. Manual walk-through of `docs/runbook.md` §4 on the real stack still to do.

## Phase 3 — Gold layer and data quality

Built in three slices (3a gold, 3b quality, 3c lineage + partitioning), each pushed and checked in CI. Design: [ADR 0005](decisions/0005-phase3-gold-quality-lineage.md).

- [x] `forecast_verification` via `merge_asof` (nearest observation within 30 min, error = forecast − observed, whole-day lead bucket); `silver.dim_*` tables for the station join
- [x] `accuracy_daily` and the `model_leaderboard` view (rolling 7/30 days, exact weighted aggregates)
- [x] Incremental recompute (watermark on `updated_at`, lookback, ±1-day observation expansion, config-fingerprinted watermark) and idempotent (days synced, not appended; re-run leaves tables identical)
- [x] pandera checks before every load — blocking vs warning, hard vs plausible ranges — results in `ops.quality_results`; silver consumers quarantine to the DLQ, gold aborts the day, `reconcile` applies the same gate
- [x] Freshness monitoring (per station, per live source) and `make quality`
- [x] `make trace` (API request → topic/partition/offset → bronze file → silver → gold verification and accuracy rows)
- [x] Monthly partitioning of `silver.forecast` (migration 0008, `make partitions`) and a retention policy (implemented, **off by default**; `reconcile` compares only the retained window)
- [x] Real-provider run (30 days + a 3-day `make trace` run): reconcile `MATCH`, gold re-run identical, the quality gate quarantined 3 of 27,102 real observation messages (a truncated IEM METAR)
- [x] Independent review of the phase diff against this plan: 1 high, 2 medium, 3 low findings; all six fixed with tests (empty forecast payload crash, config change not rebuilding, retention erasing gold history, duplicate-event quarantine, DLQ reason, hPa data migration note)
- [x] Bug found and fixed on the way: METAR pressure was stored in hPa next to forecasts in Pa

*Acceptance:*
- *jobs idempotent* — **MET**, integration-tested (an incremental re-run, `--full`, and a 24-hour-lookback re-run leave both gold tables identical, `computed_at` included; a one-observation change rebuilds exactly three days) and re-checked on real data by the live-demo workflow (checksum before/after).
- *an event traces end to end* — **MET**, integration-tested against a real lake (bronze → silver → gold, including "overwritten by a later event") and on real events in the live demo.
- *results plausible: error rises with lead time* — **MET on real data** (30 days, 25 locations, 1.43M verified values): MAE rises monotonically from lead day 1 to 7 for temperature (1.44 → 2.16 K), dew point, wind and pressure; ICON < ECMWF < GFS for day-1 temperature that month. Numbers, timings and the real METAR truncation the quality gate caught are in ADR 0005.
- Local Docker was unavailable, so all Postgres/Kafka behaviour was verified in CI (179 unit + 34 integration tests).

## Phase 4 — Streaming alerts and dashboard v1

Split in two (the phase is large): **4a** the detector, **4b** the dashboard.

**4a — anomaly detector** ([ADR 0006](decisions/0006-phase4-anomaly-detector.md))
- [x] Rules: run-to-run change, model spread, observation miss; thresholds in `config/alerts.yaml`
- [x] `python -m nimbus.alerts.detector` (`make alerts`): live events -> `weather.alert.v1` and `gold.alert`; no in-memory state (silver is read on demand), deterministic alert ids, insert-publish-mark delivery
- [x] Recorded-event replay through real Kafka + Postgres: alert published within 60 s, exactly once when replayed (integration test)
- [x] `--once` on both live producers (demos, schedulers)
- [x] ADR on how Kafka Streams / Flink would manage this state at scale
- [ ] Alerts against real live data (live-demo workflow: one live cycle, then the detector)

**4b — dashboard v1** ([ADR 0007](decisions/0007-phase4-dashboard.md))
- [x] Query layer (`nimbus.dashboard.queries`, `kafka_health`) - plain functions, integration-tested against Postgres/Kafka
- [x] Streamlit app (`make dashboard`): Pipeline Health (throughput, consumer lag, DLQ, freshness, quality, reconciliation, alerts), Forecast vs Actual, Accuracy (leaderboard, error vs lead time, best model by location), Lineage
- [x] Every page renders headlessly (`AppTest`) on an empty and a seeded database; health survives Kafka being down
- [ ] Pages rendered against the real 30-day data (live-demo workflow: `dashboard_smoke`)

*Acceptance: alerts appear within a minute of a triggering event (demonstrated by replaying a recorded event); pages work against real data.*

## Phase 5 — LLM briefings

- [ ] Fact-sheet builder, briefing generator, grounding check
- [ ] Caching by fact-sheet hash, `ops.llm_calls` logging
- [ ] Dashboard: Briefings, LLM Usage pages
- [ ] User adds Anthropic API key + billing cap (blocked until then)

*Acceptance: briefings pass schema validation; grounding check rejects an invented number; cache hits skip the API; platform runs with `LLM_ENABLED=false`.*

## Phase 6 — AI agent

- [ ] Agent loop on the raw Anthropic SDK, tools, `semantic_layer.yaml`
- [ ] Read-only Postgres role, `run_sql` via sqlglot SELECT-only enforcement
- [ ] `evals/agent_questions.yaml`, `make eval`
- [ ] Dashboard: Ask Nimbus, Replay Proposals pages

*Acceptance: `make eval` ≥ 80%; non-SELECT SQL rejected; tool trace shown; replay proposals need human approval.*

## Phase 7 — Orchestration and hardening

- [ ] Airflow 3.x DAGs (or documented fallback — see ADR risk on Airflow 3.x rewrite)
- [ ] Integration test in CI
- [ ] Completed runbook

*Acceptance: batch jobs run on schedule with no manual steps; integration test passes in CI; someone unfamiliar can recover from a crashed consumer/bad transform using only the runbook.*

## Phase 8 — Portfolio polish

- [ ] README: problem statement, diagrams, quickstart, decisions, measured results, limitations, 1000× scale section
- [ ] Resume bullets from measured numbers only

*Acceptance: every check in brief §17 passes.*

## Stretch goals (only after Phase 8, ask first)

- [ ] ML bias-correction model
- [ ] Second forecast provider
- [ ] Schema Registry + Avro
- [ ] MCP server for agent tools
- [ ] Prometheus + Grafana
- [ ] Cloud deployment
