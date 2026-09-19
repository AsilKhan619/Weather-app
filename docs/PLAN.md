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
- [ ] **Run `make demo` and `make backfill` against the live providers** and confirm several months of history for all 25 locations (needs local Docker; see below)

*Acceptance:*
- *months of history per location* — **OPEN.** The first real run (GitHub runner, 30 days, 25 locations; `live-demo.yml`) worked for `make up` and for observations (27,094 reports, 0 failures) but 5 of 15 forecast requests failed on truncated responses; fixed by batching locations, not yet re-run cleanly. Then run a longer window / `make backfill` (spans two days — ADR 0004) and record measured row counts and call usage here.
- *reconciliation counts match* — covered by `tests/integration/test_backfill_and_rebuild.py` (poison messages accounted for; missing/extra rows detected).
- *silver rebuilds from bronze via the runbook* — covered by the same test: truncate both tables, rebuild from a real lake, every row identical. Manual walk-through of `docs/runbook.md` §4 on the real stack still to do.

## Phase 3 — Gold layer and data quality

- [ ] `forecast_verification` via `merge_asof`
- [ ] `accuracy_daily`, incremental recompute
- [ ] pandera checks, freshness monitoring
- [ ] `make trace`
- [ ] Monthly partitioning + retention policy for `silver.forecast` (per volume estimate in ADR 0001)

*Acceptance: error rises with lead time; jobs idempotent; an event traces end to end.*

## Phase 4 — Streaming alerts and dashboard v1

- [ ] Anomaly detector → `weather.alert.v1`
- [ ] Dashboard: Health, Forecast vs Actual, Accuracy, Lineage pages

*Acceptance: alerts appear within a minute of a triggering event; pages work against real data.*

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
