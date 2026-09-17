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
- [ ] Write `CLAUDE.md`
- [ ] Git repo init + first commit
- [ ] Repo scaffold (`src/nimbus/...`, `config/`, `dashboard/`, `migrations/`, `orchestration/airflow/`, `tests/`, `evals/`, `docs/`)
- [ ] `uv` project (`pyproject.toml`) with pinned dependencies (SQLAlchemy 2.0.x, httpx <1.0, curated ruff ruleset — see ADR 0001)
- [ ] `.env.example`
- [ ] `docker-compose.yml`: Kafka (KRaft), Kafbat UI, Postgres, each with health checks
- [ ] Alembic migration scaffold (`silver`, `gold`, `ops` schemas)
- [ ] `Makefile`: `up`, `down`, `logs`, `demo`, `backfill`, `test`, `test-integration`, `lint`, `typecheck`, `eval`, `trace`, `replay`
- [ ] pre-commit config (ruff, mypy)
- [ ] GitHub Actions CI (lint, typecheck, unit test)
- [ ] `common/settings.py` (pydantic-settings) + `common/logging.py` (structured JSON logs)
- [ ] Smoke test: produce a Kafka message, consume it, write a row to Postgres
- [ ] README skeleton
- [ ] Docker Desktop + WSL2 installed by user; `make up` verified healthy
- [ ] Phase 0 summary + talking points in `docs/interview-notes.md`

*Acceptance (brief §15): `make up` is healthy from a fresh clone, and `make test` and CI pass.*

## Phase 1 — Forecast ingestion slice

- [ ] Forecast producer (5 locations × 3 models, Single Runs API)
- [ ] Bronze sink consumer (Parquet lake)
- [ ] Silver consumer: validation, pandas transforms, DLQ, idempotent upserts
- [ ] `config/locations.yaml` (5 starter locations, contrasting climates)

*Acceptance: forecasts queryable in silver; zero duplicates on re-run; malformed messages land in DLQ without stopping the consumer; kill/restart mid-batch loses nothing.*

## Phase 2 — Observations and backfill

- [ ] METAR producer (aviationweather.gov)
- [ ] Backfill CLI (Open-Meteo Previous Runs API + IEM ASOS `asos.py` endpoint — see ADR)
- [ ] Corrected/late report handling
- [ ] Expand to all ~25 locations

*Acceptance: months of history per location; reconciliation counts match; silver rebuilds from bronze via the runbook.*

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
