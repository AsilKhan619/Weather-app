# CLAUDE.md

Nimbus: a streaming weather-forecast accuracy platform (Kafka + Python + pandas + Postgres, grounded LLM briefings, an AI agent).

- **Requirements source of truth:** [`docs/PROJECT_BRIEF.md`](docs/PROJECT_BRIEF.md) — read it before proposing any design change.
- **Progress and phase checklist:** [`docs/PLAN.md`](docs/PLAN.md) — check/update boxes as work lands; don't duplicate its content here.
- **Design decisions:** [`docs/decisions/`](docs/decisions/) — one ADR per significant choice, including external API verification notes.
- **Interview notes / talking points:** [`docs/interview-notes.md`](docs/interview-notes.md).

## Compaction

If this conversation is compacted, the summary MUST preserve: the current phase (see `docs/PLAN.md` for the active checklist), the list of files changed so far in this phase, and the exact test/lint/typecheck commands below (so verification isn't skipped after compaction).

## Commands

```bash
make up              # start Kafka (KRaft), Kafbat UI, Postgres via Docker Compose
make down             # stop the stack
make logs             # tail service logs
make demo             # short backfill (~30 days) + populate dashboard
make backfill         # full historical backfill
make test             # unit tests (no external services required)
make test-integration # Testcontainers-based integration tests (needs Docker)
make lint             # ruff check
make typecheck        # mypy
make eval             # AI agent eval suite (evals/agent_questions.yaml)
make trace EVENT_ID=  # trace one event bronze -> silver -> gold
make replay ...       # replay runbook targets (see docs/runbook.md once written)
```

Run `uv sync` once after cloning to install dependencies (uv manages the virtualenv; no manual `pip install`).

## Conventions

- Python 3.12+, full type hints, ruff + mypy clean, pre-commit enforced.
- Pure, unit-tested transform functions in `src/nimbus/transform/`; no `iterrows`, explicit dtypes, categoricals for low-cardinality columns.
- Pydantic v2 models for every Kafka event type (`src/nimbus/common/`); pandera schemas for every DataFrame load (`src/nimbus/quality/`).
- Producers are idempotent (`enable.idempotence=true`, `acks=all`); consumers commit offsets only after a successful write; poison messages go to `weather.dlq.v1` and never block a partition.
- No fake data in pipeline code — only real APIs. Recorded fixtures live in `tests/fixtures/` for tests only.
- `LLM_ENABLED=false` is the default; the whole data platform must run without an Anthropic API key. Tests, CI, and `make eval` always use the deterministic fake LLM client, never the real API.
- Conventional Commit messages; commit in logical chunks.
- Structured JSON logs carrying `event_id` for lineage.

## Gotchas (session 1)

- **Docker was not installed** at project start. WSL2 with an `Ubuntu` distro is already present/registered, so Docker Desktop only needs installing — it can attach to the existing WSL2 backend, no separate `wsl --install` needed.
- `uv` was installed via `winget install --id=astral-sh.uv`; on this machine its PATH entry may need a fresh shell to take effect — if `uv` isn't found, check `C:\Users\<user>\AppData\Local\Microsoft\WinGet\Packages\astral-sh.uv_*\uv.exe` directly.
- Windows `python3` resolves to a Microsoft Store stub, not a real interpreter — use `python` or `py` instead.
- Pin `SQLAlchemy>=2.0,<2.1` and `httpx<1.0` — both have unreleased breaking-change versions in pre-release as of Sept 2026 (see ADR 0001).
- ruff 0.16+ enables ~413 rules by default; `pyproject.toml` must curate `[tool.ruff.lint] select` deliberately rather than rely on defaults.
- Open-Meteo's backfill archive uses `https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py` (not the human-facing download form) for historical METAR — see ADR 0001.
