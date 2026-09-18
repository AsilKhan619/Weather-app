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
make demo             # backfill 30 days, drain consumers, reconcile
make backfill         # full history since 2024-01-01 (spans 2 days of API budget)
make drain            # run bronze + silver consumers until caught up, then exit
make reconcile        # produced -> bronze -> silver check; non-zero exit on mismatch
make test             # unit tests (no external services required)
make test-integration # Testcontainers-based integration tests (needs Docker)
make lint             # ruff check
make typecheck        # mypy
make eval             # AI agent eval suite (evals/agent_questions.yaml)
make trace EVENT_ID=  # trace one event bronze -> silver -> gold
make replay ARGS=...  # `bronze --topic T --truncate` or `offsets --group G --topic T` (docs/runbook.md)
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
- **Postgres 18's official image changed its volume-mount convention**: mount the volume at `/var/lib/postgresql` (parent dir), not `/var/lib/postgresql/data` — the old convention now makes the container refuse to start ("data in an unused mount/volume"). Already fixed in `docker-compose.yml`.
- Docker Desktop's installer needs an interactive UAC click and its first run opens a window that needs a manual click-through (subscription agreement / skip sign-in) — neither can be automated from a non-interactive session.
- `docker`/`docker compose` aren't on PATH in a fresh shell after install; full path was `C:\Users\<user>\AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe` for this per-user install (add to PATH, or open a new shell after install/PATH update settles).
- **Postgres has a 65,535-bound-parameter limit per statement.** A single micro-batch's worth of exploded forecast rows can blow past that (see ADR 0002) — any bulk insert/upsert must chunk. Caught by running the real pipeline end-to-end, not by a unit test with a tiny fixture.
- `testcontainers.kafka`/`testcontainers.postgres` are deprecated in favor of `testcontainers.community.kafka`/`testcontainers.community.postgres` (same API) — use the `community` import path.
- Ad hoc manual verification scripts that create a Kafka consumer with a **brand-new group id right after a previous ungracefully-closed consumer used the same group id** can see 0 messages for 10-45s (broker waiting out the old session before rebalancing). Not a bug — just give manual scripts a generous `max_batch_seconds`, or use a fresh group id each time.
- **`INSERT ... ON CONFLICT DO UPDATE` fails if one statement touches a row twice** ("cannot affect row a second time"). Dedupe on the natural key first (`dedupe_on_key`). An original METAR + its correction in one batch triggers it; observations sort so the correction wins.
- **Naive `datetime.timestamp()` uses the machine's local timezone.** Parse user-supplied times with `parse_utc`.
- Python `Path.write_text` on Windows writes CRLF; pass the `newline` argument set to a single line-feed character. Git normalises on commit but warns.
- Windows Docker Desktop can get stuck with a stale `AppData/Local/Docker/run/sailor-ingest.sock` (`ERROR_CANT_ACCESS_FILE`, backend exits within ~15s). It survived killing Docker, unregistering the WSL distro, and `takeown`; it needs a Windows restart. Check `AppData/Local/Docker/log/host/com.docker.backend.exe.log`. A `Remove-Item` wildcard on those files reports success while deleting nothing - verify with `Test-Path`.
- `gh run watch` in a background Bash call is killed by the call's own timeout and reports failure even when CI passed; use `--exit-status` with a 600000ms timeout, or `gh run view`.
- Open-Meteo bills requests by volume (each 10 variables x 14 days per location = 1 call, fractional) against 600/min and 10,000/day. See ADR 0004 before changing backfill chunk size or throttle.
