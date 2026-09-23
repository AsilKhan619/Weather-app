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
make gold             # verification + accuracy + leaderboard; incremental and idempotent (ARGS=--full)
make quality          # pandera checks over changed rows + freshness -> ops.quality_results
make partitions       # create upcoming monthly silver.forecast partitions; apply retention (ARGS=--dry-run)
make alerts           # anomaly detector: live events -> weather.alert.v1 + gold.alert (ARGS=--drain)
make dashboard        # Streamlit dashboard on :8501 (uv sync --extra dashboard)
make briefings        # daily LLM briefings; off unless LLM_ENABLED=true (ARGS=--dry-run prints fact sheets)
make briefing-consumer # briefings on weather.alert.v1 (ARGS=--drain)
make test             # unit tests (no external services required)
make test-integration # Testcontainers-based integration tests (needs Docker)
make lint             # ruff check
make typecheck        # mypy
make eval             # AI agent eval suite (evals/agent_questions.yaml)
make trace EVENT_ID=  # trace one event bronze -> silver -> gold (or SAMPLE=forecast|observation)
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
- **Real providers behave differently from recorded fixtures** (ADR 0004). Open-Meteo returns HTTP 200 with a body truncated mid-JSON for oversized requests (~700 KB), so forecast requests are batched to 10 locations. IEM answers 503 and 429 under load (about 20 of 70 requests), so it uses `patient_http_retry`. Verify any change to request shape or size against the live API, not just mocks.
- **The live-demo workflow exists**: `gh workflow run live-demo.yml -f days=30` runs the quickstart from a clean checkout against the real providers and writes row counts, the NaN check, reconciliation and timings to the job summary. It spends real API budget (~450 Open-Meteo calls per 30 days), so run it deliberately. It is how `make demo` is exercised when local Docker is unavailable.
- Open-Meteo bills requests by volume (each 10 variables x 14 days per location = 1 call, fractional) against 600/min and 10,000/day. See ADR 0004 before changing backfill chunk size or throttle.

## Gotchas (Phase 3)

- **Units are SI everywhere in silver** (`config/variables.yaml`): K, m/s, **Pa**. METAR pressure arrives in hPa and *must* be converted (it was not, until the gold layer's first pressure comparison - ADR 0005). A new variable needs a unit conversion on both the forecast and observation side and a bounds entry in `variables.yaml`.
- **`updated_at` is only bumped when a value actually changes** (`only_if_changed` + `touch_columns` in `upsert_chunks`). Gold's incremental build depends on it; any new silver writer must go through `chunked_upsert(..., only_if_changed=True, touch_columns=("updated_at",))`.
- **Gold days are synced, not appended** (`sync_rows`): upsert differing rows, delete keys no longer produced. Re-running must leave tables identical including `computed_at`; an integration test asserts it.
- **pandera dispatches a check by its function's `__name__`**: an inner function called `in_range` silently became pandera's built-in and failed every frame. Never name a custom check function after a pandera built-in.
- **Blocking failures quarantine the whole message to the DLQ; warnings load.** `reconcile` applies the same gate, otherwise quarantined messages look like data loss. Don't add a blocking check that fixtures or real provider data can legitimately violate (a negative lead did - it is a warning now).
- **`silver.forecast` is a partitioned table** (monthly, `valid_time`). `ctid` is only unique per partition, so never use it to pick "one row"; the primary key includes `valid_time` as Postgres requires. Rows outside the created months go to `forecast_default` - run `make partitions`.
- **Two test files must not share a basename** across `tests/unit` and `tests/integration` (no `__init__.py`; mypy and pytest both refuse).
- **Local Docker was down for all of Phase 3** (stale `sailor-ingest.sock`); Postgres/Kafka behaviour was verified by CI's Testcontainers job. Migration 0008 (copy-and-swap partitioning) has only run there.

## Gotchas (Phase 4)

- **The detector keeps no state in memory** (ADR 0006): it reads the previous run and the other models' runs from `silver.forecast` (live rows only) per event. Don't add a module-level cache of "last run" - it would be lost on restart, which is the failure the design avoids.
- **Alert ids are deterministic** (rule + subject + event time + variable) and delivery is insert -> publish -> mark `published_at`. Keep any new rule's id stable across restarts and free of timestamps like `detected_at`.
- **Backfilled rows never alert** (their `init_time` is derived, not a real run). Live producers have `--once`; without live data in silver the run-change rule stays silent by design.
- **pytest shutdown in threads:** `GracefulShutdown` registers signal handlers, which only works on the main thread; tests that run a consumer loop in a thread subclass it without `signal.signal`.
- **The Bash tool chokes on heredocs with many apostrophes** (unexpected EOF); write files with the Write tool instead.
- **Dashboard pages are thin; logic lives in `src/nimbus/dashboard/queries.py`** and is tested against Postgres. Pages in `dashboard/views/` are Streamlit scripts (not importable modules) run via `st.navigation`; test them with `AppTest` (`switch_page`), and note the default selectbox choice may have no data (alphabetical first location).
- **Local Postgres without Docker:** `NIMBUS_TEST_POSTGRES=host:port` makes the Postgres-only integration tests (gold, partitions, alerts store, dashboard) use an existing disposable server instead of Testcontainers; an embedded one worked via `uv run --no-project --with pgserver` (scratch, not a dependency). Kafka tests still need Docker. The partition-retention test drops partitions in that database, so re-create it (`alembic downgrade 0007 && upgrade head`) between runs.

## Gotchas (Phase 5)

- **The LLM may only restate the fact sheet.** Anything that should be decided (confidence, most reliable model) is computed in `nimbus.llm.facts` and enforced by `nimbus.llm.grounding`; don't move a decision into the prompt. A new fact-sheet field with numbers is automatically "allowed" for grounding.
- **anthropic 1.x (1.6.0) runs on `httpx2`,** not `httpx` - it does not touch the `httpx<1.0` pin. Its `messages.parse()` raises on an invalid reply and loses the token usage, which is why the client calls `messages.create()` with `output_config` and validates itself. `transform_schema` moves `maxLength`/`maxItems` into descriptions (not enforced by decoding), so length violations are real invalid outputs.
- **Prompts are versioned files; never edit a published one.** Add `briefing_vN.md` and bump `prompt_version` in `config/llm.yaml` - the version is part of the cache key.
- **No real API call has been made** (no key). Tests use `FakeBriefingClient` and stubbed `messages.create`; the unit tests import `anthropic`, so CI installs `--extra llm`. Model ids come from settings (`claude-haiku-4-5` alias, no date suffix).
- **Haiku 4.5's minimum cacheable prompt prefix is 4,096 tokens;** the briefing system prompt is ~450, so API prompt caching is deliberately not used - the fact-sheet-hash cache is what saves money.
