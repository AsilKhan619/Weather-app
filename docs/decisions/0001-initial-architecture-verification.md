# ADR 0001: Initial architecture, API verification, and version pins

**Status:** Accepted
**Date:** 2026-09-12

## Context

Session 1 verified every external API and library named in [`docs/PROJECT_BRIEF.md`](../PROJECT_BRIEF.md) against current documentation before writing any code, per the brief's first-session workflow. This record captures what was verified, what changed, and the volume/topic-design decisions that follow from it, so later sessions don't need to re-derive them.

## Open-Meteo (forecasts)

- **Single Runs API** (live): `https://single-runs-api.open-meteo.com/v1/forecast`, `&run=<ISO8601>` pins an exact model init time and is **required** — there is no "give me the latest" mode, and the response never echoes back which run it served (no run/generation-time field to trust). Verified directly (Phase 1, Sept 2026): requesting a run that isn't ready yet, is off the model's cadence, or has aged out of the archive all return the same clear signal — `{"error":true,"reason":"The requested model run is not available. Model: <id>, run: <ISO8601>Z"}` — there's no ambiguity to resolve. **Decision:** `find_latest_available_run()` scans backwards from now in 6-hour steps (00/06/12/18 UTC) and returns the first run that doesn't error; the caller bounds the lookback (8 steps = 48h) so a genuinely broken model fails loudly instead of scanning forever. Run availability is a property of the model run, not the location, so this is resolved once per model per poll using one probe location, then the same confirmed run is used to fetch all locations in one batched call. Combined with idempotent `event_id` hashing (natural key includes the run timestamp), polling the same not-yet-updated run repeatedly before a new one appears reproduces the same event_id and upserts identically — zero duplicates.
- **Previous Runs API** (backfill): fixed lead-time offsets of 1-7 days via `_previous_dayN` variable suffixes. Archived from January 2024 for most models; GFS 2m temperature back to March 2021.
- **Model identifiers** (confirmed real `&models=` values): `ecmwf_ifs025`, `gfs_seamless`, `icon_seamless`, `gem_seamless`.
- **Rate limits** (non-commercial, no key required): ~600/min, 5,000/hour, 10,000/day. Batching all locations into one request per model per poll keeps us at a few dozen to a few hundred requests/day — comfortably under the daily limit.
- **Location batching confirmed**: comma-separated `latitude=`/`longitude=` lists return a JSON array, one object per location in input order — one API call per model per confirmed run covers every location.
- **Units returned are not SI**: `wind_speed_10m` comes back in km/h and `pressure_msl` in hPa (temperature/dew point in °C). Silver converts to m/s, Pa, and Kelvin respectively (see `nimbus.transform.forecast`) — storing true SI avoids unit-mixing bugs when computing cross-model errors later, at the cost of Celsius/km-h readability, which the dashboard layer can convert back for display.

## METAR observations

- **Live**: `https://aviationweather.gov/api/data/metar?ids=...&format=json`. No API key; send a real `User-Agent`. ~100 req/min, ~400 results/query, ~30-day retention (live only).
- **Backfill — correction to the brief**: use the programmatic CGI endpoint, not the human-facing download form:
  `https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=<ID>&data=all&year1=..&month1=..&day1=..&year2=..&month2=..&day2=..&format=onlycomma` (also accepts `sts=`/`ets=` ISO timestamps; multi-station via `network=`). Returns CSV/TSV. Actively maintained but an unofficial best-effort archive — no SLA, occasional gaps expected; DLQ and quality checks must tolerate this rather than treat it as a bug.
- `COR` confirmed as a real report-modifier token in raw METAR text, appearing between the time and wind groups.

## Library version pins (as of Sept 2026)

The brief's default stack is confirmed current and correctly chosen. Adjustments needed to avoid pulling unstable pre-releases or noisy new defaults:

| Package | Pin | Reason |
|---|---|---|
| SQLAlchemy | `>=2.0,<2.1` | 2.1 is an RC with breaking changes, not GA |
| httpx | `<1.0` | 1.0 is still in dev/pre-release |
| ruff | curated `select` list in `pyproject.toml` | 0.16+ enables ~413 rules by default (up from 59) — accepting silent defaults makes lint noisy from commit 1 |
| Kafka | 4.x (KRaft only) | ZooKeeper mode was fully removed in Kafka 4.0; KRaft has been production-ready since 3.3 (KIP-833) — no fallback path needed |
| Kafbat UI | `ghcr.io/kafbat/kafka-ui` | Actively maintained community fork of the abandoned `provectus/kafka-ui` |
| Anthropic models | `claude-sonnet-5` (agent), `claude-haiku-4-5-20251001` (briefings) | Both confirmed real, current IDs; both support prompt caching (reads at 10% of base input price, writes at ~1.25×) — used for fact-sheet-hash caching in Phase 5 |
| Airflow | 3.3.1 when we get to Phase 7 | 3.x is a ground-up rewrite of 2.x (new UI, task SDK, split DAG processor), not a drop-in upgrade — budget extra research time; the brief's lightweight-scheduler fallback stays available |
| pandas | `>=3.0.5,<4` | Missed in the original Phase 0 sweep (pandas wasn't in the checked package list) - caught in Phase 1 when adding it as a real dependency. 3.0 is a major version: Copy-on-Write is now the *only* mode (no opt-out), and columns of strings default to a new `str` dtype instead of `object`. Neither affects this codebase's transform functions (they set dtypes explicitly and don't rely on view/mutation semantics), but it's the reason to pin `<4` rather than leave it floating. |

## Kafka topic design

| Topic | Key | Partitions | Retention | Rationale |
|---|---|---|---|---|
| `weather.forecast.raw.v1` | `location_id` | 6 | 7 days | ~400 msgs/day at full scale; 6 partitions gives bronze+silver consumer-group headroom without over-provisioning a single laptop broker. Short retention because bronze Parquet is the real replay source. |
| `weather.observation.raw.v1` | `station_id` | 6 | 7 days | ~720 msgs/day at full scale; same reasoning. |
| `weather.dlq.v1` | none | 3 | 30 days | Low volume, no ordering requirement; longer retention so failures stay investigable. |
| `weather.alert.v1` | `location_id` | 3 | 14 days | Low volume; permanent record lives in Postgres, topic is just transport. |
| `weather.briefing.v1` | `location_id` | 3 | 14 days | Same reasoning as alerts. |

Single broker locally. Production note: replication factor 3, `min.insync.replicas=2`; partition counts would scale with real consumer parallelism, not laptop constraints.

## Volume estimates

Assumptions: full scale = 25 locations × 4 models × 4 variables (temp, dewpoint, wind speed, MSLP); live forecast polling every 6h; 168 hourly steps per pull (7-day horizon, needed for 1-7 day lead-bucket verification); METAR ~1.2 reports/station/hour.

| Flow | Messages/day (full scale) | Silver rows/day |
|---|---|---|
| Forecasts | 25×4×4 = 400 | 400 × 168h × 4 vars ≈ 269,000 |
| Observations | 25×24×1.2 ≈ 720 | 720 × 4 vars ≈ 2,900 |
| Accuracy daily (gold) | — | 25×4×4×7 ≈ 2,800 |

~269K forecast rows/day (~8M/month) drives the decision (confirmed in Phase 3) to partition `silver.forecast` monthly by `valid_time` and define a retention/archival policy — this volume is inherent to storing every lead time for verification, not a design flaw.

## Cost

Every component in this design is free at the scale used here: Open-Meteo, aviationweather.gov, and IEM ASOS require no key and no paid tier; Docker Desktop is free for personal use; Kafka/Postgres/Kafbat UI/Streamlit/Airflow and all pinned Python libraries are open source and self-hosted; GitHub Actions free tier easily covers this project's CI. The only component that can ever cost money is the Anthropic API (Phases 5-6), and only once the user adds a key with billing enabled — `LLM_ENABLED=false` is the default everywhere else, tests/CI/`make eval` always use the deterministic fake LLM client, and briefings cache by fact-sheet hash to avoid repeat paid calls.

## Consequences

- No architectural disagreement with the brief; changes here are version pins and one backfill-endpoint correction.
- The Single Runs API run-detection heuristic (poll cadence + trust response metadata) needs a short comment where it's implemented, since it's a heuristic rather than a guaranteed signal.
- `pyproject.toml`'s ruff config must be written deliberately in Phase 0, not left to defaults.
