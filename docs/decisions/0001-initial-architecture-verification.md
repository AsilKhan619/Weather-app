# ADR 0001: Initial architecture, API verification, and version pins

**Status:** Accepted
**Date:** 2026-09-12

## Context

Session 1 verified every external API and library named in [`docs/PROJECT_BRIEF.md`](../PROJECT_BRIEF.md) against current documentation before writing any code, per the brief's first-session workflow. This record captures what was verified, what changed, and the volume/topic-design decisions that follow from it, so later sessions don't need to re-derive them.

## Open-Meteo (forecasts)

- **Single Runs API** (live): `https://single-runs-api.open-meteo.com/v1/forecast`, `&run=<ISO8601>` pins an exact model init time. No push notification for "new run ready." Global models are typically available ~4-6h after init (00Z ready ~04-06 UTC); regional models 1-3h. **Decision:** poll on a schedule derived from each model's known update cadence, and trust the response's own run/generation metadata (not our poll timing) as the source of truth for which run we actually received. Combined with idempotent `event_id` hashing, a missed or duplicate poll never creates duplicate silver rows.
- **Previous Runs API** (backfill): fixed lead-time offsets of 1-7 days via `_previous_dayN` variable suffixes. Archived from January 2024 for most models; GFS 2m temperature back to March 2021.
- **Model identifiers** (confirmed real `&models=` values): `ecmwf_ifs025`, `gfs_seamless`, `icon_seamless`, `gem_seamless`.
- **Rate limits** (non-commercial, no key required): ~600/min, 5,000/hour, 10,000/day. Batching all locations into one request per model per poll keeps us at a few dozen to a few hundred requests/day — comfortably under the daily limit.

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
