# ADR 0003: Phase 2 observation design (METAR ingestion, corrections, location expansion)

**Status:** Accepted
**Date:** 2026-09-18

## Context

Phase 2 adds live METAR observations alongside the existing forecast pipeline, and expands from 5 to ~25 locations. A few things were verified live and a few design calls came up worth recording.

## aviationweather.gov's METAR Data API, verified directly

`https://aviationweather.gov/api/data/metar?ids=<CSV>&format=json` batches every station into one request (confirmed: a 3-station request returns a JSON array, one object per station). Key facts confirmed by inspecting real responses rather than assuming from the field names:

- `temp`, `dewp` are already in Celsius; `wspd` is in **knots** (confirmed against the raw METAR text - e.g. `wspd: 7` matches `28007KT` in `rawOb`); `altim` and `slp` are already in **hPa** despite the raw text encoding altimeter in inHg (`A3018` → `altim: 1022.1`, consistent with a unit conversion the API already performs). Only wind speed needs converting to SI (m/s); temperature/dew point convert to Kelvin like the forecast side; pressure only needs hPa→Pa.
- `slp` (true sea-level pressure) is absent for some stations that don't report it in their remarks; `altim` (altimeter setting, adjusted to sea level under a standard-atmosphere assumption - not identical to true SLP, but the standard fallback) is used when `slp` is missing.
- Wind direction can be `"VRB"` (variable) as a string - but direction isn't one of the brief's 4 stored variables, so this never affects wind *speed* extraction. Verified with a unit test rather than assumed.
- The API returns no field naming whether a report is a "COR" (corrected) report - that has to be parsed from `rawOb` directly. `COR` is a standalone token (e.g. `KDEN 172353Z COR 28007KT ...`), matched with a word-boundary regex so a remark merely *containing* "COR" as a substring (e.g. `CORONA`) isn't misflagged.

## Natural key doesn't include raw text - by design

The forecast natural key includes `init_time` because a new model run is genuinely new data. For observations, the brief explicitly wants "keep the latest version of corrected reports" (section 7) - so the natural key is `(station, observed_at, variable)` **without** `raw_text`. A correction for the same slot upserts over the original automatically; no special-case "if corrected, delete-then-insert" logic was needed. The *event_id* (Kafka-level, brief section 6) is a different story: it includes `raw_text`, because a correction is legitimately a different event that deserves its own bronze record for lineage, even though silver only keeps the latest.

## Location expansion to 25, every station verified live

All 20 new stations (`config/locations.yaml`) were checked against the live METAR API in one batched request before being added - all 20 returned real, current data with no misses. Coordinates and elevations in the config come from the API's own response (`lat`/`lon`/`elev`), not from an independent estimate, so they're consistent with what the pipeline will actually ingest. Climate/continent spread: coastal, mountain, desert, continental, tropical, subarctic, Mediterranean, highland-tropical, and humid-continental across North America, South America, Europe, Africa, Asia, and Oceania.

## Shared helpers extracted (not duplicated) for the second producer/consumer pair

Building `observation_producer` and `observation_silver` right after `forecast_producer`/`forecast_silver` made the actually-shared pieces obvious, so they moved to `nimbus.common`: `GracefulShutdown` (was streaming-only, but producers need identical signal handling), `record_ingestion_run`, `chunked_upsert` (the Postgres param-limit chunking from ADR 0002, now generic over any table), and `send_to_dlq`. Each producer/consumer pair still owns its own domain logic (payload shape, transform, natural key) - only the truly generic infrastructure moved.

## Scope note

This ADR covers the *live* observation pipeline only. Backfill (Previous Runs API for forecasts, IEM ASOS archive for historical METAR), cross-run reconciliation counts, and the bronze-rebuild runbook are Phase 2's remaining acceptance criteria and are tracked as follow-on work in `docs/PLAN.md` rather than bundled into this same session - the live-ingestion slice alone was already comparable in size to all of Phase 1.
