# ADR 0004: Backfill, replay, and reconciliation (Phase 2)

**Status:** Accepted
**Date:** 2026-09-18

## Context

Phase 2 loads history through the same topics as live data, and must be able to
prove that silver is intact and rebuildable from bronze. Everything below was
verified against the live providers before being designed around.

## Forecast backfill: the Previous Runs API

Verified directly (Sept 2026):

- `<variable>_previous_dayN` columns with **different N combine in one request**,
  and comma-separated `latitude`/`longitude` batches locations (a JSON array, one
  object per location, in input order) - same as the Single Runs API.
- **A full year is accepted in a single request** (8,784 hours returned), so
  chunking is a message-size choice, not an API limit. I chunk at 7 days.
- Archive depth varies by model. For GFS, `2023-06-01` returned real values and
  `2020-01-01` returned all nulls; ~5% of hours in 2024 were null. **Only GFS was
  probed** - ECMWF and ICON depth is unverified. Nulls flow through as NaN (the
  transform never drops them), so an unavailable range degrades to empty rows
  rather than errors.
- The API returns **lead offsets, not run times**, so `init_time` cannot be known.

**Decision: derive it.** `init_time = valid_time - N days`, `lead_hours = N * 24`,
and `ingestion_mode = 'backfill'` marks the row as derived. This lets live and
backfilled forecasts share one table, one natural key, and one silver consumer
(routed by `event_type`), with no schema change. The cost is honest: backfilled
lead times are day-granular (multiples of 24h), whereas live runs are 6-hourly.
Anything comparing lead buckets must remember that.

### Rate limits drive the request shape

Open-Meteo weights requests by data volume: each 10 variables and each 14 days
per location counts as one call, fractionally, against **600/min and 10,000/day**
(non-commercial). One 7-day chunk for 25 locations x 28 variable-lead columns is
25 x 2.8 x 0.5 = **35 calls**, so:

- throttle ~4s between requests (15/min x 35 = 525 calls/min, under the 600 cap);
- `make demo` (30 days) is ~450 calls; a full history since 2024-01-01 is
  ~15,000, **more than one day's budget**, so it must span two days;
- on HTTP 429 the job stops and prints the date to resume from
  (`--start-date`), because chunks are ordered chunk-outer/model-inner - every
  model has finished everything before that date - and re-work is idempotent.

The CLI prints its estimate up front and warns above 90% of a day's budget.
Estimating from the documented weighting is not the same as measuring it; the
first real `make backfill` should be watched against the actual counter.

## Observation backfill: IEM ASOS

- `asos.py` returns CSV with UTC `valid`, `tmpc`/`dwpc` (degC), `sknt` (knots),
  `mslp` (hPa), `alti` (**inHg**, unlike the live API's hPa `altim`), and the raw
  `metar` text.
- **All 25 stations return data by ICAO id** (checked for 2024-06-01 to 06-03; row
  counts 48-171 depending on how often a station files SPECIs). IEM's own
  station column uses FAA ids for US stations ("DEN"), so requests are made **one
  station at a time** and results are labelled with the ICAO id we asked for -
  no id mapping to maintain.
- Windows are half-open `[start, end)` via `sts`/`ets`, so adjacent chunks never
  overlap or leave a gap at midnight. METAR and SPECI (report types 3, 4).
- **Rows are normalised to the live METAR shape** (`icaoId`, `obsTime`, `temp`,
  `dewp`, `wspd`, `slp`, `altim` = inHg x 33.8639, `rawOb`) so the existing
  `explode_observation_payload` is reused unchanged: one code path, not two.
  The original row is kept under `_source` for lineage. Rows with no METAR text
  or an unparseable time are skipped - without text there's no stable event
  identity and no COR flag.
- IEM's raw text omits the leading `METAR ` that aviationweather includes, so the
  same report backfilled vs. live has a different `event_id`. That's harmless:
  silver's key ignores text, and the overlap upserts to a single row.

## Defects found while building and reviewing this (all regression-tested)

1. **Duplicate keys inside one upsert.** Postgres rejects an
   `INSERT ... ON CONFLICT DO UPDATE` that would touch a row twice in one
   statement. An original METAR and its correction landing in the same
   micro-batch, or two overlapping backfill windows, trigger it. `dedupe_on_key`
   keeps the last row per key; for observations a correction outranks its
   original within the batch.
   *Independent review found that this alone was not enough:* the original can
   arrive in a **later** batch (an overlapping backfill window, a live poll that
   still lists it), and an unconditional upsert would flip the stored correction
   back to the uncorrected report. The upsert now carries a guard
   (`WHERE excluded.is_corrected OR NOT existing.is_corrected`), so a stored
   correction is only replaced by another correction. The outcome no longer
   depends on arrival order, within or across batches, and a rebuild (different
   batch boundaries) agrees with live.
2. **Naive datetimes are local time.** `datetime.fromisoformat("2026-09-15T00:00:00")`
   has no timezone, and `.timestamp()` silently uses the machine's zone - a
   `--from-time` replay window would shift by hours with no error. `parse_utc`
   treats offset-less input as UTC.

3. **Missing values were stored as NaN, not NULL** (found in review). The
   transforms deliberately keep missing values as NaN; `to_dict()` hands the
   driver a real `float('nan')`, and psycopg sends it as `'NaN'::float8` -
   verified. That passes `IS NOT NULL` and turns `avg()`/`sum()` over the column
   into NaN, which would have silently broken Phase 3's accuracy metrics (about 5%
   of 2024 GFS hours are missing). `chunked_upsert` now converts NaN to NULL, and
   an integration test asserts zero NaN rows and a correct `avg()`.
4. **A truncating rebuild could destroy the only copy of data** (found in
   review). If the bronze sink was down while silver kept consuming, silver holds
   rows the lake lacks, and `TRUNCATE` would delete them. The rebuild now refuses
   when silver holds rows the lake can't explain (before deleting anything);
   `--force` accepts the loss deliberately.
5. **A rate-limited backfill was recorded as `success`.** `record_ingestion_run`
   now takes an `error_message`, and an aborted run is recorded as failed with the
   exact resume date.

Also: a bulk backfill can overflow librdkafka's local queue (`BufferError`);
`produce_json` now serves delivery callbacks and retries instead of crashing.

## Drain mode is lag-based

`--drain` exits when every assigned partition's position reaches its high
watermark. An idle timeout ("no message for N seconds") was rejected: a new
consumer group can wait many seconds for its first assignment and would be
mistaken for "done". A restart after everything was committed reports
`OFFSET_INVALID` for position, so the check falls back to committed offsets
(otherwise a drain would spin forever).

## Replay: two independent paths

| Path | Source | When | Needs |
| --- | --- | --- | --- |
| `replay offsets` | Kafka | data still inside 7-day retention | consumer group stopped |
| `replay bronze` | Parquet lake | retention expired, or any time | nothing running |

`replay bronze` reuses `load_messages` - the exact function the live consumers
run - fed by `BronzeMessage`s that satisfy the `KafkaMessageLike` protocol. There
is no second implementation of "what silver should contain", so a rebuild and the
live path cannot drift. Bronze files now carry a UTC-timestamp prefix so **name
order is write order is arrival order**, which keeps a correction after the report
it corrects.

## Reconciliation is "rebuild equivalence", not a count comparison

Comparing produced vs. consumed counts can't work: identical events are
legitimately re-produced on every poll, and an upsert overwrites
`source_event_id`, so distinct event ids in silver don't match bronze. Instead,
`make reconcile` replays the lake **in memory** through the silver transform and
compares the resulting *set of natural keys* to what silver holds, reporting rows
missing and rows unexplained in each direction. `MATCH` means "silver has exactly
the rows a rebuild would produce". **It does not compare values**, `is_corrected`,
or `raw_text`: a hand-edited or stale value still reports `MATCH`. Value-level
equality is established separately - by the integration test that rebuilds from a
real lake and compares every column - and belongs in Phase 3's quality suite for
ongoing use. (Checking values here would mean re-applying the loader's ordering
rules across the whole lake in memory; deliberately not attempted.)

Keys are compared as hashes of a canonical string (timestamps normalised to epoch
seconds). This matters: pandas builds categoricals and nanosecond timestamps,
Postgres returns strings and microsecond timestamps, and a naive comparison would
report every row as mismatched. A unit test pins that the same key hashes
identically under both.

Results are stored in `ops.reconciliation_results` (migration 0004). "Produced" is
shown from `ops.ingestion_runs` for context but is informational, not a pass/fail
input.

## What is and isn't proven

Proven by automated tests (unit + Testcontainers integration):
live and backfilled rows share tables; drain terminates on real Kafka; reconcile
matches and accounts for poison messages; silver truncated and rebuilt from the
lake is row-for-row identical (values, flags, lineage ids); reconcile detects both
missing and unexplained rows.

**Not yet proven:** a real multi-month backfill against the live providers. The
code and request shapes were verified against live responses, but `make demo` /
`make backfill` have not been run end to end, so the "months of history for every
location" acceptance criterion is open until they are, and the throughput and
call-budget figures above are estimates. Local Docker was unavailable when this
was written.

## Limitations

- Only 3 of the 4 planned models (GEM deferred, ADR 0001).
- Backfilled lead times are day-granular (see above).
- Live and backfilled data overlapping the same natural key upsert to whichever
  wrote last, flipping `ingestion_mode` and `source_event_id`. Values should agree;
  it isn't reconciled beyond that.
- Reconciliation checks keys, not values (see above).
- Replay processes bronze file-by-file in order; it does not globally re-sort by
  Kafka timestamp. That is correct for one sink and monotonic restarts (write order
  is arrival order) but would not be for multiple concurrent sinks.
