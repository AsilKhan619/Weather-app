# ADR 0005: Gold layer, data quality, lineage, and partitioning (Phase 3)

**Status:** Accepted
**Date:** 2026-09-19

## Context

Phase 3 turns silver into answers ("which model is most accurate, at which lead
time?") and makes the platform's correctness inspectable: how forecasts are scored,
how bad data is stopped, how one event is traced, and how the biggest table stays
manageable. Most of these are decisions about *what a number means*, so they are
written down here.

## Verification: what "error" means

`gold.forecast_verification` matches every forecast value to an observation with
`pandas.merge_asof` (`direction="nearest"`, by station and variable).

- **error = forecast − observed**, both already in the SI unit silver stores
  (`config/variables.yaml`). Positive bias means the model runs high.
- **Tolerance: 30 minutes** (`config/gold.yaml`). METARs are filed a few minutes
  before the hour, so 30 minutes matches every routine report, and a station that
  has gone quiet leaves its forecasts *unverified* rather than compared to
  hours-old data. `obs_offset_seconds` is stored (signed), and a blocking
  quality check asserts it never exceeds the tolerance.
- **Missing (NaN) observations are dropped before matching.** Otherwise the nearest
  observation could be a NaN and the forecast silently unscored, when a valid report
  10 minutes further away exists.
- **Lead buckets are whole days:** `lead_day = ceil(lead_hours / 24)`. Backfilled
  forecasts only know their lead in whole days (the Previous Runs API, ADR 0004), so
  a daily bucket is the finest grain every ingestion mode shares. Lead 0 is the
  model's analysis at its own initialisation time, not a forecast, and would flatter
  the shortest bucket, so `min_lead_hours = 1`.
- **Days are UTC valid dates.** A day's rows are one contiguous `valid_time` range,
  which is what makes per-day recomputation cheap.

`gold.accuracy_daily` holds count, bias, MAE and RMSE per (date, location, model,
variable, lead day). **Windows are exact, not approximated:** `n·mae = Σ|e|`,
`n·rmse² = Σe²`, `n·bias = Σe`, so the `gold.model_leaderboard` view's 7- and 30-day
figures are `Σ(n·mae)/Σn` and `√(Σ(n·rmse²)/Σn)`, equal to computing them over the raw
errors (a unit test proves it). The windows end at the latest verified date, not
`now()`: a historical load ranks its own most recent days instead of returning
nothing.

### A bug this found (fixed)

Observation pressure was stored in **hPa** while forecast pressure is in **Pa**
(ADR 0003 said "pressure only needs hPa→Pa" but the transform never did it). Every
pressure error would have been ≈ −100,000. It was found while writing the plausible
range for `pressure_msl`, and a regression test now shows the quality gate rejects a
pressure of 1013 Pa. Any rows already stored are corrected by a replay from bronze
(the upsert sees a changed value).

## Incremental and idempotent

The build's unit is one UTC valid date, and re-running it must change nothing.

- **Change detection.** silver had no way to tell "this row was revised" (only
  `inserted_at`, set once). Added `updated_at` (indexed) to both silver tables, bumped
  by the upsert **only when a value actually differs** (`IS DISTINCT FROM` guard).
  Redelivering the same message therefore leaves `updated_at` alone; a correction or a
  revised forecast advances it.
- **Watermark** in `ops.job_state`. A run snapshots `now()` *before* reading, selects
  days with rows changed since `watermark − lookback`, builds them, and only then
  advances the watermark - so a failed run is simply retried. `now()` in Postgres is the
  transaction *start* time, so a transaction that began before the snapshot can commit
  after it; the one-hour `lookback_hours` covers that.
- **Which days.** A changed forecast dirties its valid date. A changed observation
  dirties its date and both neighbours (a match can straddle midnight). Days after the
  newest observation are skipped - nothing can match yet - and picked up when the
  observation that makes them verifiable arrives.
- **Sync, not append.** A day's rows are reconciled into the table: upsert what
  differs, delete keys the day no longer produces (an observation corrected to
  "missing" must remove its verification row). Unchanged rows are not written, so
  `computed_at` is untouched too. Integration tests assert that an incremental re-run,
  a `--full` re-run, and a re-run with a 24-hour lookback leave both gold tables
  identical *including `computed_at`*, and that changing one observation rebuilds
  exactly three days and leaves the other days' rows byte-identical.
- **Rule changes rebuild everything.** The watermark's name embeds a fingerprint of what
  changes a day's *meaning* but is not data - match tolerance, minimum lead, and the
  location→station mapping - so changing any of them finds no watermark and the next run is
  a full rebuild, instead of silently mixing old and new rules across days. (Found by the
  independent review: a tolerance change originally left already-built days on the old rule.)
- **Days with no silver forecasts are left alone.** Retention drops old forecast partitions;
  a later observation revision would otherwise dirty those days and `sync_rows` would delete
  the metrics that outlive the forecasts.

`silver.dim_location/dim_model/dim_variable` were added because verification must join
a forecast's `location_id` to its observing station, a mapping that lived only in
config. They are loaded idempotently from config on every `make gold`.

## Data quality

pandera schemas guard every DataFrame load. **Each table has two schemas, and which one
a check lives in is its severity:**

| | Blocking | Warning |
|---|---|---|
| Checks | non-null keys, tz-aware times, finite values, known variable, **hard** physical limits (e.g. 150–360 K), derived columns consistent (`error = forecast − observed`, `lead_day = ceil(lead_hours/24)`), match within tolerance, `rmse ≥ mae ≥ |bias|`, key uniqueness (gold/suite) | **plausible** range (−90..60 °C for temperature); a `valid_time` before `init_time` |
| Effect | the row's **message goes to the DLQ** (silver) / the **day's build aborts** (gold) | the row loads and is flagged |

A negative lead is a warning, not a block: the first CI run of the quality gate
quarantined every live forecast in the fixture-based integration tests (a recent run paired
with fixed valid times), which showed the check was stricter than the data contract - gold
never scores lead below `min_lead_hours` anyway. (The same run also caught a test that
corrected a temperature to 99 °C = 372 K; the gate was right and the fixture was wrong.)

Two range tiers, deliberately: unusual weather is real and must not be discarded, but a
temperature of 500 K is a unit bug. Bounds live in `config/variables.yaml`.

- **Silver:** the consumer validates each batch (one pandera pass took ~70 ms for 135k rows in a scratch script; loading that many rows
  at the ~4,300 rows/s assumed in ADR 0004's projection is ~30 s - the live pipeline itself
  was not re-timed).
  A blocking failure quarantines the *whole message* (a provider unit bug affects every
  value in the payload) via the existing DLQ path (`QualityError` is a `ValueError`); the DLQ
  record names the failed check(s), and the sampled rows are in `ops.quality_results`. Every
  copy of a duplicated event id in the batch is dead-lettered.
  Uniqueness is *not* checked pre-load: a batch may legitimately repeat a key
  (overlapping backfill windows, a report and its correction) and is deduplicated by the
  upsert.
- **Reconciliation applies the same gate** when it recomputes what silver should
  contain, so a quarantined message counts as "unparseable (→ DLQ)" there exactly as it
  does for the live consumer. Without that, quality gating would break "rebuild
  equivalence" and every quarantined message would read as data loss.
- **Gold:** the verification and accuracy frames are validated before the load. A
  blocking failure aborts the run (the day's transaction rolls back, gold keeps its
  previous rows, the watermark does not advance, results are still recorded). It aborts
  rather than skips: skipping one day and advancing the watermark would leave that day
  wrong forever.
- **Results** go to `ops.quality_results`: one summary row per table and severity plus
  one per failed check, with a sample of the failing rows. Consumers write only
  failures (a row per clean batch would be noise); the suite writes everything.
- **`make quality`** runs the same schemas over recently changed silver/gold rows
  (selected by `updated_at`/`computed_at`; `--all` streams whole tables in 200k-row
  chunks) and exits non-zero on a blocking failure.
- **Freshness:** per configured station (newest observation) and per live source (last
  successful ingestion run), against `config/quality.yaml`. Always a warning. The forecast
  limit (14 h) is one 6-hour cycle plus the 4–6 h before a run is published, with slack.
- pandera became a **core** dependency (it was an optional extra), since every load path
  imports it.

Gotcha worth remembering: pandera dispatches a check by the *name of its function*, so an
inner function called `in_range` silently became pandera's built-in `in_range` and failed
every frame with "missing arguments". Custom check functions must not reuse built-in names.

## Lineage: `make trace`

`make trace EVENT_ID=<id>` (or `SAMPLE=observation`) follows an event API request →
topic/partition/offset → bronze file → silver rows → gold verification rows and daily
accuracy rows. Silver and gold are found by the event's **natural keys**, recomputed from
the bronze payload with the same transform silver uses - not by `source_event_id`, which
names only the *latest* writer of a row. A later event that overwrote some rows would
otherwise make an event look as if it never reached silver; the trace reports how many
rows still carry the event's id and which event overwrote the rest. The "API request"
step is the envelope (source, mode, run/date range) plus the `ops.ingestion_runs` row
whose window contains `produced_at`; the exact HTTP request URL is not stored.

## Partitioning and retention for `silver.forecast`

Volume is ~269k rows/day at full scale (ADR 0001) - about 8M a month - and a
full-history backfill is ~50M. `silver.forecast` is **range-partitioned by `valid_time`,
one partition per month.**

- **Why `valid_time`:** gold reads one valid date at a time and retention is by
  valid date, so both touch a single partition. The primary key
  `(model, location_id, init_time, valid_time, variable)` already contains it, which
  Postgres requires; `INSERT … ON CONFLICT` keeps working across partitions (tested).
- **Migration 0008** creates 2024-01 … 2027-12 (the archive starts January 2024) plus a
  `DEFAULT` partition so an out-of-range row never fails an insert. It copies and swaps the
  old table - fine for a fresh clone or dev database; a production-sized table would be
  migrated with a dual-write period instead.
- **`make partitions`** keeps `partitions_ahead_months` (6) ahead of today. Postgres
  refuses to create a partition overlapping rows already in the default partition, so the
  job first moves those rows into the new table and attaches it, in one transaction.
- **Retention** (`forecast_retention_months`, **off by default**) drops whole monthly
  partitions - instant, no vacuum. It is safe in principle because gold keeps the metrics
  and the bronze lake keeps every raw event, so a replay can restore a month. It is off
  because the demo dataset *is* the full history. When on, `make reconcile` compares only
  the retained window, otherwise an intentional drop would read as data loss - the tension
  between retention and "silver is exactly what a rebuild produces" is real, and resolved by
  making the window explicit rather than by weakening the check.

## What is and isn't verified

Local Docker was unavailable for this phase (a stale Docker Desktop lock needing a
Windows restart), so everything that needs Postgres or Kafka ran in CI's Testcontainers job.
Unit tests cover the pure transforms, schemas and gate. **Not measured:** how long a
full-history `make gold` takes (~1,000 days × ~50k forecast rows per day is the design
estimate), and pandera's cost inside the *live* consumer (only measured in isolation).
