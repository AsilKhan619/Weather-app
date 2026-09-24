# Runbook

Operational procedures for Nimbus. Every procedure here is safe to repeat: all
silver writes are idempotent upserts on a natural key, so re-running a step
never duplicates data.

Commands assume the stack is up (`make up`). On Windows, use Git Bash or WSL2.

## Mental model (read this first)

```
producers ─► Kafka raw topics ─┬─► bronze sink ─► Parquet lake   (long-term truth)
                               └─► silver consumers ─► Postgres silver (derived)
```

- **Kafka retention is short (7 days).** Don't rely on it for recovery past that.
- **The bronze lake is the source of truth.** Silver is *derived* and can always
  be rebuilt from it. Nothing in silver is irreplaceable.
- Bronze and silver are independent consumer groups (`bronze-sink`,
  `silver-forecast`, `silver-observation`). One being behind never affects another.
- A message that fails validation goes to `weather.dlq.v1` and never blocks its
  partition. If silver looks short, check the DLQ before assuming data loss.

## 1. Is the pipeline healthy? (`make reconcile`)

```bash
make reconcile
```

Prints, per topic: messages produced, messages in bronze, distinct events,
unparseable messages (these went to the DLQ), the silver rows bronze *implies*,
the silver rows that *exist*, and the difference in each direction.

| Result | Meaning | Go to |
| --- | --- | --- |
| `MATCH` | Silver has exactly the rows a rebuild from bronze would produce (same natural keys). | nothing to do |
| `missing from silver > 0` | A consumer lost or hasn't yet processed data. | §2, then §3 |
| `extra in silver > 0` | Silver holds rows bronze can't explain: a manual insert, a transform that once emitted different keys, or **the bronze sink was down/behind while silver kept consuming** (those rows may exist *only* in silver). | **read §4 before rebuilding** |
| `bronze unparseable > 0` | Poison messages. Not an error by itself. | §5 |

**What `MATCH` does and does not prove.** It compares the *set of natural keys*.
It catches lost rows and rows nothing can explain. It does **not** compare
values, `is_corrected`, or `raw_text`, so a hand-edited value or a stale wrong
value still reports `MATCH`. If you suspect value-level damage, rebuild per §4
(that is the guarantee). Value-level checks belong with Phase 3's data-quality
suite.

Exit code is non-zero on mismatch, and every run is recorded in
`ops.reconciliation_results`, so history is queryable:

```sql
select checked_at, topic, matched, missing_from_silver, extra_in_silver
from ops.reconciliation_results order by checked_at desc limit 10;
```

**"Missing" right after a backfill is normal** - the consumers simply haven't
run yet. Run `make drain` first, then reconcile.

## 2. A consumer crashed or was killed mid-batch

Nothing to do beyond restarting it. Offsets are committed only *after* a batch
is written, so the uncommitted batch is redelivered and re-applied idempotently.

```bash
uv run python -m nimbus.streaming.forecast_silver      # or observation_silver / bronze_sink
```

To catch up and exit instead of running forever, add `--drain`.

Confirm with `make reconcile`.

## 3. Rebuild silver by resetting a consumer group's offsets

Use when a transform was wrong (or a silver table was damaged) **and the data is
still inside Kafka retention** (7 days).

1. **Stop the consumer.** A group with live members refuses an offset reset.
2. Fix the transform (if that was the problem) and deploy it.
3. Reset the group. Earliest retained offset:

   ```bash
   make replay ARGS="offsets --group silver-forecast --topic weather.forecast.raw.v1"
   ```

   Or from a point in time (ISO-8601, UTC) - e.g. everything since a bad deploy:

   ```bash
   make replay ARGS="offsets --group silver-forecast --topic weather.forecast.raw.v1 --from-time 2026-09-15T00:00:00"
   ```

4. Restart the consumer (or `--drain` it). It re-reads and upserts.
5. `make reconcile`.

Note: a time-based reset re-reads from that time forward only; it does not
remove rows already written. If the old rows were *wrong* (not just missing),
also run §4 for that table.

## 4. Rebuild silver from the bronze lake (Kafka retention already expired)

The lake is complete history, so this always works. It uses the exact
`load_messages` code the live consumers run - a rebuild and the live path cannot
drift apart.

**Before you truncate: make sure the lake is complete.** `TRUNCATE` destroys
whatever bronze can't reproduce. If the bronze sink was down or behind while the
silver consumer kept running, silver may hold rows that exist nowhere else, and
silver's consumer offsets are already past them (only §3 could re-read them, and
only within Kafka retention). So the rebuild **refuses to run** when silver holds
rows the lake can't explain, and tells you why. Then either:

- the messages are still in Kafka: run
  `uv run python -m nimbus.streaming.bronze_sink --drain` to catch bronze up, run
  `make reconcile`, and retry; or
- they've expired and you accept losing them: add `--force`.

```bash
# 1. stop the silver consumer for the table you're rebuilding
# 2. rebuild. --truncate empties the table first, so the result is *exactly*
#    what bronze explains. (Refuses if silver holds rows bronze can't explain;
#    --force overrides and accepts that loss.)
make replay ARGS="bronze --topic weather.forecast.raw.v1 --truncate"
make replay ARGS="bronze --topic weather.observation.raw.v1 --truncate"
# 3. verify
make reconcile
# 4. restart the consumer
```

Without `--truncate` the replay only re-applies bronze on top of what's there:
it repairs *missing* rows but won't remove *extra* ones, and it never deletes
anything, so it needs no safety check. Use it when you only need repair, or can't
afford an empty table while it runs.

Replay reads files in write order (filenames start with a UTC timestamp), so a
correction (COR) still lands after the report it corrects. Unparseable messages
are counted and skipped; the summary line reports them.

Time is bounded by the pandas transform and the Postgres upserts, not by Kafka
(there is no broker in this path). Measured on a GitHub runner (ADR 0004):
forecast silver loads ~4,300 rows/s (6.05M rows in ~23 min, linear from 30 to
120 days); observation silver ~1,150 messages/s (109k messages in 95 s, after the
transform was made cheaper per message). Those are consumer drain rates on the
live path; a `replay bronze` rebuild uses the same load code but has not been
timed separately. Figures will be recorded in the README in Phase 8.

**This is tested**: `tests/integration/test_backfill_and_rebuild.py` truncates
both silver tables, rebuilds from a real lake, and asserts every row (values,
flags, lineage ids) is identical to the original; and asserts the rebuild refuses
(without deleting anything) when silver holds a row the lake can't explain.

## 5. Investigating the dead-letter queue

```bash
docker exec nimbus-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic weather.dlq.v1 --from-beginning --max-messages 5
```

(Or browse the topic in Kafbat UI at http://localhost:8080.) Each record has the
original payload, `error_type`/`error_message`, and the source topic/partition/offset.

- A one-off malformed message: ignore it.
- A burst with the same `error_type`: a producer or transform changed shape.
  Fix it, then rebuild per §4 - the fixed transform will now accept them because
  they are still in bronze.

## 6. Loading history (`make demo` / `make backfill`)

```bash
make demo        # last 30 days, all 25 locations, then drain + reconcile + gold + quality
make backfill    # everything since 2024-01-01, then the same
```

Both first *produce* (`nimbus.jobs.backfill`), then `make drain` runs the bronze
sink and both silver consumers to completion, then `make reconcile`, `make gold` and
`make quality`.

**IEM (observations) is a free academic service that sheds load with HTTP 503 and
429** - about 20 of 70 requests in a 120-day run. The client retries patiently
(5 attempts, waits growing toward a minute) and every request eventually
succeeded; if any still fail, the job names them and re-running is safe.

**Rate limits (Open-Meteo, non-commercial): 600 calls/min, 10,000/day.** Requests
are weighted by volume (each 10 variables x 14 days per location counts as one).
Forecast requests are sent 10 locations at a time (larger requests come back
truncated - ADR 0004); a 7-day chunk of 10 locations is ~14 calls, so the job
waits ~2 s between requests. `make demo` (30 days) costs ~450 calls; a full history is
~15,000 and **must span two days**. The job prints its estimate up front, and if the provider
returns 429 it stops and prints the exact date to resume from:

```bash
uv run python -m nimbus.jobs.backfill --start-date 2025-03-01
```

Re-running an overlapping window is safe (deterministic event ids + upserts).
Observations come from the free IEM archive, one polite sequential request per
station per 90 days.

Useful flags: `--days N`, `--full`, `--start-date`, `--end-date`,
`--only forecasts|observations`.

## 7. The gold layer (`make gold`)

```bash
make gold                 # incremental: rebuilds only the days that saw new or revised data
make gold ARGS=--full     # force every day (a change to the match tolerance, min lead or the
                          # location->station mapping already triggers one automatically)
```

`gold.forecast_verification` (each forecast value matched to the nearest observation
within 30 minutes), `gold.accuracy_daily` (count/bias/MAE/RMSE per day, location, model,
variable, lead day) and the `gold.model_leaderboard` view (rolling 7/30 days) are built by
`nimbus.jobs.build_gold`. It is safe to run any time and as often as you like: an unchanged
day is not rewritten, so a re-run leaves the tables identical.

```bash
docker exec nimbus-postgres psql -U nimbus -d nimbus -c   "select lead_day, round(avg(mae)::numeric, 2) as mae from gold.accuracy_daily
   where variable = 'temperature_2m' group by 1 order by 1"
```

- **`make gold` says 0 days recomputed:** nothing changed since the last build (or no
  observation is newer than the forecasts, so nothing can be scored yet).
- **A run fails with `failed blocking checks`:** a day's data broke a blocking quality
  check (e.g. a value beyond a hard physical limit reached silver). Gold was left as it
  was and the watermark did not move. Read the failing check in
  `ops.quality_results where context like 'gold-build%' and not passed`, fix or replay the
  cause, and re-run.
- **Silver rows stored before the pressure unit fix** (hPa instead of Pa) trip the hard
  range check and stop the build at the first day with a pressure match. Replay silver from
  bronze (§4); a fresh `make demo` is unaffected.
- `ops.gold_build_log` records, per rebuilt day, how many forecasts were eligible and how
  many found an observation - low matching means a quiet station, not a bug.

## 8. Data quality (`make quality`)

```bash
make quality              # pandera checks over rows changed in the last 24 h + freshness
make quality ARGS=--all   # every row of every table (streams in 200k-row chunks)
```

Exits non-zero if a **blocking** check failed; warnings and stale sources are reported
but do not fail it. Results are written to `ops.quality_results`.

| Severity | Examples | What happens |
|---|---|---|
| blocking | null key, naive timestamp, infinite value, a value beyond a hard physical limit, `error` not equal to forecast minus observed | silver: the **message goes to the DLQ**; gold: the **build aborts** |
| warning | a value outside the plausible range (e.g. above 60 degC) | the row loads and is flagged |

Bounds are in `config/variables.yaml`; freshness limits in `config/quality.yaml`.

```bash
docker exec nimbus-postgres psql -U nimbus -d nimbus -c   "select context, table_name, check_name, severity, rows_failed, detail
   from ops.quality_results where not passed order by id desc limit 20"
```

A quality-quarantined message is in the DLQ like any other (§5, `error_type` is
`QualityError`); `make reconcile` counts it under "unparseable", so it is not reported
as lost data. **Freshness** warnings (`check_name = 'freshness'`) name the station or
source with no new data inside its interval - with the live producers stopped they will
all be stale, which is the correct answer.

## 9. Tracing one event (`make trace`)

```bash
make trace SAMPLE=observation          # pick a recent event from the lake
make trace EVENT_ID=<event_id>         # or name one (event_id is in every structured log line)
```

Prints the API request (source, run or date range, the ingestion run), the Kafka topic,
partition and offset, the bronze file, how many silver rows the event yields and how many
still carry its id (a later event may have overwritten the rest - it says which), and the
gold verification rows and daily accuracy metrics it fed. "not (yet) part of any gold
metric" means `make gold` has not run since it landed, or nothing was scored against it.

## 10. Partitions and retention (`make partitions`)

`silver.forecast` is range-partitioned by `valid_time`, one partition per month
(`silver.forecast_y2026m09`, ...). The migration creates 2024-01 to 2027-12 and a
`forecast_default` partition that catches anything outside that.

```bash
make partitions                   # create the next 6 months; adopt rows stranded in the default partition
make partitions ARGS=--dry-run    # preview what retention would drop
```

Run it monthly (or from the scheduler). **Retention is off by default**
(`forecast_retention_months: null` in `config/storage.yaml`). When set, `make partitions`
drops whole months older than the window and `make reconcile` compares only the retained
window. To get a dropped month back, replay it from the bronze lake (§4). A non-empty
`forecast_default` partition means data arrived outside the created months - run
`make partitions`.

## 11. Alerts (`make alerts`)

```bash
make alerts                    # long-running anomaly detector (Ctrl+C to stop)
make alerts ARGS=--drain       # catch up on the live events already on the topics, then exit
make produce-forecasts ARGS=--once      # one poll cycle of a live producer, then exit
```

The detector reads the live forecast and observation topics and writes `weather.alert.v1` and
`gold.alert`. Rules and thresholds: `config/alerts.yaml` (ADR 0006). It holds no state in
memory, so **there is nothing to recover after a crash: just start it again** - it re-reads
from its last committed offset, and an alert it already published is not sent twice.

```bash
docker exec nimbus-postgres psql -U nimbus -d nimbus -c   "select detected_at, rule, severity, location_id, coalesce(model, station) as subject,
          variable, round(metric::numeric, 2) as metric, threshold
   from gold.alert order by detected_at desc limit 20"
```

- **No alerts at all:** expected until two consecutive live runs of a model are in silver
  (the run-change rule) or live forecasts exist for the hour an observation is for. Backfilled
  history never alerts. Check `select count(*) from silver.forecast where ingestion_mode = 'live'`.
- **`published_at` is null on old rows:** the detector stopped between storing and
  publishing. Start it; it republishes them on the next matching event, or replay the event
  (`make alerts` after resetting its offsets, runbook section 3, group `alert-detector`).
- **Alerts on `pressure_msl` at a high-elevation location:** should not occur (spread and observation-miss ignore pressure above 300 m). If you see them, `config/alerts.yaml` `pressure_max_elevation_m` was changed.
- **Too many alerts / too few:** thresholds are starting points. Change `config/alerts.yaml`
  and restart; already-stored alerts are not rewritten.

## 12. The dashboard (`make dashboard`)

```bash
uv sync --extra dashboard     # once
make dashboard                # http://localhost:8501
uv run python -m nimbus.jobs.dashboard_smoke   # render every page headlessly and print what each shows
```

Four pages (ADR 0007). It reads Postgres, and Kafka for the health page, and never writes.

- **Every page says "run `make gold`" / "nothing verified yet":** gold is empty. Load data
  (`make demo`), then `make gold`.
- **Pipeline Health says "Kafka is not reachable":** the broker is down or `KAFKA_BOOTSTRAP_SERVERS`
  is wrong; everything else still works.
- **Freshness shows every station stale:** the live producers are not running. Start
  `make produce-forecasts` / `make produce-observations`.
- **Lineage says the bronze lake does not exist:** it reads `data/lake/bronze` relative to where
  Streamlit was started; start it from the repository root.
- **Numbers look stale:** results are cached for 30 seconds (Kafka 15 s); press `r` to rerun.

## 13. Rebuilding after a transform fix

When a transform bug is fixed (the altimeter-fallback fix is the example), rows already in silver keep the
old values. Rebuild silver from the lake (section 4), then `make gold ARGS=--full`. Nothing else needs to
change: bronze holds the untouched events.

## 14. LLM briefings (`make briefings`)

Off by default, and **staying off: the project is kept at $0 by decision.** Everything below except
`--dry-run` is for a future where that changes. Turning it on would mean a **billing cap in the
Anthropic console first**, then `ANTHROPIC_API_KEY=...` and `LLM_ENABLED=true` in `.env`, and
`uv sync --extra llm`.

```bash
make briefings ARGS=--dry-run                           # print the fact sheets; no API call
make briefings ARGS="--dry-run --location london"       # one location
make briefings ARGS="--as-of 2026-09-01T12:00"          # brief from a point inside loaded history
make briefing-consumer                                  # brief when alerts arrive (ARGS=--drain)
```

`make briefings` ends with a line such as `published=24, flagged=1` or, with the LLM off,
`disabled=25`. Every request is a row in `ops.llm_calls`:

```bash
docker exec nimbus-postgres psql -U nimbus -d nimbus -c \
  "select outcome, count(*), round(sum(cost_usd)::numeric, 4) as usd, left(max(error), 120)
   from ops.llm_calls where called_at > now() - interval '1 day' group by 1"
```

- **`no_data` for every location:** there are no forecasts in the 48 hours after `as_of`. With
  only backfilled history, pass an `--as-of` inside it; for "now", run a live cycle first
  (`make produce-forecasts ARGS=--once && make drain`).
- **`error` outcomes:** the API was unreachable, rate limited after retries, or the key is wrong
  (`AuthenticationError` in `error`). The run carried on; re-run later - an identical fact sheet
  is served from cache, a new one is generated.
- **`invalid_output` then `success`:** the one retry worked. Two `invalid_output` rows for one
  fact sheet mean nothing was stored; read `error`.
- **`grounding_failed` / flagged briefings:** the model wrote a number that is not in the fact
  sheet, or changed the confidence or the model. They are on the dashboard's Briefings page with
  the reasons, never published. A steady rate means the prompt needs work: add
  `prompts/briefing_v2.md` and bump `prompt_version` in `config/llm.yaml` (never edit v1 - the
  version is part of the cache key and of every log row).
- **Costs look high:** check the cache hit rate on the LLM Usage page. The fact sheet is taken as of
  the top of the hour, so repeated runs within an hour should all be cache hits.

## 15. Provider attribution

Forecast data is from [Open-Meteo](https://open-meteo.com/) (CC BY 4.0;
non-commercial use). Historical observations are from the Iowa Environmental
Mesonet ASOS archive; live observations from aviationweather.gov. Nimbus is an
analytics project, not a safety tool - use official weather services for warnings.
