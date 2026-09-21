# ADR 0006: Streaming anomaly detector (Phase 4a)

**Status:** Accepted
**Date:** 2026-09-21

## Context

Brief section 9 asks for a consumer that publishes to `weather.alert.v1` when (1) a new model
run changes a location's next-48-hour forecast by more than a threshold, (2) the models
disagree by more than a threshold, or (3) an observation misses the latest short-range
forecast by more than a threshold - keeping state small and recoverable after restarts, and
saying how Kafka Streams or Flink would manage that state at scale.

## The rules

All thresholds are in `config/alerts.yaml`, per variable, in the SI units silver stores. A
metric at or above the threshold is a `warning`; at or above `critical_multiplier` × threshold
(2×) it is `critical`.

| Rule | Fires on | Metric | Default (temp / dew / wind / pressure) |
|---|---|---|---|
| `run_change` | a new **live** run of a model | mean \|new − previous run\| over the hours both cover in the next 48 h | 3 K / 3 K / 3 m/s / 500 Pa |
| `model_spread` | a new live run, compared with the other models' latest runs | mean over hours of (max − min across models) | 4 / 4 / 4 / 600 |
| `observation_miss` | a new live METAR | \|observed − mean of the models' forecasts for that hour\| (lead ≤ 24 h) | 6 / 6 / 8 / 800 |

Choices worth defending:

- **Mean, not maximum**, over the window. A single bad hour is noise (or a front arriving two
  hours earlier); a run that is 4 K off *on average* for two days is a genuinely different
  forecast. A rule stays silent below `min_overlap_hours` (24): a short overlap is not evidence.
- **Absolute differences**, so a run that swings +5 K and −5 K on alternate hours is a change
  even though its signed mean is zero (unit-tested).
- **Only live events alert.** Backfilled `init_time`s are derived (`valid_time − N days`,
  ADR 0004), not real runs, so "previous run" is meaningless for them; and 30 days of history
  must not raise 30 days of alerts.
- **Messages that fail the silver quality gate never alert** (ADR 0005). An implausible
  observation is in the DLQ; alerting on it would be alerting on a data error.
- **The observation rule compares with the mean of the models**, not each model separately:
  one alert per observation and variable, and "everyone missed" is the interesting signal. The
  per-model forecasts are in the alert's `details`.
- **Thresholds are starting points.** They were set relative to the measured forecast error
  (day-1 temperature MAE 1.4 K, ADR 0005): an alert should mean "well beyond ordinary wobble".
  They are *not* tuned against a measured alert rate - that needs weeks of live data, and I
  will not claim otherwise.

## State: none in memory, by design

The state a run-to-run comparison needs is the previous run per (model, location); the spread
rule needs the other models' latest runs; the observation rule needs recent short-range
forecasts. All of it is already in `silver.forecast`, so the detector **reads it there on
demand** (`alerts/store.py`) and keeps nothing in memory. A restart therefore loses nothing,
there is no snapshot or changelog to maintain, and recovery is "start the consumer".

Consumer offsets are the only other state, committed after the batch is handled (as everywhere
in this project). A restart re-detects the uncommitted events; that is safe because:

- **Alert ids are deterministic** - a hash of (rule, subject, event time, variable): model +
  location + run for `run_change`; location + 6-hour cycle for `model_spread` (so the three
  models of one cycle share an id rather than each raising their own); station + observation
  time for `observation_miss`.
- **Delivery is insert, publish, mark.** The alert row goes into `gold.alert` first
  (`ON CONFLICT DO NOTHING`), is published, and `published_at` is set only after Kafka confirms
  delivery (`flush` reports zero undelivered). A crash after the insert leaves an unpublished
  row that the restart publishes; an alert already marked is never sent twice. The cost: a
  crash between a *confirmed* delivery and the mark can duplicate one message - at-least-once,
  with the id as the consumer's dedupe key.
- **Race with the silver consumer.** The detector reads the topic directly (that is what makes
  it fast) and the triggering run comes from the event itself; only the *comparison* data comes
  from silver. A previous run is 6 hours old, so it is in silver. A model whose run has not
  landed yet is simply absent from that evaluation, and one that is a whole cycle behind is
  excluded as not comparable.

The topic is the transport; `gold.alert` (migration 0009) is what the dashboard, the briefings
(Phase 5) and the agent (Phase 6) read.

## Latency

A 1-second batch window, a couple of index lookups per event and one produce. The
integration test replays a recorded event through real Kafka and Postgres and asserts the
alert is published within 60 seconds (the acceptance criterion); it prints the measured
latency, recorded in the results section below.

## Kafka Streams or Flink at scale

This design trades a database round trip per event for having no local state. That is right
for 300 forecast events and ~700 observations a day; at millions of events it would change.

- **State store.** The comparison state becomes a keyed state store: Kafka Streams' RocksDB
  store, or Flink's keyed state, holding the latest run per (model, location) - about
  25 locations × 3 models × 48 h × 4 variables ≈ 14,400 floats, i.e. tiny, so state *size* is
  never the problem here; state *recovery* is.
- **Recovery.** Kafka Streams backs each store with a compacted **changelog topic** and restores
  by replaying it (mitigated with **standby replicas**, which keep a warm copy on another
  instance). Flink checkpoints state to durable storage and restores from the last checkpoint,
  rewinding the source offsets to match - exactly-once against the state, not just the sink.
- **Partitioning.** State is local to a partition, so the streams must be *co-partitioned* on
  the same key. Forecasts are keyed by `location_id` and observations by `station`; the
  observation rule needs a re-key by location (a lookup of station → location, held as a
  `GlobalKTable` or Flink broadcast state) so both sides of the join land on one task.
- **Time.** Rule 1 is naturally *event-time*: the run's `init_time`, not the arrival time. A
  windowed join or an "as of" lookup would replace my `init_time < :init` query, with a
  watermark and an allowed-lateness policy deciding how long to wait for a slower model
  before evaluating the spread. My approach evaluates immediately with what has landed and
  lets the deterministic alert id absorb the later evaluations.
- **Exactly-once.** Kafka's transactions (producer `transactional.id`, `read_committed`
  consumers) or a Flink two-phase-commit sink give exactly-once alert emission without my
  insert-publish-mark protocol.
- **What I would not do:** keep the state in a Python dict and hope. It is the simplest thing
  and it loses the previous run on every restart, which is exactly what the brief warns about.

## Results on the real providers

`live-demo.yml` (14 days of history, then one live cycle of both producers, `make drain`,
`make alerts --drain`; run 35660002454) on a clean GitHub runner:

- The detector raised **12 alerts, all `warning`, all published**: 10 `model_spread` (5 dew point, 5
  pressure) and 2 `observation_miss` (both pressure). `run_change` did not fire: it needs two
  consecutive live runs of a model in silver, and one cycle gives one.
- **All seven pressure alerts (of the twelve) were an artifact, and they exposed a real data bug.**
  They were all at Mexico City, Bogota and Kathmandu. I checked live METARs: at those stations
  `slp` is absent, so the Phase 2 transform fell back to the *altimeter setting* (QNH) - which is
  not sea-level pressure at altitude. Denver, which reports both, shows SLP 1014.6 vs QNH
  1021.1 hPa (1,656 m); Bogota's QNH of 1026 hPa is ~12 hPa above the true value, matching the
  1,407 Pa "miss" the detector reported. The same substitution had been quietly biasing gold's
  pressure accuracy at every high station.
- **Fix:** the altimeter fallback now applies only at stations at or below 300 m
  (`ALTIMETER_FALLBACK_MAX_ELEVATION_M`, from `config/locations.yaml`; there QNH and SLP agree to
  about 1 hPa) and pressure is left missing elsewhere - better missing than wrong. Silver rows
  stored earlier are corrected by a replay from bronze (runbook section 4). The cost: pressure is
  no longer verified at the high-elevation stations without SLP.
- **Detector change:** above `pressure_max_elevation_m` (300 m) the spread and observation-miss
  rules ignore `pressure_msl`, because each model reduces to sea level in its own way and the
  reductions drift apart with altitude. Run-to-run change (a model against itself) is unaffected.
- The other five spread alerts were dew point at Phoenix, Dubai and Beijing (dry climates). I did
  not investigate them; models disagreeing about dew point in dry air is plausible but unverified.
- Alert latency on the real path was not measured (the live run consumed the events with
  `--drain`). The replay integration test asserts the sub-60-second criterion; CI prints its
  measured latency to the run summary.

## What is not verified

- Thresholds are untuned (above); with the pressure artifact removed the observed alert rate is
  five spread alerts per live cycle, which is a starting point, not a calibration.
- The run-change rule has not fired on two real consecutive runs.
- Producers are one process each and the detector is a single consumer; scaling the group out
  works (it is stateless) but was not tried.
