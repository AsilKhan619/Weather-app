# ADR 0008: Grounded LLM briefings (Phase 5)

**Status:** Accepted
**Date:** 2026-09-23

## Context

Brief section 10 asks for a daily briefing per location (and one when an alert arrives) that
explains the forecast in plain language, uses only facts the pipeline computed, is validated as
structured output, is automatically checked for invented numbers, never pays twice for the same
input, logs every call's tokens, latency and cost, and never stops the data pipeline when the LLM
is unavailable. The whole platform must run without an Anthropic API key (`LLM_ENABLED=false` is
the default), and tests, CI and `make eval` never call the real API.

**No API key exists yet** (PLAN.md: "user adds Anthropic API key + billing cap - blocked until
then"). Everything below is built and tested with a deterministic fake client and stubbed SDK
replies; no request has been sent to the real API. That is stated wherever it matters.

## Design

```
make briefings (daily)  ┐
weather.alert.v1 ───────┴─> fact sheet (code) ─> cache? ─> LLM (1 retry on invalid)
                                                              │
                                        grounding check <─────┘
                                   pass │           │ fail
                    gold.briefing (published)   gold.briefing (flagged)
                    weather.briefing.v1          never published
```

### The fact sheet is the whole world

`nimbus.llm.facts.build_fact_sheet` (pure, unit-tested) produces a JSON document with: each
model's min/mean/max over the next 48 hours per variable, the spread between models per variable,
each model's recent day-1 temperature accuracy at the location (30 days of `gold.accuracy_daily`),
the alerts active in the last 24 hours, and a **confidence level decided by code**.

- **"Next 48 hours" is the latest forecast issued at or before `as_of`, per model and hour**
  (`DISTINCT ON (model, variable, valid_time) ... ORDER BY init_time DESC`). That works identically
  for live runs and for backfilled rows (whose derived `init_time`s make a single "run" sparse),
  and it never looks at a forecast issued after `as_of` - an integration test seeds a later run at
  350 K and checks it does not appear.
- **Display units, one decimal.** degC, hPa, m/s (the dashboard's conversions, now shared in
  `nimbus.common.units`). The model can quote any number exactly, and the grounding check can hold
  it to them.
- **Confidence** is the mean over the 48 hours of (max - min across models) of 2 m temperature:
  <= 1.5 degC high, <= 3.0 medium, else low (`config/llm.yaml`). The thresholds sit against the
  measured day-1-2 temperature error (MAE 1.4-1.5 K, ADR 0005): models closer together than one
  typical error is "high". Fewer than two models is "low". The model must copy the level; it only
  explains it.
- **Canonical JSON (sorted keys) is hashed**, and that hash is the cache key.

### Structured output, validated here

The request uses the API's structured outputs (`output_config.format` with a JSON schema generated
from the Pydantic `BriefingOutput` by the SDK's public `anthropic.transform_schema`). Two facts
drove the details, both checked against the installed SDK (anthropic 1.6.0) rather than recalled:

- `messages.parse()` would validate too, but it **raises on an invalid reply and discards the
  response** - including its token usage. Calling `messages.create()` and validating the text with
  Pydantic here means every attempt, including a bad one, is logged with what it cost.
- `transform_schema` moves `maxLength`/`maxItems` into descriptions: constrained decoding cannot
  enforce them. So a headline over 120 characters is a *realistic* invalid output, and the brief's
  "retry once" path is not theoretical. It is exactly one retry (`MAX_ATTEMPTS = 2`), tested with a
  client that fails once (recovers) and one that fails five times (two calls, nothing stored).

A `max_tokens` or `refusal` stop reason is treated as invalid output. SDK errors (timeouts,
connection errors, 4xx/5xx after the SDK's own two retries) are caught by their typed classes only
and become `LLMUnavailableError`: the briefing is skipped, `ops.llm_calls` records the error, and
the job moves to the next location.

### Grounding check

Every number in the headline, summary, reason and risks must appear in the fact sheet, or round
from one of its numeric values at the precision quoted (21.4 may be written 21; half rounds up).
**Signs must agree** - "-16.9" is not grounded by 16.9, and the prompt tells the model to write
negative numbers with a minus sign. **Digits inside the sheet's names are not facts**: before the
briefing's numbers are read, every sheet string containing digits (model ids like `ecmwf_ifs025`,
variable names like `temperature_2m`, the `date`) is removed from its text, so naming them is never
a failure but "gusts to 25 m/s" is. A hyphen between numbers ("20-24") is a range, not a minus
sign; "1,013" is one number; ".5" and malformed runs like "3.14.15" are read, not skipped.
Beyond numbers: `confidence` must equal the sheet's level, and `most_reliable_model` must be the
sheet's most accurate model when it names one.

A failing briefing is **stored with `grounding_passed = false` and its failures, and never
published** - kept, so the failure rate is measurable and each failure inspectable (the dashboard
shows them). The brief's acceptance test - a briefing with an invented number is rejected - is a
unit test (`97 is not in the fact sheet`) and an integration test (stored flagged, the producer
never called, and a repeat request is a cache hit that is still not published).

**Known gaps:** numbers written as words ("three") are not detected; a number can still be grounded
by coincidence - any 16.9 in the sheet grounds any "16.9" in the text, whatever it refers to. The check
guarantees "no number from outside the facts", not "every number used correctly".

### Cache: identical inputs never pay twice

`briefing_id = sha256(fact sheet hash | prompt version | model)`, the primary key of
`gold.briefing`. Before calling the API the generator looks the id up; a hit returns the stored
briefing (flagged or not), logs `cache_hit` at zero cost, and publishes it only if it is grounded
and not yet published. A daily run and an alert in the same hour share one fact sheet (`as_of` is
floored to the hour) and so one paid call. Tested: the second request never reaches the client.

**API-side prompt caching is not used.** The system prompt (~450 tokens) is far under Haiku 4.5's
minimum cacheable prefix of 4,096 tokens - a cache marker on it would silently do nothing - and the
rest of each request, the fact sheet, is different every time. The application-level cache above is what saves money here.

### Model, prices, logging

- Model: `claude-haiku-4-5` (setting `NIMBUS_BRIEFING_MODEL`), chosen in the approved plan for a
  short, high-volume, fully grounded writing task; the alias replaces the dated id used before.
  No extended thinking: the task is transcription and explanation of computed facts.
- Prices live in `config/llm.yaml` (Haiku 4.5 $1/$5 per million tokens; cache writes 1.25x, reads
  0.1x input). `ops.llm_calls` records purpose, model, prompt version, fact-sheet hash, attempt,
  input/output/cache tokens, latency, estimated cost, outcome (`success`, `invalid_output`,
  `grounding_failed`, `error`, `cache_hit`, `disabled`), the error text and the API request id.
- **Expected cost**, estimated, not measured: a realistic fact sheet (3 models x 4 variables, one
  alert) is 1,839 characters and the system prompt 1,789 - roughly 460 and 450 tokens at ~4
  characters a token - plus the JSON schema; call it ~1,100 input and ~200 output tokens, so about
  $0.002 a briefing on Haiku 4.5 and about $0.06 a day for 25 locations before cache hits. To be
  replaced by the logged numbers once a key exists.
- Prompts are versioned files (`src/nimbus/llm/prompts/briefing_v1.md`); a wording change is a new
  file and a new version, which is part of the cache key and of every log row.

### Delivery

Same pattern as the anomaly detector (ADR 0006): store first, publish to `weather.briefing.v1`,
set `published_at` only after Kafka confirms. At-least-once; the briefing id is the dedupe key. A
failed delivery is contained (the run carries on) and retried by `publish_pending` at the start of
the next daily run or consumer start.

### LLM disabled

`LLM_ENABLED=false` (the default) is a first-class path, not an error: the generator still reads
the data and builds the fact sheet (proving the data side on real data - the live-demo workflow
prints one), serves an already-cached briefing if one exists, and otherwise logs `disabled` and
moves on. There is no silent fallback to the fake client in the pipeline; `LLM_ENABLED=true` with
a bad key fails loudly on the first call (logged as `error`).

## Independent review: eight findings, all fixed

A review of the phase diff against this ADR and the brief found eight issues; four were reproduced
against the real functions. All are fixed with regression tests.

1. **Alert briefings could omit their alert** (high). The consumer briefed as of the top of the
   hour, and the fact sheet counts alerts up to `as_of`, so an observation alert at 14:51 was cut
   off by a 14:00 sheet - identical to the daily one, served from cache, trigger `daily`.
   `as_of` is now the later of the hour and the alert's own time.
2. **A flipped sign passed** ("-16.9" on a sheet saying 16.9): grounding ignored sign by design.
   Signs must now agree.
3. **Identifier digits were allowed numbers**, so 25 (`ecmwf_ifs025`), 10 (`wind_speed_10m`) and
   the date's 20 were quotable on every sheet. Identifiers are now stripped from the text instead.
4. **".5" was invisible** to the number scanner, and "3.14.15" hid its last part. Both are read.
5. **The same facts could hash differently** - two run-change alerts in one cycle tied on the sort
   key, so their order (and the cache key) followed Postgres's row order: a second paid call for
   the same facts. The sort is now total, and the query ordered.
6. **A Kafka failure aborted the daily run** and stranded the stored briefing unpublished. A
   publish failure is now logged and contained, and `publish_pending` sends grounded, unpublished
   briefings at the start of every run.
7. **Failed calls were logged at 0 ms** (a timeout can take minutes), and the LLM Usage page
   averaged per-outcome averages. Failures now log wall-clock time; the page weights by calls.
8. **Accuracy included the as-of day** - mostly in the future of `as_of`, and cast in SQL under
   the session time zone. The window is now whole UTC days strictly before `as_of`, computed in
   Python.

Also fixed: "1,013 hPa" used to be read as 1 and 13, rejecting a faithful briefing.

## Alternatives considered

- **Forced tool use** for the JSON (`tool_choice` naming one tool) - works on Haiku 4.5, but
  structured outputs are the purpose-built feature and forced tool use is being removed on newer
  models (it 400s on Claude Fable 5.1 and Opus 5.5), which would make a later model upgrade a rewrite.
- **Letting the LLM compute confidence or pick the best model** - rejected by the brief and by the
  grounding design: anything the model decides is something that cannot be checked.
- **Discarding briefings that fail grounding** - rejected: without them there is no failure rate to
  measure and nothing to debug.

## What is and isn't verified

- Verified: 30 unit tests (fact sheet, confidence, grounding incl. the acceptance test and the
  review's regressions, schema validation, cost, the real client against stubbed SDK replies -
  JSON schema sent, invalid reply logged with usage, truncation/refusal, timeout mapped to
  unavailable) and 15 Postgres integration tests (store and publish, cache hit skips the client,
  flagged never published, one retry, unavailable, disabled, no data, alert bursts, a late alert,
  a Kafka failure then republish, failure latency, the accuracy window), plus the Briefings and LLM
  Usage pages rendered headlessly on seeded data.
- **Not verified: any real API call.** The real client is tested only against stubbed replies. The
  first run with a key must confirm the request shape is accepted, the grounding pass rate on real
  model output, latency and cost - and a billing cap should be set in the Anthropic console before
  that run (the user's decision; not done here).
