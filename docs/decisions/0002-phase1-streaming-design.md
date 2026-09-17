# ADR 0002: Phase 1 streaming design (bronze/silver fan-out, DLQ, idempotent upserts)

**Status:** Accepted
**Date:** 2026-09-17

## Context

Phase 1 builds the first vertical slice: a forecast producer, bronze sink, and silver consumer, all running against real infrastructure (verified live, not just unit-tested). A few decisions came up during implementation that are worth recording.

## Shared micro-batch primitive, not two separate consumer loops

Both the bronze sink and the silver consumer need identical batching semantics (brief section 6: collect N messages or T seconds, write, commit only after success). Rather than duplicate that loop, `nimbus.streaming.microbatch.run_microbatch_loop` is the one implementation both build on, parameterized by a `handle_batch` callback. This is what makes the restart-safety guarantee a property of one well-tested primitive instead of something each consumer has to get right independently.

## `KafkaMessageLike` Protocol instead of `confluent_kafka.Message`

`confluent_kafka.Message` is a C-extension type with no public constructor, so it can't be instantiated directly in a unit test. Streaming code is typed against a structural `Protocol` (`nimbus.common.kafka.KafkaMessageLike`) covering only the methods actually used (`partition`, `offset`, `timestamp`, `key`, `value`, `topic`). A real `Message` satisfies it structurally, and so does a plain duck-typed `FakeMessage` in tests — this is what let the bronze sink get real unit tests instead of needing Testcontainers for every test.

## Chunked upserts (a real bug caught by manual verification, not by a unit test)

The first live run against the actual stack failed: `psycopg.OperationalError: number of parameters must be between 0 and 65535`. One micro-batch of 15 events (5 locations × 3 models) explodes to 15 × 168 hours × 4 variables = 10,080 rows, and a single multi-row `INSERT ... ON CONFLICT` with 9 columns binds 9 parameters per row — over 90,000 for that batch, well past Postgres's per-statement limit. `upsert_forecast_rows` now chunks at 5,000 rows/statement (45,000 params, comfortably clear of the limit) inside one transaction. This is exactly the kind of bug that only shows up at realistic batch sizes — the unit tests use tiny fixtures and never would have caught it, which is why the brief's "run it against real infrastructure, don't just claim it works" rule mattered here.

## DLQ payload shape

`weather.dlq.v1` carries a `DlqRecord` (original payload as text, error type/message, source topic/partition/offset, failure time) rather than being wrapped in the same `EventEnvelope` used for domain topics — it's an operational record about a processing failure, not domain data, so it doesn't need `schema_version`/`ingestion_mode`. A malformed message is caught per-message inside the batch (`ValidationError`, `ValueError`, `KeyError`, `TypeError`), routed to the DLQ, and the rest of the batch still gets written and committed — verified directly (`tests/integration/test_forecast_pipeline.py`) with a poison message injected alongside two valid ones.

## No `dim_location` / `dim_model` / `dim_variable` tables yet

The brief's full data model (section 7) includes dimension tables, but `silver.forecast` uses plain natural-key text columns (`model`, `location_id`, `variable`) for Phase 1. Nothing yet needs to join against richer attributes (display names, join paths for the dashboard) - that need arrives with the dashboard (Phase 4) and the agent's semantic layer (Phase 6). Adding the tables now would be speculative.

## Consequences

- `upsert_forecast_rows`'s chunk size (5,000) is a one-line constant if it ever needs tuning for a much larger location/model count.
- The DLQ schema doesn't currently preserve the original `event_id` when the envelope itself fails to parse (e.g. invalid JSON) - only when the envelope parses but the payload transform fails. Full envelope corruption is rare (it would mean the producer itself is broken) and `sample_dlq` (Phase 6) can still see the raw bytes either way.
