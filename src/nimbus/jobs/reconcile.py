"""Reconciliation (brief section 8): do the counts add up from what was produced,
to what bronze captured, to what silver contains?

The strong check is *rebuild equivalence*. For every bronze message we compute,
with the exact transform the silver consumer uses, which silver rows it should
produce, and compare that key set to the rows silver actually holds. If they
match, silver is provably what a rebuild from bronze would give - which is also
the acceptance test for the replay runbook. Missing rows mean a consumer lost
data; extra rows mean silver holds something bronze can't explain.

Keys are compared as hashes of a canonical string form (timestamps normalised to
epoch seconds), so the comparison doesn't depend on datetime resolution or
categorical-vs-string dtypes differing between pandas and Postgres."""

import argparse
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from pydantic import ValidationError
from sqlalchemy import Engine, text

from nimbus.common.db import make_engine
from nimbus.common.logging import configure_logging
from nimbus.common.settings import get_settings
from nimbus.streaming import forecast_silver, observation_silver
from nimbus.streaming.bronze_reader import DEFAULT_LAKE_ROOT, iter_bronze_batches

logger = logging.getLogger(__name__)

_EPOCH = pd.Timestamp("1970-01-01", tz="UTC")


@dataclass(frozen=True)
class TopicSpec:
    topic: str
    table: str
    key_columns: tuple[str, ...]
    time_columns: tuple[str, ...]
    to_frame: Callable[[bytes], pd.DataFrame]
    produced_sources: tuple[str, ...]


TOPIC_SPECS: dict[str, TopicSpec] = {
    "weather.forecast.raw.v1": TopicSpec(
        topic="weather.forecast.raw.v1",
        table="silver.forecast",
        key_columns=("model", "location_id", "init_time", "valid_time", "variable"),
        time_columns=("init_time", "valid_time"),
        to_frame=forecast_silver.message_to_frame,
        produced_sources=("forecast_producer", "forecast_backfill"),
    ),
    "weather.observation.raw.v1": TopicSpec(
        topic="weather.observation.raw.v1",
        table="silver.observation",
        key_columns=("station", "observed_at", "variable"),
        time_columns=("observed_at",),
        # plain frame: reconciliation only needs keys, so skip the categorical casts
        to_frame=lambda raw: pd.DataFrame(observation_silver.message_to_rows(raw)),
        produced_sources=("observation_producer", "observation_backfill"),
    ),
}


@dataclass(frozen=True)
class ReconciliationResult:
    topic: str
    produced_messages: int
    bronze_messages: int
    bronze_distinct_events: int
    bronze_unparseable: int
    expected_silver_rows: int
    silver_rows: int
    missing_from_silver: int
    extra_in_silver: int

    @property
    def matched(self) -> bool:
        return self.missing_from_silver == 0 and self.extra_in_silver == 0


def key_hashes(
    frame: pd.DataFrame, key_columns: tuple[str, ...], time_columns: tuple[str, ...]
) -> np.ndarray:
    """One uint64 per row, hashing a canonical string of the natural key."""
    parts: list[pd.Series] = []
    for column in key_columns:
        if column in time_columns:
            seconds = (frame[column] - _EPOCH) // pd.Timedelta(seconds=1)
            parts.append(seconds.astype("int64").astype(str))
        else:
            parts.append(frame[column].astype(str))
    joined = parts[0]
    for part in parts[1:]:
        joined = joined + "|" + part
    return pd.util.hash_pandas_object(joined, index=False).to_numpy()


def diff_key_sets(expected: np.ndarray, actual: np.ndarray) -> tuple[int, int]:
    """(missing_from_actual, extra_in_actual) between two arrays of unique hashes."""
    return (
        int(np.setdiff1d(expected, actual, assume_unique=True).size),
        int(np.setdiff1d(actual, expected, assume_unique=True).size),
    )


@dataclass(frozen=True)
class BronzeSummary:
    messages: int
    distinct_events: int
    unparseable: int
    expected_keys: np.ndarray


def summarize_bronze(spec: TopicSpec, lake_root: Path = DEFAULT_LAKE_ROOT) -> BronzeSummary:
    """Replay the lake through the silver transform *in memory* - nothing is
    written - and collect the unique silver keys it would produce."""
    messages = unparseable = 0
    event_ids: set[str] = set()
    hash_chunks: list[np.ndarray] = []

    for batch in iter_bronze_batches(spec.topic, lake_root):
        frames: list[pd.DataFrame] = []
        for message in batch:
            messages += 1
            raw = message.value()
            if raw is None:
                unparseable += 1
                continue
            try:
                frame = spec.to_frame(raw)
            except (ValidationError, ValueError, KeyError, TypeError):
                unparseable += 1  # these are the messages the live consumer sent to the DLQ
                continue
            if not frame.empty:
                frames.append(frame)
        if not frames:
            continue
        # Hash once per batch: per-message pandas work is the expensive part.
        batch_frame = pd.concat(frames, ignore_index=True)
        event_ids.update(batch_frame["source_event_id"].astype(str).unique())
        hash_chunks.append(key_hashes(batch_frame, spec.key_columns, spec.time_columns))

    expected = np.unique(np.concatenate(hash_chunks)) if hash_chunks else np.array([], np.uint64)
    return BronzeSummary(messages, len(event_ids), unparseable, expected)


def silver_key_hashes(engine: Engine, spec: TopicSpec, chunksize: int = 200_000) -> np.ndarray:
    columns = ", ".join(spec.key_columns)
    chunks: list[np.ndarray] = []
    with engine.connect() as conn:
        # Table and column names come from TOPIC_SPECS constants above, never input.
        query = text(f"SELECT {columns} FROM {spec.table}")
        for chunk in pd.read_sql(query, conn, chunksize=chunksize):
            chunks.append(key_hashes(chunk, spec.key_columns, spec.time_columns))
    return np.unique(np.concatenate(chunks)) if chunks else np.array([], np.uint64)


def produced_message_count(engine: Engine, spec: TopicSpec) -> int:
    with engine.connect() as conn:
        total = conn.execute(
            text(
                "SELECT coalesce(sum(messages_produced), 0) FROM ops.ingestion_runs "
                "WHERE source = ANY(:sources)"
            ),
            {"sources": list(spec.produced_sources)},
        ).scalar_one()
    return int(total)


def reconcile_topic(
    engine: Engine, spec: TopicSpec, lake_root: Path = DEFAULT_LAKE_ROOT
) -> ReconciliationResult:
    bronze = summarize_bronze(spec, lake_root)
    silver = silver_key_hashes(engine, spec)
    missing, extra = diff_key_sets(bronze.expected_keys, silver)
    return ReconciliationResult(
        topic=spec.topic,
        produced_messages=produced_message_count(engine, spec),
        bronze_messages=bronze.messages,
        bronze_distinct_events=bronze.distinct_events,
        bronze_unparseable=bronze.unparseable,
        expected_silver_rows=int(bronze.expected_keys.size),
        silver_rows=int(silver.size),
        missing_from_silver=missing,
        extra_in_silver=extra,
    )


def record_result(engine: Engine, result: ReconciliationResult) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO ops.reconciliation_results "
                "(topic, produced_messages, bronze_messages, bronze_distinct_events, "
                "bronze_unparseable, expected_silver_rows, silver_rows, "
                "missing_from_silver, extra_in_silver, matched) "
                "VALUES (:topic, :produced, :bronze, :distinct, :unparseable, :expected, "
                ":silver, :missing, :extra, :matched)"
            ),
            {
                "topic": result.topic,
                "produced": result.produced_messages,
                "bronze": result.bronze_messages,
                "distinct": result.bronze_distinct_events,
                "unparseable": result.bronze_unparseable,
                "expected": result.expected_silver_rows,
                "silver": result.silver_rows,
                "missing": result.missing_from_silver,
                "extra": result.extra_in_silver,
                "matched": result.matched,
            },
        )


def format_result(result: ReconciliationResult) -> str:
    status = "MATCH" if result.matched else "MISMATCH"
    return "\n".join(
        [
            f"{result.topic}  [{status}]",
            f"  produced (ops.ingestion_runs)   {result.produced_messages:>12,}",
            f"  bronze messages                 {result.bronze_messages:>12,}",
            f"  bronze distinct events          {result.bronze_distinct_events:>12,}",
            f"  bronze unparseable (-> DLQ)     {result.bronze_unparseable:>12,}",
            f"  expected silver rows            {result.expected_silver_rows:>12,}",
            f"  actual silver rows              {result.silver_rows:>12,}",
            f"  missing from silver             {result.missing_from_silver:>12,}",
            f"  extra in silver                 {result.extra_in_silver:>12,}",
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile produced -> bronze -> silver counts.")
    parser.add_argument("--topic", choices=sorted(TOPIC_SPECS), help="default: every topic")
    parser.add_argument("--lake-root", type=Path, default=DEFAULT_LAKE_ROOT)
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)
    engine = make_engine(settings)

    specs = [TOPIC_SPECS[args.topic]] if args.topic else list(TOPIC_SPECS.values())
    results = [reconcile_topic(engine, spec, args.lake_root) for spec in specs]
    for result in results:
        record_result(engine, result)
        print(format_result(result))

    if not all(r.matched for r in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
