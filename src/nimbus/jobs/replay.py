"""Replay tooling (brief section 6; see docs/runbook.md).

Two independent recovery paths:

  offsets   Reset a consumer group's committed offsets so it re-reads from Kafka.
            For a bad transform or a lost silver table *while the data is still
            inside Kafka retention*. The group must be stopped first.

  bronze    Rebuild silver straight from the Parquet lake, no broker involved.
            For when Kafka retention has already expired. Reuses the exact
            `load_messages` code the live consumers run, so a rebuild and the
            live path can never drift apart.

Both are safe to repeat: every silver write is an idempotent upsert."""

import argparse
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import OFFSET_INVALID, Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient
from sqlalchemy import Engine, text

from nimbus.common.db import make_engine
from nimbus.common.kafka import KafkaMessageLike
from nimbus.common.logging import configure_logging
from nimbus.common.settings import Settings, get_settings
from nimbus.jobs.reconcile import TOPIC_SPECS, diff_key_sets, silver_key_hashes, summarize_bronze
from nimbus.streaming import forecast_silver, observation_silver
from nimbus.streaming.bronze_reader import DEFAULT_LAKE_ROOT, iter_bronze_batches

logger = logging.getLogger(__name__)

LoadFn = Callable[
    [Sequence[KafkaMessageLike], Engine, Callable[[KafkaMessageLike, Exception], None]], int
]


@dataclass(frozen=True)
class ReplayTarget:
    load: LoadFn
    table: str


TARGETS: dict[str, ReplayTarget] = {
    "weather.forecast.raw.v1": ReplayTarget(forecast_silver.load_messages, "silver.forecast"),
    "weather.observation.raw.v1": ReplayTarget(
        observation_silver.load_messages, "silver.observation"
    ),
}


@dataclass
class ReplayResult:
    messages: int = 0
    loaded: int = 0
    poison: int = 0


class UnsafeRebuildError(RuntimeError):
    """A truncating rebuild would delete rows that exist nowhere else."""


def unexplained_silver_rows(topic: str, engine: Engine, lake_root: Path) -> int:
    """Silver rows the lake cannot account for. If the bronze sink was down or
    behind while data was consumed into silver, those rows exist *only* in silver."""
    spec = TOPIC_SPECS[topic]
    expected = summarize_bronze(spec, lake_root).expected_keys
    _missing, extra = diff_key_sets(expected, silver_key_hashes(engine, spec))
    return extra


def replay_from_bronze(
    topic: str,
    engine: Engine,
    lake_root: Path = DEFAULT_LAKE_ROOT,
    *,
    truncate: bool = False,
    batch_size: int = 50,
    force: bool = False,
) -> ReplayResult:
    """Rebuild a silver table from the lake. `truncate=True` empties it first, so
    the result is exactly what bronze explains (a *rebuild*); without it the
    replay is merely a re-apply on top of what's there.

    A truncating rebuild refuses to run if silver holds rows the lake can't
    explain, because truncating would destroy the only copy. `force=True`
    accepts that loss deliberately."""
    target = TARGETS[topic]
    result = ReplayResult()

    if truncate and not force:
        unexplained = unexplained_silver_rows(topic, engine, lake_root)
        if unexplained:
            raise UnsafeRebuildError(
                f"{target.table} holds {unexplained:,} rows the bronze lake cannot explain; "
                "a truncating rebuild would delete them permanently. If the bronze sink was "
                "down or behind and Kafka still holds those messages, run "
                "`python -m nimbus.streaming.bronze_sink --drain` first and reconcile again. "
                "Otherwise pass --force to accept the loss."
            )

    if truncate:
        logger.warning("truncating before rebuild", extra={"table": target.table})
        with engine.begin() as conn:
            # Table name comes from the TARGETS constant above, never input.
            conn.execute(text(f"TRUNCATE TABLE {target.table}"))

    def on_poison(message: KafkaMessageLike, error: Exception) -> None:
        result.poison += 1
        logger.warning(
            "skipping unparseable bronze message",
            extra={"offset": message.offset(), "error": str(error)},
        )

    for batch in iter_bronze_batches(topic, lake_root, batch_size):
        result.messages += len(batch)
        result.loaded += target.load(batch, engine, on_poison)
    return result


def parse_utc(value: str) -> datetime:
    """ISO-8601 -> aware datetime; a value with no offset is taken as UTC. A naive
    datetime's .timestamp() silently uses the machine's local timezone, which
    would shift a replay window by hours without any error."""
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def resolve_offset_targets(
    consumer: Consumer, topic: str, from_time: datetime | None
) -> list[TopicPartition]:
    """Per-partition offsets to reset a group to: the earliest retained offset, or
    the first offset at/after `from_time`. A partition with no message at/after
    that time resolves to its end, so the group simply has nothing to re-read."""
    partitions = sorted(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
    watermarks = {
        p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10) for p in partitions
    }
    if from_time is None:
        return [TopicPartition(topic, p, watermarks[p][0]) for p in partitions]

    epoch_ms = int(from_time.timestamp() * 1000)
    resolved = consumer.offsets_for_times(
        [TopicPartition(topic, p, epoch_ms) for p in partitions], timeout=10
    )
    return [
        TopicPartition(
            topic,
            tp.partition,
            watermarks[tp.partition][1]
            if tp.offset < 0 or tp.offset == OFFSET_INVALID
            else tp.offset,
        )
        for tp in resolved
    ]


def reset_offsets(
    settings: Settings, group: str, topic: str, from_time: datetime | None = None
) -> list[TopicPartition]:
    probe = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"replay-probe-{group}",  # never the group being reset
            "enable.auto.commit": False,
        }
    )
    try:
        targets = resolve_offset_targets(probe, topic, from_time)
    finally:
        probe.close()

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    futures = admin.alter_consumer_group_offsets([ConsumerGroupTopicPartitions(group, targets)])
    for future in futures.values():
        future.result(timeout=30)  # raises if the group still has active members
    return targets


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay tooling - see docs/runbook.md")
    sub = parser.add_subparsers(dest="command", required=True)

    bronze = sub.add_parser("bronze", help="rebuild silver from the Parquet lake")
    bronze.add_argument("--topic", required=True, choices=sorted(TARGETS))
    bronze.add_argument("--lake-root", type=Path, default=DEFAULT_LAKE_ROOT)
    bronze.add_argument(
        "--truncate", action="store_true", help="empty the silver table first (a true rebuild)"
    )
    bronze.add_argument(
        "--force",
        action="store_true",
        help="truncate even if silver holds rows the lake cannot explain (they are lost)",
    )

    offsets = sub.add_parser("offsets", help="reset a consumer group's offsets (group stopped)")
    offsets.add_argument("--group", required=True)
    offsets.add_argument("--topic", required=True, choices=sorted(TARGETS))
    offsets.add_argument(
        "--from-time", type=parse_utc, help="ISO-8601 time, UTC if no offset; default earliest"
    )

    args = parser.parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)

    if args.command == "bronze":
        try:
            replay = replay_from_bronze(
                args.topic,
                make_engine(settings),
                args.lake_root,
                truncate=args.truncate,
                force=args.force,
            )
        except UnsafeRebuildError as exc:
            raise SystemExit(f"refusing to rebuild: {exc}") from exc
        print(
            f"replayed {replay.messages:,} bronze messages: "
            f"{replay.loaded:,} loaded, {replay.poison:,} unparseable (skipped)"
        )
    else:
        targets = reset_offsets(settings, args.group, args.topic, args.from_time)
        for tp in targets:
            print(f"{tp.topic}[{tp.partition}] -> offset {tp.offset}")


if __name__ == "__main__":
    main()
