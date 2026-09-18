from typing import Any

from confluent_kafka import OFFSET_INVALID, KafkaError, KafkaException, TopicPartition

from nimbus.streaming.microbatch import is_caught_up

TOPIC = "t"


class _FakeConsumer:
    """Just the four calls is_caught_up makes, with per-partition state."""

    def __init__(
        self,
        assigned: list[int],
        positions: dict[int, int],
        watermarks: dict[int, tuple[int, int]],
        committed: dict[int, int] | None = None,
        raise_on_watermarks: bool = False,
    ) -> None:
        self.assigned = assigned
        self.positions = positions
        self.watermarks = watermarks
        self.committed_offsets = committed or {}
        self.raise_on_watermarks = raise_on_watermarks

    def assignment(self) -> list[TopicPartition]:
        return [TopicPartition(TOPIC, p) for p in self.assigned]

    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]:
        return [TopicPartition(TOPIC, p.partition, self.positions[p.partition]) for p in partitions]

    def committed(self, partitions: list[TopicPartition], timeout: float) -> list[TopicPartition]:
        return [
            TopicPartition(
                TOPIC, p.partition, self.committed_offsets.get(p.partition, OFFSET_INVALID)
            )
            for p in partitions
        ]

    def get_watermark_offsets(self, tp: TopicPartition, timeout: float) -> tuple[int, int]:
        if self.raise_on_watermarks:
            raise KafkaException(KafkaError(KafkaError._TRANSPORT))
        return self.watermarks[tp.partition]


def _caught_up(consumer: _FakeConsumer) -> bool:
    arg: Any = consumer
    return is_caught_up(arg)


def test_not_caught_up_before_any_partition_is_assigned() -> None:
    assert _caught_up(_FakeConsumer([], {}, {})) is False


def test_caught_up_when_every_position_reached_the_high_watermark() -> None:
    consumer = _FakeConsumer([0, 1], {0: 10, 1: 5}, {0: (0, 10), 1: (0, 5)})
    assert _caught_up(consumer) is True


def test_not_caught_up_while_any_partition_still_has_messages() -> None:
    consumer = _FakeConsumer([0, 1], {0: 10, 1: 4}, {0: (0, 10), 1: (0, 5)})
    assert _caught_up(consumer) is False


def test_empty_partitions_never_block_a_drain() -> None:
    consumer = _FakeConsumer([0, 1], {0: 10, 1: OFFSET_INVALID}, {0: (0, 10), 1: (0, 0)})
    assert _caught_up(consumer) is True


def test_restart_after_everything_was_committed_is_caught_up() -> None:
    # Nothing fetched this session, so position is invalid - the committed
    # offset is the real position. Without the fallback a drain would spin forever.
    consumer = _FakeConsumer([0], {0: OFFSET_INVALID}, {0: (0, 10)}, committed={0: 10})
    assert _caught_up(consumer) is True


def test_invalid_position_with_no_commit_and_messages_waiting_is_not_caught_up() -> None:
    consumer = _FakeConsumer([0], {0: OFFSET_INVALID}, {0: (0, 10)})
    assert _caught_up(consumer) is False


def test_a_broker_error_while_checking_is_treated_as_not_caught_up() -> None:
    consumer = _FakeConsumer([0], {0: 10}, {0: (0, 10)}, raise_on_watermarks=True)
    assert _caught_up(consumer) is False
