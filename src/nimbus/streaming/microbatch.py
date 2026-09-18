"""Generic micro-batch consumer loop (brief section 6): collect up to N
messages or T seconds, hand the batch to a callback, and commit offsets only
after the callback returns without raising. A crash mid-batch just
reprocesses the same messages on restart - safe because every write this
project makes (bronze files, silver upserts) is idempotent. Shared by the
bronze sink and silver consumers so both get identical batching semantics."""

import logging
import time
from collections.abc import Callable, Sequence

from confluent_kafka import OFFSET_INVALID, Consumer, KafkaError, KafkaException

from nimbus.common.kafka import KafkaMessageLike
from nimbus.common.shutdown import GracefulShutdown

__all__ = ["GracefulShutdown", "is_caught_up", "run_microbatch_loop"]

logger = logging.getLogger(__name__)

BatchHandler = Callable[[Sequence[KafkaMessageLike]], None]


def run_microbatch_loop(
    consumer: Consumer,
    topics: list[str],
    handle_batch: BatchHandler,
    *,
    max_batch_size: int = 500,
    max_batch_seconds: float = 5.0,
    shutdown: GracefulShutdown | None = None,
    drain: bool = False,
) -> None:
    """With `drain=True` the loop exits once the consumer has caught up to the
    end of every assigned partition - for `make demo` and replays, which need a
    consumer that finishes rather than runs forever. Long-running services leave
    it False."""
    consumer.subscribe(topics)
    shutdown = shutdown or GracefulShutdown()

    try:
        while not shutdown.should_stop:
            batch = _collect_batch(consumer, max_batch_size, max_batch_seconds, shutdown)
            if not batch:
                if drain and is_caught_up(consumer):
                    logger.info("drained: caught up to the end of every assigned partition")
                    break
                continue
            handle_batch(batch)
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()


def is_caught_up(consumer: Consumer) -> bool:
    """True once the consumer's position has reached the high watermark of every
    assigned partition. Deliberately lag-based rather than "no message for N
    seconds": a fresh consumer group can wait many seconds for its first
    partition assignment, and an idle timeout would mistake that for "done"."""
    assignment = consumer.assignment()
    if not assignment:
        return False  # not assigned yet (still rebalancing) - can't be caught up
    try:
        committed = {
            (tp.topic, tp.partition): tp.offset
            for tp in consumer.committed(assignment, timeout=5.0)
        }
        for position in consumer.position(assignment):
            low, high = consumer.get_watermark_offsets(position, timeout=5.0)
            if low == high:
                continue  # empty partition
            current = position.offset
            if current == OFFSET_INVALID:
                # Nothing fetched this session (e.g. a restart after everything was
                # already committed): the committed offset is the real position.
                current = committed.get((position.topic, position.partition), OFFSET_INVALID)
            if current == OFFSET_INVALID or current < high:
                return False
    except KafkaException:
        return False
    return True


def _collect_batch(
    consumer: Consumer,
    max_batch_size: int,
    max_batch_seconds: float,
    shutdown: GracefulShutdown,
) -> list[KafkaMessageLike]:
    batch: list[KafkaMessageLike] = []
    deadline = time.monotonic() + max_batch_seconds
    while len(batch) < max_batch_size and time.monotonic() < deadline and not shutdown.should_stop:
        remaining = max(0.0, deadline - time.monotonic())
        msg = consumer.poll(timeout=min(1.0, remaining))
        if msg is None:
            continue
        error = msg.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                continue
            logger.error("consumer poll error", extra={"kafka_error": str(error)})
            continue
        batch.append(msg)
    return batch
