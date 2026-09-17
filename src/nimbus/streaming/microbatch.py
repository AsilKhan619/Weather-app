"""Generic micro-batch consumer loop (brief section 6): collect up to N
messages or T seconds, hand the batch to a callback, and commit offsets only
after the callback returns without raising. A crash mid-batch just
reprocesses the same messages on restart - safe because every write this
project makes (bronze files, silver upserts) is idempotent. Shared by the
bronze sink and silver consumers so both get identical batching semantics."""

import logging
import signal
import time
from collections.abc import Callable, Sequence
from types import FrameType

from confluent_kafka import Consumer, KafkaError

from nimbus.common.kafka import KafkaMessageLike

logger = logging.getLogger(__name__)

BatchHandler = Callable[[Sequence[KafkaMessageLike]], None]


class GracefulShutdown:
    """Flips `should_stop` on SIGTERM/SIGINT so a running batch loop can exit
    cleanly between batches instead of being killed mid-write."""

    def __init__(self) -> None:
        self.should_stop = False
        signal.signal(signal.SIGTERM, self._handle)
        signal.signal(signal.SIGINT, self._handle)

    def _handle(self, signum: int, frame: FrameType | None) -> None:
        logger.info("shutdown signal received", extra={"signal": signum})
        self.should_stop = True


def run_microbatch_loop(
    consumer: Consumer,
    topics: list[str],
    handle_batch: BatchHandler,
    *,
    max_batch_size: int = 500,
    max_batch_seconds: float = 5.0,
    shutdown: GracefulShutdown | None = None,
) -> None:
    consumer.subscribe(topics)
    shutdown = shutdown or GracefulShutdown()

    try:
        while not shutdown.should_stop:
            batch = _collect_batch(consumer, max_batch_size, max_batch_seconds, shutdown)
            if not batch:
                continue
            handle_batch(batch)
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()


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
