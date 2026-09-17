"""Phase 1 acceptance: killing and restarting a consumer mid-batch loses
nothing. Proven directly against the shared micro-batch primitive
(nimbus.streaming.microbatch) that both the bronze sink and silver consumer
build on, rather than re-enacting a full pipeline crash."""

import pytest
from confluent_kafka import Consumer, Producer
from testcontainers.community.kafka import KafkaContainer

from nimbus.streaming.microbatch import GracefulShutdown, _collect_batch

TOPIC = "test.restart-safety.v1"


def _consumer(bootstrap: str, group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


@pytest.mark.integration
def test_uncommitted_batch_is_redelivered_after_restart() -> None:
    with KafkaContainer() as kafka:
        bootstrap = kafka.get_bootstrap_server()
        producer = Producer({"bootstrap.servers": bootstrap})
        for i in range(3):
            producer.produce(TOPIC, key=str(i).encode(), value=str(i).encode())
        producer.flush(10)

        shutdown = GracefulShutdown()

        # "Process" 1: reads the batch, then crashes before committing -
        # handle_batch raising is exactly what run_microbatch_loop guards
        # against by committing only after the callback returns cleanly.
        consumer1 = _consumer(bootstrap, "restart-safety-group")
        consumer1.subscribe([TOPIC])
        first_batch = _collect_batch(consumer1, 10, 15.0, shutdown)
        assert len(first_batch) == 3
        consumer1.close()  # no commit() call - simulates a crash mid-batch

        # "Process" 2 (the restart): same group, nothing was ever committed,
        # so it must see the exact same messages again - zero loss.
        consumer2 = _consumer(bootstrap, "restart-safety-group")
        consumer2.subscribe([TOPIC])
        second_batch = _collect_batch(consumer2, 10, 15.0, shutdown)
        assert [m.value() for m in second_batch] == [m.value() for m in first_batch]

        # This time it succeeds and commits.
        consumer2.commit(asynchronous=False)
        consumer2.close()

        # "Process" 3: a fresh consumer in the same group sees nothing new -
        # the successful batch was not redelivered, and nothing was skipped.
        consumer3 = _consumer(bootstrap, "restart-safety-group")
        consumer3.subscribe([TOPIC])
        third_batch = _collect_batch(consumer3, 10, 5.0, shutdown)
        assert third_batch == []
        consumer3.close()
