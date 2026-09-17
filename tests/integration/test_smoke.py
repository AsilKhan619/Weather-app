"""Phase 0 smoke test: produce a message to Kafka, consume it, and write a row
to Postgres — against real containers, not mocks. Requires Docker."""

import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from confluent_kafka import Consumer, Producer
from sqlalchemy import create_engine, text
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer

REPO_ROOT = Path(__file__).resolve().parents[2]
TOPIC = "nimbus.smoke.test.v1"


@pytest.mark.integration
def test_produce_consume_and_write_row() -> None:
    with PostgresContainer("postgres:18.6-alpine") as pg, KafkaContainer() as kafka:
        pg_env = {
            **os.environ,
            "POSTGRES_HOST": pg.get_container_host_ip(),
            "POSTGRES_PORT": str(pg.get_exposed_port(5432)),
            "POSTGRES_DB": pg.dbname,
            "POSTGRES_USER": pg.username,
            "POSTGRES_PASSWORD": pg.password,
        }
        subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=REPO_ROOT,
            env=pg_env,
            check=True,
        )

        db_url = (
            f"postgresql+psycopg://{pg.username}:{pg.password}"
            f"@{pg.get_container_host_ip()}:{pg.get_exposed_port(5432)}/{pg.dbname}"
        )
        engine = create_engine(db_url)

        bootstrap_servers = kafka.get_bootstrap_server()
        producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
            }
        )
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": "smoke-test",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([TOPIC])

        run_id = str(uuid.uuid4())
        producer.produce(TOPIC, key=run_id.encode(), value=run_id.encode())
        producer.flush(10)

        received = None
        for _ in range(20):
            polled = consumer.poll(1.0)
            if polled is not None and polled.error() is None:
                received = polled
                break
        consumer.close()

        assert received is not None, "did not receive the produced message within timeout"
        received_value = received.value()
        assert received_value is not None
        assert received_value.decode() == run_id

        started_at = datetime.now(UTC)
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO ops.ingestion_runs "
                    "(source, ingestion_mode, status, started_at, finished_at, messages_produced) "
                    "VALUES (:source, :mode, :status, :started_at, :finished_at, :count)"
                ),
                {
                    "source": "smoke_test",
                    "mode": "live",
                    "status": "success",
                    "started_at": started_at,
                    "finished_at": datetime.now(UTC),
                    "count": 1,
                },
            )

        with engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT source, messages_produced FROM ops.ingestion_runs "
                    "WHERE source = 'smoke_test'"
                )
            ).one()

        assert row.source == "smoke_test"
        assert row.messages_produced == 1

        engine.dispose()
