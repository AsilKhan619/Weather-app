"""Shared Testcontainers stack (real Kafka + Postgres, migrated and with
topics provisioned) for every integration test - brief section 14: tests
never call live APIs or the real LLM, but they do run against real infra."""

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer

from nimbus.common.kafka import ensure_topics
from nimbus.common.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def stack() -> Iterator[tuple[Settings, KafkaContainer, PostgresContainer]]:
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
        settings = Settings(
            _env_file=None,
            postgres_host=pg.get_container_host_ip(),
            postgres_port=int(pg.get_exposed_port(5432)),
            postgres_db=pg.dbname,
            postgres_user=pg.username,
            postgres_password=pg.password,
            kafka_bootstrap_servers=kafka.get_bootstrap_server(),
        )
        ensure_topics(settings)
        yield settings, kafka, pg
