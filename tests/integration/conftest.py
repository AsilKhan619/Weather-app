"""Shared Testcontainers stack (real Kafka + Postgres, migrated and with
topics provisioned) for every integration test - brief section 14: tests
never call live APIs or the real LLM, but they do run against real infra."""

import os
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from testcontainers.community.kafka import KafkaContainer
from testcontainers.community.postgres import PostgresContainer

from nimbus.common.kafka import ensure_topics
from nimbus.common.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[2]

Stack = tuple[Settings, KafkaContainer, PostgresContainer]


@contextmanager
def _running_stack() -> Iterator[Stack]:
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


@pytest.fixture
def stack() -> Iterator[Stack]:
    """A fresh stack per test - full isolation."""
    with _running_stack() as running:
        yield running


@pytest.fixture(scope="module")
def module_stack() -> Iterator[Stack]:
    """One stack shared by a whole module, for scenarios that are expensive to
    load. Tests using it must leave the data in a consistent state."""
    with _running_stack() as running:
        yield running


@pytest.fixture(scope="module")
def pg_settings() -> Iterator[Settings]:
    """Postgres only (migrated) - for tests of SQL-level behaviour that never touch
    Kafka, which is most of the startup cost. Tests share it and clean up after
    themselves.

    Without Docker, set NIMBUS_TEST_POSTGRES=host:port to use an already-running,
    disposable Postgres (user `postgres`, database `postgres`, no password) instead of a
    container; it is migrated to head and the tests truncate what they touch."""
    external = os.environ.get("NIMBUS_TEST_POSTGRES")
    if external:
        host, port = external.split(":")
        settings = Settings(
            _env_file=None,
            postgres_host=host,
            postgres_port=int(port),
            postgres_db="postgres",
            postgres_user="postgres",
            postgres_password="x",
        )
        env = {
            **os.environ,
            "POSTGRES_HOST": host,
            "POSTGRES_PORT": port,
            "POSTGRES_DB": "postgres",
            "POSTGRES_USER": "postgres",
            "POSTGRES_PASSWORD": "x",
        }
        subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"], cwd=REPO_ROOT, env=env, check=True
        )
        yield settings
        return
    with PostgresContainer("postgres:18.6-alpine") as pg:
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
        yield Settings(
            _env_file=None,
            postgres_host=pg.get_container_host_ip(),
            postgres_port=int(pg.get_exposed_port(5432)),
            postgres_db=pg.dbname,
            postgres_user=pg.username,
            postgres_password=pg.password,
        )
