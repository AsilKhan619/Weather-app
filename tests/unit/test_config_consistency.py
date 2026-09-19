"""The documented quickstart is `cp .env.example .env && make up`. If the example
file, the code defaults, and docker-compose.yml disagree, a fresh clone breaks in
a way no other test can see (every test builds its own Settings). This pins them
together. It caught `.env.example` pointing Kafka at host port 9092 while compose
publishes 29092."""

from pathlib import Path

import pytest
import yaml

from nimbus.common.settings import Settings

REPO = Path(__file__).resolve().parents[2]
ENV_EXAMPLE = REPO / ".env.example"


@pytest.fixture(autouse=True)
def _clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for field in Settings.model_fields:
        monkeypatch.delenv(field.upper(), raising=False)


def test_env_example_yields_the_same_settings_as_the_code_defaults() -> None:
    from_example = Settings(_env_file=ENV_EXAMPLE).model_dump()
    from_defaults = Settings(_env_file=None).model_dump()

    # the example intentionally leaves the API key blank (LLM stays off)
    assert from_example.pop("anthropic_api_key") in ("", None)
    from_defaults.pop("anthropic_api_key")

    assert from_example == from_defaults


def test_llm_is_off_in_the_example_so_the_platform_runs_without_a_key() -> None:
    assert Settings(_env_file=ENV_EXAMPLE).llm_enabled is False


def _compose_ports(service: str) -> list[str]:
    compose = yaml.safe_load((REPO / "docker-compose.yml").read_text(encoding="utf-8"))
    return [str(p) for p in compose["services"][service]["ports"]]


def test_kafka_bootstrap_port_is_actually_published_by_compose() -> None:
    settings = Settings(_env_file=ENV_EXAMPLE)
    port = settings.kafka_bootstrap_servers.rsplit(":", 1)[1]

    assert f"{port}:{port}" in _compose_ports("kafka")


def test_postgres_port_is_published_by_compose() -> None:
    settings = Settings(_env_file=ENV_EXAMPLE)

    assert f"{settings.postgres_port}:5432" in _compose_ports("postgres")
