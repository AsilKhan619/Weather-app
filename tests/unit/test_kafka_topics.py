from typing import Any
from unittest.mock import MagicMock

import pytest

from nimbus.common.kafka import TOPIC_SPECS, ensure_topics
from nimbus.common.settings import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings(_env_file=None)


def test_ensure_topics_creates_nothing_when_all_exist(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    fake_admin = MagicMock()
    fake_admin.list_topics.return_value.topics = dict.fromkeys(TOPIC_SPECS, MagicMock())
    monkeypatch.setattr("nimbus.common.kafka.AdminClient", lambda conf: fake_admin)

    ensure_topics(settings)

    fake_admin.create_topics.assert_not_called()


def test_ensure_topics_creates_only_the_missing_ones(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    all_names = list(TOPIC_SPECS)
    missing_name = all_names[-1]
    fake_admin = MagicMock()
    fake_admin.list_topics.return_value.topics = dict.fromkeys(all_names[:-1], MagicMock())
    future = MagicMock()
    fake_admin.create_topics.return_value = {missing_name: future}
    monkeypatch.setattr("nimbus.common.kafka.AdminClient", lambda conf: fake_admin)

    ensure_topics(settings)

    created: list[Any] = fake_admin.create_topics.call_args[0][0]
    assert [topic.topic for topic in created] == [missing_name]
    assert created[0].num_partitions == TOPIC_SPECS[missing_name]["partitions"]
    future.result.assert_called_once()


def test_ensure_topics_uses_configured_partitions_and_retention(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> None:
    fake_admin = MagicMock()
    fake_admin.list_topics.return_value.topics = {}
    fake_admin.create_topics.return_value = {name: MagicMock() for name in TOPIC_SPECS}
    monkeypatch.setattr("nimbus.common.kafka.AdminClient", lambda conf: fake_admin)

    ensure_topics(settings)

    created: list[Any] = fake_admin.create_topics.call_args[0][0]
    by_name = {topic.topic: topic for topic in created}
    for name, spec in TOPIC_SPECS.items():
        assert by_name[name].num_partitions == spec["partitions"]
