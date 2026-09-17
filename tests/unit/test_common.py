import json
import logging

from nimbus.common.events import compute_event_id
from nimbus.common.logging import JsonFormatter, event_id_ctx
from nimbus.common.settings import Settings


def test_settings_default_to_llm_disabled() -> None:
    settings = Settings(_env_file=None)
    assert settings.llm_enabled is False
    assert settings.nimbus_agent_model == "claude-sonnet-5"
    assert settings.nimbus_briefing_model == "claude-haiku-4-5-20251001"


def test_settings_postgres_dsn_uses_psycopg_driver() -> None:
    settings = Settings(_env_file=None)
    assert settings.postgres_dsn.startswith("postgresql+psycopg://")
    assert settings.postgres_readonly_dsn != settings.postgres_dsn


def test_compute_event_id_is_deterministic_per_natural_key() -> None:
    first = compute_event_id("gfs_seamless", "boston", "2026-09-17T00:00:00")
    second = compute_event_id("gfs_seamless", "boston", "2026-09-17T00:00:00")
    different_run = compute_event_id("gfs_seamless", "boston", "2026-09-17T06:00:00")

    assert first == second
    assert first != different_run


def test_json_formatter_emits_valid_json_with_event_id() -> None:
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="nimbus.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )

    token = event_id_ctx.set("abc123")
    try:
        formatted = formatter.format(record)
    finally:
        event_id_ctx.reset(token)

    payload = json.loads(formatted)
    assert payload["message"] == "hello"
    assert payload["event_id"] == "abc123"
    assert payload["level"] == "INFO"


def test_json_formatter_omits_event_id_when_unset() -> None:
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="nimbus.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="no event id here",
        args=(),
        exc_info=None,
    )

    payload = json.loads(formatter.format(record))
    assert "event_id" not in payload
