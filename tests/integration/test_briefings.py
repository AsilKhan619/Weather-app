"""Phase 5 acceptance against a real Postgres, with the deterministic fake LLM client (tests
never call the real API): briefings pass schema validation and are stored; a cache hit skips
the API; an invented number is caught, stored flagged and never published; invalid output is
retried exactly once; and a disabled or unavailable LLM leaves the pipeline running."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest
from sqlalchemy import Engine, text

from nimbus.common.config import load_llm_config, load_locations
from nimbus.common.db import chunked_upsert, make_engine
from nimbus.common.settings import Settings
from nimbus.common.tables import accuracy_daily_table
from nimbus.jobs.load_dimensions import load_dimensions
from nimbus.llm.briefings import BRIEFING_TOPIC, generate_briefing
from nimbus.llm.client import FakeBriefingClient
from nimbus.streaming.forecast_silver import upsert_forecast_rows

pytestmark = pytest.mark.integration

CONFIG = load_llm_config()
PLACE = load_locations()[0]
AS_OF = datetime(2026, 9, 20, 12, tzinfo=UTC)


@pytest.fixture
def engine(pg_settings: Settings) -> Iterator[Engine]:
    eng = make_engine(pg_settings)
    _clean(eng)
    load_dimensions(eng)
    _seed(eng)
    yield eng
    _clean(eng)
    eng.dispose()


def _clean(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE silver.forecast, gold.accuracy_daily, gold.alert, gold.briefing, "
                "ops.llm_calls"
            )
        )


def _run(model: str, init: datetime, temp_k: float, hours: int = 72) -> pd.DataFrame:
    valid = [init + timedelta(hours=h) for h in range(1, hours + 1)]
    return pd.DataFrame(
        {
            "model": model,
            "location_id": PLACE.id,
            "init_time": init,
            "valid_time": valid,
            "variable": "temperature_2m",
            "value": temp_k,
            "lead_hours": list(range(1, hours + 1)),
            "ingestion_mode": "live",
            "source_event_id": f"{model}-{init:%H}",
        }
    )


def _seed(engine: Engine) -> None:
    upsert_forecast_rows(engine, _run("ecmwf_ifs025", AS_OF - timedelta(hours=6), 290.0))
    upsert_forecast_rows(engine, _run("gfs_seamless", AS_OF - timedelta(hours=6), 291.0))
    # A run issued *after* as_of must not leak into the fact sheet.
    upsert_forecast_rows(engine, _run("gfs_seamless", AS_OF + timedelta(hours=6), 350.0))
    chunked_upsert(
        engine,
        accuracy_daily_table,
        ["valid_date", "location_id", "model", "variable", "lead_day"],
        ["n", "bias", "mae", "rmse"],
        [
            {"valid_date": (AS_OF - timedelta(days=d)).date(), "location_id": PLACE.id,
             "model": model, "variable": "temperature_2m", "lead_day": 1, "n": 24,
             "bias": 0.1, "mae": mae, "rmse": mae + 0.3}
            for d in range(1, 11)
            for model, mae in (("ecmwf_ifs025", 1.2), ("gfs_seamless", 1.6))
        ],
    )  # fmt: skip


def _calls(engine: Engine) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM ops.llm_calls ORDER BY id")).mappings()
        return [dict(r) for r in rows]


def _briefings(engine: Engine) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM gold.briefing")).mappings()
        return [dict(r) for r in rows]


def _producer() -> MagicMock:
    producer = MagicMock()
    producer.flush.return_value = 0  # everything delivered
    return producer


def _generate(engine: Engine, client: FakeBriefingClient | None, **kw: Any) -> Any:
    return generate_briefing(
        engine, PLACE, kw.pop("as_of", AS_OF), client=client, config=CONFIG,
        producer=kw.pop("producer", None), model="fake-briefing-model", **kw,
    )  # fmt: skip


def test_a_grounded_briefing_is_stored_and_published(engine: Engine) -> None:
    producer = _producer()
    result = _generate(engine, FakeBriefingClient(), producer=producer)

    assert result.status == "published"
    (briefing,) = _briefings(engine)
    assert briefing["grounding_passed"] and briefing["published_at"] is not None
    assert briefing["confidence"] == "high"  # models 1 K apart
    assert briefing["most_reliable_model"] == "ecmwf_ifs025"  # the lower MAE
    sheet = briefing["fact_sheet"]
    assert sheet["forecast_by_model"]["gfs_seamless"]["temperature_2m"]["max"] == 17.9  # not 350 K
    assert sheet["accuracy"]["by_model"]["ecmwf_ifs025"]["verified_forecasts"] == 240

    (topic, key, value) = (
        producer.produce.call_args.kwargs["topic"],
        producer.produce.call_args.kwargs["key"],
        producer.produce.call_args.kwargs["value"],
    )
    assert topic == BRIEFING_TOPIC and key == PLACE.id.encode()
    assert b'"briefing.generated"' in value and briefing["briefing_id"].encode() in value
    (call,) = _calls(engine)
    assert (call["outcome"], call["attempt"], call["prompt_version"]) == (
        "success",
        1,
        "briefing_v1",
    )
    assert call["input_tokens"] > 0 and call["output_tokens"] == 120


def test_a_cache_hit_skips_the_api(engine: Engine) -> None:
    """Brief section 15, Phase 5 acceptance: identical inputs never trigger a paid call."""
    client, producer = FakeBriefingClient(), _producer()
    first = _generate(engine, client, producer=producer)
    second = _generate(engine, client, producer=producer, trigger="alert", trigger_ref="a1")

    assert (first.status, second.status) == ("published", "cached")
    assert first.briefing_id == second.briefing_id
    assert len(client.calls) == 1  # the second request never reached the client
    assert producer.produce.call_count == 1  # and was not published twice
    assert [c["outcome"] for c in _calls(engine)] == ["success", "cache_hit"]
    assert _calls(engine)[1]["cost_usd"] == 0.0


def test_new_facts_mean_a_new_briefing(engine: Engine) -> None:
    client = FakeBriefingClient()
    _generate(engine, client)
    _generate(engine, client, as_of=AS_OF + timedelta(hours=1))  # a different window

    assert len(client.calls) == 2 and len(_briefings(engine)) == 2


def test_an_invented_number_is_stored_flagged_and_never_published(engine: Engine) -> None:
    producer = _producer()
    result = _generate(engine, FakeBriefingClient(invent_number=True), producer=producer)

    assert result.status == "flagged"
    assert result.failures == ("summary: 97 is not in the fact sheet",)
    (briefing,) = _briefings(engine)
    assert not briefing["grounding_passed"] and briefing["published_at"] is None
    assert briefing["grounding_failures"] == ["summary: 97 is not in the fact sheet"]
    producer.produce.assert_not_called()
    (call,) = _calls(engine)
    assert call["outcome"] == "grounding_failed" and "97" in call["error"]

    # Asking again is a cache hit on the flagged briefing: still not published, not re-paid.
    again = _generate(engine, FakeBriefingClient(invent_number=True), producer=producer)
    assert again.status == "cached" and again.failures == result.failures
    producer.produce.assert_not_called()


def test_invalid_output_is_retried_exactly_once(engine: Engine) -> None:
    recovers = FakeBriefingClient(invalid_attempts=1)
    assert _generate(engine, recovers).status == "stored"
    assert len(recovers.calls) == 2
    assert [(c["outcome"], c["attempt"]) for c in _calls(engine)] == [
        ("invalid_output", 1),
        ("success", 2),
    ]


def test_output_invalid_twice_stores_nothing(engine: Engine) -> None:
    stubborn = FakeBriefingClient(invalid_attempts=5)
    assert _generate(engine, stubborn).status == "invalid"
    assert len(stubborn.calls) == 2  # never a third, paid attempt
    assert _briefings(engine) == []
    assert [c["outcome"] for c in _calls(engine)] == ["invalid_output", "invalid_output"]


def test_an_unavailable_llm_is_logged_and_the_run_carries_on(engine: Engine) -> None:
    result = _generate(engine, FakeBriefingClient(unavailable=True))

    assert result.status == "unavailable"
    assert _briefings(engine) == []
    (call,) = _calls(engine)
    assert call["outcome"] == "error" and "timeout" in call["error"]


def test_with_the_llm_disabled_facts_are_built_and_the_skip_is_logged(engine: Engine) -> None:
    """Brief section 15: the platform runs with LLM_ENABLED=false."""
    result = _generate(engine, None)

    assert result.status == "disabled"
    assert _briefings(engine) == []
    (call,) = _calls(engine)
    assert call["outcome"] == "disabled" and call["fact_sheet_hash"] is not None


def test_a_disabled_llm_still_serves_a_cached_briefing(engine: Engine) -> None:
    _generate(engine, FakeBriefingClient())
    assert _generate(engine, None).status == "cached"


def test_no_forecasts_means_no_briefing_and_no_call(engine: Engine) -> None:
    result = _generate(engine, FakeBriefingClient(), as_of=AS_OF + timedelta(days=30))
    assert result.status == "no_data"
    assert _calls(engine) == []


# --- briefings on alerts ------------------------------------------------------------------


class _Msg:
    def __init__(self, value: bytes | None) -> None:
        self._value = value

    def value(self) -> bytes | None:
        return self._value

    def topic(self) -> str:
        return "weather.alert.v1"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 0

    def timestamp(self) -> tuple[int, int]:
        return (1, 0)

    def key(self) -> bytes | None:
        return None


def _alert_event(alert_id: str, location_id: str, detected_hour: int) -> bytes:
    from nimbus.common.events import EventEnvelope
    from nimbus.common.schemas import AlertPayload

    payload = AlertPayload(
        alert_id=alert_id, rule="model_spread", severity="warning", location_id=location_id,
        variable="temperature_2m", event_time=AS_OF, metric=4.2, threshold=4.0,
        details={}, triggered_by_event_id="evt", detected_at=AS_OF.replace(hour=detected_hour),
    )  # fmt: skip
    envelope = EventEnvelope[AlertPayload](
        event_id=alert_id, source="anomaly_detector", event_type="alert.anomaly",
        produced_at=AS_OF, ingestion_mode="live", payload=payload,
    )  # fmt: skip
    return envelope.model_dump_json().encode()


def test_a_burst_of_alerts_for_one_location_makes_one_briefing(engine: Engine) -> None:
    from nimbus.llm.alert_briefings import process_batch

    client = FakeBriefingClient()
    batch = [
        _Msg(_alert_event("a1", PLACE.id, 10)),
        _Msg(_alert_event("a2", PLACE.id, 11)),  # the latest one names the trigger
        _Msg(b"not json"),
        _Msg(_alert_event("a3", "not-a-configured-location", 11)),
    ]

    results = process_batch(
        batch, engine, {PLACE.id: PLACE}, client=client, config=CONFIG, producer=None,
        model="fake-briefing-model", as_of=AS_OF,
    )  # fmt: skip

    assert [r.status for r in results] == ["stored"]
    assert len(client.calls) == 1
    (briefing,) = _briefings(engine)
    assert (briefing["trigger"], briefing["trigger_ref"]) == ("alert", "a2")
