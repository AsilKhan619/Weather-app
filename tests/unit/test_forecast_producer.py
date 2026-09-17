import json
from unittest.mock import MagicMock

import httpx

from nimbus.common.config import Location, ModelsConfig, ModelSpec
from nimbus.ingestion.forecast_producer import produce_one_poll_cycle

_HOURLY = {"hourly": {"time": ["2026-01-01T00:00"], "temperature_2m": [20.0]}}


def _locations() -> list[Location]:
    return [
        Location(
            id="san-francisco",
            name="SF",
            climate="coastal",
            latitude=37.6213,
            longitude=-122.379,
            elevation_m=4,
            timezone="America/Los_Angeles",
            station="KSFO",
        ),
        Location(
            id="denver",
            name="Denver",
            climate="mountain",
            latitude=39.8561,
            longitude=-104.6737,
            elevation_m=1655,
            timezone="America/Denver",
            station="KDEN",
        ),
    ]


def _models() -> ModelsConfig:
    return ModelsConfig(
        models=[ModelSpec(id="gfs_seamless", name="NOAA GFS")],
        run_cadence_hours=6,
        run_lookback_steps=8,
        variables=["temperature_2m"],
        forecast_days=1,
    )


def _always_available(request: httpx.Request) -> httpx.Response:
    n_locations = len(request.url.params["latitude"].split(","))
    if n_locations == 1:
        return httpx.Response(200, json=_HOURLY)
    return httpx.Response(200, json=[_HOURLY for _ in range(n_locations)])


def _always_unavailable(request: httpx.Request) -> httpx.Response:
    return httpx.Response(400, json={"error": True, "reason": "not available"})


def _make_engine() -> tuple[MagicMock, MagicMock]:
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    return engine, conn


def test_produces_one_event_per_location() -> None:
    engine, conn = _make_engine()
    producer = MagicMock()
    with httpx.Client(transport=httpx.MockTransport(_always_available)) as client:
        produced = produce_one_poll_cycle(client, producer, engine, _locations(), _models())

    assert produced == 2
    assert producer.produce.call_count == 2
    conn.execute.assert_called_once()
    insert_params = conn.execute.call_args[0][1]
    assert insert_params["status"] == "success"
    assert insert_params["produced"] == 2
    assert insert_params["failed"] == 0


def test_produced_events_are_keyed_by_location_and_carry_the_model() -> None:
    engine, _ = _make_engine()
    producer = MagicMock()
    with httpx.Client(transport=httpx.MockTransport(_always_available)) as client:
        produce_one_poll_cycle(client, producer, engine, _locations(), _models())

    keys = {call.kwargs["key"] for call in producer.produce.call_args_list}
    assert keys == {b"san-francisco", b"denver"}

    bodies = [json.loads(call.kwargs["value"]) for call in producer.produce.call_args_list]
    for body in bodies:
        assert body["payload"]["model"] == "gfs_seamless"
        assert body["ingestion_mode"] == "live"


def test_same_run_produces_identical_event_ids_across_polls() -> None:
    engine, _ = _make_engine()

    def one_cycle() -> list[str]:
        producer = MagicMock()
        with httpx.Client(transport=httpx.MockTransport(_always_available)) as client:
            produce_one_poll_cycle(client, producer, engine, _locations(), _models())
        return [
            json.loads(call.kwargs["value"])["event_id"] for call in producer.produce.call_args_list
        ]

    first_ids = sorted(one_cycle())
    second_ids = sorted(one_cycle())
    assert first_ids == second_ids


def test_model_with_no_available_run_is_skipped_not_fatal() -> None:
    engine, conn = _make_engine()
    producer = MagicMock()
    with httpx.Client(transport=httpx.MockTransport(_always_unavailable)) as client:
        produced = produce_one_poll_cycle(client, producer, engine, _locations(), _models())

    assert produced == 0
    producer.produce.assert_not_called()
    insert_params = conn.execute.call_args[0][1]
    assert insert_params["status"] == "failed"
    assert insert_params["failed"] == 1
