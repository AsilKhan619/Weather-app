import json
from unittest.mock import MagicMock

import httpx

from nimbus.common.config import Location
from nimbus.ingestion.observation_producer import produce_one_poll_cycle

_METAR = {
    "icaoId": "KSFO",
    "obsTime": 1789689360,
    "temp": 21.7,
    "dewp": 12.8,
    "wspd": 10,
    "slp": 1015.0,
    "rawOb": "METAR KSFO 172356Z 28010KT 10SM CLR 22/13 A3000 RMK AO2 SLP150",
}


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


def _make_engine() -> tuple[MagicMock, MagicMock]:
    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    return engine, conn


def test_produces_one_event_per_returned_report_and_flags_missing_stations() -> None:
    # Only KSFO reports this cycle - KDEN is temporarily missing (a real,
    # expected condition per the API, not an error).
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_METAR])

    engine, conn = _make_engine()
    producer = MagicMock()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        produced = produce_one_poll_cycle(client, producer, engine, _locations())

    assert produced == 1
    assert producer.produce.call_count == 1
    insert_params = conn.execute.call_args[0][1]
    assert insert_params["produced"] == 1
    assert insert_params["failed"] == 1  # KDEN missing
    assert insert_params["status"] == "failed"


def test_event_keyed_by_station_and_carries_the_raw_report() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_METAR])

    engine, _ = _make_engine()
    producer = MagicMock()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        produce_one_poll_cycle(client, producer, engine, _locations())

    call = producer.produce.call_args
    assert call.kwargs["key"] == b"KSFO"
    body = json.loads(call.kwargs["value"])
    assert body["payload"]["station"] == "KSFO"
    assert body["payload"]["api_response"]["rawOb"] == _METAR["rawOb"]
    assert body["ingestion_mode"] == "live"


def test_a_correction_gets_a_different_event_id_than_the_original() -> None:
    original = dict(_METAR)
    corrected = dict(_METAR, rawOb="METAR KSFO 172356Z COR 28010KT 10SM CLR 22/13 A3000")

    def make_handler(report: dict[str, object]) -> httpx.MockTransport:
        return httpx.MockTransport(lambda request: httpx.Response(200, json=[report]))

    engine, _ = _make_engine()

    producer1 = MagicMock()
    with httpx.Client(transport=make_handler(original)) as client:
        produce_one_poll_cycle(client, producer1, engine, _locations())
    original_id = json.loads(producer1.produce.call_args.kwargs["value"])["event_id"]

    producer2 = MagicMock()
    with httpx.Client(transport=make_handler(corrected)) as client:
        produce_one_poll_cycle(client, producer2, engine, _locations())
    corrected_id = json.loads(producer2.produce.call_args.kwargs["value"])["event_id"]

    assert original_id != corrected_id


def test_identical_report_redelivered_produces_the_same_event_id() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_METAR])

    engine, _ = _make_engine()

    ids = []
    for _ in range(2):
        producer = MagicMock()
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            produce_one_poll_cycle(client, producer, engine, _locations())
        ids.append(json.loads(producer.produce.call_args.kwargs["value"])["event_id"])

    assert ids[0] == ids[1]


def test_fetch_failure_records_a_failed_run_without_raising() -> None:
    # 404, not 500/429 - a non-retryable failure, so this stays fast (no
    # tenacity backoff sleep) while still exercising the "raised, don't
    # crash the whole poll cycle" path.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, text="not found")

    engine, conn = _make_engine()
    producer = MagicMock()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        produced = produce_one_poll_cycle(client, producer, engine, _locations())

    assert produced == 0
    producer.produce.assert_not_called()
    insert_params = conn.execute.call_args[0][1]
    assert insert_params["status"] == "failed"
    assert insert_params["failed"] == len(_locations())
