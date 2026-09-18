import json
import time
from datetime import date
from unittest.mock import MagicMock

import httpx
import pytest

from nimbus.common.config import Location, ModelsConfig, ModelSpec
from nimbus.ingestion.backfill import backfill_forecasts, backfill_observations, chunk_dates


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
        models=[
            ModelSpec(id="gfs_seamless", name="GFS"),
            ModelSpec(id="icon_seamless", name="ICON"),
        ],
        run_cadence_hours=6,
        run_lookback_steps=8,
        variables=["temperature_2m"],
        forecast_days=7,
        backfill_lead_days=[1, 2],
    )


def _payloads(producer: MagicMock) -> list[dict[str, object]]:
    return [json.loads(call.kwargs["value"]) for call in producer.produce.call_args_list]


def _api_ok(request: httpx.Request) -> httpx.Response:
    n = len(request.url.params["latitude"].split(","))
    body = {"hourly": {"time": ["2024-06-01T00:00"], "temperature_2m_previous_day1": [1.0]}}
    return httpx.Response(200, json=[body] * n if n > 1 else body)


# --- chunk_dates -----------------------------------------------------------


def test_chunk_dates_covers_the_range_without_gaps_or_overlap() -> None:
    chunks = list(chunk_dates(date(2024, 6, 1), date(2024, 6, 20), 7))

    assert chunks == [
        (date(2024, 6, 1), date(2024, 6, 7)),
        (date(2024, 6, 8), date(2024, 6, 14)),
        (date(2024, 6, 15), date(2024, 6, 20)),  # short final chunk
    ]


def test_chunk_dates_single_day_and_empty_range() -> None:
    assert list(chunk_dates(date(2024, 6, 1), date(2024, 6, 1), 7)) == [
        (date(2024, 6, 1), date(2024, 6, 1))
    ]
    assert list(chunk_dates(date(2024, 6, 2), date(2024, 6, 1), 7)) == []


def test_chunk_dates_rejects_a_non_positive_chunk_size() -> None:
    with pytest.raises(ValueError):
        list(chunk_dates(date(2024, 6, 1), date(2024, 6, 2), 0))


# --- forecasts -------------------------------------------------------------


def test_forecast_backfill_produces_one_event_per_location_model_and_chunk() -> None:
    producer = MagicMock()
    sleeps: list[float] = []
    with httpx.Client(transport=httpx.MockTransport(_api_ok)) as client:
        result = backfill_forecasts(
            client,
            producer,
            _locations(),
            _models(),
            date(2024, 6, 1),
            date(2024, 6, 14),
            chunk_days=7,
            sleep=sleeps.append,
        )

    # 2 chunks x 2 models x 2 locations
    assert result.produced == 8
    assert result.failed == 0
    assert result.aborted_at is None
    bodies = _payloads(producer)
    assert {b["event_type"] for b in bodies} == {"forecast.backfill.raw"}
    assert {b["ingestion_mode"] for b in bodies} == {"backfill"}
    assert len(sleeps) == 4  # throttled once per request (chunk x model)


def test_forecast_backfill_event_ids_are_deterministic_so_reruns_are_noops() -> None:
    def run() -> list[str]:
        producer = MagicMock()
        with httpx.Client(transport=httpx.MockTransport(_api_ok)) as client:
            backfill_forecasts(
                client,
                producer,
                _locations(),
                _models(),
                date(2024, 6, 1),
                date(2024, 6, 7),
                sleep=lambda s: None,
            )
        return sorted(str(b["event_id"]) for b in _payloads(producer))

    first, second = run(), run()

    assert first == second
    assert len(set(first)) == len(first)  # and distinct per (model, location, chunk)


def test_forecast_backfill_requests_every_variable_and_lead_offset() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.params["hourly"])
        return _api_ok(request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        backfill_forecasts(
            client,
            MagicMock(),
            _locations(),
            _models(),
            date(2024, 6, 1),
            date(2024, 6, 7),
            sleep=lambda s: None,
        )

    assert seen[0] == "temperature_2m_previous_day1,temperature_2m_previous_day2"


def test_one_failing_chunk_is_counted_and_does_not_stop_the_rest() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(404, text="nope")  # non-retryable failure
        return _api_ok(request)

    producer = MagicMock()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = backfill_forecasts(
            client,
            producer,
            _locations(),
            _models(),
            date(2024, 6, 1),
            date(2024, 6, 7),
            sleep=lambda s: None,
        )

    assert result.failed == 1
    assert result.produced == 2  # the second model's chunk still landed
    assert result.aborted_at is None


def test_rate_limiting_aborts_and_reports_the_date_to_resume_from(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(time, "sleep", lambda s: None)  # skip tenacity's backoff waits

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["start_date"] == "2024-06-01":
            return _api_ok(request)
        return httpx.Response(429, text="Too many requests")

    producer = MagicMock()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = backfill_forecasts(
            client,
            producer,
            _locations(),
            _models(),
            date(2024, 6, 1),
            date(2024, 6, 21),
            chunk_days=7,
            sleep=lambda s: None,
        )

    assert result.aborted_at == date(2024, 6, 8)  # first chunk done, second refused
    assert result.produced == 4  # 2 models x 2 locations for the first chunk only


# --- observations ----------------------------------------------------------

_CSV = (
    "station,valid,tmpc,dwpc,sknt,mslp,alti,metar\n"
    "SFO,2024-06-01 00:56,17.78,10.00,18.00,1010.80,29.85,KSFO 010056Z 28018KT 10SM 18/10 A2985\n"
    "SFO,2024-06-01 01:56,16.11,9.44,14.00,1010.90,29.85,KSFO 010156Z 30014KT 10SM 16/09 A2985\n"
)


def test_observation_backfill_produces_one_event_per_report() -> None:
    windows: list[tuple[str, str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        p = request.url.params
        windows.append((p["station"], p["sts"], p["ets"]))
        return httpx.Response(200, text=_CSV)

    producer = MagicMock()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = backfill_observations(
            client,
            producer,
            _locations(),
            date(2024, 6, 1),
            date(2024, 6, 3),
            sleep=lambda s: None,
        )

    assert result.produced == 4  # 2 reports x 2 stations
    bodies = _payloads(producer)
    assert {b["ingestion_mode"] for b in bodies} == {"backfill"}
    assert {b["source"] for b in bodies} == {"observation_backfill"}
    # inclusive end date 06-03 becomes the exclusive bound 06-04
    assert windows[0] == ("KSFO", "2024-06-01T00:00Z", "2024-06-04T00:00Z")


def test_observation_backfill_failure_is_counted_and_continues() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["station"] == "KSFO":
            return httpx.Response(404, text="nope")
        return httpx.Response(200, text=_CSV)

    producer = MagicMock()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = backfill_observations(
            client,
            producer,
            _locations(),
            date(2024, 6, 1),
            date(2024, 6, 3),
            sleep=lambda s: None,
        )

    assert result.failed == 1
    assert result.produced == 2  # Denver still landed
