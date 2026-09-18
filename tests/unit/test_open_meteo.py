from collections.abc import Callable
from datetime import UTC, datetime

import httpx
import pytest

from nimbus.ingestion.open_meteo import (
    RunNotAvailableError,
    fetch_forecast_run,
    find_latest_available_run,
)

NOW = datetime(2026, 9, 17, 21, 4, tzinfo=UTC)


def _not_available_response(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        400, json={"error": True, "reason": "The requested model run is not available."}
    )


def _available_response(
    payload: dict[str, object] | None = None,
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json=payload or {"latitude": 37.6, "longitude": -122.4, "hourly": {}}
        )

    return handler


def test_find_latest_available_run_returns_first_working_candidate() -> None:
    transport = httpx.MockTransport(_available_response())
    with httpx.Client(transport=transport) as client:
        run = find_latest_available_run(client, "gfs_seamless", 37.6213, -122.3790, now=NOW)

    assert run == datetime(2026, 9, 17, 18, 0, tzinfo=UTC)


def test_find_latest_available_run_scans_backwards_past_unavailable_runs() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        run_param = request.url.params["run"]
        calls.append(run_param)
        # Only the run two cadence-steps back is available.
        if run_param == "2026-09-17T06:00":
            return httpx.Response(200, json={"hourly": {}})
        return httpx.Response(400, json={"error": True, "reason": "not available"})

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as client:
        run = find_latest_available_run(client, "icon_seamless", 37.6213, -122.3790, now=NOW)

    assert run == datetime(2026, 9, 17, 6, 0, tzinfo=UTC)
    assert calls == ["2026-09-17T18:00", "2026-09-17T12:00", "2026-09-17T06:00"]


def test_find_latest_available_run_raises_after_exhausting_lookback() -> None:
    transport = httpx.MockTransport(_not_available_response)
    with httpx.Client(transport=transport) as client, pytest.raises(RunNotAvailableError):
        find_latest_available_run(
            client, "gfs_seamless", 37.6213, -122.3790, lookback_steps=3, now=NOW
        )


def test_fetch_forecast_run_batches_locations_in_one_request() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(request.url.params)
        return httpx.Response(200, json=[{"latitude": 37.6}, {"latitude": 39.9}])

    transport = httpx.MockTransport(handler)
    run = datetime(2026, 9, 17, 12, 0, tzinfo=UTC)
    with httpx.Client(transport=transport) as client:
        result = fetch_forecast_run(
            client,
            "gfs_seamless",
            run,
            [(37.6213, -122.3790), (39.8561, -104.6737)],
            ["temperature_2m", "wind_speed_10m"],
            forecast_days=7,
        )

    assert len(result) == 2
    assert captured["latitude"] == "37.6213,39.8561"
    assert captured["longitude"] == "-122.379,-104.6737"
    assert captured["run"] == "2026-09-17T12:00"
    assert captured["hourly"] == "temperature_2m,wind_speed_10m"


def test_fetch_forecast_run_wraps_single_location_dict_in_list() -> None:
    transport = httpx.MockTransport(_available_response({"latitude": 37.6}))
    run = datetime(2026, 9, 17, 12, 0, tzinfo=UTC)
    with httpx.Client(transport=transport) as client:
        result = fetch_forecast_run(
            client, "gfs_seamless", run, [(37.6213, -122.3790)], ["temperature_2m"], forecast_days=7
        )

    assert result == [{"latitude": 37.6}]
