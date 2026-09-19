import json
import time
from datetime import date

import httpx
import pytest

from nimbus.common.http import http_retry, is_retryable_http_error
from nimbus.ingestion.iem_asos import fetch_station_history


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [(429, True), (500, True), (503, True), (400, False), (404, False)],
)
def test_is_retryable_for_http_status_errors(status_code: int, expected: bool) -> None:
    response = httpx.Response(status_code, request=httpx.Request("GET", "https://example.com"))
    exc = httpx.HTTPStatusError("boom", request=response.request, response=response)
    assert is_retryable_http_error(exc) is expected


def test_is_retryable_for_transport_error() -> None:
    assert is_retryable_http_error(httpx.ConnectTimeout("timed out")) is True


def test_is_retryable_for_unrelated_exception() -> None:
    assert is_retryable_http_error(ValueError("not a network error")) is False


def test_a_body_truncated_mid_json_is_retryable() -> None:
    # Open-Meteo returned HTTP 200 with a body cut off at ~692 KB on an oversized
    # request; json.loads raises JSONDecodeError. Not a transport error, but worth retrying.
    try:
        json.loads('{"hourly": {"time": ["2024-06-01T00:00", "2024')
    except json.JSONDecodeError as exc:
        assert is_retryable_http_error(exc) is True
    else:  # pragma: no cover
        raise AssertionError("expected a JSONDecodeError")


def _always(status: int, counter: dict[str, int]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        counter["n"] += 1
        return httpx.Response(status, text="busy")

    return httpx.MockTransport(handler)


def test_the_default_policy_gives_up_after_three_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(time, "sleep", lambda s: None)
    calls = {"n": 0}

    @http_retry()
    def fetch(client: httpx.Client) -> None:
        client.get("https://example.test/").raise_for_status()

    with (
        httpx.Client(transport=_always(503, calls)) as client,
        pytest.raises(httpx.HTTPStatusError),
    ):
        fetch(client)

    assert calls["n"] == 3


def test_iem_uses_the_patient_policy_and_survives_a_run_of_503s(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """6 of 50 IEM station requests failed with 503 in the first 120-day run; the
    default 3 attempts gave up too fast for a service shedding load."""
    monkeypatch.setattr(time, "sleep", lambda s: None)
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] <= 4:
            return httpx.Response(503, text="busy")
        return httpx.Response(200, text="station,valid\n")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        text = fetch_station_history(client, "KORD", date(2026, 5, 22), date(2026, 8, 20))

    assert text == "station,valid\n"
    assert calls["n"] == 5  # four 503s, then success on the 5th (and last) attempt


def test_iem_still_gives_up_eventually(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda s: None)
    calls = {"n": 0}

    with (
        httpx.Client(transport=_always(503, calls)) as client,
        pytest.raises(httpx.HTTPStatusError),
    ):
        fetch_station_history(client, "KORD", date(2026, 5, 22), date(2026, 8, 20))

    assert calls["n"] == 5  # bounded, not infinite
