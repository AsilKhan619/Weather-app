import json

import httpx
import pytest

from nimbus.common.http import is_retryable_http_error


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
