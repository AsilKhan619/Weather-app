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
