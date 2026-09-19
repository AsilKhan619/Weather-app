"""Shared HTTP retry policy for ingestion clients (brief section 6: retries
with exponential backoff and jitter). Only covers genuine transient failures
(network errors, 429, 5xx) - each client still owns its own "success but not
what we wanted" logic, e.g. Open-Meteo's HTTP 400 for a not-yet-available run."""

import json
from collections.abc import Callable

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_random_exponential


def is_retryable_http_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TransportError):
        return True
    # HTTP 200 with a body cut off mid-JSON: seen from Open-Meteo on oversized
    # responses (ADR 0004). The connection "succeeded", so it isn't a transport
    # error, but the request is worth retrying.
    if isinstance(exc, json.JSONDecodeError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return False


def with_http_retry[**P, T](func: Callable[P, T]) -> Callable[P, T]:
    """3 attempts, exponential backoff with jitter, only for transient errors."""
    return retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=1, max=10),
        retry=retry_if_exception(is_retryable_http_error),
    )(func)
