"""Shared HTTP retry policy for ingestion clients (brief section 6: retries
with exponential backoff and jitter). Only covers genuine transient failures
(network errors, 429, 5xx, truncated bodies) - each client still owns its own
"success but not what we wanted" logic, e.g. Open-Meteo's HTTP 400 for a
not-yet-available run."""

import json
from collections.abc import Callable
from typing import Protocol

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


class RetryDecorator(Protocol):
    def __call__[**P, T](self, func: Callable[P, T], /) -> Callable[P, T]: ...


def http_retry(
    *, attempts: int = 3, multiplier: float = 1.0, max_wait: float = 10.0
) -> RetryDecorator:
    """Exponential backoff with jitter, only for transient errors. The default
    (3 attempts, waits of a few seconds) suits APIs that fail fast; a service
    that sheds load with 503s needs a more patient policy - see `patient_http_retry`."""

    def decorate[**P, T](func: Callable[P, T]) -> Callable[P, T]:
        return retry(
            reraise=True,
            stop=stop_after_attempt(attempts),
            wait=wait_random_exponential(multiplier=multiplier, max=max_wait),
            retry=retry_if_exception(is_retryable_http_error),
        )(func)

    return decorate


with_http_retry: RetryDecorator = http_retry()

# For a free academic service that answers 503 under load (IEM: 6 of 50 station
# requests in the first 120-day run, ADR 0004): 5 attempts, waits growing to ~a minute.
patient_http_retry: RetryDecorator = http_retry(attempts=5, multiplier=2.0, max_wait=60.0)
