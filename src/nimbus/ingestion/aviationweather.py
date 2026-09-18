"""HTTP client for aviationweather.gov's METAR Data API (live observations,
brief section 5). No API key; ~100 req/min limit, so one batched request per
poll for every station comfortably clears it (verified directly)."""

from typing import Any

import httpx

from nimbus.common.http import with_http_retry

METAR_URL = "https://aviationweather.gov/api/data/metar"

# The API docs ask clients to identify themselves via User-Agent.
_HEADERS = {"User-Agent": "nimbus-weather-platform (github.com/AsilKhan619/Weather-app)"}


@with_http_retry
def fetch_current_metars(client: httpx.Client, station_ids: list[str]) -> list[dict[str, Any]]:
    """One batched request for every station id; returns whatever the
    network currently has (a station with no report in range simply won't
    appear in the result - callers should account for that, not treat it as
    an error)."""
    response = client.get(
        METAR_URL,
        params={"ids": ",".join(station_ids), "format": "json"},
        headers=_HEADERS,
    )
    response.raise_for_status()
    result: list[dict[str, Any]] = response.json()
    return result
