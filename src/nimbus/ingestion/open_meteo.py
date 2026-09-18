"""HTTP client for Open-Meteo's Single Runs API (live forecasts, brief section
5 / ADR 0001). No API key, batches every location into one request per model
per confirmed run, and never guesses at run availability — it asks the API."""

from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx

from nimbus.common.http import with_http_retry

SINGLE_RUNS_URL = "https://single-runs-api.open-meteo.com/v1/forecast"
PREVIOUS_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"


class RunNotAvailableError(Exception):
    """No run within the configured lookback window is available from Open-Meteo."""


@with_http_retry
def _get(client: httpx.Client, params: dict[str, Any], url: str = SINGLE_RUNS_URL) -> Any:
    response = client.get(url, params=params)
    # Open-Meteo uses HTTP 400 for "this run isn't available" - a normal,
    # expected outcome here, not a request bug - so don't raise for it.
    if response.status_code not in (200, 400):
        response.raise_for_status()
    return response.json()


def _round_down_to_cadence(moment: datetime, cadence_hours: int) -> datetime:
    hour = (moment.hour // cadence_hours) * cadence_hours
    return moment.replace(hour=hour, minute=0, second=0, microsecond=0)


def find_latest_available_run(
    client: httpx.Client,
    model: str,
    probe_latitude: float,
    probe_longitude: float,
    *,
    cadence_hours: int = 6,
    lookback_steps: int = 8,
    now: datetime | None = None,
) -> datetime:
    """Scan backwards from now in `cadence_hours` steps for the newest run
    Open-Meteo will actually serve. There's no "give me the latest" mode and
    no metadata field naming the served run (verified directly, ADR 0001), so
    the API's own accept/reject response is the only signal available."""
    current = now or datetime.now(UTC)
    candidate = _round_down_to_cadence(current, cadence_hours)

    for _ in range(lookback_steps):
        payload = _get(
            client,
            {
                "latitude": probe_latitude,
                "longitude": probe_longitude,
                "models": model,
                "hourly": "temperature_2m",
                "forecast_days": 1,
                "run": candidate.strftime("%Y-%m-%dT%H:%M"),
            },
        )
        if not payload.get("error"):
            return candidate
        candidate -= timedelta(hours=cadence_hours)

    raise RunNotAvailableError(
        f"No available run found for model={model!r} within "
        f"{lookback_steps * cadence_hours}h of {current.isoformat()}"
    )


def fetch_forecast_run(
    client: httpx.Client,
    model: str,
    run: datetime,
    locations: list[tuple[float, float]],
    variables: list[str],
    forecast_days: int,
) -> list[dict[str, Any]]:
    """Fetch one already-confirmed run for every location in one batched call
    (Open-Meteo returns a JSON array, one object per location, in input order)."""
    result = _get(
        client,
        {
            "latitude": ",".join(str(lat) for lat, _ in locations),
            "longitude": ",".join(str(lon) for _, lon in locations),
            "models": model,
            "hourly": ",".join(variables),
            "forecast_days": forecast_days,
            "run": run.strftime("%Y-%m-%dT%H:%M"),
            "timeformat": "iso8601",
            "timezone": "UTC",
        },
    )
    result_list: list[dict[str, Any]] = result if isinstance(result, list) else [result]
    return result_list


def fetch_previous_runs(
    client: httpx.Client,
    model: str,
    locations: list[tuple[float, float]],
    variables: list[str],
    lead_days: list[int],
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """Backfill: fixed lead-time offsets (`<var>_previous_dayN`) for a date range,
    every location in one batched call. Unlike the Single Runs API a 400 here is
    a genuine request error, never "not available yet", so it raises."""
    hourly = [f"{var}_previous_day{n}" for var in variables for n in lead_days]
    result = _get(
        client,
        {
            "latitude": ",".join(str(lat) for lat, _ in locations),
            "longitude": ",".join(str(lon) for _, lon in locations),
            "models": model,
            "hourly": ",".join(hourly),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "timeformat": "iso8601",
            "timezone": "UTC",
        },
        PREVIOUS_RUNS_URL,
    )
    if isinstance(result, dict) and result.get("error"):
        raise ValueError(f"Previous Runs API error: {result.get('reason')}")
    result_list: list[dict[str, Any]] = result if isinstance(result, list) else [result]
    return result_list
