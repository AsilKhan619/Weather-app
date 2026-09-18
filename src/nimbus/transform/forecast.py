"""Pure, unit-tested transform: one raw Open-Meteo payload -> tidy SI-unit
rows (brief section 7). No `iterrows`, explicit dtypes, categoricals for
low-cardinality columns."""

import re
from collections.abc import Callable

import pandas as pd

from nimbus.common.events import IngestionMode
from nimbus.common.schemas import ForecastBackfillRawPayload, ForecastRawPayload

# Open-Meteo does not return SI units (verified live, ADR 0001): temperature/
# dew point in Celsius, wind speed in km/h, pressure in hPa.
_UNIT_CONVERSIONS: dict[str, Callable[[pd.Series], pd.Series]] = {
    "temperature_2m": lambda s: s + 273.15,  # degC -> K
    "dew_point_2m": lambda s: s + 273.15,  # degC -> K
    "wind_speed_10m": lambda s: s / 3.6,  # km/h -> m/s
    "pressure_msl": lambda s: s * 100.0,  # hPa -> Pa
}

FORECAST_COLUMNS = [
    "model",
    "location_id",
    "init_time",
    "valid_time",
    "variable",
    "value",
    "lead_hours",
    "ingestion_mode",
]


def explode_forecast_payload(
    payload: ForecastRawPayload, ingestion_mode: IngestionMode
) -> pd.DataFrame:
    """One row per (variable, valid_time), SI units, with lead_hours."""
    hourly = payload.api_response["hourly"]
    times = pd.to_datetime(list(hourly["time"]), utc=True)
    variable_columns = [column for column in hourly if column != "time"]

    wide = pd.DataFrame({"valid_time": times})
    for column in variable_columns:
        values = pd.Series(hourly[column], dtype="float64")
        convert = _UNIT_CONVERSIONS.get(column)
        wide[column] = convert(values) if convert else values

    tidy = wide.melt(id_vars="valid_time", var_name="variable", value_name="value")
    tidy["model"] = payload.model
    tidy["location_id"] = payload.location_id
    tidy["init_time"] = pd.Timestamp(payload.run)
    tidy["ingestion_mode"] = ingestion_mode
    tidy["lead_hours"] = (
        (tidy["valid_time"] - tidy["init_time"]).dt.total_seconds() // 3600
    ).astype("int32")

    tidy = tidy.astype(
        {
            "model": "category",
            "location_id": "category",
            "variable": "category",
            "ingestion_mode": "category",
        }
    )
    return tidy[FORECAST_COLUMNS]


_PREVIOUS_DAY_COLUMN = re.compile(r"^(?P<variable>.+)_previous_day(?P<lead_days>\d+)$")


def explode_previous_runs_payload(payload: ForecastBackfillRawPayload) -> pd.DataFrame:
    """Backfill counterpart of `explode_forecast_payload`, emitting the *same*
    columns so live and backfilled rows land in one silver table (brief section
    5). The Previous Runs API gives lead offsets (`temperature_2m_previous_day3`
    = the forecast made ~3 days before each valid hour), not exact run times, so
    init_time is *derived*: valid_time minus N days, lead_hours = N * 24. The
    `ingestion_mode='backfill'` flag marks those rows as derived (ADR 0004)."""
    hourly = payload.api_response["hourly"]
    times = pd.to_datetime(list(hourly["time"]), utc=True)

    frames: list[pd.DataFrame] = []
    for column, raw_values in hourly.items():
        match = _PREVIOUS_DAY_COLUMN.match(column)
        if match is None:
            continue
        variable = match["variable"]
        lead_days = int(match["lead_days"])
        values = pd.Series(raw_values, dtype="float64")
        convert = _UNIT_CONVERSIONS.get(variable)
        frames.append(
            pd.DataFrame(
                {
                    "valid_time": times,
                    "variable": variable,
                    "value": convert(values) if convert else values,
                    "lead_hours": lead_days * 24,
                }
            )
        )

    if not frames:
        raise KeyError("no *_previous_dayN columns in payload")

    tidy = pd.concat(frames, ignore_index=True)
    lead_offset = pd.to_timedelta(tidy["lead_hours"], unit="h")
    tidy["init_time"] = tidy["valid_time"] - lead_offset
    tidy["model"] = payload.model
    tidy["location_id"] = payload.location_id
    tidy["ingestion_mode"] = "backfill"
    tidy["lead_hours"] = tidy["lead_hours"].astype("int32")

    tidy = tidy.astype(
        {
            "model": "category",
            "location_id": "category",
            "variable": "category",
            "ingestion_mode": "category",
        }
    )
    return tidy[FORECAST_COLUMNS]
