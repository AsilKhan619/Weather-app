"""Pure, unit-tested transform: one raw METAR payload -> tidy SI-unit rows
(brief section 7). No `iterrows`, explicit dtypes, categoricals. Handles the
messiness the brief calls out (section 5): missing fields, variable winds,
corrected reports, and stations that don't report sea-level pressure."""

import re

import pandas as pd

from nimbus.common.events import IngestionMode
from nimbus.common.schemas import ObservationRawPayload

# "COR" is a standalone token in the METAR body (e.g. "KDEN 172353Z COR
# 28007KT ..."), never a substring of another group - word-boundary match.
_CORRECTED_PATTERN = re.compile(r"\bCOR\b")

OBSERVATION_COLUMNS = [
    "station",
    "observed_at",
    "variable",
    "value",
    "raw_text",
    "is_corrected",
    "ingestion_mode",
]


def _is_corrected(raw_text: str) -> bool:
    return bool(_CORRECTED_PATTERN.search(raw_text))


def explode_observation_payload(
    payload: ObservationRawPayload, ingestion_mode: IngestionMode
) -> pd.DataFrame:
    """One row per variable. temp/dewp/slp/altim already arrive in degC/hPa
    (verified against the live API - see ADR 0003); only wind speed (knots)
    needs converting. Wind *direction* being "VRB" (variable) never affects
    this - direction isn't one of the 4 stored variables, only speed is."""
    report = payload.api_response
    raw_text = str(report.get("rawOb") or "")

    # mean sea-level pressure: prefer the true SLP reduction: fall back to
    # the altimeter setting (already hPa in this API) when a station doesn't
    # report SLP in its remarks - common at smaller airports.
    pressure = report.get("slp")
    if pressure is None:
        pressure = report.get("altim")

    raw_values: dict[str, float | None] = {
        "temperature_2m": report.get("temp"),
        "dew_point_2m": report.get("dewp"),
        "wind_speed_10m": report.get("wspd"),
        "pressure_msl": pressure,
    }

    tidy = pd.DataFrame(
        {
            "variable": list(raw_values.keys()),
            "value": pd.Series(list(raw_values.values()), dtype="float64"),
        }
    )
    is_temp_like = tidy["variable"].isin(["temperature_2m", "dew_point_2m"])
    tidy.loc[is_temp_like, "value"] = tidy.loc[is_temp_like, "value"] + 273.15  # degC -> K
    is_wind = tidy["variable"] == "wind_speed_10m"
    tidy.loc[is_wind, "value"] = tidy.loc[is_wind, "value"] * 0.514444  # kt -> m/s

    tidy["station"] = payload.station
    tidy["observed_at"] = pd.Timestamp(payload.observed_at)
    tidy["raw_text"] = raw_text
    tidy["is_corrected"] = _is_corrected(raw_text)
    tidy["ingestion_mode"] = ingestion_mode

    tidy = tidy.astype(
        {"station": "category", "variable": "category", "ingestion_mode": "category"}
    )
    return tidy[OBSERVATION_COLUMNS]
