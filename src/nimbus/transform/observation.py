"""Pure, unit-tested transform: raw METAR payloads -> tidy SI-unit rows (brief
section 7). Handles the messiness the brief calls out (section 5): missing
fields, variable winds, corrected reports, and stations that don't report
sea-level pressure.

Granularity matters here. One METAR is only 4 rows, and building a DataFrame
per message (with its category casts and column assignments) cost ~9 ms each -
~112 messages/second, measured on the first real 30-day run (ADR 0004), which
would have made a full-history backfill and its reconciliation take hours. So
the per-message work is plain Python (`observation_rows`) and pandas is used
once per *batch* (`observation_frame`), with explicit dtypes and categoricals."""

import math
import re
from collections.abc import Callable, Sequence
from typing import Any

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

_CATEGORICAL_COLUMNS = ("station", "variable", "ingestion_mode")

# temp/dewp/slp/altim arrive in degC/hPa (verified against the live API, ADR 0003);
# everything is converted to the SI units the forecast side uses (config/variables.yaml)
# so a gold-layer error is always a difference in one unit. Pressure was left in hPa
# here until the gold layer's first pressure verification (ADR 0005).
_TO_SI: dict[str, Callable[[float], float]] = {
    "temperature_2m": lambda v: v + 273.15,  # degC -> K
    "dew_point_2m": lambda v: v + 273.15,  # degC -> K
    "wind_speed_10m": lambda v: v * 0.514444,  # kt -> m/s
    "pressure_msl": lambda v: v * 100.0,  # hPa -> Pa
}


def _is_corrected(raw_text: str) -> bool:
    return bool(_CORRECTED_PATTERN.search(raw_text))


def observation_rows(
    payload: ObservationRawPayload, ingestion_mode: IngestionMode
) -> list[dict[str, Any]]:
    """One row per variable, as plain dicts. A missing value is NaN (never
    dropped). Wind *direction* being "VRB" (variable) never matters: direction
    isn't a stored variable, only speed is. A non-numeric value raises ValueError,
    which the silver consumer routes to the DLQ."""
    report = payload.api_response
    raw_text = str(report.get("rawOb") or "")

    # mean sea-level pressure: prefer the true SLP reduction; fall back to the
    # altimeter setting (already hPa in this API) when a station doesn't report
    # SLP in its remarks - common at smaller airports.
    pressure = report.get("slp")
    if pressure is None:
        pressure = report.get("altim")

    raw_values = {
        "temperature_2m": report.get("temp"),
        "dew_point_2m": report.get("dewp"),
        "wind_speed_10m": report.get("wspd"),
        "pressure_msl": pressure,
    }
    observed_at = pd.Timestamp(payload.observed_at)
    is_corrected = _is_corrected(raw_text)

    return [
        {
            "station": payload.station,
            "observed_at": observed_at,
            "variable": variable,
            "value": math.nan if raw is None else _TO_SI[variable](float(raw)),
            "raw_text": raw_text,
            "is_corrected": is_corrected,
            "ingestion_mode": ingestion_mode,
        }
        for variable, raw in raw_values.items()
    ]


def observation_frame(rows: Sequence[dict[str, Any]]) -> pd.DataFrame:
    """One DataFrame for any number of messages' rows: explicit dtypes, and
    categoricals for the low-cardinality columns. Extra keys (e.g. the lineage
    `source_event_id`) are kept."""
    if not rows:
        return pd.DataFrame(columns=OBSERVATION_COLUMNS)
    frame = pd.DataFrame(list(rows))
    frame["value"] = frame["value"].astype("float64")
    return frame.astype(dict.fromkeys(_CATEGORICAL_COLUMNS, "category"))


def explode_observation_payload(
    payload: ObservationRawPayload, ingestion_mode: IngestionMode
) -> pd.DataFrame:
    """Single-payload convenience wrapper (used by tests and one-off callers);
    bulk paths should batch `observation_rows` into one `observation_frame`."""
    return observation_frame(observation_rows(payload, ingestion_mode))[OBSERVATION_COLUMNS]
