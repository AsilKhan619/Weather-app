"""Pure, unit-tested gold transforms (brief section 7): match each forecast value
to the nearest observation with `pandas.merge_asof`, then aggregate the errors.

Conventions (ADR 0005):
- error = forecast - observed, in the SI unit stored in silver, so a positive bias
  means the model runs high.
- lead_day = ceil(lead_hours / 24). Backfilled forecasts only know their lead in
  whole days (Previous Runs API), so daily lead buckets are the coarsest grain
  every ingestion mode shares. lead_hours below `min_lead_hours` (0 = the analysis
  step, not a forecast) are excluded.
- Missing (NaN) observations are dropped *before* matching, so a gap in the METAR
  record resolves to the nearest observation that has a value rather than to a
  useless NaN. Forecasts with no observation inside the tolerance are left
  unverified, never matched to something far away."""

import numpy as np
import pandas as pd

VERIFICATION_COLUMNS = [
    "model",
    "location_id",
    "init_time",
    "valid_time",
    "variable",
    "lead_hours",
    "lead_day",
    "forecast_value",
    "observed_value",
    "observed_at",
    "obs_offset_seconds",
    "error",
    "ingestion_mode",
]

ACCURACY_COLUMNS = [
    "valid_date",
    "location_id",
    "model",
    "variable",
    "lead_day",
    "n",
    "bias",
    "mae",
    "rmse",
]

_TIME_DTYPE = "datetime64[ns, UTC]"


def lead_day(lead_hours: pd.Series) -> pd.Series:
    """Whole-day lead bucket: 1..24h -> 1, 25..48h -> 2, ..."""
    return ((lead_hours.astype("int64") + 23) // 24).astype("int16")


def eligible_forecasts(forecasts: pd.DataFrame, *, min_lead_hours: int) -> pd.DataFrame:
    """Forecast rows that can be verified: a value present, and a real lead time."""
    keep = forecasts["value"].notna() & (forecasts["lead_hours"] >= min_lead_hours)
    return forecasts.loc[keep]


def verify_forecasts(
    forecasts: pd.DataFrame,
    observations: pd.DataFrame,
    location_stations: pd.DataFrame,
    *,
    tolerance_minutes: int,
    min_lead_hours: int,
) -> pd.DataFrame:
    """One row per forecast value that has an observation within the tolerance.

    `forecasts`: model, location_id, init_time, valid_time, variable, value,
    lead_hours, ingestion_mode. `observations`: station, observed_at, variable,
    value. `location_stations`: location_id, station. Locations without a station
    mapping are unverifiable and dropped."""
    empty = pd.DataFrame({column: pd.Series(dtype="object") for column in VERIFICATION_COLUMNS})

    left = eligible_forecasts(forecasts, min_lead_hours=min_lead_hours)
    left = left.astype({"location_id": "string"}).merge(
        location_stations.astype({"location_id": "string", "station": "string"}),
        on="location_id",
        how="inner",
    )
    right = observations.loc[observations["value"].notna()]
    if left.empty or right.empty:
        return empty

    left = left.astype({"variable": "string", "valid_time": _TIME_DTYPE}).sort_values("valid_time")
    right = (
        right[["station", "observed_at", "variable", "value"]]
        .rename(columns={"value": "observed_value"})
        .astype({"station": "string", "variable": "string", "observed_at": _TIME_DTYPE})
        .sort_values("observed_at")
    )

    matched = pd.merge_asof(
        left,
        right,
        left_on="valid_time",
        right_on="observed_at",
        by=["station", "variable"],
        direction="nearest",
        tolerance=pd.Timedelta(minutes=tolerance_minutes),
    )
    matched = matched.dropna(subset=["observed_at"])
    if matched.empty:
        return empty

    out = pd.DataFrame(
        {
            "model": matched["model"].astype("string"),
            "location_id": matched["location_id"],
            "init_time": matched["init_time"],
            "valid_time": matched["valid_time"],
            "variable": matched["variable"],
            "lead_hours": matched["lead_hours"].astype("int32"),
            "lead_day": lead_day(matched["lead_hours"]),
            "forecast_value": matched["value"].astype("float64"),
            "observed_value": matched["observed_value"].astype("float64"),
            "observed_at": matched["observed_at"],
            # Signed: negative = the observation was taken before the valid time.
            "obs_offset_seconds": (
                (matched["observed_at"] - matched["valid_time"]).dt.total_seconds().astype("int32")
            ),
            "error": (matched["value"] - matched["observed_value"]).astype("float64"),
            "ingestion_mode": matched["ingestion_mode"].astype("string"),
        }
    )
    return out.sort_values(
        ["valid_time", "location_id", "model", "variable", "init_time"], ignore_index=True
    )[VERIFICATION_COLUMNS]


def aggregate_accuracy(verification: pd.DataFrame) -> pd.DataFrame:
    """Count, bias, MAE and RMSE per (UTC valid date, location, model, variable,
    lead day). Days are UTC so a day's rows are one contiguous valid_time range."""
    if verification.empty:
        return pd.DataFrame({column: pd.Series(dtype="object") for column in ACCURACY_COLUMNS})

    frame = verification.assign(
        valid_date=verification["valid_time"].dt.tz_convert("UTC").dt.date,
        abs_error=verification["error"].abs(),
        sq_error=np.square(verification["error"]),
    )
    grouped = frame.groupby(
        ["valid_date", "location_id", "model", "variable", "lead_day"],
        observed=True,
        sort=True,
    )
    out = grouped.agg(
        n=("error", "size"),
        bias=("error", "mean"),
        mae=("abs_error", "mean"),
        sq_mean=("sq_error", "mean"),
    ).reset_index()
    out["rmse"] = np.sqrt(out.pop("sq_mean"))
    out["n"] = out["n"].astype("int32")
    return out[ACCURACY_COLUMNS]
