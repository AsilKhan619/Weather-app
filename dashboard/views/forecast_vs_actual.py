"""Forecast vs Actual: every model's forecast against what the station reported."""

from datetime import timedelta

import pandas as pd
import streamlit as st
from shared import frame, line_chart, verified_dates

from nimbus.dashboard.queries import VARIABLE_LABELS, display_unit

st.title("Forecast vs actual")

bounds = verified_dates()
if bounds is None:
    st.info("Nothing has been verified yet - load data and run `make gold`.")
    st.stop()
first_day, last_day = bounds

places = frame("locations")
if places.empty:
    st.info("No locations loaded - run `make gold` (it loads the dimensions).")
    st.stop()

left, middle, right = st.columns([2, 2, 1])
place: str | None = left.selectbox("Location", places["name"], index=0)
variable = middle.selectbox(
    "Variable", list(VARIABLE_LABELS), format_func=VARIABLE_LABELS.__getitem__
)
lead = right.selectbox("Lead (days)", [1, 2, 3, 4, 5, 6, 7], index=0)
span = st.date_input(
    "Valid dates (UTC)",
    value=(max(first_day, last_day - timedelta(days=6)), last_day),
    min_value=first_day,
    max_value=last_day,
)
if not isinstance(span, tuple) or len(span) != 2:
    st.info("Pick an end date.")
    st.stop()

location = places.loc[places["name"] == place].iloc[0]
data = frame("forecast_vs_actual", location["location_id"], variable, lead, span[0], span[1])
unit = display_unit(variable)

if data.empty:
    st.info("No verified forecasts for this selection (a station that reported nothing?).")
    st.stop()

observed = (
    data.drop_duplicates("valid_time")[["valid_time", "observed"]]
    .rename(columns={"observed": "value"})
    .assign(series="observed")
)
forecast = data.rename(columns={"forecast": "value", "model": "series"})[
    ["valid_time", "value", "series"]
]
chart_data = pd.concat([observed, forecast], ignore_index=True)

st.subheader(f"{VARIABLE_LABELS[variable]} at {place} ({location['station']}), {lead}-day lead")
st.altair_chart(line_chart(chart_data, "valid_time", "value", "series", unit))
st.caption(
    "Each forecast point was issued about "
    f"{lead} day(s) before its valid time and matched to the nearest station report within "
    "30 minutes. Values are UTC; only hours with an observation appear."
)

errors = data.assign(error=data["forecast"] - data["observed"])
summary = (
    errors.groupby("model")["error"]
    .agg(points="size", bias="mean", mae=lambda e: e.abs().mean())
    .reset_index()
    .sort_values("mae")
)
for column in ("bias", "mae"):
    summary[column] = summary[column].round(2)
st.subheader("Error over this selection")
st.dataframe(summary, hide_index=True, width="stretch")
st.caption(f"Error = forecast - observed, in {unit}; a positive bias means the model ran high.")
