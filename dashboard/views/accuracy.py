"""Accuracy: which model should I trust, where, and how far ahead?"""

from collections.abc import Callable
from typing import Any

import streamlit as st
from shared import frame, line_chart, verified_dates

from nimbus.dashboard.queries import VARIABLE_LABELS, display_unit

st.title("Accuracy")

if verified_dates() is None:
    st.info("Nothing has been verified yet - load data and run `make gold`.")
    st.stop()

places = frame("locations")
left, middle, right, last = st.columns(4)
variable = left.selectbox(
    "Variable", list(VARIABLE_LABELS), format_func=VARIABLE_LABELS.__getitem__
)
window = middle.radio("Window", [7, 30], format_func=lambda d: f"last {d} days", horizontal=True)
lead = right.selectbox("Lead (days)", [1, 2, 3, 4, 5, 6, 7], index=0)
place = last.selectbox("Location", ["All locations", *places["name"]])
location_id = (
    None if place == "All locations" else places.loc[places["name"] == place, "location_id"].iloc[0]
)
unit = display_unit(variable)

st.subheader(
    f"Leaderboard: {VARIABLE_LABELS[variable].lower()}, {lead}-day lead, last {window} days"
)
board = frame("leaderboard", variable, window, lead, location_id)
if board.empty:
    st.info("No verified forecasts for this selection.")
else:
    shown = board.rename(
        columns={"mae": f"MAE ({unit})", "rmse": f"RMSE ({unit})", "bias": f"bias ({unit})"}
    )
    st.dataframe(
        shown.style.format(
            {
                f"MAE ({unit})": "{:.2f}",
                f"RMSE ({unit})": "{:.2f}",
                f"bias ({unit})": "{:+.2f}",
                "n": "{:,}",
            }
        ),
        hide_index=True,
        width="stretch",
    )
    st.caption(
        "The window ends at the latest verified date. MAE = mean absolute error; lower is "
        "better. Statistics from daily rows are combined by weighting with the verified count, "
        "which is exact."
    )

st.subheader("Error grows with lead time")
curve = frame("error_by_lead", variable, window, location_id)
if curve.empty:
    st.info("No verified forecasts for this selection.")
else:
    st.altair_chart(
        line_chart(
            curve.astype({"lead_day": "int64"}), "lead_day:O", "mae", "model", f"MAE ({unit})"
        )
    )
    st.caption("Mean absolute error by lead day, per model, over the window.")

st.subheader(f"Best model by location ({lead}-day lead, last {window} days)")
best = frame("best_model_by_location", variable, window, lead)
if best.empty:
    st.info("No verified forecasts for this selection.")
else:
    table = best.drop(columns=["location_id"]).rename(
        columns={"mae": f"MAE ({unit})", "margin": f"margin over runner-up ({unit})"}
    )
    formats: dict[Any, str | Callable[[object], str] | None] = {
        f"MAE ({unit})": "{:.2f}",
        f"margin over runner-up ({unit})": "{:.2f}",
        "n": "{:,}",
    }
    st.dataframe(table.style.format(formats), hide_index=True, width="stretch")
    wins = best["best_model"].value_counts().rename("locations won")
    st.bar_chart(wins)
