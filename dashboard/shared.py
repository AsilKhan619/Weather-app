"""Helpers shared by the dashboard pages: one cached database engine, and cached calls
into `nimbus.dashboard.queries` (short TTL, so the page follows a running pipeline
without hammering Postgres)."""

from datetime import date
from typing import Any, cast

import altair as alt
import pandas as pd
import streamlit as st
from sqlalchemy import Engine

from nimbus.common.db import make_engine
from nimbus.common.settings import get_settings
from nimbus.dashboard import queries

TTL_SECONDS = 30


@st.cache_resource
def engine() -> Engine:
    return make_engine(get_settings())


@st.cache_resource
def readonly_engine() -> Engine:
    """The agent's engine: the read-only role (migration 0011)."""
    return make_engine(get_settings(), readonly=True)


@st.cache_data(ttl=TTL_SECONDS, show_spinner=False)
def query(name: str, *args: Any) -> Any:
    """`queries.<name>(engine, *args)`; args must be hashable (str, int, date)."""
    return getattr(queries, name)(engine(), *args)


def verified_dates() -> tuple[date, date] | None:
    result: tuple[date, date] | None = query("verified_range")
    return result


def frame(name: str, *args: Any) -> pd.DataFrame:
    result: pd.DataFrame = query(name, *args)
    return result


def line_chart(data: pd.DataFrame, x: str, y: str, series: str, y_title: str) -> alt.Chart:
    """One line per series with a labelled value axis (st.line_chart cannot label axes)."""
    chart = (
        alt.Chart(data)
        .mark_line(point=alt.OverlayMarkDef(size=18))
        .encode(
            x=alt.X(x, title=None),
            y=alt.Y(y, title=y_title, scale=alt.Scale(zero=False)),
            color=alt.Color(series, title=None),
            tooltip=[x, series, alt.Tooltip(y, format=".2f")],
        )
        .properties(height=340)
        .interactive()
    )
    return cast(alt.Chart, chart)
