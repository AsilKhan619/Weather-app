"""`python -m nimbus.jobs.dashboard_smoke`: render every dashboard page headlessly against
the configured database and report what each one shows. Exits non-zero if any page
raises. Used by the live-demo workflow to show the pages working on real data, and handy
after `make demo` on a laptop (needs `uv sync --extra dashboard`)."""

import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

from nimbus.common.db import make_engine
from nimbus.common.settings import get_settings
from nimbus.dashboard import queries

APP = Path(__file__).resolve().parents[3] / "dashboard" / "app.py"
PAGES = [
    ("Pipeline Health", None),
    ("Forecast vs Actual", "views/forecast_vs_actual.py"),
    ("Accuracy", "views/accuracy.py"),
    ("Lineage", "views/lineage.py"),
    ("Briefings", "views/briefings.py"),
    ("LLM Usage", "views/llm_usage.py"),
]


def _busiest_location() -> str | None:
    """A location that has verified forecasts, so Forecast vs Actual has something to draw."""
    frame = queries._read(
        make_engine(get_settings()),
        "SELECT d.name FROM gold.accuracy_daily a JOIN silver.dim_location d USING (location_id) "
        "GROUP BY d.name ORDER BY sum(a.n) DESC LIMIT 1",
    )
    return None if frame.empty else str(frame.iloc[0]["name"])


def main() -> None:
    failed = False
    for title, script in PAGES:
        at = AppTest.from_file(str(APP), default_timeout=180)
        at.run()
        if script is not None:
            at.switch_page(script)
            at.run()
            if script.endswith("forecast_vs_actual.py") and (place := _busiest_location()):
                at.selectbox[0].select(place).run()
        problems = [str(e.value) for e in at.exception]
        failed |= bool(problems)
        print(f"== {title}: {'FAILED' if problems else 'ok'}")
        charts = len(at.get("vega_lite_chart"))
        print(f"   tables: {len(at.dataframe)}  metrics: {len(at.metric)}  charts: {charts}")
        for metric in at.metric:
            print(f"   metric {metric.label}: {metric.value}")
        for section in at.subheader:
            print(f"   section: {section.value}")
        for message in [*at.warning, *at.info]:
            print(f"   note: {message.value[:110]}")
        for problem in problems:
            print(f"   ERROR: {problem}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
