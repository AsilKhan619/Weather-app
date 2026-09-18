from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pytest

from nimbus.common.config import ModelsConfig, ModelSpec
from nimbus.ingestion.backfill import BackfillResult
from nimbus.jobs import backfill as backfill_job
from nimbus.jobs.backfill import FULL_HISTORY_START, estimate_forecast_calls, resolve_window

TODAY = date(2026, 9, 18)


def test_default_window_is_the_last_n_complete_days_ending_yesterday() -> None:
    start, end = resolve_window(days=30, full=False, start=None, end=None, today=TODAY)

    assert end == date(2026, 9, 17)  # today is still incomplete
    assert start == date(2026, 8, 19)
    assert (end - start).days + 1 == 30


def test_full_starts_at_the_archive_start() -> None:
    start, end = resolve_window(days=30, full=True, start=None, end=None, today=TODAY)

    assert start == FULL_HISTORY_START
    assert end == date(2026, 9, 17)


def test_an_explicit_start_wins_and_is_how_a_rate_limited_run_resumes() -> None:
    start, end = resolve_window(days=30, full=True, start=date(2025, 3, 1), end=None, today=TODAY)

    assert (start, end) == (date(2025, 3, 1), date(2026, 9, 17))


def test_a_start_after_the_end_is_rejected() -> None:
    with pytest.raises(ValueError):
        resolve_window(
            days=30, full=False, start=date(2026, 9, 18), end=date(2026, 9, 1), today=TODAY
        )


def _models(n_models: int = 3) -> ModelsConfig:
    return ModelsConfig(
        models=[ModelSpec(id=f"m{i}", name=f"M{i}") for i in range(n_models)],
        run_cadence_hours=6,
        run_lookback_steps=8,
        variables=["a", "b", "c", "d"],
        forecast_days=7,
        backfill_lead_days=[1, 2, 3, 4, 5, 6, 7],
    )


def test_call_estimate_matches_open_meteos_fractional_weighting() -> None:
    # 4 vars x 7 leads = 28 columns -> 2.8 per 10-var unit; 7 days -> 0.5 per 14-day unit
    one_chunk = estimate_forecast_calls(_models(1), 25, date(2024, 6, 1), date(2024, 6, 7))

    assert one_chunk == pytest.approx(25 * 2.8 * 0.5)  # 35 calls, as budgeted in ADR 0004


def test_a_full_history_exceeds_one_days_budget() -> None:
    full = estimate_forecast_calls(_models(3), 25, date(2024, 1, 1), date(2026, 9, 17))

    assert full > 10_000  # so `make backfill` has to span two days; the CLI warns and resumes


def test_a_rate_limited_run_is_recorded_as_failed_with_the_resume_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: list[dict[str, Any]] = []
    monkeypatch.setattr(
        backfill_job,
        "backfill_forecasts",
        lambda *a, **k: BackfillResult(produced=4, failed=0, aborted_at=date(2025, 3, 1)),
    )
    monkeypatch.setattr(
        backfill_job,
        "record_ingestion_run",
        lambda engine, source, mode, started, produced, failed, error_message=None: recorded.append(
            {"source": source, "produced": produced, "failed": failed, "error": error_message}
        ),
    )

    ok = backfill_job.run(
        MagicMock(),
        MagicMock(),
        MagicMock(),
        [],
        _models(),
        date(2025, 1, 1),
        date(2025, 6, 1),
        "forecasts",
    )

    assert ok is False  # the CLI exits non-zero
    assert recorded == [
        {
            "source": "forecast_backfill",
            "produced": 4,
            "failed": 0,
            "error": "rate limited; resume with --start-date 2025-03-01",
        }
    ]
