from datetime import UTC, date, datetime

import pytest

from nimbus.common.config import StorageConfig, load_storage_config
from nimbus.jobs.manage_partitions import add_months, partition_name, retention_cutoff


@pytest.mark.parametrize(
    ("start", "months", "expected"),
    [
        (date(2026, 9, 1), 1, date(2026, 10, 1)),
        (date(2026, 12, 1), 1, date(2027, 1, 1)),
        (date(2026, 1, 1), -1, date(2025, 12, 1)),
        (date(2026, 9, 1), 15, date(2027, 12, 1)),
        (date(2026, 9, 1), -20, date(2025, 1, 1)),
    ],
)
def test_add_months_rolls_over_years(start: date, months: int, expected: date) -> None:
    assert add_months(start, months) == expected


def test_partition_names_are_zero_padded_and_sortable() -> None:
    assert partition_name(date(2024, 1, 1)) == "forecast_y2024m01"
    assert partition_name(date(2027, 12, 1)) == "forecast_y2027m12"


def test_retention_is_off_by_default_so_the_full_history_is_kept() -> None:
    assert load_storage_config().forecast_retention_months is None
    assert retention_cutoff(load_storage_config(), date(2026, 9, 19)) is None


def test_retention_cutoff_is_month_aligned() -> None:
    config = StorageConfig(partitions_ahead_months=6, forecast_retention_months=12)
    # keep the current month and the 12 before it
    assert retention_cutoff(config, date(2026, 9, 19)) == datetime(2025, 9, 1, tzinfo=UTC)
    assert retention_cutoff(config, date(2026, 9, 1)) == datetime(2025, 9, 1, tzinfo=UTC)
