"""Run a table's blocking and warning schemas over a DataFrame and turn what pandera
reports into `CheckResult` rows for `ops.quality_results` (brief section 8)."""

import json
import logging
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd
import pandera.pandas as pa
from sqlalchemy import Engine, insert

from nimbus.common.tables import quality_results_table
from nimbus.quality.schemas import TableChecks

logger = logging.getLogger(__name__)

Severity = Literal["blocking", "warning"]

# One summary row per (table, severity) so "everything passed" is recorded too,
# next to a row for each individual check that failed.
SUMMARY_CHECK = "all_checks"
_DETAIL_LIMIT = 400


class QualityError(ValueError):
    """A blocking quality check failed. A ValueError so the silver consumers'
    existing poison-message handling (-> DLQ) and reconciliation's "unparseable"
    accounting treat it like any other message that cannot be loaded. Carries the
    results so a caller that aborts (the gold build) can still record them."""

    def __init__(self, message: str, results: Sequence["CheckResult"] = ()) -> None:
        super().__init__(message)
        self.results = list(results)


@dataclass(frozen=True)
class CheckResult:
    table: str
    check: str
    severity: Severity
    rows_checked: int
    rows_failed: int
    subject: str | None = None
    detail: str | None = None

    @property
    def passed(self) -> bool:
        return self.rows_failed == 0


@dataclass(frozen=True)
class Validation:
    """Outcome of validating one frame. `blocked_rows` are the frame's index labels
    that failed a blocking check; `frame_failed` means a failure that cannot be
    pinned to rows (a missing column), so the whole frame is unusable."""

    results: list[CheckResult]
    blocked_rows: frozenset[object] = field(default_factory=frozenset)
    frame_failed: bool = False

    @property
    def has_blocking_failures(self) -> bool:
        return self.frame_failed or bool(self.blocked_rows)

    @property
    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]


def _failures(
    schema: pa.DataFrameSchema, frame: pd.DataFrame
) -> tuple[pd.DataFrame | None, list[object]]:
    try:
        schema.validate(frame, lazy=True)
    except pa.errors.SchemaErrors as exc:
        cases = exc.failure_cases
        return cases, list(cases["index"])
    return None, []


def _sample(frame: pd.DataFrame, labels: Sequence[object]) -> str:
    rows = frame.loc[list(labels)[:3]]
    text = json.dumps(rows.astype(str).to_dict("records"))
    return text if len(text) <= _DETAIL_LIMIT else text[: _DETAIL_LIMIT - 3] + "..."


def _run_schema(
    schema: pa.DataFrameSchema, frame: pd.DataFrame, table: str, severity: Severity
) -> tuple[list[CheckResult], set[object], bool]:
    if not schema.columns and not schema.checks and not schema.unique:
        return [], set(), False

    cases, _ = _failures(schema, frame)
    rows_checked = len(frame)
    if cases is None:
        return [CheckResult(table, SUMMARY_CHECK, severity, rows_checked, 0)], set(), False

    results: list[CheckResult] = []
    failed_rows: set[object] = set()
    frame_failed = False
    by_check: dict[str, set[object]] = defaultdict(set)
    whole_frame: set[str] = set()

    for case in cases.to_dict("records"):
        # Column-level checks are named per column (`value:finite`); frame-level
        # ones (range, uniqueness) already carry a descriptive name.
        if case["schema_context"] == "Column":
            name = f"{case['column']}:{case['check']}"
        else:
            name = str(case["check"])
        label = case["index"]
        if label is None or pd.isna(label):
            whole_frame.add(name)
        else:
            by_check[name].add(label)

    for name in sorted(whole_frame | by_check.keys()):
        labels = by_check.get(name, set())
        if name in whole_frame:
            frame_failed = True
            results.append(
                CheckResult(table, name, severity, rows_checked, rows_checked, detail="whole frame")
            )
        else:
            failed_rows |= labels
            results.append(
                CheckResult(
                    table,
                    name,
                    severity,
                    rows_checked,
                    len(labels),
                    detail=_sample(frame, sorted(labels, key=repr)),
                )
            )
    results.append(CheckResult(table, SUMMARY_CHECK, severity, rows_checked, len(failed_rows)))
    return results, failed_rows, frame_failed


def validate_frame(frame: pd.DataFrame, checks: TableChecks) -> Validation:
    """Validate with both schemas. The frame's index must be unique (callers
    `reset_index(drop=True)`) because pandera reports failures by index label. An empty
    frame has nothing to load and nothing to check."""
    if frame.empty:
        return Validation([])
    blocking, blocked_rows, frame_failed = _run_schema(
        checks.blocking, frame, checks.table, "blocking"
    )
    # Warnings are only meaningful on frames that have the columns to check.
    warning: list[CheckResult] = []
    if not frame_failed:
        warning, _, _ = _run_schema(checks.warning, frame, checks.table, "warning")
    return Validation(blocking + warning, frozenset(blocked_rows), frame_failed)


def merge_results(results: Iterable[CheckResult]) -> list[CheckResult]:
    """Combine per-chunk results into one row per (table, check, severity, subject):
    rows add up; the first failure's detail is kept."""
    merged: dict[tuple[str, str, Severity, str | None], CheckResult] = {}
    for result in results:
        key = (result.table, result.check, result.severity, result.subject)
        current = merged.get(key)
        if current is None:
            merged[key] = result
        else:
            merged[key] = CheckResult(
                result.table,
                result.check,
                result.severity,
                current.rows_checked + result.rows_checked,
                current.rows_failed + result.rows_failed,
                result.subject,
                current.detail or result.detail,
            )
    return list(merged.values())


def persist_results(
    engine: Engine,
    results: Sequence[CheckResult],
    *,
    context: str,
    only_failures: bool = False,
) -> int:
    """Write results to ops.quality_results; returns how many rows were written.
    `only_failures` keeps the hot consumer path from logging a row per clean batch."""
    rows = [
        {
            "context": context,
            "table_name": r.table,
            "check_name": r.check,
            "severity": r.severity,
            "subject": r.subject,
            "rows_checked": r.rows_checked,
            "rows_failed": r.rows_failed,
            "passed": r.passed,
            "detail": r.detail,
        }
        for r in results
        if not (only_failures and r.passed)
    ]
    if rows:
        with engine.begin() as conn:
            conn.execute(insert(quality_results_table), rows)
    return len(rows)
