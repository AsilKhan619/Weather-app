"""The silver load gate: validate a batch before it is upserted.

A message is the unit that reaches the DLQ and the unit a replay re-reads, so a
row that fails a *blocking* check takes its whole message with it (the message is
suspect as a whole - a provider unit bug affects every value in the payload).
Warnings never remove anything. `reconcile` applies the same gate when it
recomputes what silver should contain, so a quarantined message counts as
"unparseable" there exactly as it does for the live consumer."""

from dataclasses import dataclass

import pandas as pd

from nimbus.quality.runner import SUMMARY_CHECK, CheckResult, validate_frame
from nimbus.quality.schemas import TableChecks

EVENT_COLUMN = "source_event_id"


@dataclass(frozen=True)
class Gate:
    clean: pd.DataFrame
    blocked_events: frozenset[str]
    results: list[CheckResult]

    @property
    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]

    @property
    def reason(self) -> str:
        """Why messages were quarantined, for the DLQ record; the per-check detail
        (which rows, which values) is in ops.quality_results."""
        checks = sorted(
            {
                r.check
                for r in self.results
                if r.severity == "blocking" and not r.passed and r.check != SUMMARY_CHECK
            }
        )
        return "failed blocking quality check(s): " + ", ".join(checks or ["unknown"])


def gate_frame(frame: pd.DataFrame, checks: TableChecks) -> Gate:
    """Split a batch frame into the rows that may load and the events that may not."""
    frame = frame.reset_index(drop=True)
    validation = validate_frame(frame, checks)
    if not validation.has_blocking_failures:
        return Gate(frame, frozenset(), validation.results)

    if validation.frame_failed:
        blocked = frozenset(frame[EVENT_COLUMN].astype(str))
    else:
        bad_events = frame[EVENT_COLUMN][frame.index.isin(list(validation.blocked_rows))]
        blocked = frozenset(bad_events.astype(str))
    clean = frame[~frame[EVENT_COLUMN].astype(str).isin(blocked)]
    return Gate(clean, blocked, validation.results)
