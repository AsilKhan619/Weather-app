"""Grounding check (brief section 10): every number in a briefing must appear in, or round
from, the fact sheet. A briefing that fails is stored and flagged, never published.

Rules (ADR 0008):
- The allowed numbers are every numeric value in the fact sheet plus every number written
  inside its strings (an as-of date, a model id such as ecmwf_ifs025, the 48-hour horizon).
- A briefing number with k decimals is grounded if some allowed value rounds to it at k
  decimals, ignoring sign: "1.2 degrees low" may quote a bias of -1.2.
- `confidence` must be the fact sheet's level, and `most_reliable_model` must be the sheet's
  most accurate model when it names one (otherwise one of its models): code decided both.

Numbers spelled out in words ("three") are not detected; that is a known gap."""

import re
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from nimbus.llm.schemas import BriefingOutput

# A leading minus counts only when it is not a hyphen between two numbers ("20-24" is two
# positive numbers) or part of an identifier.
_NUMBER = re.compile(r"(?<![\w.])-?\d+(?:\.\d+)?|(?<=[A-Za-z_-])\d+(?:\.\d+)?")


def numbers_in(text: str) -> list[str]:
    return _NUMBER.findall(text)


def _allowed_values(node: Any, into: set[Decimal]) -> None:
    if isinstance(node, bool) or node is None:
        return
    if isinstance(node, int | float):
        into.add(Decimal(str(node)))
    elif isinstance(node, str):
        for token in numbers_in(node):
            into.add(Decimal(token))
    elif isinstance(node, dict):
        for key, value in node.items():
            _allowed_values(key, into)
            _allowed_values(value, into)
    elif isinstance(node, list | tuple):
        for value in node:
            _allowed_values(value, into)


def allowed_numbers(sheet: dict[str, Any]) -> set[Decimal]:
    values: set[Decimal] = set()
    _allowed_values(sheet, values)
    return values


def _decimals(token: str) -> int:
    return len(token.split(".", 1)[1]) if "." in token else 0


def is_grounded(token: str, allowed: set[Decimal]) -> bool:
    quoted = abs(Decimal(token))
    quantum = Decimal(1).scaleb(-_decimals(token))
    return any(abs(value).quantize(quantum, rounding=ROUND_HALF_UP) == quoted for value in allowed)


def check_grounding(briefing: BriefingOutput, sheet: dict[str, Any]) -> list[str]:
    """Human-readable failures; an empty list means the briefing is grounded."""
    failures: list[str] = []
    allowed = allowed_numbers(sheet)
    fields = {
        "headline": briefing.headline,
        "summary": briefing.summary,
        "reliable_reason": briefing.reliable_reason,
        **{f"notable_risks[{i}]": risk for i, risk in enumerate(briefing.notable_risks)},
    }
    for field, text in fields.items():
        for token in numbers_in(text):
            if not is_grounded(token, allowed):
                failures.append(f"{field}: {token} is not in the fact sheet")

    expected_confidence = sheet["confidence"]["level"]
    if briefing.confidence != expected_confidence:
        failures.append(
            f"confidence: {briefing.confidence!r} but the fact sheet says {expected_confidence!r}"
        )
    best = sheet["accuracy"]["most_accurate_model"]
    if best is not None and briefing.most_reliable_model != best:
        failures.append(
            f"most_reliable_model: {briefing.most_reliable_model!r} but the most accurate "
            f"model in the fact sheet is {best!r}"
        )
    elif best is None and briefing.most_reliable_model not in sheet["models"]:
        failures.append(
            f"most_reliable_model: {briefing.most_reliable_model!r} is not in the fact sheet"
        )
    return failures
