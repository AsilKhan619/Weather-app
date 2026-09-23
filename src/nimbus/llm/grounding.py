"""Grounding check (brief section 10): every number in a briefing must appear in, or round
from, the fact sheet. A briefing that fails is stored and flagged, never published.

Rules (ADR 0008):
- The allowed numbers are the fact sheet's *numeric values*. Digits inside its names are not
  facts: `ecmwf_ifs025`, `temperature_2m` or the as-of date do not make 25, 2 or 20 quotable.
  Instead, those identifiers are removed from the briefing's text before its numbers are read,
  so naming a model or quoting the date is never a failure either.
- A briefing number with k decimals is grounded if some allowed value rounds to it at k
  decimals (half up). Signs must agree: "-16.9" is not grounded by 16.9, and the prompt tells
  the model to write negative numbers with a minus sign.
- `confidence` must be the fact sheet's level, and `most_reliable_model` must be the sheet's
  most accurate model when it names one (otherwise one of its models): code decided both.

Found by the phase review and fixed: signs were once ignored (so a flipped sign passed), digits
inside identifiers were once allowed numbers (so "gusts to 25 m/s" passed on every sheet), and
".5" was invisible to the number scanner. Numbers spelled out in words ("three") are still not
detected; that is a known gap."""

import re
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any

from nimbus.llm.schemas import BriefingOutput

# Digits with optional decimal parts, including a leading-dot form (".5") and malformed
# multi-dot runs ("3.14.15", checked part by part so nothing slips through).
_NUMBER = re.compile(r"\.?\d+(?:\.\d+)*")
# A thousands separator between digit groups: "1,013" is one number, 1013.
_THOUSANDS = re.compile(r"(?<=\d),(?=\d{3}(?!\d))")


def _is_negative(text: str, start: int) -> bool:
    """A minus sign directly before the number counts unless it joins two tokens: "20-24" is
    a range and "day-1" a label, not negative numbers."""
    if start == 0 or text[start - 1] != "-":
        return False
    before = text[start - 2] if start >= 2 else " "
    return not (before.isalnum() or before in "._")


def numbers_in(text: str) -> list[str]:
    """Every number in the text as a signed decimal string."""
    text = _THOUSANDS.sub("", text)
    tokens: list[str] = []
    for match in _NUMBER.finditer(text):
        raw = match.group()
        parts = [raw] if raw.count(".") <= 1 else [p for p in raw.split(".") if p]
        negative = _is_negative(text, match.start())
        for i, part in enumerate(parts):
            sign = "-" if negative and i == 0 else ""
            tokens.append(sign + (part if not part.startswith(".") else "0" + part))
    return tokens


def _walk(node: Any, values: set[Decimal], identifiers: set[str]) -> None:
    if isinstance(node, bool) or node is None:
        return
    if isinstance(node, int | float):
        values.add(Decimal(str(node)))
    elif isinstance(node, str):
        if any(ch.isdigit() for ch in node):
            identifiers.add(node)
    elif isinstance(node, dict):
        for key, value in node.items():
            _walk(key, values, identifiers)
            _walk(value, values, identifiers)
    elif isinstance(node, list | tuple):
        for value in node:
            _walk(value, values, identifiers)


def allowed_numbers(sheet: dict[str, Any]) -> set[Decimal]:
    values: set[Decimal] = set()
    _walk(sheet, values, set())
    return values


def sheet_identifiers(sheet: dict[str, Any]) -> list[str]:
    """Strings in the sheet that contain digits (model ids, variable names, dates), longest
    first so a longer identifier is removed before any shorter one it contains."""
    identifiers: set[str] = set()
    _walk(sheet, set(), identifiers)
    return sorted(identifiers, key=len, reverse=True)


def strip_identifiers(text: str, identifiers: list[str]) -> str:
    for identifier in identifiers:
        text = re.sub(rf"(?<![\w.]){re.escape(identifier)}(?![\w])", " ", text)
    return text


def _decimals(token: str) -> int:
    return len(token.split(".", 1)[1]) if "." in token else 0


def is_grounded(token: str, allowed: set[Decimal]) -> bool:
    try:
        quoted = Decimal(token)
    except InvalidOperation:
        return False
    quantum = Decimal(1).scaleb(-_decimals(token))
    return any(value.quantize(quantum, rounding=ROUND_HALF_UP) == quoted for value in allowed)


def check_grounding(briefing: BriefingOutput, sheet: dict[str, Any]) -> list[str]:
    """Human-readable failures; an empty list means the briefing is grounded."""
    failures: list[str] = []
    allowed = allowed_numbers(sheet)
    identifiers = sheet_identifiers(sheet)
    fields = {
        "headline": briefing.headline,
        "summary": briefing.summary,
        "reliable_reason": briefing.reliable_reason,
        **{f"notable_risks[{i}]": risk for i, risk in enumerate(briefing.notable_risks)},
    }
    for field, text in fields.items():
        for token in numbers_in(strip_identifiers(text, identifiers)):
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
