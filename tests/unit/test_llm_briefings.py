"""Phase 5, without a database or network: the fact sheet, the confidence rule, the grounding
check (the brief's acceptance test: a briefing with an invented number is rejected), the
structured-output validation, cost estimates, and the real client's handling of API replies
(the SDK is exercised with a stubbed transport-free `messages.create`)."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pandas as pd
import pytest

from nimbus.common.config import Location, load_llm_config
from nimbus.llm.client import (
    AnthropicBriefingClient,
    FakeBriefingClient,
    LLMUnavailableError,
    LLMUsage,
    estimate_cost_usd,
    fake_briefing,
    validate_reply,
)
from nimbus.llm.facts import (
    FactInputs,
    build_fact_sheet,
    canonical_json,
    confidence_level,
    fact_sheet_hash,
)
from nimbus.llm.grounding import allowed_numbers, check_grounding, is_grounded, numbers_in
from nimbus.llm.schemas import BriefingOutput

CONFIG = load_llm_config()
AS_OF = datetime(2026, 9, 20, 12, tzinfo=UTC)
PLACE = Location(
    id="denver", name="Denver, CO, USA", climate="semi-arid", latitude=39.86, longitude=-104.67,
    elevation_m=1655, timezone="America/Denver", station="KDEN",
)  # fmt: skip


def _forecasts(offsets_k: dict[str, float], hours: int = 48) -> pd.DataFrame:
    """Each model forecasts 290 K + its offset for temperature, and a flat 101300 Pa."""
    rows: list[dict[str, Any]] = []
    for model, offset in offsets_k.items():
        for h in range(1, hours + 1):
            valid = AS_OF + timedelta(hours=h)
            rows.append({"model": model, "variable": "temperature_2m", "valid_time": valid,
                         "value": 290.0 + offset})  # fmt: skip
            rows.append({"model": model, "variable": "pressure_msl", "valid_time": valid,
                         "value": 101300.0})  # fmt: skip
    return pd.DataFrame(rows)


def _accuracy(maes: dict[str, float]) -> pd.DataFrame:
    return pd.DataFrame({"model": list(maes), "n": [700] * len(maes), "mae": list(maes.values())})


def _no_alerts() -> pd.DataFrame:
    return pd.DataFrame(columns=["rule", "severity", "variable", "subject", "metric", "event_time"])


def _sheet(offsets: dict[str, float] | None = None, maes: dict[str, float] | None = None,
           alerts: pd.DataFrame | None = None) -> dict[str, Any]:  # fmt: skip
    inputs = FactInputs(
        _forecasts(offsets if offsets is not None else {"ecmwf_ifs025": 0.0, "gfs_seamless": 1.0}),
        _accuracy(maes if maes is not None else {"ecmwf_ifs025": 1.42, "gfs_seamless": 1.71}),
        alerts if alerts is not None else _no_alerts(),
    )
    return build_fact_sheet(PLACE, AS_OF, inputs, CONFIG)


# --- the fact sheet ---------------------------------------------------------------------


def test_fact_sheet_is_in_display_units_and_rounded() -> None:
    sheet = _sheet()

    ecmwf = sheet["forecast_by_model"]["ecmwf_ifs025"]
    assert ecmwf["temperature_2m"] == {"min": 16.9, "mean": 16.9, "max": 16.9}  # 290 K
    assert ecmwf["pressure_msl"]["mean"] == 1013.0  # Pa -> hPa
    assert sheet["units"] == {"pressure_msl": "hPa", "temperature_2m": "degC"}
    assert sheet["model_spread"]["temperature_2m"] == {"mean": 1.0, "max": 1.0}
    assert sheet["horizon_hours"] == 48 and sheet["models"] == ["ecmwf_ifs025", "gfs_seamless"]


def test_accuracy_names_the_most_accurate_model() -> None:
    accuracy = _sheet()["accuracy"]

    assert accuracy["most_accurate_model"] == "ecmwf_ifs025"
    assert accuracy["by_model"]["gfs_seamless"] == {
        "temperature_mae_degc": 1.7,
        "verified_forecasts": 700,
    }


def test_alerts_are_listed_in_display_units() -> None:
    alerts = pd.DataFrame(
        {
            "rule": ["model_spread"], "severity": ["warning"], "variable": ["pressure_msl"],
            "subject": [None], "metric": [711.25], "event_time": [AS_OF],
        }
    )  # fmt: skip
    (alert,) = _sheet(alerts=alerts)["active_alerts"]
    assert alert == {"rule": "model_spread", "severity": "warning", "variable": "pressure_msl",
                     "subject": None, "size": 7.1, "unit": "hPa"}  # fmt: skip


def test_alert_order_does_not_change_the_hash() -> None:
    """Review finding: two models' run-change alerts in one cycle tied on the sort key, so
    their order - and the hash, and the cache - followed whatever order Postgres returned."""
    alerts = pd.DataFrame(
        {
            "rule": ["run_change", "run_change"], "severity": ["warning", "warning"],
            "variable": ["temperature_2m", "temperature_2m"],
            "subject": ["gfs_seamless", "ecmwf_ifs025"], "metric": [3.4, 3.1],
            "event_time": [AS_OF, AS_OF],
        }
    )  # fmt: skip
    forward = _sheet(alerts=alerts)
    reverse = _sheet(alerts=alerts.iloc[::-1].reset_index(drop=True))
    assert fact_sheet_hash(forward) == fact_sheet_hash(reverse)
    assert [a["subject"] for a in forward["active_alerts"]] == ["ecmwf_ifs025", "gfs_seamless"]


def test_the_hash_is_stable_and_changes_with_any_fact() -> None:
    assert fact_sheet_hash(_sheet()) == fact_sheet_hash(_sheet())
    assert fact_sheet_hash(_sheet()) != fact_sheet_hash(_sheet(maes={"ecmwf_ifs025": 1.5}))
    assert canonical_json(_sheet()) == canonical_json(json.loads(canonical_json(_sheet())))


def test_no_forecasts_gives_an_empty_sheet_with_low_confidence() -> None:
    sheet = build_fact_sheet(
        PLACE,
        AS_OF,
        FactInputs(pd.DataFrame(columns=["model", "variable", "valid_time", "value"]),
                   _accuracy({}), _no_alerts()),
        CONFIG,
    )  # fmt: skip
    assert sheet["models"] == [] and sheet["confidence"]["level"] == "low"


# --- confidence is decided by code ----------------------------------------------------------


@pytest.mark.parametrize(
    ("spread", "models", "level"),
    [(0.4, 3, "high"), (1.5, 3, "high"), (1.51, 3, "medium"), (3.0, 2, "medium"),
     (3.1, 3, "low"), (0.1, 1, "low"), (None, 3, "low")],
)  # fmt: skip
def test_confidence_levels(spread: float | None, models: int, level: str) -> None:
    assert confidence_level(spread, models, CONFIG)["level"] == level


def test_confidence_follows_model_agreement_end_to_end() -> None:
    assert _sheet({"a": 0.0, "b": 0.5, "c": 1.0})["confidence"]["level"] == "high"
    assert _sheet({"a": 0.0, "b": 2.0})["confidence"]["level"] == "medium"
    assert _sheet({"a": 0.0, "b": 6.0})["confidence"]["level"] == "low"


# --- grounding ------------------------------------------------------------------------------


def test_a_briefing_written_from_the_fact_sheet_is_grounded() -> None:
    sheet = _sheet()
    assert check_grounding(fake_briefing(sheet), sheet) == []


def test_the_grounding_check_rejects_an_invented_number() -> None:
    """Brief section 15, Phase 5 acceptance."""
    sheet = _sheet()
    failures = check_grounding(fake_briefing(sheet, invent_number=True), sheet)

    assert failures == ["summary: 97 is not in the fact sheet"]


def test_rounding_from_a_fact_is_allowed_but_changing_it_is_not() -> None:
    allowed = {Decimal("16.94"), Decimal("-1.25"), Decimal("1013.0")}

    assert is_grounded("16.9", allowed) and is_grounded("17", allowed)
    assert is_grounded("-1.3", allowed)  # half rounds up in magnitude
    assert is_grounded("1013", allowed)
    assert not is_grounded("16.8", allowed)
    assert not is_grounded("18", allowed)


def test_a_flipped_sign_is_not_grounded() -> None:
    """Review finding: signs used to be ignored, so "-16.9" passed on a sheet saying 16.9."""
    allowed = {Decimal("16.9"), Decimal("-1.25")}

    assert not is_grounded("-16.9", allowed)
    assert not is_grounded("1.3", allowed)  # a -1.25 is not a 1.3
    sheet = _sheet()
    flipped = fake_briefing(sheet).model_copy(update={"summary": "Overnight lows near -16.9 degC."})
    assert check_grounding(flipped, sheet) == ["summary: -16.9 is not in the fact sheet"]


def test_number_extraction() -> None:
    assert numbers_in("from 20-24 degC, then -3.5") == ["20", "24", "-3.5"]
    assert numbers_in("ecmwf_ifs025 and temperature_2m over 48 hours") == ["025", "2", "48"]
    assert numbers_in("day-1 error (-2.0)") == ["1", "-2.0"]


def test_numbers_the_scanner_used_to_miss() -> None:
    """Review finding: a leading-dot number was invisible, and multi-dot runs hid a part."""
    assert numbers_in("temperatures .5 degC above normal") == ["0.5"]
    assert numbers_in("version 3.14.15") == ["3", "14", "15"]
    assert numbers_in("near 1,013 hPa") == ["1013"]  # a separator is not two numbers


def test_digits_inside_identifiers_are_not_facts() -> None:
    """Review finding: `ecmwf_ifs025`, `wind_speed_10m` and the as-of date made 25, 10 and 20
    quotable on every sheet. Now the identifiers are removed from the text instead, so naming
    them is fine but borrowing their digits is not."""
    sheet = _sheet()
    allowed = allowed_numbers(sheet)
    assert Decimal("25") not in allowed and Decimal("2026") not in allowed
    assert Decimal("48") in allowed  # horizon_hours is a real numeric fact

    honest = fake_briefing(sheet)
    naming = honest.model_copy(
        update={"summary": f"As of {sheet['date']}, ecmwf_ifs025 leads on temperature_2m."}
    )
    borrowing = honest.model_copy(update={"summary": "Gusts to 25 m/s on the 20th."})
    assert check_grounding(naming, sheet) == []
    assert check_grounding(borrowing, sheet) == [
        "summary: 25 is not in the fact sheet",
        "summary: 20 is not in the fact sheet",
    ]


def test_confidence_and_model_must_match_what_code_decided() -> None:
    sheet = _sheet()
    honest = fake_briefing(sheet)
    hedged = honest.model_copy(update={"confidence": "low"})
    wrong_model = honest.model_copy(update={"most_reliable_model": "gfs_seamless"})

    assert sheet["confidence"]["level"] == "high"  # spread 1.0 <= 1.5
    assert check_grounding(honest, sheet) == []
    assert check_grounding(hedged, sheet) == ["confidence: 'low' but the fact sheet says 'high'"]
    assert any("most_reliable_model" in f for f in check_grounding(wrong_model, sheet))


def test_without_accuracy_data_any_listed_model_may_be_chosen() -> None:
    sheet = _sheet(maes={})
    briefing = fake_briefing(sheet)

    assert briefing.most_reliable_model in sheet["models"]
    assert check_grounding(briefing, sheet) == []
    unknown = briefing.model_copy(update={"most_reliable_model": "made_up"})
    assert check_grounding(unknown, sheet) != []


# --- output validation and the fake client -----------------------------------------------------


def test_reply_validation() -> None:
    good = fake_briefing(_sheet()).model_dump_json()
    output, error = validate_reply(good)
    assert output is not None and error is None

    too_long = json.loads(good) | {"headline": "x" * 121}
    output, error = validate_reply(json.dumps(too_long))
    assert output is None and error is not None and "invalid output" in error

    assert validate_reply('{"headline": "hi"')[0] is None  # truncated JSON
    assert validate_reply(json.dumps(json.loads(good) | {"extra": 1}))[0] is None


def test_fake_client_knobs() -> None:
    from nimbus.llm.briefings import user_message

    user = user_message(_sheet())
    flaky = FakeBriefingClient(invalid_attempts=1)
    assert flaky.generate("s", user).output is None
    assert flaky.generate("s", user).output is not None
    with pytest.raises(LLMUnavailableError):
        FakeBriefingClient(unavailable=True).generate("s", user)


def test_cost_estimate_uses_configured_prices() -> None:
    usage = LLMUsage(input_tokens=1_000_000, output_tokens=100_000,
                     cache_read_tokens=1_000_000, cache_write_tokens=0)  # fmt: skip
    # haiku 4.5: $1 in, $5 out, cache read 0.1x input
    assert estimate_cost_usd("claude-haiku-4-5", usage, CONFIG) == pytest.approx(1.0 + 0.5 + 0.1)
    assert estimate_cost_usd("fake-briefing-model", usage, CONFIG) == 0.0


# --- the real client, against stubbed API replies --------------------------------------------


def _message(text: str, stop_reason: str = "end_turn") -> Any:
    import anthropic

    return anthropic.types.Message.model_validate(
        {
            "id": "msg_1", "type": "message", "role": "assistant", "model": "claude-haiku-4-5",
            "content": [{"type": "text", "text": text}], "stop_reason": stop_reason,
            "stop_sequence": None,
            "usage": {"input_tokens": 900, "output_tokens": 150, "cache_read_input_tokens": 0,
                      "cache_creation_input_tokens": 0},
        }
    )  # fmt: skip


Sent = list[dict[str, Any]]


def _real_client(
    monkeypatch: pytest.MonkeyPatch, reply: Any
) -> tuple[AnthropicBriefingClient, Sent]:
    client = AnthropicBriefingClient("claude-haiku-4-5", CONFIG, api_key="test-key-not-real")
    sent: Sent = []

    def create(**kwargs: Any) -> Any:
        sent.append(kwargs)
        if isinstance(reply, Exception):
            raise reply
        return reply

    monkeypatch.setattr(client._client.messages, "create", create)
    return client, sent


def test_real_client_sends_a_json_schema_and_validates_the_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sheet = _sheet()
    reply = _message(fake_briefing(sheet).model_dump_json())
    client, sent = _real_client(monkeypatch, reply)

    result = client.generate("system prompt", "user message")

    assert result.output is not None and result.error is None
    assert result.usage == LLMUsage(input_tokens=900, output_tokens=150)
    request = sent[0]
    assert request["model"] == "claude-haiku-4-5" and request["system"] == "system prompt"
    schema = request["output_config"]["format"]["schema"]
    assert request["output_config"]["format"]["type"] == "json_schema"
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == set(BriefingOutput.model_fields)


def test_real_client_reports_an_invalid_reply_with_its_usage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _real_client(monkeypatch, _message('{"headline": ""}'))
    result = client.generate("s", "u")

    assert result.output is None and result.error is not None
    assert result.usage.output_tokens == 150  # still logged, still costed


def test_real_client_treats_truncation_and_refusal_as_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for stop in ("max_tokens", "refusal"):
        client, _ = _real_client(monkeypatch, _message("{", stop_reason=stop))
        result = client.generate("s", "u")
        assert result.output is None and result.error == f"stopped early: {stop}"


def test_real_client_maps_api_failures_to_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    import anthropic
    import httpx2

    request = httpx2.Request("POST", "https://api.anthropic.com/v1/messages")
    client, _ = _real_client(monkeypatch, anthropic.APITimeoutError(request=request))

    with pytest.raises(LLMUnavailableError, match="APITimeoutError"):
        client.generate("s", "u")
