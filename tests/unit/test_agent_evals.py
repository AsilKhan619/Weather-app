"""The eval's grader and question file, without a database: the grader must fail wrong
answers (not just pass right ones), and every scripted plan must be something the real tools
accept - including passing the SQL guard - so a baseline can never fail for a silly reason."""

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from nimbus.agent.evals import Question, _datetimes, _fill, grade, load_questions
from nimbus.agent.llm import render_answer
from nimbus.agent.sql_guard import validate_select
from nimbus.agent.tools import TOOLS

QUESTIONS = load_questions()


def test_the_question_file_has_about_twenty_uniquely_named_questions() -> None:
    ids = [q.id for q in QUESTIONS]
    assert 18 <= len(ids) <= 25
    assert len(set(ids)) == len(ids)


def test_the_briefs_example_questions_are_in_the_eval() -> None:
    asked = " ".join(q.question.lower() for q in QUESTIONS)
    for phrase in (
        "lowest 3-day temperature error",
        "grow faster with lead time at mountain locations than at coastal",
        "consistently over-forecast wind speed",
        "no observation data arrived for",
    ):
        assert phrase in asked


@pytest.mark.parametrize("question", QUESTIONS, ids=lambda q: q.id)
def test_every_baseline_plan_is_accepted_by_the_real_tools(question: Question) -> None:
    for call in question.plan:
        tool = TOOLS[call.tool]
        schema = tool.input_schema
        filled = _fill(call.input, {"station": "EGLL", "from_time": "2026-09-20T06:00Z"})
        assert set(filled) <= set(schema["properties"]), call.tool
        assert set(schema.get("required", [])) <= set(filled), call.tool
        if call.tool == "run_sql":
            validate_select(filled["query"])  # raises if the guard would reject it


def test_parameters_fill_questions_plans_and_sql() -> None:
    filled = _fill({"q": "SELECT '$station'", "n": [1, "at $station"]}, {"station": "KDEN"})
    assert filled == {"q": "SELECT 'KDEN'", "n": [1, "at KDEN"]}


def test_an_unknown_expectation_kind_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "q.yaml"
    path.write_text(
        "questions:\n  - id: x\n    question: q\n    reference_sql: select 1\n"
        "    expect: [{kind: vibes, column: a}]\n    baseline: {plan: [], answer: a}\n",
        encoding="utf-8",
        newline="\n",
    )
    with pytest.raises(ValueError, match="vibes"):
        load_questions(path)


def test_render_answer_joins_a_list_of_records() -> None:
    results = [{"checks": [{"name": "hard_range"}, {"name": "freshness"}], "empty": []}]
    assert render_answer("{r0.checks|name} / {r0.empty|name}", results) == (
        "hard_range, freshness / none"
    )


# --- grading --------------------------------------------------------------------------------------

ROW = {"model": "icon_seamless", "mae": 1.104, "bias": -0.31, "wins": 25, "direction": "warm"}


def test_a_correct_answer_passes() -> None:
    answer = "icon_seamless was best: MAE 1.10 degC, bias -0.31 degC; it runs warm."
    models = ["ecmwf_ifs025", "icon_seamless"]
    expect: list[dict[str, Any]] = [
        {"kind": "first_mentioned", "column": "model", "candidates": models},
        {"kind": "number", "column": "mae", "tolerance": 0.01},
        {"kind": "number", "column": "bias", "tolerance": 0.01},
        {"kind": "label", "column": "direction"},
    ]  # fmt: skip
    assert grade(answer, ROW, expect) == []


@pytest.mark.parametrize(
    ("answer", "expect", "reason"),
    [
        ("ecmwf_ifs025 beat icon_seamless", [{"kind": "first_mentioned", "column": "model",
         "candidates": ["ecmwf_ifs025", "icon_seamless"]}], "mentioned first"),
        ("MAE 1.2 degC", [{"kind": "number", "column": "mae", "tolerance": 0.01}], "mae"),
        # the sign matters: a flipped bias is wrong
        ("bias 0.31 degC", [{"kind": "number", "column": "bias", "tolerance": 0.01}], "bias"),
        # digits inside a model id are not a number in the answer
        ("ecmwf_ifs025 ranks first", [{"kind": "number", "column": "wins", "tolerance": 0}],
         "wins"),
        ("it runs cold", [{"kind": "label", "column": "direction"}], "warm"),
        # "warm" inside another word does not count
        ("it runs lukewarmish", [{"kind": "label", "column": "direction"}], "warm"),
    ],
)  # fmt: skip
def test_a_wrong_answer_fails(answer: str, expect: list[dict[str, Any]], reason: str) -> None:
    failures = grade(answer, ROW, expect)
    assert failures and reason in failures[0]


def test_labels_csv_needs_every_item_and_none_must_be_said() -> None:
    expect = [{"kind": "labels_csv", "column": "c"}]
    assert (
        grade("gfs_seamless and icon_seamless", {"c": "gfs_seamless, icon_seamless"}, expect) == []
    )
    assert grade("only gfs_seamless", {"c": "gfs_seamless, icon_seamless"}, expect)
    assert grade("None of them.", {"c": "none"}, expect) == []
    assert grade("gfs_seamless does", {"c": "none"}, expect)


def test_a_model_prefix_is_not_the_model() -> None:
    expect = [{"kind": "label", "column": "model"}]
    assert grade("gfs_seamless", {"model": "gfs"}, expect)


def test_timestamps_are_compared_as_instants_in_any_offset() -> None:
    row = {"t": datetime(2026, 9, 20, 6, 0, tzinfo=UTC)}
    expect = [{"kind": "timestamp", "column": "t", "tolerance_minutes": 1}]
    for answer in (
        "at 2026-09-20 06:00 UTC",
        "at 2026-09-20T06:00:30+00:00",
        "at 2026-09-20T08:00:00+02:00",
        "at 2026-09-20T06:00Z",
    ):
        assert grade(answer, row, expect) == [], answer
    assert grade("at 2026-09-20 07:00", row, expect)
    assert grade("at 2026-09-20T06:00:00-01:00", row, expect)


def test_a_missing_time_must_be_reported_as_never() -> None:
    expect = [{"kind": "timestamp", "column": "t", "tolerance_minutes": 1}]
    assert grade("it has never reported", {"t": None}, expect) == []
    assert grade("it reported at 2026-09-20 06:00", {"t": None}, expect)


def test_a_reference_without_the_number_fails_rather_than_crashing() -> None:
    assert grade("42", {"x": None}, [{"kind": "number", "column": "x"}])


def test_datetimes_parse_the_formats_tools_produce() -> None:
    plus_two = timezone(timedelta(hours=2))
    assert _datetimes("2026-09-20T06:00:00.123+02:00 and 2026-09-21 07:15") == [
        datetime(2026, 9, 20, 6, 0, tzinfo=plus_two),
        datetime(2026, 9, 21, 7, 15, tzinfo=UTC),
    ]
