"""The agent eval (brief section 11, ADR 0009): questions whose expected answers are computed
by reference SQL at eval time - so they never go stale - graded by comparing values in the
answer against the reference result with tolerances, never by an LLM judge.

Each question also carries a `baseline`: a fixed plan of tool calls and an answer template
for the deterministic `ScriptedAgentClient`. The project runs at $0, so the baseline is what
`make eval` runs. Read its score for what it is: the tools, the SQL guard, the semantic layer,
the loop and the data answering these questions correctly - not a language model's ability
to plan them. Where possible the reference SQL takes a different route to the answer than the
baseline (raw per-forecast errors against daily aggregates), so agreement is a real check.

Expectation kinds (all must hold for a question to pass):
- label:            the answer contains the reference value (any of `columns`), case-insensitive
- labels_csv:       every item of a comma-separated reference value appears ("none" included)
- number:           some number in the answer is within `tolerance` of the reference value
- first_mentioned:  of `candidates`, the one the answer mentions first is the reference value
- timestamp:        some date-time in the answer is within `tolerance_minutes` of the
                    reference (a NULL reference means the answer must say "never")
- pending_proposal: the session filed a pending replay proposal for the reference consumer
                    group; the grader then closes it so eval runs do not fill the queue
"""

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from string import Template
from typing import Any

import yaml
from sqlalchemy import Engine, text

from nimbus.agent.llm import AgentLLM, PlannedCall, ScriptedAgentClient
from nimbus.agent.loop import run_agent
from nimbus.common.config import LLMConfig
from nimbus.common.settings import Settings
from nimbus.llm.grounding import numbers_in, strip_identifiers

EVALS_DIR = Path(__file__).resolve().parents[3] / "evals"
QUESTIONS_FILE = EVALS_DIR / "agent_questions.yaml"
RESULTS_DIR = EVALS_DIR / "results"
# Identifiers whose digits must not count as numbers in an answer (ecmwf_ifs025 -> 25).
_IDENTIFIERS = [
    "ecmwf_ifs025", "temperature_2m", "dew_point_2m", "wind_speed_10m", "pressure_msl",
]  # fmt: skip
# A date-time with optional seconds, fraction and UTC offset; no offset means UTC.
_DATETIME = re.compile(
    r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::(\d{2})(?:\.\d+)?)?\s*(Z|[+-]\d{2}:?\d{2})?"
)
_ANSWER_KINDS = {"label", "labels_csv", "number", "first_mentioned", "timestamp"}


@dataclass(frozen=True)
class Question:
    id: str
    question: str
    reference_sql: str
    expect: list[dict[str, Any]]
    plan: list[PlannedCall]
    answer_template: str
    params_sql: str | None = None


@dataclass
class Graded:
    question_id: str
    question: str
    status: str  # passed, failed, skipped
    answer: str | None = None
    reference: dict[str, Any] | None = None
    failures: list[str] = field(default_factory=list)
    stop_reason: str | None = None
    tool_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    latency_ms: int = 0
    session_id: str | None = None


def load_questions(path: Path = QUESTIONS_FILE) -> list[Question]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    questions = []
    for item in raw["questions"]:
        baseline = item["baseline"]
        kinds = {e["kind"] for e in item["expect"]}
        if unknown := kinds - _ANSWER_KINDS - {"pending_proposal"}:
            raise ValueError(f"{item['id']}: unknown expectation kinds {sorted(unknown)}")
        questions.append(
            Question(
                id=item["id"],
                question=item["question"],
                reference_sql=item["reference_sql"],
                expect=item["expect"],
                plan=[
                    PlannedCall(step["tool"], step.get("input", {})) for step in baseline["plan"]
                ],
                answer_template=baseline["answer"],
                params_sql=item.get("params_sql"),
            )
        )
    return questions


def _fill(value: Any, params: dict[str, str]) -> Any:
    """Substitute `$name` parameters into strings, recursively through plan inputs."""
    if isinstance(value, str):
        return Template(value).safe_substitute(params)
    if isinstance(value, dict):
        return {k: _fill(v, params) for k, v in value.items()}
    if isinstance(value, list):
        return [_fill(v, params) for v in value]
    return value


def resolve(q: Question, engine: Engine) -> Question | None:
    """The question with its parameters filled in; None when params_sql finds nothing."""
    if q.params_sql is None:
        return q
    row = first_row(engine, q.params_sql)
    if row is None:
        return None
    params = {k: str(v) for k, v in row.items()}
    return Question(
        q.id,
        _fill(q.question, params),
        _fill(q.reference_sql, params),
        q.expect,
        [PlannedCall(c.tool, _fill(c.input, params)) for c in q.plan],
        _fill(q.answer_template, params),
    )


def first_row(engine: Engine, sql: str) -> dict[str, Any] | None:
    with engine.connect() as conn:
        row = conn.execute(text(sql)).mappings().first()
    return None if row is None or all(v is None for v in row.values()) else dict(row)


def _norm(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M")
    return str(value).strip().lower()


def _mentions(answer: str, value: str) -> int:
    """Where `value` first appears in the answer as a whole word (a hyphen or underscore
    continues a word, so "gfs" does not match inside "gfs_seamless"), or -1."""
    match = re.search(rf"(?<![\w-]){re.escape(value)}(?![\w-])", answer, re.IGNORECASE)
    return match.start() if match else -1


def _datetimes(answer: str) -> list[datetime]:
    found = []
    for day, hm, seconds, offset in _DATETIME.findall(answer):
        zone = "+00:00" if offset in ("", "Z") else offset
        if len(zone) == 5:  # +0100
            zone = zone[:3] + ":" + zone[3:]
        try:
            found.append(datetime.fromisoformat(f"{day}T{hm}:{seconds or '00'}{zone}"))
        except ValueError:
            continue
    return found


def grade(answer: str, row: dict[str, Any], expect: list[dict[str, Any]]) -> list[str]:
    """Human-readable failures; empty means every expectation on the answer text holds."""
    failures: list[str] = []
    numbers = [Decimal(n) for n in numbers_in(strip_identifiers(answer, _IDENTIFIERS))]
    for e in expect:
        kind = e["kind"]
        if kind not in _ANSWER_KINDS:
            continue
        columns = e.get("columns") or [e["column"]]
        expected = row.get(columns[0])
        if kind == "label":
            wanted = [_norm(row[c]) for c in columns if row.get(c) is not None]
            if not any(_mentions(answer, w) >= 0 for w in wanted):
                failures.append(f"expected {' or '.join(wanted) or 'a value'!r} in the answer")
        elif kind == "labels_csv":
            items = [i.strip() for i in _norm(expected).split(",") if i.strip()]
            if missing := [i for i in items if _mentions(answer, i) < 0]:
                failures.append(f"missing {', '.join(missing)}")
        elif kind == "number":
            if expected is None:
                failures.append(f"the reference has no {columns[0]}")
                continue
            target = Decimal(str(expected))
            tolerance = Decimal(str(e.get("tolerance", 0)))
            if not any(abs(n - target) <= tolerance for n in numbers):
                failures.append(f"expected {columns[0]} = {target:.4f} (+/- {tolerance})")
        elif kind == "first_mentioned":
            positions = {c: p for c in e["candidates"] if (p := _mentions(answer, str(c))) >= 0}
            first = min(positions, key=positions.__getitem__) if positions else None
            if first is None or str(first).lower() != _norm(expected):
                failures.append(f"expected {_norm(expected)!r} mentioned first, got {first!r}")
        elif kind == "timestamp":
            if expected is None:
                if _mentions(answer, "never") < 0:
                    failures.append("expected the answer to say there is no such time (never)")
                continue
            tolerance_s = 60 * float(e.get("tolerance_minutes", 0))
            if not any(
                abs((f - expected.astimezone(UTC)).total_seconds()) <= tolerance_s
                for f in _datetimes(answer)
            ):
                failures.append(f"expected a time near {_norm(expected)} UTC")
    return failures


def check_proposal(
    engine: Engine, session_id: str, expect: list[dict[str, Any]], row: dict[str, Any]
) -> list[str]:
    """For `pending_proposal`: the session filed a pending proposal for the expected group.
    Then close whatever the session filed, so eval runs never leave work for a person."""
    wanted = [row[e["column"]] for e in expect if e["kind"] == "pending_proposal"]
    if not wanted:
        return []
    with engine.begin() as conn:
        filed = conn.execute(
            text("SELECT consumer_group, status FROM ops.replay_proposals WHERE session_id = :s"),
            {"s": session_id},
        ).all()
        conn.execute(
            text(
                "UPDATE ops.replay_proposals SET status = 'rejected', decided_at = now(), "
                "decided_by = 'make eval', decision_note = 'closed automatically: filed during "
                "an eval run' WHERE session_id = :s AND status = 'pending'"
            ),
            {"s": session_id},
        )
    pending = {group for group, status in filed if status == "pending"}
    return [f"expected a pending replay proposal for {g}" for g in wanted if g not in pending]


def run_question(
    q: Question,
    *,
    readonly_engine: Engine,
    engine: Engine,
    settings: Settings,
    config: LLMConfig,
    client: AgentLLM | None = None,
) -> Graded:
    """Resolve parameters, compute the reference, run one agent session (stored with purpose
    'eval'), grade it. The reference runs on the main engine: it is the grader's own SQL from
    this repository, not the agent's, and must not be cut short by the agent's timeout."""
    resolved = resolve(q, engine)
    reference = first_row(engine, resolved.reference_sql) if resolved is not None else None
    if resolved is None or reference is None:
        return Graded(q.id, q.question, "skipped", failures=["no data to grade against yet"])
    agent = client or ScriptedAgentClient(resolved.plan, resolved.answer_template)
    result = run_agent(
        resolved.question, agent, readonly_engine=readonly_engine, engine=engine,
        settings=settings, config=config, purpose="eval",
    )  # fmt: skip
    failures = check_proposal(engine, result.session_id, q.expect, reference)
    if result.stop_reason == "answered":
        failures += grade(result.answer or "", reference, q.expect)
    else:
        failures.append(f"no answer: {result.stop_reason}")
    return Graded(
        question_id=q.id,
        question=resolved.question,
        status="failed" if failures else "passed",
        answer=result.answer,
        reference={k: _norm(v) if isinstance(v, datetime) else v for k, v in reference.items()},
        failures=failures,
        stop_reason=result.stop_reason,
        tool_calls=len(result.trace),
        input_tokens=result.usage.input_tokens,
        output_tokens=result.usage.output_tokens,
        cost_usd=result.cost_usd,
        latency_ms=result.latency_ms,
        session_id=result.session_id,
    )


@dataclass
class EvalRun:
    started_at: datetime
    model: str
    prompt_version: str
    results: list[Graded]

    @property
    def graded(self) -> list[Graded]:
        return [r for r in self.results if r.status != "skipped"]

    @property
    def accuracy(self) -> float | None:
        graded = self.graded
        return sum(r.status == "passed" for r in graded) / len(graded) if graded else None

    def summary(self) -> dict[str, Any]:
        graded = self.graded
        return {
            "started_at": self.started_at.isoformat(),
            "model": self.model,
            "prompt_version": self.prompt_version,
            "questions": len(self.results),
            "graded": len(graded),
            "skipped": len(self.results) - len(graded),
            "passed": sum(r.status == "passed" for r in graded),
            "accuracy": None if self.accuracy is None else round(self.accuracy, 4),
            "tool_calls": sum(r.tool_calls for r in graded),
            "input_tokens": sum(r.input_tokens for r in graded),
            "output_tokens": sum(r.output_tokens for r in graded),
            "cost_usd": round(sum(r.cost_usd for r in graded), 6),
            "latency_ms_total": sum(r.latency_ms for r in graded),
        }


def run_eval(
    questions: list[Question],
    *,
    readonly_engine: Engine,
    engine: Engine,
    settings: Settings,
    config: LLMConfig,
) -> EvalRun:
    started = datetime.now(UTC)
    results = [
        run_question(q, readonly_engine=readonly_engine, engine=engine, settings=settings,
                     config=config)
        for q in questions
    ]  # fmt: skip
    return EvalRun(started, ScriptedAgentClient([], "").model, config.agent.prompt_version, results)


def save_run(run: EvalRun, directory: Path = RESULTS_DIR) -> Path:
    """One JSON file per run, plus a line in history.jsonl, so scores can be compared over
    time (brief section 11)."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"eval-{run.started_at.strftime('%Y%m%dT%H%M%SZ')}.json"
    body = {"summary": run.summary(), "results": [asdict(r) for r in run.results]}
    path.write_text(json.dumps(body, indent=2, default=str) + "\n", encoding="utf-8", newline="\n")
    with (directory / "history.jsonl").open("a", encoding="utf-8", newline="\n") as history:
        history.write(json.dumps(run.summary(), default=str) + "\n")
    return path
