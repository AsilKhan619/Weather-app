"""Human decisions on the agent's replay proposals (brief section 11, ADR 0009).

The agent can only file a proposal (`propose_replay`). A person approves or rejects it on the
dashboard's Replay Proposals page, which calls `decide`. Approving records the decision and
hands the person the exact runbook command; it runs nothing. A replay needs the consumer
stopped first (runbook section 3), which is a judgement about a running system - it stays with
a person, at a terminal."""

from datetime import UTC, datetime

from sqlalchemy import Engine, text


class DecisionError(ValueError):
    """The decision could not be recorded; the message says why."""


def replay_command(consumer_group: str, topic: str, from_time: datetime) -> str:
    # replay reads a time without an offset as UTC, so convert before dropping the offset
    stamp = from_time.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return (
        f'make replay ARGS="offsets --group {consumer_group} --topic {topic} --from-time {stamp}"'
    )


def decide(engine: Engine, proposal_id: int, *, approve: bool, decided_by: str, note: str) -> str:
    """Approve or reject a pending proposal; returns the new status. Only a pending proposal
    can be decided, so two people clicking at once cannot both win, and a decision is never
    silently overwritten."""
    who = decided_by.strip()
    if not who:
        raise DecisionError("say who is deciding")
    if who == "agent":
        raise DecisionError("'agent' cannot decide its own proposals")
    status = "approved" if approve else "rejected"
    with engine.begin() as conn:
        updated = conn.execute(
            text(
                "UPDATE ops.replay_proposals SET status = :status, decided_at = now(), "
                "decided_by = :who, decision_note = :note "
                "WHERE id = :id AND status = 'pending' RETURNING id"
            ),
            {"status": status, "who": who[:100], "note": note.strip()[:1000] or None,
             "id": proposal_id},
        ).first()  # fmt: skip
    if updated is None:
        raise DecisionError(f"proposal {proposal_id} is not pending (already decided?)")
    return status
