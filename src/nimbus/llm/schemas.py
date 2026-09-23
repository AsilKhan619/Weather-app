"""The briefing the LLM must return, and the event published for it (brief section 10).

`BriefingOutput` is both the structured-output schema sent to the API and the Pydantic
model its reply is validated against. Some limits (lengths, list size) cannot be enforced
by the API's constrained decoding - the SDK moves them into the schema's descriptions - so
they are checked here, after the fact, and a violation is an invalid output that gets one
retry (ADR 0008)."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Confidence = Literal["high", "medium", "low"]
Trigger = Literal["daily", "alert", "manual"]


class BriefingOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    headline: str = Field(
        min_length=1, max_length=120, description="One line, the most useful thing to know."
    )
    summary: str = Field(
        min_length=1,
        max_length=900,
        description="Two to four sentences on the next 48 hours, using only fact-sheet numbers.",
    )
    confidence: Confidence = Field(
        description="Copy the fact sheet's confidence.level exactly; explain it, never change it."
    )
    most_reliable_model: str = Field(
        description=(
            "The fact sheet's accuracy.most_accurate_model when present, else one of models."
        )
    )
    reliable_reason: str = Field(
        min_length=1, max_length=300, description="Why, citing the fact sheet's accuracy numbers."
    )
    notable_risks: list[str] = Field(
        max_length=5, description="At most five short risks; empty if nothing stands out."
    )


class BriefingPayload(BaseModel):
    """weather.briefing.v1 payload: the briefing plus what it was built from."""

    briefing_id: str
    location_id: str
    as_of: datetime
    trigger: Trigger
    trigger_ref: str | None
    model: str
    prompt_version: str
    fact_sheet_hash: str
    headline: str
    summary: str
    confidence: Confidence
    most_reliable_model: str
    reliable_reason: str
    notable_risks: list[str]
