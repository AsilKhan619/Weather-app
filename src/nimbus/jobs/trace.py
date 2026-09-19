"""`make trace EVENT_ID=...`: follow one event through the platform (brief section 7,
"Lineage"): API request -> topic/partition/offset -> bronze file -> silver rows ->
the gold verification rows and daily accuracy metrics it contributed to.

Every stage is looked up by the event's *natural keys*, recomputed from the bronze
payload with the same transform silver uses, not by `source_event_id` alone.
`source_event_id` names only the latest event to write a row, so a later event that
overwrote some rows (a correction, an overlapping backfill window) would otherwise
make this event look as if it never reached silver. The trace reports both."""

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import Engine, text

from nimbus.common.db import make_engine
from nimbus.common.logging import configure_logging
from nimbus.common.settings import get_settings
from nimbus.jobs.reconcile import TOPIC_SPECS, TopicSpec
from nimbus.streaming.bronze_reader import DEFAULT_LAKE_ROOT, list_bronze_files

# Where each event source's data comes from, for the "API request" line.
_API_BY_EVENT_TYPE = {
    "forecast.raw": "Open-Meteo Single Runs API (live run)",
    "forecast.backfill.raw": "Open-Meteo Previous Runs API (backfill)",
    "observation.raw": "aviationweather.gov METAR API or IEM ASOS archive",
}

# natural-key column -> Postgres array element type, per silver table
_KEY_TYPES: dict[str, dict[str, str]] = {
    "silver.forecast": {
        "model": "text",
        "location_id": "text",
        "init_time": "timestamptz",
        "valid_time": "timestamptz",
        "variable": "text",
    },
    "silver.observation": {"station": "text", "observed_at": "timestamptz", "variable": "text"},
}

# How the keys join to gold.forecast_verification (alias f) from a key table (alias k).
_GOLD_JOIN = {
    "silver.forecast": (
        "f.model = k.model AND f.location_id = k.location_id AND f.init_time = k.init_time "
        "AND f.valid_time = k.valid_time AND f.variable = k.variable"
    ),
    "silver.observation": (
        "f.observed_at = k.observed_at AND f.variable = k.variable AND f.location_id IN "
        "(SELECT d.location_id FROM silver.dim_location d WHERE d.station = k.station)"
    ),
}


@dataclass(frozen=True)
class BronzeHit:
    topic: str
    partition: int
    offset: int
    kafka_timestamp: datetime
    file: Path
    envelope: dict[str, Any]
    raw: bytes


@dataclass
class Trace:
    event_id: str
    bronze: list[BronzeHit] = field(default_factory=list)
    ingestion_run: dict[str, Any] | None = None
    silver_table: str | None = None
    silver_expected: int = 0
    silver_by_event: dict[str, int] = field(default_factory=dict)
    gold_verification: int = 0
    gold_accuracy: list[dict[str, Any]] = field(default_factory=list)

    @property
    def silver_current(self) -> int:
        return self.silver_by_event.get(self.event_id, 0)


def find_in_bronze(event_id: str, lake_root: Path = DEFAULT_LAKE_ROOT) -> list[BronzeHit]:
    """Every bronze copy of the event (a redelivery appears more than once). The
    cheap substring test on the raw bytes avoids parsing every message in the lake."""
    needle = event_id.encode()
    hits: list[BronzeHit] = []
    for topic in TOPIC_SPECS:
        for path in list_bronze_files(topic, lake_root):
            table = pq.read_table(path)
            values = table.column("value").to_pylist()
            for i, raw in enumerate(values):
                if raw is None or needle not in raw:
                    continue
                envelope = json.loads(raw)
                if envelope.get("event_id") != event_id:
                    continue
                hits.append(
                    BronzeHit(
                        topic=topic,
                        partition=table.column("kafka_partition")[i].as_py(),
                        offset=table.column("kafka_offset")[i].as_py(),
                        kafka_timestamp=datetime.fromtimestamp(
                            table.column("kafka_timestamp_ms")[i].as_py() / 1000, tz=UTC
                        ),
                        file=path,
                        envelope=envelope,
                        raw=raw,
                    )
                )
    return hits


def sample_event_id(topic: str, lake_root: Path = DEFAULT_LAKE_ROOT) -> str | None:
    """The first event of the newest bronze file - something to trace in a demo."""
    files = list_bronze_files(topic, lake_root)
    if not files:
        return None
    values = pq.read_table(files[-1]).column("value").to_pylist()
    return str(json.loads(values[0])["event_id"]) if values and values[0] else None


def _key_arrays(frame: pd.DataFrame, columns: dict[str, str]) -> dict[str, list[Any]]:
    keys = frame[list(columns)].drop_duplicates()
    arrays: dict[str, list[Any]] = {}
    for column, pg_type in columns.items():
        values = keys[column]
        if pg_type == "timestamptz":
            arrays[column] = [ts.to_pydatetime() for ts in pd.to_datetime(values, utc=True)]
        else:
            arrays[column] = [str(v) for v in values]
    return arrays


def _keys_relation(columns: dict[str, str]) -> str:
    arrays = ", ".join(f"CAST(:{c} AS {t}[])" for c, t in columns.items())
    return f"unnest({arrays}) AS k({', '.join(columns)})"


def trace_event(engine: Engine, event_id: str, lake_root: Path = DEFAULT_LAKE_ROOT) -> Trace:
    trace = Trace(event_id)
    trace.bronze = find_in_bronze(event_id, lake_root)
    if not trace.bronze:
        return trace

    first = trace.bronze[0]
    spec: TopicSpec = TOPIC_SPECS[first.topic]
    trace.silver_table = spec.table
    columns = _KEY_TYPES[spec.table]
    frame = spec.to_frame(first.raw)
    trace.silver_expected = len(frame.drop_duplicates(subset=list(columns)))
    arrays = _key_arrays(frame, columns)
    keys = _keys_relation(columns)
    key_match = " AND ".join(f"s.{c} = k.{c}" for c in columns)

    produced_at = datetime.fromisoformat(first.envelope["produced_at"])
    with engine.connect() as conn:
        run = (
            conn.execute(
                text(
                    "SELECT id, source, ingestion_mode, status, started_at, finished_at, "
                    "messages_produced FROM ops.ingestion_runs "
                    "WHERE source = :source AND started_at <= :at AND finished_at >= :at "
                    "ORDER BY started_at DESC LIMIT 1"
                ),
                {"source": first.envelope["source"], "at": produced_at},
            )
            .mappings()
            .first()
        )
        trace.ingestion_run = dict(run) if run else None

        # Table names below come from the constants above, never from input.
        by_event = conn.execute(
            text(
                f"SELECT s.source_event_id, count(*) FROM {spec.table} s "
                f"JOIN {keys} ON {key_match} GROUP BY 1"
            ),
            arrays,
        ).all()
        trace.silver_by_event = {str(event): int(count) for event, count in by_event}

        verification = (
            f"SELECT f.* FROM gold.forecast_verification f JOIN {keys} ON {_GOLD_JOIN[spec.table]}"
        )
        trace.gold_verification = int(
            conn.execute(text(f"SELECT count(*) FROM ({verification}) v"), arrays).scalar_one()
        )
        accuracy = conn.execute(
            text(
                f"WITH v AS ({verification}) "
                "SELECT a.valid_date, a.location_id, a.model, a.variable, a.lead_day, "
                "a.n, a.bias, a.mae, a.rmse FROM gold.accuracy_daily a WHERE EXISTS ("
                "SELECT 1 FROM v WHERE (v.valid_time AT TIME ZONE 'UTC')::date = a.valid_date "
                "AND v.location_id = a.location_id AND v.model = a.model "
                "AND v.variable = a.variable AND v.lead_day = a.lead_day) "
                "ORDER BY a.valid_date, a.location_id, a.model, a.variable, a.lead_day"
            ),
            arrays,
        )
        trace.gold_accuracy = [dict(row) for row in accuracy.mappings()]
    return trace


def _describe_payload(envelope: dict[str, Any]) -> str:
    payload = envelope.get("payload", {})
    parts = [
        f"{key}={payload[key]}"
        for key in (
            "model",
            "location_id",
            "run",
            "start_date",
            "end_date",
            "station",
            "observed_at",
        )
        if key in payload
    ]
    return ", ".join(parts)


def format_trace(trace: Trace) -> str:
    lines = [f"event {trace.event_id}", ""]
    if not trace.bronze:
        lines.append("1. bronze     NOT FOUND in the lake (never consumed by the bronze sink?)")
        return "\n".join(lines)

    first = trace.bronze[0]
    envelope = first.envelope
    api = _API_BY_EVENT_TYPE.get(envelope["event_type"], envelope["event_type"])
    lines += [
        f"1. API request   {api}",
        f"                 source={envelope['source']}  mode={envelope['ingestion_mode']}  "
        f"produced_at={envelope['produced_at']}",
        f"                 {_describe_payload(envelope)}",
    ]
    run = trace.ingestion_run
    if run:
        lines.append(
            f"                 ingestion run #{run['id']}: {run['status']}, "
            f"{run['messages_produced']} message(s) produced, "
            f"{run['started_at']:%Y-%m-%d %H:%M} UTC"
        )
    lines += ["", "2. Kafka + bronze"]
    for hit in trace.bronze:
        lines += [
            f"   {hit.topic}  partition {hit.partition}  offset {hit.offset}  "
            f"@ {hit.kafka_timestamp:%Y-%m-%d %H:%M:%S} UTC",
            f"   bronze file: {hit.file}",
        ]
    if len(trace.bronze) > 1:
        lines.append(f"   ({len(trace.bronze)} copies in bronze: the message was redelivered)")

    lines += ["", f"3. silver ({trace.silver_table})"]
    lines.append(
        f"   this event yields {trace.silver_expected} row(s); "
        f"{trace.silver_current} still carry its source_event_id"
    )
    for event, count in sorted(trace.silver_by_event.items()):
        if event != trace.event_id:
            lines.append(f"   {count} row(s) since overwritten by event {event[:16]}...")
    missing = trace.silver_expected - sum(trace.silver_by_event.values())
    if missing:
        lines.append(f"   {missing} row(s) NOT in silver (quarantined by a quality check, or lost)")

    lines += ["", "4. gold"]
    lines.append(f"   {trace.gold_verification} row(s) in gold.forecast_verification")
    if trace.gold_accuracy:
        lines.append(f"   contributes to {len(trace.gold_accuracy)} gold.accuracy_daily row(s):")
        for row in trace.gold_accuracy[:12]:
            lines.append(
                f"     {row['valid_date']}  {row['location_id']:<16}{row['model']:<14}"
                f"{row['variable']:<16}lead {row['lead_day']}  n={row['n']:<4}"
                f"bias={row['bias']:+.3f}  mae={row['mae']:.3f}  rmse={row['rmse']:.3f}"
            )
        if len(trace.gold_accuracy) > 12:
            lines.append(f"     ... and {len(trace.gold_accuracy) - 12} more")
    else:
        lines.append(
            "   not (yet) part of any gold metric - unverified, or `make gold` has not run"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Trace one event bronze -> silver -> gold.")
    parser.add_argument("--event-id", help="the event_id to trace")
    parser.add_argument(
        "--sample",
        choices=["forecast", "observation"],
        help="trace the first event of the newest bronze file for this kind",
    )
    parser.add_argument("--lake-root", type=Path, default=DEFAULT_LAKE_ROOT)
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)

    event_id = args.event_id
    if not event_id and args.sample:
        event_id = sample_event_id(f"weather.{args.sample}.raw.v1", args.lake_root)
    if not event_id:
        parser.error("give --event-id, or --sample when the lake has data")

    trace = trace_event(make_engine(settings), event_id, args.lake_root)
    print(format_trace(trace))
    if not trace.bronze:
        sys.exit(1)


if __name__ == "__main__":
    main()
