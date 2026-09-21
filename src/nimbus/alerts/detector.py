"""Streaming anomaly detector (brief section 9, ADR 0006): consumes the live forecast
and observation topics and publishes to `weather.alert.v1`.

State is small and recoverable *by construction*: none is kept in memory. Each event is
compared against silver (`ForecastLookup`), and an alert id is a hash of what the alert is
about, so re-detecting after a restart or a replay yields the same alert. Delivery is
insert-then-publish: the alert row is written first and marked published only once Kafka
confirms delivery, so a crash between the two republishes on restart instead of losing
the alert - and an alert already published is never sent twice."""

import logging
from collections.abc import Sequence
from datetime import UTC, datetime

import pandas as pd
from confluent_kafka import Producer
from pydantic import ValidationError
from sqlalchemy import Engine

from nimbus.alerts import rules
from nimbus.alerts.rules import Finding
from nimbus.alerts.store import (
    ForecastLookup,
    PostgresForecasts,
    insert_alerts,
    mark_published,
    unpublished,
)
from nimbus.common.config import (
    AlertsConfig,
    Location,
    load_alerts_config,
    load_locations,
    load_models_config,
)
from nimbus.common.db import make_engine
from nimbus.common.events import ALERT_EVENT_TYPE, EventEnvelope, compute_event_id
from nimbus.common.kafka import KafkaMessageLike, make_consumer, make_producer, produce_json
from nimbus.common.logging import configure_logging
from nimbus.common.schemas import AlertPayload, AlertRule
from nimbus.common.settings import get_settings
from nimbus.quality.gate import gate_frame
from nimbus.quality.schemas import silver_forecast_gate, silver_observation_gate
from nimbus.streaming import forecast_silver, observation_silver
from nimbus.streaming.cli import parse_drain_flag
from nimbus.streaming.microbatch import run_microbatch_loop

logger = logging.getLogger(__name__)

FORECAST_TOPIC = "weather.forecast.raw.v1"
OBSERVATION_TOPIC = "weather.observation.raw.v1"
ALERT_TOPIC = "weather.alert.v1"
CONSUMER_GROUP = "alert-detector"


class Detector:
    def __init__(
        self,
        lookup: ForecastLookup,
        config: AlertsConfig,
        locations: Sequence[Location],
        models: Sequence[str],
        run_cadence_hours: int,
    ) -> None:
        self._lookup = lookup
        self._config = config
        self._station_location = {loc.station: loc.id for loc in locations}
        # where sea-level pressure is not comparable across sources (see config/alerts.yaml)
        self._high_ground = {
            loc.id for loc in locations if loc.elevation_m > config.pressure_max_elevation_m
        }
        self._models = list(models)
        self._cadence = run_cadence_hours

    def detect(self, topic: str, raw: bytes) -> list[AlertPayload]:
        """Alerts for one raw event. Backfilled history and messages that fail the
        silver quality gate never alert (the gate's rejects are in the DLQ)."""
        if topic == FORECAST_TOPIC:
            return self._detect_forecast(raw)
        if topic == OBSERVATION_TOPIC:
            return self._detect_observation(raw)
        return []

    # --- forecast events: run-to-run change and model spread -------------------------

    def _detect_forecast(self, raw: bytes) -> list[AlertPayload]:
        frame = forecast_silver.message_to_frame(raw)
        if frame.empty or str(frame["ingestion_mode"].iloc[0]) != "live":
            return []
        gate = gate_frame(frame, silver_forecast_gate())
        if gate.blocked_events:
            return []
        run = gate.clean
        model = str(run["model"].iloc[0])
        location_id = str(run["location_id"].iloc[0])
        init = pd.Timestamp(run["init_time"].iloc[0])
        event_id = str(run["source_event_id"].iloc[0])
        hours = self._config.horizon_hours
        alerts: list[AlertPayload] = []

        previous = self._lookup.previous_run(model, location_id, init.to_pydatetime(), hours)
        if previous is not None:
            previous_init, previous_frame = previous
            findings = rules.run_change(
                run,
                previous_frame,
                init=init,
                previous_init=pd.Timestamp(previous_init),
                config=self._config,
            )
            for finding in findings:
                alerts.append(
                    self._alert(
                        "run_change",
                        finding,
                        location_id,
                        init,
                        event_id,
                        id_parts=(model, location_id, init.isoformat(), finding.variable),
                        model=model,
                    )
                )

        runs = {model: run}
        for other in self._models:
            if other == model:
                continue
            latest = self._lookup.latest_run(
                other, location_id, init.to_pydatetime(), hours, self._cadence
            )
            if latest is not None:
                runs[other] = latest[1]
        cycle = init.floor(f"{self._cadence}h")
        for finding in rules.model_spread(runs, init=init, config=self._config):
            if self._ignores_pressure(location_id, finding.variable):
                continue
            alerts.append(
                self._alert(
                    "model_spread",
                    finding,
                    location_id,
                    cycle,
                    event_id,
                    id_parts=(location_id, cycle.isoformat(), finding.variable),
                )
            )
        return alerts

    # --- observation events: a miss against the latest short-range forecast ----------

    def _detect_observation(self, raw: bytes) -> list[AlertPayload]:
        rows = observation_silver.message_to_rows(raw)
        if not rows or str(rows[0]["ingestion_mode"]) != "live":
            return []
        gate = gate_frame(pd.DataFrame(rows), silver_observation_gate())
        if gate.blocked_events:
            return []
        first = rows[0]
        station = str(first["station"])
        location_id = self._station_location.get(station)
        if location_id is None:
            return []
        observed_at = pd.Timestamp(first["observed_at"])
        hour = observed_at.round("h")  # METARs are filed a few minutes off the hour
        event_id = str(first["source_event_id"])
        max_lead = self._config.observation_miss_max_lead_hours
        alerts: list[AlertPayload] = []
        for row in rows:
            variable, value = str(row["variable"]), float(row["value"])
            if pd.isna(value) or self._ignores_pressure(location_id, variable):
                continue
            forecasts = self._lookup.forecasts_at(
                location_id, variable, hour.to_pydatetime(), max_lead
            )
            finding = rules.observation_miss(variable, value, forecasts, self._config)
            if finding:
                alerts.append(
                    self._alert(
                        "observation_miss",
                        finding,
                        location_id,
                        observed_at,
                        event_id,
                        id_parts=(station, observed_at.isoformat(), variable),
                        station=station,
                    )
                )
        return alerts

    def _ignores_pressure(self, location_id: str, variable: str) -> bool:
        return variable == "pressure_msl" and location_id in self._high_ground

    def _alert(
        self,
        rule: AlertRule,
        finding: Finding,
        location_id: str,
        event_time: pd.Timestamp,
        triggered_by: str,
        *,
        id_parts: tuple[str, ...],
        model: str | None = None,
        station: str | None = None,
    ) -> AlertPayload:
        return AlertPayload(
            alert_id=compute_event_id(rule, *id_parts),
            rule=rule,
            severity=finding.severity,
            location_id=location_id,
            variable=finding.variable,
            model=model,
            station=station,
            event_time=event_time.to_pydatetime(),
            metric=finding.metric,
            threshold=finding.threshold,
            details=finding.details,
            triggered_by_event_id=triggered_by,
            detected_at=datetime.now(UTC),
        )


def publish_batch(
    engine: Engine, producer: Producer, alerts: list[AlertPayload]
) -> list[AlertPayload]:
    """Record then publish; returns the alerts actually sent. See the module docstring."""
    unique = list({a.alert_id: a for a in alerts}.values())
    insert_alerts(engine, unique)
    pending_ids = unpublished(engine, [a.alert_id for a in unique])
    to_send = [a for a in unique if a.alert_id in pending_ids]
    for alert in to_send:
        envelope = EventEnvelope[AlertPayload](
            event_id=alert.alert_id,
            source="anomaly_detector",
            event_type=ALERT_EVENT_TYPE,
            produced_at=datetime.now(UTC),
            ingestion_mode="live",
            payload=alert,
        )
        produce_json(producer, ALERT_TOPIC, alert.location_id, envelope.model_dump(mode="json"))
    if to_send:
        if producer.flush(10) > 0:
            raise RuntimeError("alert delivery not confirmed; the batch will be retried")
        mark_published(engine, [a.alert_id for a in to_send])
        for alert in to_send:
            logger.info(
                "published alert",
                extra={
                    "event_id": alert.alert_id,
                    "rule": alert.rule,
                    "severity": alert.severity,
                    "location_id": alert.location_id,
                    "variable": alert.variable,
                },
            )
    return to_send


def process_batch(
    messages: Sequence[KafkaMessageLike], detector: Detector, engine: Engine, producer: Producer
) -> list[AlertPayload]:
    alerts: list[AlertPayload] = []
    for msg in messages:
        raw, topic = msg.value(), msg.topic()
        if raw is None or topic is None:
            continue
        try:
            alerts.extend(detector.detect(topic, raw))
        except (ValidationError, ValueError, KeyError, TypeError):
            # malformed events are the silver consumers' to dead-letter, not the detector's
            logger.warning("detector skipped an unparseable event", extra={"topic": topic})
    return publish_batch(engine, producer, alerts)


def main() -> None:
    drain = parse_drain_flag("Anomaly detector: live forecasts + observations -> weather.alert.v1")
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = make_engine(settings)
    models = load_models_config()
    detector = Detector(
        PostgresForecasts(engine),
        load_alerts_config(),
        load_locations(),
        [m.id for m in models.models],
        models.run_cadence_hours,
    )
    producer = make_producer(settings)
    consumer = make_consumer(settings, group_id=CONSUMER_GROUP)

    def handle_batch(messages: Sequence[KafkaMessageLike]) -> None:
        process_batch(messages, detector, engine, producer)

    # 1 s batches: an alert should follow its triggering event within seconds.
    run_microbatch_loop(
        consumer,
        [FORECAST_TOPIC, OBSERVATION_TOPIC],
        handle_batch,
        max_batch_size=50,
        max_batch_seconds=1.0,
        drain=drain,
    )


if __name__ == "__main__":
    main()
