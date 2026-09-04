"""Golden fixture loading and compatibility checks for adapter implementers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .aggregate import TwinAggregate, TwinState
from .errors import IntegrityError, InvariantViolation
from .ownership import EventOwnershipPolicy, sole_event_owner
from .types import ActorContext, EventEnvelope, EventPlane, ProducerRole, parse_time


def event_from_primitive(value: dict[str, Any]) -> EventEnvelope:
    required = {
        "schema_version",
        "event_id",
        "twin_id",
        "sequence",
        "event_type",
        "plane",
        "payload",
        "occurred_at",
        "recorded_at",
        "producer",
        "idempotency_key",
        "previous_hash",
        "event_hash",
    }
    missing = required.difference(value)
    if missing:
        raise InvariantViolation(
            f"golden event missing fields: {', '.join(sorted(missing))}"
        )
    schema_version = str(value["schema_version"])
    if schema_version not in {"argo.dt.event/v1", "argo.dt.event/v2"}:
        raise InvariantViolation("unsupported golden event schema version")
    if schema_version == "argo.dt.event/v2" and "producer_role" not in value:
        raise InvariantViolation("golden v2 event missing producer_role")
    allowed = required | {"producer_role", "causation_id", "correlation_id"}
    extra = set(value).difference(allowed)
    if extra:
        raise InvariantViolation(
            f"golden event has unknown fields: {', '.join(sorted(extra))}"
        )
    role = (
        ProducerRole(str(value["producer_role"]))
        if "producer_role" in value
        else sole_event_owner(str(value["event_type"]))
    )
    return EventEnvelope(
        event_id=str(value["event_id"]),
        twin_id=str(value["twin_id"]),
        event_type=str(value["event_type"]),
        plane=EventPlane(str(value["plane"])),
        payload=dict(value["payload"]),
        occurred_at=parse_time(str(value["occurred_at"])),
        recorded_at=parse_time(str(value["recorded_at"])),
        producer=str(value["producer"]),
        producer_role=role,
        idempotency_key=str(value["idempotency_key"]),
        schema_version=schema_version,
        sequence=int(value["sequence"]),
        causation_id=value.get("causation_id"),
        correlation_id=value.get("correlation_id"),
        previous_hash=str(value["previous_hash"]),
        event_hash=str(value["event_hash"]),
    )


def load_golden_replay(path: str | Path) -> tuple[list[EventEnvelope], dict[str, Any]]:
    document = json.loads(Path(path).read_text(encoding="utf-8"))
    if document.get("fixture_version") != "argo.dt.fixture/v1":
        raise InvariantViolation("unsupported fixture version")
    events = [event_from_primitive(item) for item in document.get("events", [])]
    if not events:
        raise InvariantViolation("golden replay requires at least one event")
    previous_hash = ""
    ownership = EventOwnershipPolicy()
    for expected_sequence, event in enumerate(events, start=1):
        if event.sequence != expected_sequence:
            raise IntegrityError("golden replay sequence is not contiguous")
        event.verify(previous_hash)
        ownership.authorize(
            event,
            ActorContext(event.producer, frozenset({event.producer_role})),
        )
        previous_hash = event.event_hash
    return events, dict(document.get("expected_state", {}))


def replay_events(events: list[EventEnvelope]) -> TwinState:
    state = TwinState(twin_id=events[0].twin_id)
    for event in events:
        TwinAggregate.apply(state, event)
    return state
