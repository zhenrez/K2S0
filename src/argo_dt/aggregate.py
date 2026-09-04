"""Deterministic projection from events into current and as-of twin state."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .errors import InvariantViolation
from .ports import EventStore
from .types import EventEnvelope, EventPlane, Sensitivity, parse_time


@dataclass(slots=True)
class TwinState:
    twin_id: str
    sequence: int = 0
    evidence: dict[str, dict[str, Any]] = field(default_factory=dict)
    claims: dict[str, dict[str, Any]] = field(default_factory=dict)
    accepted_claim_ids: set[str] = field(default_factory=set)
    contested_claim_ids: set[str] = field(default_factory=set)
    contradictions: dict[str, dict[str, Any]] = field(default_factory=dict)
    revoked_projection_ids: set[str] = field(default_factory=set)
    stale_claim_ids: set[str] = field(default_factory=set)
    degraded_reasons: list[str] = field(default_factory=list)
    last_event_hash: str = ""

    def accepted_claims(
        self,
        *,
        valid_at: datetime | None = None,
        known_at: datetime | None = None,
        max_sensitivity: Sensitivity | None = None,
    ) -> list[dict[str, Any]]:
        selected: list[dict[str, Any]] = []
        for claim_id in sorted(self.accepted_claim_ids):
            if claim_id in self.stale_claim_ids:
                continue
            claim = self.claims[claim_id]
            if max_sensitivity is not None:
                sensitivity = Sensitivity(claim.get("sensitivity", Sensitivity.INTERNAL.value))
                if sensitivity.rank > max_sensitivity.rank:
                    continue
            if valid_at is not None:
                start = parse_time(claim["valid_from"])
                end_raw = claim.get("valid_until")
                end = parse_time(end_raw) if end_raw else None
                if valid_at < start or (end is not None and valid_at >= end):
                    continue
            if known_at is not None:
                recorded_raw = claim.get("recorded_at")
                if recorded_raw and parse_time(recorded_raw) > known_at:
                    continue
            selected.append(dict(claim))
        return selected


class TwinAggregate:
    """Applies authoritative facts without allowing simulation contamination."""

    CLAIM_EVENTS = {
        "ClaimProposed",
        "ClaimAccepted",
        "ClaimContested",
        "ClaimRetired",
        "ClaimSuperseded",
    }

    @staticmethod
    def validate_event(event: EventEnvelope) -> None:
        payload = event.payload
        if not event.twin_id or not event.producer or not event.idempotency_key:
            raise InvariantViolation("event twin_id, producer, and idempotency_key are required")
        if event.event_type == "EvidenceIngested":
            required = {"evidence_id", "content_hash", "source", "rights", "sensitivity"}
            missing = required.difference(payload)
            if missing:
                raise InvariantViolation(
                    f"EvidenceIngested missing fields: {', '.join(sorted(missing))}"
                )
        if event.event_type == "ClaimProposed":
            provenance = payload.get("provenance")
            if not isinstance(provenance, list) or not provenance:
                raise InvariantViolation("ClaimProposed requires non-empty provenance")
            if not payload.get("claim_id") or not payload.get("statement"):
                raise InvariantViolation("ClaimProposed requires claim_id and statement")
        if event.event_type == "EvidenceDeleted" and not payload.get("evidence_id"):
            raise InvariantViolation("EvidenceDeleted requires evidence_id")
        if event.event_type in TwinAggregate.CLAIM_EVENTS - {"ClaimProposed"}:
            if not payload.get("claim_id"):
                raise InvariantViolation(f"{event.event_type} requires claim_id")
        if event.event_type == "ProjectionIssued":
            required = {"projection_id", "purpose", "recipient_id", "receipt_hash"}
            missing = required.difference(payload)
            if missing:
                raise InvariantViolation(
                    f"ProjectionIssued missing fields: {', '.join(sorted(missing))}"
                )
        if event.event_type == "ProjectionRevoked" and not payload.get("projection_id"):
            raise InvariantViolation("ProjectionRevoked requires projection_id")

    @classmethod
    def apply(cls, state: TwinState, event: EventEnvelope) -> None:
        cls.validate_event(event)
        if event.plane is EventPlane.SIMULATION:
            raise InvariantViolation("simulation events cannot mutate authoritative state")
        payload = dict(event.payload)
        event_type = event.event_type

        if event_type == "EvidenceIngested":
            state.evidence[str(payload["evidence_id"])] = payload
        elif event_type == "EvidenceDeleted":
            evidence_id = str(payload["evidence_id"])
            state.evidence.pop(evidence_id, None)
            for claim_id, claim in state.claims.items():
                refs = claim.get("provenance", [])
                if any(ref.get("evidence_id") == evidence_id for ref in refs):
                    state.stale_claim_ids.add(claim_id)
        elif event_type == "ClaimProposed":
            claim_id = str(payload["claim_id"])
            claim = dict(payload)
            claim["recorded_at"] = event.recorded_at.isoformat()
            state.claims[claim_id] = claim
        elif event_type == "ClaimAccepted":
            claim_id = str(payload["claim_id"])
            if claim_id not in state.claims:
                raise InvariantViolation("cannot accept an unknown claim")
            state.accepted_claim_ids.add(claim_id)
            state.contested_claim_ids.discard(claim_id)
        elif event_type == "ClaimContested":
            claim_id = str(payload["claim_id"])
            if claim_id not in state.claims:
                raise InvariantViolation("cannot contest an unknown claim")
            state.contested_claim_ids.add(claim_id)
            state.accepted_claim_ids.discard(claim_id)
        elif event_type == "ClaimRetired":
            claim_id = str(payload["claim_id"])
            state.accepted_claim_ids.discard(claim_id)
        elif event_type == "ClaimSuperseded":
            claim_id = str(payload["claim_id"])
            successor_id = str(payload.get("successor_claim_id", ""))
            if not successor_id:
                raise InvariantViolation("ClaimSuperseded requires successor_claim_id")
            state.accepted_claim_ids.discard(claim_id)
        elif event_type == "ContradictionDetected":
            contradiction_id = str(payload["contradiction_id"])
            state.contradictions[contradiction_id] = payload
        elif event_type == "ProjectionRevoked":
            state.revoked_projection_ids.add(str(payload["projection_id"]))
        elif event_type == "DegradationDeclared":
            state.degraded_reasons.append(str(payload["reason"]))
        elif event_type == "DegradationCleared":
            reason = str(payload["reason"])
            state.degraded_reasons = [item for item in state.degraded_reasons if item != reason]

        state.sequence = event.sequence
        state.last_event_hash = event.event_hash

    @classmethod
    def rebuild(
        cls,
        store: EventStore,
        twin_id: str,
        *,
        up_to_sequence: int | None = None,
        as_of_recorded_time: datetime | None = None,
    ) -> TwinState:
        state = TwinState(twin_id=twin_id)
        events = store.load(
            twin_id,
            up_to_sequence=up_to_sequence,
            planes=frozenset({EventPlane.AUTHORITATIVE, EventPlane.PROJECTION}),
        )
        for event in events:
            if as_of_recorded_time is not None and event.recorded_at > as_of_recorded_time:
                break
            cls.apply(state, event)
        return state
