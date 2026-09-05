"""Deterministic projection from events into current and as-of twin state."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from .errors import IntegrityError, InvariantViolation
from .ports import EventStore, SnapshotStore
from .types import (
    EpistemicVector,
    EventEnvelope,
    EventPlane,
    Sensitivity,
    SnapshotRecord,
    parse_time,
    to_primitive,
)


@dataclass(slots=True)
class TwinState:
    twin_id: str
    subject_id: str | None = None
    sequence: int = 0
    evidence: dict[str, dict[str, Any]] = field(default_factory=dict)
    claims: dict[str, dict[str, Any]] = field(default_factory=dict)
    accepted_claim_ids: set[str] = field(default_factory=set)
    contested_claim_ids: set[str] = field(default_factory=set)
    contradictions: dict[str, dict[str, Any]] = field(default_factory=dict)
    issued_projections: dict[str, dict[str, Any]] = field(default_factory=dict)
    revoked_projection_ids: set[str] = field(default_factory=set)
    stale_claim_ids: set[str] = field(default_factory=set)
    stale_projection_ids: set[str] = field(default_factory=set)
    degraded_reasons: list[str] = field(default_factory=list)
    last_event_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        return to_primitive(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> TwinState:
        required = {
            "twin_id",
            "subject_id",
            "sequence",
            "evidence",
            "claims",
            "accepted_claim_ids",
            "contested_claim_ids",
            "contradictions",
            "issued_projections",
            "revoked_projection_ids",
            "stale_claim_ids",
            "stale_projection_ids",
            "degraded_reasons",
            "last_event_hash",
        }
        if set(value) != required:
            raise InvariantViolation("snapshot state fields do not match TwinState v1")
        try:
            state = cls(
                twin_id=str(value["twin_id"]),
                subject_id=(
                    str(value["subject_id"])
                    if value["subject_id"] is not None
                    else None
                ),
                sequence=int(value["sequence"]),
                evidence={
                    str(key): dict(item)
                    for key, item in dict(value["evidence"]).items()
                },
                claims={
                    str(key): dict(item)
                    for key, item in dict(value["claims"]).items()
                },
                accepted_claim_ids=set(value["accepted_claim_ids"]),
                contested_claim_ids=set(value["contested_claim_ids"]),
                contradictions={
                    str(key): dict(item)
                    for key, item in dict(value["contradictions"]).items()
                },
                issued_projections={
                    str(key): dict(item)
                    for key, item in dict(value["issued_projections"]).items()
                },
                revoked_projection_ids=set(value["revoked_projection_ids"]),
                stale_claim_ids=set(value["stale_claim_ids"]),
                stale_projection_ids=set(value["stale_projection_ids"]),
                degraded_reasons=[str(item) for item in value["degraded_reasons"]],
                last_event_hash=str(value["last_event_hash"]),
            )
        except (TypeError, ValueError) as exc:
            raise InvariantViolation("snapshot state has invalid field types") from exc
        if state.sequence < 1 or not state.twin_id or not state.last_event_hash:
            raise InvariantViolation("snapshot state has an invalid stream position")
        return state

    def to_snapshot(self) -> SnapshotRecord:
        return SnapshotRecord.new(
            twin_id=self.twin_id,
            sequence=self.sequence,
            last_event_hash=self.last_event_hash,
            state=self.to_dict(),
        )

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
    PROJECTION_EVENTS = {"ProjectionIssued", "ProjectionRevoked"}
    EPISTEMIC_DIMENSIONS = {
        "evidence_quality",
        "confidence",
        "salience",
        "stability",
        "freshness",
        "scope_confidence",
        "contradiction_load",
    }

    @staticmethod
    def _bind_subject(state: TwinState, subject_id: str) -> None:
        if not subject_id:
            raise InvariantViolation("subject_id is required")
        if state.subject_id is not None and state.subject_id != subject_id:
            raise InvariantViolation("event subject does not match twin subject")
        state.subject_id = subject_id

    @staticmethod
    def validate_epistemic(value: Any) -> None:
        if not isinstance(value, dict) or set(value) != TwinAggregate.EPISTEMIC_DIMENSIONS:
            raise InvariantViolation(
                "epistemic vector requires exactly seven independent dimensions"
            )
        try:
            EpistemicVector(**value)
        except (TypeError, ValueError) as exc:
            raise InvariantViolation("epistemic dimensions must be numeric") from exc

    @staticmethod
    def validate_event(event: EventEnvelope) -> None:
        payload = event.payload
        if not event.twin_id or not event.producer or not event.idempotency_key:
            raise InvariantViolation("event twin_id, producer, and idempotency_key are required")
        if event.schema_version not in {"argo.dt.event/v1", "argo.dt.event/v2"}:
            raise InvariantViolation("unsupported event schema version")
        if event.schema_version == "argo.dt.event/v1" and event.sequence == 0:
            raise InvariantViolation("new writes must use EventEnvelope v2")
        if event.occurred_at.tzinfo is None:
            raise InvariantViolation("event occurred_at must be timezone-aware")
        if event.plane is EventPlane.PROJECTION and event.event_type not in (
            TwinAggregate.PROJECTION_EVENTS
        ):
            raise InvariantViolation("projection plane accepts projection events only")
        if (
            event.event_type in TwinAggregate.PROJECTION_EVENTS
            and event.plane is not EventPlane.PROJECTION
        ):
            raise InvariantViolation("projection events must use the projection plane")
        if event.event_type == "EvidenceIngested":
            required = {
                "evidence_id",
                "subject_id",
                "content_hash",
                "source",
                "rights",
                "sensitivity",
                "independence_group",
            }
            missing = required.difference(payload)
            if missing:
                raise InvariantViolation(
                    f"EvidenceIngested missing fields: {', '.join(sorted(missing))}"
                )
            if any(not payload[field] for field in required - {"rights"}):
                raise InvariantViolation("EvidenceIngested required fields cannot be empty")
            if not isinstance(payload["rights"], dict):
                raise InvariantViolation("EvidenceIngested rights must be an object")
            try:
                Sensitivity(str(payload["sensitivity"]))
            except ValueError as exc:
                raise InvariantViolation("EvidenceIngested sensitivity is invalid") from exc
        if event.event_type == "ClaimProposed":
            provenance = payload.get("provenance")
            if not isinstance(provenance, list) or not provenance:
                raise InvariantViolation("ClaimProposed requires non-empty provenance")
            evidence_ids = [
                ref.get("evidence_id")
                for ref in provenance
                if isinstance(ref, dict)
            ]
            if len(evidence_ids) != len(provenance) or any(not item for item in evidence_ids):
                raise InvariantViolation("every provenance reference requires evidence_id")
            if len(set(evidence_ids)) != len(evidence_ids):
                raise InvariantViolation("duplicate evidence cannot count as independent support")
            for ref in provenance:
                if not ref.get("relation") or not ref.get("independence_group"):
                    raise InvariantViolation(
                        "every provenance reference requires relation and independence_group"
                    )
            if (
                not payload.get("claim_id")
                or not payload.get("subject_id")
                or not payload.get("statement")
            ):
                raise InvariantViolation(
                    "ClaimProposed requires claim_id, subject_id, and statement"
                )
            TwinAggregate.validate_epistemic(payload.get("epistemic"))
        if event.event_type == "EvidenceDeleted" and not payload.get("evidence_id"):
            raise InvariantViolation("EvidenceDeleted requires evidence_id")
        if (
            event.event_type in TwinAggregate.CLAIM_EVENTS - {"ClaimProposed"}
            and not payload.get("claim_id")
        ):
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
            cls._bind_subject(state, str(payload["subject_id"]))
            evidence_id = str(payload["evidence_id"])
            if evidence_id in state.evidence:
                raise InvariantViolation(
                    "evidence identity already exists; append a correction event"
                )
            state.evidence[evidence_id] = payload
        elif event_type == "EvidenceDeleted":
            evidence_id = str(payload["evidence_id"])
            if evidence_id not in state.evidence:
                raise InvariantViolation("cannot delete unknown evidence")
            state.evidence.pop(evidence_id)
            for claim_id, claim in state.claims.items():
                refs = claim.get("provenance", [])
                if any(ref.get("evidence_id") == evidence_id for ref in refs):
                    state.stale_claim_ids.add(claim_id)
            for projection_id, projection in state.issued_projections.items():
                source_claim_ids = set(projection.get("source_claim_ids", []))
                if source_claim_ids.intersection(state.stale_claim_ids):
                    state.stale_projection_ids.add(projection_id)
        elif event_type == "ClaimProposed":
            cls._bind_subject(state, str(payload["subject_id"]))
            for ref in payload["provenance"]:
                evidence_id = str(ref["evidence_id"])
                if evidence_id not in state.evidence:
                    raise InvariantViolation("claim provenance references unknown evidence")
                expected_group = state.evidence[evidence_id].get("independence_group")
                if ref.get("independence_group") != expected_group:
                    raise InvariantViolation(
                        "claim provenance independence group does not match evidence"
                    )
            claim_id = str(payload["claim_id"])
            if claim_id in state.claims:
                raise InvariantViolation(
                    "claim identity already exists; append a supersession event"
                )
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
            if claim_id not in state.claims:
                raise InvariantViolation("cannot retire an unknown claim")
            state.accepted_claim_ids.discard(claim_id)
        elif event_type == "ClaimSuperseded":
            claim_id = str(payload["claim_id"])
            successor_id = str(payload.get("successor_claim_id", ""))
            if not successor_id:
                raise InvariantViolation("ClaimSuperseded requires successor_claim_id")
            if claim_id not in state.claims or successor_id not in state.claims:
                raise InvariantViolation("supersession requires known predecessor and successor")
            state.accepted_claim_ids.discard(claim_id)
        elif event_type == "ContradictionDetected":
            contradiction_id = str(payload["contradiction_id"])
            if contradiction_id in state.contradictions:
                raise InvariantViolation("contradiction identity already exists")
            state.contradictions[contradiction_id] = payload
        elif event_type == "ProjectionRevoked":
            projection_id = str(payload["projection_id"])
            if projection_id not in state.issued_projections:
                raise InvariantViolation("cannot revoke an unknown projection")
            state.revoked_projection_ids.add(projection_id)
        elif event_type == "ProjectionIssued":
            if state.subject_id is None:
                raise InvariantViolation("cannot issue a projection for an unbound twin")
            projection_id = str(payload["projection_id"])
            if projection_id in state.issued_projections:
                raise InvariantViolation("projection identity already exists")
            state.issued_projections[projection_id] = payload
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
        snapshot_store: SnapshotStore | None = None,
        up_to_sequence: int | None = None,
        as_of_recorded_time: datetime | None = None,
    ) -> TwinState:
        state = TwinState(twin_id=twin_id)
        after_sequence = 0
        previous_hash = ""
        if snapshot_store is not None and as_of_recorded_time is None:
            snapshot = snapshot_store.load_snapshot(
                twin_id,
                up_to_sequence=up_to_sequence,
            )
            if snapshot is not None:
                snapshot.verify()
                state = TwinState.from_dict(dict(snapshot.state))
                if (
                    state.twin_id != snapshot.twin_id
                    or state.sequence != snapshot.sequence
                    or state.last_event_hash != snapshot.last_event_hash
                ):
                    raise InvariantViolation("snapshot state does not match its stream link")
                after_sequence = snapshot.sequence
                previous_hash = snapshot.last_event_hash
        events = store.load(
            twin_id,
            after_sequence=after_sequence,
            up_to_sequence=up_to_sequence,
            planes=frozenset({EventPlane.AUTHORITATIVE, EventPlane.PROJECTION}),
        )
        for event in events:
            if as_of_recorded_time is not None and event.recorded_at > as_of_recorded_time:
                break
            expected_sequence = state.sequence + 1
            if event.sequence != expected_sequence:
                raise IntegrityError(
                    f"replay sequence gap: expected {expected_sequence}, found {event.sequence}"
                )
            event.verify(previous_hash)
            cls.apply(state, event)
            previous_hash = event.event_hash
        return state
