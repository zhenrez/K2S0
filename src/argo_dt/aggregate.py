"""Deterministic projection from events into current and as-of twin state."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast

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
    entity_links: dict[str, dict[str, Any]] = field(default_factory=dict)
    unlinked_entity_ids: set[str] = field(default_factory=set)
    stale_entity_link_ids: set[str] = field(default_factory=set)
    claims: dict[str, dict[str, Any]] = field(default_factory=dict)
    accepted_claim_ids: set[str] = field(default_factory=set)
    contested_claim_ids: set[str] = field(default_factory=set)
    retired_claim_ids: set[str] = field(default_factory=set)
    superseded_evidence_ids: set[str] = field(default_factory=set)
    superseded_claim_ids: set[str] = field(default_factory=set)
    contradictions: dict[str, dict[str, Any]] = field(default_factory=dict)
    adjudications: dict[str, dict[str, Any]] = field(default_factory=dict)
    resolved_contradiction_ids: set[str] = field(default_factory=set)
    corrections: dict[str, dict[str, Any]] = field(default_factory=dict)
    issued_projections: dict[str, dict[str, Any]] = field(default_factory=dict)
    revoked_projection_ids: set[str] = field(default_factory=set)
    stale_claim_ids: set[str] = field(default_factory=set)
    stale_projection_ids: set[str] = field(default_factory=set)
    degraded_reasons: list[str] = field(default_factory=list)
    last_event_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        return cast(dict[str, Any], to_primitive(self))

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> TwinState:
        legacy_required = {
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
        added = {
            "entity_links",
            "unlinked_entity_ids",
            "stale_entity_link_ids",
            "retired_claim_ids",
            "superseded_evidence_ids",
            "superseded_claim_ids",
            "adjudications",
            "resolved_contradiction_ids",
            "corrections",
        }
        if not legacy_required.issubset(value) or set(value).difference(legacy_required | added):
            raise InvariantViolation("snapshot state fields do not match a supported TwinState")
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
                entity_links={
                    str(key): dict(item)
                    for key, item in dict(value.get("entity_links", {})).items()
                },
                unlinked_entity_ids=set(value.get("unlinked_entity_ids", [])),
                stale_entity_link_ids=set(value.get("stale_entity_link_ids", [])),
                claims={
                    str(key): dict(item)
                    for key, item in dict(value["claims"]).items()
                },
                accepted_claim_ids=set(value["accepted_claim_ids"]),
                contested_claim_ids=set(value["contested_claim_ids"]),
                retired_claim_ids=set(value.get("retired_claim_ids", [])),
                superseded_evidence_ids=set(value.get("superseded_evidence_ids", [])),
                superseded_claim_ids=set(value.get("superseded_claim_ids", [])),
                contradictions={
                    str(key): dict(item)
                    for key, item in dict(value["contradictions"]).items()
                },
                adjudications={
                    str(key): dict(item)
                    for key, item in dict(value.get("adjudications", {})).items()
                },
                resolved_contradiction_ids=set(
                    value.get("resolved_contradiction_ids", [])
                ),
                corrections={
                    str(key): dict(item)
                    for key, item in dict(value.get("corrections", {})).items()
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

    @staticmethod
    def _valid_at(value: dict[str, Any], instant: datetime) -> bool:
        start_raw = value.get("valid_from")
        if not start_raw:
            return True
        start = parse_time(str(start_raw))
        end_raw = value.get("valid_until")
        end = parse_time(str(end_raw)) if end_raw else None
        return start <= instant and (end is None or instant < end)

    def select_valid_at(self, instant: datetime) -> TwinState:
        """Return the current knowledge restricted to facts valid at ``instant``."""

        if instant.tzinfo is None:
            raise InvariantViolation("valid-time query must be timezone-aware")
        selected = copy.deepcopy(self)
        selected.evidence = {
            key: value
            for key, value in selected.evidence.items()
            if self._valid_at(value, instant)
        }
        selected.entity_links = {
            key: value
            for key, value in selected.entity_links.items()
            if self._valid_at(value, instant)
        }
        entity_link_ids = set(selected.entity_links)
        selected.unlinked_entity_ids.intersection_update(entity_link_ids)
        selected.stale_entity_link_ids.intersection_update(entity_link_ids)
        selected.claims = {
            key: value
            for key, value in selected.claims.items()
            if self._valid_at(value, instant)
        }
        claim_ids = set(selected.claims)
        selected.accepted_claim_ids.intersection_update(claim_ids)
        selected.contested_claim_ids.intersection_update(claim_ids)
        selected.retired_claim_ids.intersection_update(claim_ids)
        selected.superseded_claim_ids.intersection_update(claim_ids)
        selected.stale_claim_ids.intersection_update(claim_ids)
        selected.contradictions = {
            key: value
            for key, value in selected.contradictions.items()
            if set(str(item) for item in value.get("claim_ids", [])).issubset(claim_ids)
        }
        contradiction_ids = set(selected.contradictions)
        selected.resolved_contradiction_ids.intersection_update(contradiction_ids)
        selected.adjudications = {
            key: value
            for key, value in selected.adjudications.items()
            if str(value.get("contradiction_id", "")) in contradiction_ids
        }
        return selected

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
            if (
                claim_id in self.stale_claim_ids
                or claim_id in self.retired_claim_ids
                or claim_id in self.superseded_claim_ids
            ):
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
    CONTRADICTION_RESOLUTIONS = {
        "uphold",
        "retain_competing",
        "insufficient_evidence",
        "corrected",
    }

    @staticmethod
    def _validate_provenance(value: Any, *, event_type: str) -> list[dict[str, Any]]:
        if not isinstance(value, list) or not value:
            raise InvariantViolation(f"{event_type} requires non-empty provenance")
        if any(not isinstance(item, dict) for item in value):
            raise InvariantViolation("provenance references must be objects")
        provenance = cast(list[dict[str, Any]], value)
        evidence_ids = [reference.get("evidence_id") for reference in provenance]
        if any(not item for item in evidence_ids):
            raise InvariantViolation("every provenance reference requires evidence_id")
        if len(set(evidence_ids)) != len(evidence_ids):
            raise InvariantViolation("duplicate evidence cannot count as independent support")
        for reference in provenance:
            if not reference.get("relation") or not reference.get("independence_group"):
                raise InvariantViolation(
                    "every provenance reference requires relation and independence_group"
                )
        return provenance

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
            TwinAggregate._validate_provenance(
                payload.get("provenance"), event_type="ClaimProposed"
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
        if event.event_type == "EntityLinked":
            required = {"entity_link_id", "subject_id", "namespace", "entity_id"}
            if any(not payload.get(field) for field in required):
                raise InvariantViolation("EntityLinked identity fields are required")
            TwinAggregate._validate_provenance(
                payload.get("provenance"), event_type="EntityLinked"
            )
            confidence = payload.get("confidence")
            if not isinstance(confidence, (int, float)) or not 0.0 <= confidence <= 1.0:
                raise InvariantViolation("EntityLinked confidence must be in [0, 1]")
        if event.event_type == "EntityUnlinked" and (
            not payload.get("entity_link_id") or not payload.get("reason")
        ):
            raise InvariantViolation("EntityUnlinked requires entity_link_id and reason")
        if event.event_type == "ContradictionDetected":
            claim_ids = payload.get("claim_ids")
            if (
                not payload.get("contradiction_id")
                or not isinstance(claim_ids, list)
                or len(claim_ids) < 2
                or len(set(claim_ids)) != len(claim_ids)
                or any(not item for item in claim_ids)
            ):
                raise InvariantViolation(
                    "ContradictionDetected requires an identity and distinct claims"
                )
        if event.event_type == "ContradictionAdjudicated":
            preferred = payload.get("preferred_claim_ids")
            resolution = payload.get("resolution")
            if (
                not payload.get("contradiction_id")
                or not payload.get("reviewer_identity_id")
                or resolution not in TwinAggregate.CONTRADICTION_RESOLUTIONS
                or not payload.get("rationale")
                or not isinstance(preferred, list)
                or len(set(preferred)) != len(preferred)
                or any(not item for item in preferred)
            ):
                raise InvariantViolation(
                    "ContradictionAdjudicated requires resolution, rationale, and preferences"
                )
        if event.event_type == "CorrectionRecorded":
            required = {
                "correction_id",
                "subject_id",
                "target_kind",
                "target_id",
                "replacement_id",
                "rationale",
            }
            if any(not payload.get(field) for field in required):
                raise InvariantViolation("CorrectionRecorded fields are required")
            if payload.get("target_kind") not in {"evidence", "claim"}:
                raise InvariantViolation("correction target_kind must be evidence or claim")
            if payload.get("target_id") == payload.get("replacement_id"):
                raise InvariantViolation("correction target and replacement must differ")
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
        elif event_type == "EntityLinked":
            cls._bind_subject(state, str(payload["subject_id"]))
            for reference in payload["provenance"]:
                evidence_id = str(reference["evidence_id"])
                evidence = state.evidence.get(evidence_id)
                if evidence is None:
                    raise InvariantViolation("entity provenance references unknown evidence")
                if reference.get("independence_group") != evidence.get("independence_group"):
                    raise InvariantViolation(
                        "entity provenance independence group does not match evidence"
                    )
            link_id = str(payload["entity_link_id"])
            if link_id in state.entity_links:
                raise InvariantViolation("entity link identity already exists")
            link = dict(payload)
            link["recorded_at"] = event.recorded_at.isoformat()
            state.entity_links[link_id] = link
        elif event_type == "EntityUnlinked":
            link_id = str(payload["entity_link_id"])
            if link_id not in state.entity_links:
                raise InvariantViolation("cannot unlink an unknown entity")
            if link_id in state.unlinked_entity_ids:
                raise InvariantViolation("entity link is already inactive")
            state.unlinked_entity_ids.add(link_id)
        elif event_type == "EvidenceDeleted":
            evidence_id = str(payload["evidence_id"])
            if evidence_id not in state.evidence:
                raise InvariantViolation("cannot delete unknown evidence")
            state.evidence.pop(evidence_id)
            for link_id, link in state.entity_links.items():
                refs = link.get("provenance", [])
                if any(ref.get("evidence_id") == evidence_id for ref in refs):
                    state.stale_entity_link_ids.add(link_id)
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
            if claim_id in state.retired_claim_ids or claim_id in state.superseded_claim_ids:
                raise InvariantViolation("cannot accept a retired or superseded claim")
            state.accepted_claim_ids.add(claim_id)
            state.contested_claim_ids.discard(claim_id)
        elif event_type == "ClaimContested":
            claim_id = str(payload["claim_id"])
            if claim_id not in state.claims:
                raise InvariantViolation("cannot contest an unknown claim")
            if claim_id in state.retired_claim_ids or claim_id in state.superseded_claim_ids:
                raise InvariantViolation("cannot contest a retired or superseded claim")
            state.contested_claim_ids.add(claim_id)
            state.accepted_claim_ids.discard(claim_id)
        elif event_type == "ClaimRetired":
            claim_id = str(payload["claim_id"])
            if claim_id not in state.claims:
                raise InvariantViolation("cannot retire an unknown claim")
            state.accepted_claim_ids.discard(claim_id)
            state.contested_claim_ids.discard(claim_id)
            state.retired_claim_ids.add(claim_id)
        elif event_type == "ClaimSuperseded":
            claim_id = str(payload["claim_id"])
            successor_id = str(payload.get("successor_claim_id", ""))
            if not successor_id:
                raise InvariantViolation("ClaimSuperseded requires successor_claim_id")
            if claim_id not in state.claims or successor_id not in state.claims:
                raise InvariantViolation("supersession requires known predecessor and successor")
            if claim_id in state.superseded_claim_ids:
                raise InvariantViolation("claim is already superseded")
            state.accepted_claim_ids.discard(claim_id)
            state.contested_claim_ids.discard(claim_id)
            state.superseded_claim_ids.add(claim_id)
            for projection_id, projection in state.issued_projections.items():
                if claim_id in set(projection.get("source_claim_ids", [])):
                    state.stale_projection_ids.add(projection_id)
        elif event_type == "ContradictionDetected":
            contradiction_id = str(payload["contradiction_id"])
            if contradiction_id in state.contradictions:
                raise InvariantViolation("contradiction identity already exists")
            subject_id = payload.get("subject_id")
            if subject_id is not None:
                cls._bind_subject(state, str(subject_id))
            if any(str(item) not in state.claims for item in payload["claim_ids"]):
                raise InvariantViolation("contradiction references unknown claims")
            contradiction = dict(payload)
            contradiction["recorded_at"] = event.recorded_at.isoformat()
            state.contradictions[contradiction_id] = contradiction
        elif event_type == "ContradictionAdjudicated":
            contradiction_id = str(payload["contradiction_id"])
            contradiction_record = state.contradictions.get(contradiction_id)
            if contradiction_record is None:
                raise InvariantViolation("cannot adjudicate an unknown contradiction")
            if contradiction_id in state.resolved_contradiction_ids:
                raise InvariantViolation("contradiction is already adjudicated")
            claim_ids = set(
                str(item) for item in contradiction_record.get("claim_ids", [])
            )
            preferred = set(str(item) for item in payload["preferred_claim_ids"])
            if not preferred.issubset(claim_ids):
                raise InvariantViolation("preferred claims must belong to the contradiction")
            if payload["resolution"] in {"uphold", "corrected"} and not preferred:
                raise InvariantViolation("this resolution requires a preferred claim")
            adjudication = dict(payload)
            adjudication["recorded_at"] = event.recorded_at.isoformat()
            state.adjudications[contradiction_id] = adjudication
            state.resolved_contradiction_ids.add(contradiction_id)
        elif event_type == "CorrectionRecorded":
            cls._bind_subject(state, str(payload["subject_id"]))
            correction_id = str(payload["correction_id"])
            if correction_id in state.corrections:
                raise InvariantViolation("correction identity already exists")
            target_kind = str(payload["target_kind"])
            target_id = str(payload["target_id"])
            replacement_id = str(payload["replacement_id"])
            records = state.evidence if target_kind == "evidence" else state.claims
            if target_id not in records or replacement_id not in records:
                raise InvariantViolation("correction requires known target and replacement")
            superseded = (
                state.superseded_evidence_ids
                if target_kind == "evidence"
                else state.superseded_claim_ids
            )
            if target_id in superseded:
                raise InvariantViolation("correction target is already superseded")
            if replacement_id in superseded:
                raise InvariantViolation("correction replacement is already superseded")
            superseded.add(target_id)
            correction = dict(payload)
            correction["recorded_at"] = event.recorded_at.isoformat()
            state.corrections[correction_id] = correction
            if target_kind == "evidence":
                for link_id, link in state.entity_links.items():
                    if any(
                        reference.get("evidence_id") == target_id
                        for reference in link.get("provenance", [])
                    ):
                        state.stale_entity_link_ids.add(link_id)
                for claim_id, claim in state.claims.items():
                    if any(
                        reference.get("evidence_id") == target_id
                        for reference in claim.get("provenance", [])
                    ):
                        state.stale_claim_ids.add(claim_id)
            else:
                state.accepted_claim_ids.discard(target_id)
                state.contested_claim_ids.discard(target_id)
            for projection_id, projection in state.issued_projections.items():
                sources = set(str(item) for item in projection.get("source_claim_ids", []))
                if target_kind == "claim" and target_id in sources:
                    state.stale_projection_ids.add(projection_id)
                if target_kind == "evidence" and sources.intersection(state.stale_claim_ids):
                    state.stale_projection_ids.add(projection_id)
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
