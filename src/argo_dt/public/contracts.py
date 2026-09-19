from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol

from seed_contracts import (
    AdmissionDisposition,
    AdmissionResult,
    AdmissionStatus,
    CanonicalEffect,
    RepresentationSnapshotRef,
    SubjectRef,
)


class PublicErrorCode(StrEnum):
    INVALID_REQUEST = "invalid_request"
    AUTHORIZATION_DENIED = "authorization_denied"
    STALE_STATE = "stale_state"
    NOT_FOUND = "not_found"
    INVARIANT_VIOLATION = "invariant_violation"
    TEMPORARILY_UNAVAILABLE = "temporarily_unavailable"


class K2PublicError(RuntimeError):
    """Stable public bridge failure without leaking private exception types."""

    def __init__(self, code: PublicErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code


def _require(value: str, name: str) -> str:
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _format_time(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.isoformat()


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return parsed


@dataclass(frozen=True, slots=True)
class AuthorityContext:
    identity_id: str
    roles: tuple[str, ...]
    subject_id: str | None = None

    def __post_init__(self) -> None:
        _require(self.identity_id, "identity_id")
        if not self.roles:
            raise ValueError("authority roles are required")
        if any(not role for role in self.roles):
            raise ValueError("authority roles cannot be empty strings")

    def to_wire(self) -> dict[str, object]:
        return {
            "identity_id": self.identity_id,
            "roles": list(self.roles),
            "subject_id": self.subject_id,
        }

    @classmethod
    def from_wire(cls, value: Mapping[str, object]) -> AuthorityContext:
        raw_roles = value.get("roles")
        if not isinstance(raw_roles, list):
            raise ValueError("authority roles must be a list")
        return cls(
            identity_id=str(value.get("identity_id", "")),
            roles=tuple(str(role) for role in raw_roles),
            subject_id=(
                str(value["subject_id"]) if value.get("subject_id") is not None else None
            ),
        )


@dataclass(frozen=True, slots=True)
class AdmitEvidenceRequest:
    """Transport-independent evidence admission request for the first vertical slice."""

    request_id: str
    representation_id: str
    subject_ref: SubjectRef
    source: str
    source_record_id: str
    payload: Mapping[str, Any]
    rights: Mapping[str, Any]
    sensitivity: str
    valid_from: datetime
    valid_until: datetime | None
    independence_group: str
    based_on_sequence: int
    authority: AuthorityContext
    connector_version: str | None = None
    media_type: str | None = None
    source_content_hash: str | None = None

    def __post_init__(self) -> None:
        for value, name in (
            (self.request_id, "request_id"),
            (self.representation_id, "representation_id"),
            (self.source, "source"),
            (self.source_record_id, "source_record_id"),
            (self.sensitivity, "sensitivity"),
            (self.independence_group, "independence_group"),
        ):
            _require(value, name)
        if self.based_on_sequence < 0:
            raise ValueError("based_on_sequence cannot be negative")
        _format_time(self.valid_from)
        if self.valid_until is not None:
            _format_time(self.valid_until)
            if self.valid_until < self.valid_from:
                raise ValueError("valid_until precedes valid_from")

    def to_wire(self) -> dict[str, object]:
        return {
            "schema_version": "argo.dt.public.admit-evidence/v1",
            "request_id": self.request_id,
            "representation_id": self.representation_id,
            "subject_ref": {"subject_id": self.subject_ref.subject_id},
            "source": self.source,
            "source_record_id": self.source_record_id,
            "payload": dict(self.payload),
            "rights": dict(self.rights),
            "sensitivity": self.sensitivity,
            "valid_from": _format_time(self.valid_from),
            "valid_until": (
                _format_time(self.valid_until) if self.valid_until is not None else None
            ),
            "independence_group": self.independence_group,
            "based_on_sequence": self.based_on_sequence,
            "authority": self.authority.to_wire(),
            "connector_version": self.connector_version,
            "media_type": self.media_type,
            "source_content_hash": self.source_content_hash,
        }

    @classmethod
    def from_wire(cls, value: Mapping[str, object]) -> AdmitEvidenceRequest:
        if value.get("schema_version") != "argo.dt.public.admit-evidence/v1":
            raise ValueError("unsupported admit-evidence request schema")
        subject_raw = value.get("subject_ref")
        authority_raw = value.get("authority")
        payload_raw = value.get("payload")
        rights_raw = value.get("rights")
        if not isinstance(subject_raw, dict):
            raise ValueError("subject_ref must be an object")
        if not isinstance(authority_raw, dict):
            raise ValueError("authority must be an object")
        if not isinstance(payload_raw, dict) or not isinstance(rights_raw, dict):
            raise ValueError("payload and rights must be objects")
        valid_until_raw = value.get("valid_until")
        return cls(
            request_id=str(value.get("request_id", "")),
            representation_id=str(value.get("representation_id", "")),
            subject_ref=SubjectRef(str(subject_raw.get("subject_id", ""))),
            source=str(value.get("source", "")),
            source_record_id=str(value.get("source_record_id", "")),
            payload=dict(payload_raw),
            rights=dict(rights_raw),
            sensitivity=str(value.get("sensitivity", "")),
            valid_from=_parse_time(str(value.get("valid_from", ""))),
            valid_until=(
                _parse_time(str(valid_until_raw)) if valid_until_raw is not None else None
            ),
            independence_group=str(value.get("independence_group", "")),
            based_on_sequence=int(value.get("based_on_sequence", 0)),
            authority=AuthorityContext.from_wire(authority_raw),
            connector_version=(
                str(value["connector_version"])
                if value.get("connector_version") is not None
                else None
            ),
            media_type=(
                str(value["media_type"]) if value.get("media_type") is not None else None
            ),
            source_content_hash=(
                str(value["source_content_hash"])
                if value.get("source_content_hash") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class AdmissionLookupRequest:
    representation_id: str
    request_id: str
    authority: AuthorityContext

    def to_wire(self) -> dict[str, object]:
        return {
            "representation_id": self.representation_id,
            "request_id": self.request_id,
            "authority": self.authority.to_wire(),
        }

    @classmethod
    def from_wire(cls, value: Mapping[str, object]) -> AdmissionLookupRequest:
        authority = value.get("authority")
        if not isinstance(authority, dict):
            raise ValueError("authority must be an object")
        return cls(
            representation_id=str(value.get("representation_id", "")),
            request_id=str(value.get("request_id", "")),
            authority=AuthorityContext.from_wire(authority),
        )


@dataclass(frozen=True, slots=True)
class RepresentationQuery:
    representation_id: str
    authority: AuthorityContext

    def to_wire(self) -> dict[str, object]:
        return {
            "representation_id": self.representation_id,
            "authority": self.authority.to_wire(),
        }

    @classmethod
    def from_wire(cls, value: Mapping[str, object]) -> RepresentationQuery:
        authority = value.get("authority")
        if not isinstance(authority, dict):
            raise ValueError("authority must be an object")
        return cls(
            representation_id=str(value.get("representation_id", "")),
            authority=AuthorityContext.from_wire(authority),
        )


@dataclass(frozen=True, slots=True)
class LineageQuery:
    representation_id: str
    node_kind: str
    node_id: str
    authority: AuthorityContext

    def to_wire(self) -> dict[str, object]:
        return {
            "representation_id": self.representation_id,
            "node_kind": self.node_kind,
            "node_id": self.node_id,
            "authority": self.authority.to_wire(),
        }

    @classmethod
    def from_wire(cls, value: Mapping[str, object]) -> LineageQuery:
        authority = value.get("authority")
        if not isinstance(authority, dict):
            raise ValueError("authority must be an object")
        return cls(
            representation_id=str(value.get("representation_id", "")),
            node_kind=str(value.get("node_kind", "")),
            node_id=str(value.get("node_id", "")),
            authority=AuthorityContext.from_wire(authority),
        )


@dataclass(frozen=True, slots=True)
class RepresentationStateView:
    representation_id: str
    subject_ref: SubjectRef
    snapshot_ref: RepresentationSnapshotRef
    evidence_refs: tuple[str, ...]
    claim_refs: tuple[str, ...]
    accepted_claim_refs: tuple[str, ...]
    contested_claim_refs: tuple[str, ...]
    contradiction_refs: tuple[str, ...]
    degraded_reasons: tuple[str, ...]

    def to_wire(self) -> dict[str, object]:
        return {
            "representation_id": self.representation_id,
            "subject_ref": {"subject_id": self.subject_ref.subject_id},
            "snapshot_ref": _snapshot_to_wire(self.snapshot_ref),
            "evidence_refs": list(self.evidence_refs),
            "claim_refs": list(self.claim_refs),
            "accepted_claim_refs": list(self.accepted_claim_refs),
            "contested_claim_refs": list(self.contested_claim_refs),
            "contradiction_refs": list(self.contradiction_refs),
            "degraded_reasons": list(self.degraded_reasons),
        }

    @classmethod
    def from_wire(cls, value: Mapping[str, object]) -> RepresentationStateView:
        subject = value.get("subject_ref")
        snapshot = value.get("snapshot_ref")
        if not isinstance(subject, dict) or not isinstance(snapshot, dict):
            raise ValueError("state subject_ref and snapshot_ref must be objects")
        return cls(
            representation_id=str(value.get("representation_id", "")),
            subject_ref=SubjectRef(str(subject.get("subject_id", ""))),
            snapshot_ref=_snapshot_from_wire(snapshot),
            evidence_refs=_string_tuple(value.get("evidence_refs")),
            claim_refs=_string_tuple(value.get("claim_refs")),
            accepted_claim_refs=_string_tuple(value.get("accepted_claim_refs")),
            contested_claim_refs=_string_tuple(value.get("contested_claim_refs")),
            contradiction_refs=_string_tuple(value.get("contradiction_refs")),
            degraded_reasons=_string_tuple(value.get("degraded_reasons")),
        )


@dataclass(frozen=True, slots=True)
class RepresentationDeficit:
    deficit_id: str
    kind: str
    target_refs: tuple[str, ...]
    rationale: str

    def to_wire(self) -> dict[str, object]:
        return {
            "deficit_id": self.deficit_id,
            "kind": self.kind,
            "target_refs": list(self.target_refs),
            "rationale": self.rationale,
        }

    @classmethod
    def from_wire(cls, value: Mapping[str, object]) -> RepresentationDeficit:
        return cls(
            deficit_id=str(value.get("deficit_id", "")),
            kind=str(value.get("kind", "")),
            target_refs=_string_tuple(value.get("target_refs")),
            rationale=str(value.get("rationale", "")),
        )


@dataclass(frozen=True, slots=True)
class LineageNodeRef:
    kind: str
    node_id: str

    def to_wire(self) -> dict[str, object]:
        return {"kind": self.kind, "node_id": self.node_id}

    @classmethod
    def from_wire(cls, value: Mapping[str, object]) -> LineageNodeRef:
        return cls(kind=str(value.get("kind", "")), node_id=str(value.get("node_id", "")))


@dataclass(frozen=True, slots=True)
class LineageTraceView:
    representation_id: str
    root: LineageNodeRef
    ancestors: tuple[LineageNodeRef, ...]
    dependents: tuple[LineageNodeRef, ...]
    evidence_by_independence_group: tuple[tuple[str, tuple[str, ...]], ...]

    def to_wire(self) -> dict[str, object]:
        return {
            "representation_id": self.representation_id,
            "root": self.root.to_wire(),
            "ancestors": [node.to_wire() for node in self.ancestors],
            "dependents": [node.to_wire() for node in self.dependents],
            "evidence_by_independence_group": [
                {"group": group, "evidence_refs": list(refs)}
                for group, refs in self.evidence_by_independence_group
            ],
        }

    @classmethod
    def from_wire(cls, value: Mapping[str, object]) -> LineageTraceView:
        root = value.get("root")
        ancestors = value.get("ancestors")
        dependents = value.get("dependents")
        groups = value.get("evidence_by_independence_group")
        if not isinstance(root, dict):
            raise ValueError("lineage root must be an object")
        if not isinstance(ancestors, list) or not isinstance(dependents, list):
            raise ValueError("lineage nodes must be arrays")
        if not isinstance(groups, list):
            raise ValueError("lineage evidence groups must be an array")
        parsed_groups: list[tuple[str, tuple[str, ...]]] = []
        for item in groups:
            if not isinstance(item, dict):
                raise ValueError("lineage evidence group must be an object")
            parsed_groups.append(
                (str(item.get("group", "")), _string_tuple(item.get("evidence_refs")))
            )
        return cls(
            representation_id=str(value.get("representation_id", "")),
            root=LineageNodeRef.from_wire(root),
            ancestors=tuple(LineageNodeRef.from_wire(item) for item in ancestors),
            dependents=tuple(LineageNodeRef.from_wire(item) for item in dependents),
            evidence_by_independence_group=tuple(parsed_groups),
        )


class K2PublicAPI(Protocol):
    async def admit_evidence(self, request: AdmitEvidenceRequest) -> AdmissionResult: ...

    async def get_admission_result(
        self, request: AdmissionLookupRequest
    ) -> AdmissionResult | None: ...

    async def read_representation_state(
        self, request: RepresentationQuery
    ) -> RepresentationStateView: ...

    async def read_representation_deficits(
        self, request: RepresentationQuery
    ) -> tuple[RepresentationDeficit, ...]: ...

    async def trace_lineage(self, request: LineageQuery) -> LineageTraceView: ...


def _string_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError("expected an array")
    return tuple(str(item) for item in value)


def _snapshot_to_wire(value: RepresentationSnapshotRef) -> dict[str, object]:
    return {
        "subject_ref": {"subject_id": value.subject_ref.subject_id},
        "snapshot_id": value.snapshot_id,
        "canonical_sequence": value.canonical_sequence,
        "integrity_ref": value.integrity_ref,
    }


def _snapshot_from_wire(value: Mapping[str, object]) -> RepresentationSnapshotRef:
    subject = value.get("subject_ref")
    if not isinstance(subject, dict):
        raise ValueError("snapshot subject_ref must be an object")
    return RepresentationSnapshotRef(
        subject_ref=SubjectRef(str(subject.get("subject_id", ""))),
        snapshot_id=str(value.get("snapshot_id", "")),
        canonical_sequence=int(value.get("canonical_sequence", 0)),
        integrity_ref=str(value.get("integrity_ref", "")),
    )


def admission_result_to_wire(value: AdmissionResult) -> dict[str, object]:
    return {
        "request_id": value.request_id,
        "admission_id": value.admission_id,
        "status": value.status.value,
        "disposition": value.disposition.value if value.disposition is not None else None,
        "canonical_effects": [
            {
                "effect_id": effect.effect_id,
                "effect_kind": effect.effect_kind,
                "affected_semantic_ref": effect.affected_semantic_ref,
                "source_request_id": effect.source_request_id,
                "canonical_position": _snapshot_to_wire(effect.canonical_position),
            }
            for effect in value.canonical_effects
        ],
        "resulting_representation_state_ref": (
            _snapshot_to_wire(value.resulting_representation_state_ref)
            if value.resulting_representation_state_ref is not None
            else None
        ),
        "replayed": value.replayed,
        "original_result_ref": value.original_result_ref,
    }


def admission_result_from_wire(value: Mapping[str, object]) -> AdmissionResult:
    effects_raw = value.get("canonical_effects")
    if not isinstance(effects_raw, list):
        raise ValueError("canonical_effects must be an array")
    effects: list[CanonicalEffect] = []
    for raw in effects_raw:
        if not isinstance(raw, dict):
            raise ValueError("canonical effect must be an object")
        position = raw.get("canonical_position")
        if not isinstance(position, dict):
            raise ValueError("canonical effect position must be an object")
        effects.append(
            CanonicalEffect(
                effect_id=str(raw.get("effect_id", "")),
                effect_kind=str(raw.get("effect_kind", "")),
                affected_semantic_ref=str(raw.get("affected_semantic_ref", "")),
                source_request_id=str(raw.get("source_request_id", "")),
                canonical_position=_snapshot_from_wire(position),
            )
        )
    state_raw = value.get("resulting_representation_state_ref")
    disposition_raw = value.get("disposition")
    original_result_ref = value.get("original_result_ref")
    return AdmissionResult(
        request_id=str(value.get("request_id", "")),
        admission_id=str(value.get("admission_id", "")),
        status=AdmissionStatus(str(value.get("status", ""))),
        disposition=(
            AdmissionDisposition(str(disposition_raw))
            if disposition_raw is not None
            else None
        ),
        canonical_effects=tuple(effects),
        resulting_representation_state_ref=(
            _snapshot_from_wire(state_raw) if isinstance(state_raw, dict) else None
        ),
        replayed=bool(value.get("replayed", False)),
        original_result_ref=(
            str(original_result_ref) if original_result_ref is not None else None
        ),
    )


def json_round_trip(value: Mapping[str, object]) -> dict[str, object]:
    parsed = json.loads(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    )
    if not isinstance(parsed, dict):
        raise ValueError("wire value must round-trip as an object")
    return parsed
