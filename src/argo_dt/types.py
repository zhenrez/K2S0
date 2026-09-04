"""Canonical language-neutral domain contracts.

The dataclasses are intentionally dependency-light. JSON Schema and protobuf
files define the external wire contracts; these types define reference
semantics and invariant checks.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Mapping, Sequence

from .errors import IntegrityError, InvariantViolation

JsonObject = dict[str, Any]


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise InvariantViolation("timestamps must include an offset")
    return parsed.astimezone(UTC)


def to_primitive(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, StrEnum):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return {key: to_primitive(item) for key, item in asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(key): to_primitive(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_primitive(item) for item in value]
    return value


def canonical_json(value: Any) -> str:
    return json.dumps(
        to_primitive(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def content_hash(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


class IdentityKind(StrEnum):
    HUMAN_PRINCIPAL = "human_principal"
    COGNITIVE_TWIN = "cognitive_twin"
    AGENT_SERVICE = "agent_service"
    AVATAR_LIKENESS = "avatar_likeness"


class EventPlane(StrEnum):
    AUTHORITATIVE = "authoritative"
    PROJECTION = "projection"
    SIMULATION = "simulation"


class Sensitivity(StrEnum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"

    @property
    def rank(self) -> int:
        return {
            Sensitivity.PUBLIC: 0,
            Sensitivity.INTERNAL: 1,
            Sensitivity.CONFIDENTIAL: 2,
            Sensitivity.RESTRICTED: 3,
        }[self]


class EvidenceGrade(StrEnum):
    STATED = "stated"
    DOCUMENTARY = "documentary"
    FIRSTHAND_STRUCTURED = "firsthand_structured"
    DIRECTLY_OBSERVED = "directly_observed"
    PARSED = "parsed"
    INFERRED = "inferred"
    EXTERNAL_WITNESS = "external_witness"


class ReviewState(StrEnum):
    UNREVIEWED = "unreviewed"
    ACCEPTED = "accepted"
    CONTESTED = "contested"
    RETIRED = "retired"


class TemporalState(StrEnum):
    EVENT = "event"
    CURRENT = "current"
    RECURRING = "recurring"
    ERA = "era"
    DURABLE = "durable"
    EMERGING = "emerging"
    DECLINING = "declining"
    RETIRED = "retired"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class BitemporalInterval:
    valid_from: datetime
    valid_until: datetime | None
    recorded_at: datetime
    superseded_at: datetime | None = None

    def __post_init__(self) -> None:
        for value in (
            self.valid_from,
            self.valid_until,
            self.recorded_at,
            self.superseded_at,
        ):
            if value is not None and value.tzinfo is None:
                raise InvariantViolation("bitemporal timestamps must be timezone-aware")
        if self.valid_until is not None and self.valid_until < self.valid_from:
            raise InvariantViolation("valid_until precedes valid_from")
        if self.superseded_at is not None and self.superseded_at < self.recorded_at:
            raise InvariantViolation("superseded_at precedes recorded_at")

    def valid_at(self, instant: datetime) -> bool:
        return self.valid_from <= instant and (
            self.valid_until is None or instant < self.valid_until
        )

    def known_at(self, instant: datetime) -> bool:
        return self.recorded_at <= instant and (
            self.superseded_at is None or instant < self.superseded_at
        )


@dataclass(frozen=True, slots=True)
class EpistemicVector:
    evidence_quality: float
    confidence: float
    salience: float
    stability: float
    freshness: float
    scope_confidence: float
    contradiction_load: float

    def __post_init__(self) -> None:
        for name, value in asdict(self).items():
            if not 0.0 <= value <= 1.0:
                raise InvariantViolation(f"{name} must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class ProvenanceRef:
    evidence_id: str
    relation: str
    independence_group: str
    transform_id: str | None = None


@dataclass(frozen=True, slots=True)
class TypedRelation:
    relation_type: str
    target_cell_id: str
    constituent: bool = False
    confidence: float | None = None

    def __post_init__(self) -> None:
        if self.confidence is not None and not 0.0 <= self.confidence <= 1.0:
            raise InvariantViolation("relation confidence must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class ARGOCell:
    """Recursive universal IR contract for Digital Twin state."""

    cell_id: str
    cell_type: str
    subject_id: str
    identity_kind: IdentityKind
    temporal: BitemporalInterval
    temporal_state: TemporalState
    observed_state: Mapping[str, Any]
    desired_state: Mapping[str, Any] = field(default_factory=dict)
    predicted_state: Mapping[str, Any] = field(default_factory=dict)
    requirements: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()
    intents: tuple[str, ...] = ()
    relations: tuple[TypedRelation, ...] = ()
    provenance: tuple[ProvenanceRef, ...] = ()
    epistemic: EpistemicVector | None = None
    sensitivity: Sensitivity = Sensitivity.INTERNAL
    projection_handles: tuple[str, ...] = ()
    schema_version: str = "argo.cell/v1"
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.cell_id or not self.subject_id or not self.cell_type:
            raise InvariantViolation("cell_id, subject_id, and cell_type are required")
        if self.cell_type in {"claim", "pattern", "kernel_artifact"} and not self.provenance:
            raise InvariantViolation(f"{self.cell_type} cells require provenance")

    def to_dict(self) -> JsonObject:
        return to_primitive(self)


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    event_id: str
    twin_id: str
    event_type: str
    plane: EventPlane
    payload: Mapping[str, Any]
    occurred_at: datetime
    recorded_at: datetime
    producer: str
    idempotency_key: str
    schema_version: str = "argo.dt.event/v1"
    sequence: int = 0
    causation_id: str | None = None
    correlation_id: str | None = None
    previous_hash: str = ""
    event_hash: str = ""

    @classmethod
    def new(
        cls,
        *,
        twin_id: str,
        event_type: str,
        plane: EventPlane,
        payload: Mapping[str, Any],
        producer: str,
        idempotency_key: str,
        occurred_at: datetime | None = None,
        causation_id: str | None = None,
        correlation_id: str | None = None,
    ) -> EventEnvelope:
        now = utc_now()
        return cls(
            event_id=str(uuid.uuid4()),
            twin_id=twin_id,
            event_type=event_type,
            plane=plane,
            payload=dict(payload),
            occurred_at=occurred_at or now,
            recorded_at=now,
            producer=producer,
            idempotency_key=idempotency_key,
            causation_id=causation_id,
            correlation_id=correlation_id,
        )

    def _hash_material(self) -> JsonObject:
        material = to_primitive(self)
        material.pop("event_hash", None)
        return material

    def seal(self, *, sequence: int, previous_hash: str, recorded_at: datetime) -> EventEnvelope:
        candidate = replace(
            self,
            sequence=sequence,
            previous_hash=previous_hash,
            recorded_at=recorded_at,
            event_hash="",
        )
        return replace(candidate, event_hash=content_hash(candidate._hash_material()))

    def verify(self, expected_previous_hash: str) -> None:
        if self.previous_hash != expected_previous_hash:
            raise IntegrityError(
                f"previous hash mismatch at sequence {self.sequence}: "
                f"{self.previous_hash!r} != {expected_previous_hash!r}"
            )
        expected = content_hash(self._hash_material())
        if self.event_hash != expected:
            raise IntegrityError(f"event hash mismatch at sequence {self.sequence}")


@dataclass(frozen=True, slots=True)
class ConsentGrant:
    consent_id: str
    subject_id: str
    recipient_id: str
    purposes: frozenset[str]
    allowed_fields: frozenset[str]
    max_sensitivity: Sensitivity
    valid_from: datetime
    valid_until: datetime
    policy_version: str
    revoked_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.valid_until <= self.valid_from:
            raise InvariantViolation("consent valid_until must follow valid_from")
        for value in (self.valid_from, self.valid_until, self.revoked_at):
            if value is not None and value.tzinfo is None:
                raise InvariantViolation("consent timestamps must be timezone-aware")

    def active_at(self, instant: datetime) -> bool:
        return (
            self.revoked_at is None
            and self.valid_from <= instant < self.valid_until
        )


@dataclass(frozen=True, slots=True)
class ProjectionRequest:
    request_id: str
    twin_id: str
    subject_id: str
    recipient_id: str
    purpose: str
    requested_fields: frozenset[str]
    maximum_sensitivity: Sensitivity
    as_of_valid_time: datetime
    as_of_recorded_time: datetime

    def __post_init__(self) -> None:
        if not self.requested_fields:
            raise InvariantViolation("projection requested_fields cannot be empty")
        if self.as_of_valid_time.tzinfo is None or self.as_of_recorded_time.tzinfo is None:
            raise InvariantViolation("projection timestamps must be timezone-aware")


@dataclass(frozen=True, slots=True)
class ProjectionReceipt:
    projection_id: str
    request_id: str
    consent_id: str
    policy_version: str
    issued_at: datetime
    expires_at: datetime
    source_sequence: int
    disclosed_fields: tuple[str, ...]
    source_claim_ids: tuple[str, ...]
    artifact_hash: str
    decision_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ActionEnvelope:
    action_id: str
    principal_id: str
    actor_identity_id: str
    purpose: str
    target: str
    requested_capabilities: frozenset[str]
    impact: str
    reversible: bool
    evidence_ids: tuple[str, ...]
    constraint_version: str
    idempotency_key: str
    issued_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        if self.expires_at <= self.issued_at:
            raise InvariantViolation("action expires_at must follow issued_at")
        if not self.requested_capabilities:
            raise InvariantViolation("action requires at least one capability")
        if self.issued_at.tzinfo is None or self.expires_at.tzinfo is None:
            raise InvariantViolation("action timestamps must be timezone-aware")


@dataclass(frozen=True, slots=True)
class AuthorityGrant:
    grant_id: str
    principal_id: str
    actor_identity_id: str
    purposes: frozenset[str]
    capabilities: frozenset[str]
    maximum_impact: str
    require_reversible: bool
    valid_from: datetime
    valid_until: datetime
    revoked_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.valid_until <= self.valid_from:
            raise InvariantViolation("authority valid_until must follow valid_from")
        for value in (self.valid_from, self.valid_until, self.revoked_at):
            if value is not None and value.tzinfo is None:
                raise InvariantViolation("authority timestamps must be timezone-aware")

    def active_at(self, instant: datetime) -> bool:
        return (
            self.revoked_at is None
            and self.valid_from <= instant < self.valid_until
        )


@dataclass(frozen=True, slots=True)
class KernelArtifact:
    artifact_id: str
    twin_id: str
    artifact_type: str
    source_sequence: int
    source_claim_ids: tuple[str, ...]
    compiler_version: str
    status: str
    payload: Mapping[str, Any]
    loss_report: Mapping[str, Any]
    compiled_at: datetime
    artifact_hash: str


def unique_independence_groups(refs: Sequence[ProvenanceRef]) -> int:
    return len({ref.independence_group for ref in refs})
