from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class ContractViolation(ValueError):
    """Raised when a public semantic contract is internally inconsistent."""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _stable_id(namespace: str, value: Any) -> str:
    digest = hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()
    return f"{namespace}:sha256:{digest}"


def _require(value: str, name: str) -> str:
    if not value:
        raise ContractViolation(f"{name} is required")
    return value


@dataclass(frozen=True, slots=True)
class SubjectRef:
    """Stable identity of the represented person."""

    subject_id: str

    def __post_init__(self) -> None:
        _require(self.subject_id, "subject_id")


@dataclass(frozen=True, slots=True)
class RepresentationSnapshotRef:
    """Identity of one canonical representation state for one subject."""

    subject_ref: SubjectRef
    snapshot_id: str
    canonical_sequence: int
    integrity_ref: str

    def __post_init__(self) -> None:
        _require(self.snapshot_id, "snapshot_id")
        _require(self.integrity_ref, "integrity_ref")
        if self.canonical_sequence < 0:
            raise ContractViolation("canonical_sequence cannot be negative")


@dataclass(frozen=True, slots=True)
class SemanticBasisRef:
    """Immutable interpretation basis over one representation snapshot.

    Readiness, fidelity, purpose, and other derived assessments are deliberately
    excluded from this identity.
    """

    semantic_basis_id: str
    subject_ref: SubjectRef
    representation_snapshot_ref: RepresentationSnapshotRef
    schema_registry_snapshot_ref: str
    construct_registry_snapshot_ref: str
    admission_policy_ref: str
    epistemic_policy_ref: str
    semantic_constraint_policy_ref: str
    other_interpretation_critical_refs: tuple[str, ...] = ()
    integrity_ref: str = ""
    sealed_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        _require(self.semantic_basis_id, "semantic_basis_id")
        _require(self.schema_registry_snapshot_ref, "schema_registry_snapshot_ref")
        _require(self.construct_registry_snapshot_ref, "construct_registry_snapshot_ref")
        _require(self.admission_policy_ref, "admission_policy_ref")
        _require(self.epistemic_policy_ref, "epistemic_policy_ref")
        _require(self.semantic_constraint_policy_ref, "semantic_constraint_policy_ref")
        _require(self.integrity_ref, "integrity_ref")
        if self.representation_snapshot_ref.subject_ref != self.subject_ref:
            raise ContractViolation(
                "semantic basis subject must match representation snapshot subject"
            )
        canonical_refs = tuple(sorted(self.other_interpretation_critical_refs))
        if len(canonical_refs) != len(set(canonical_refs)):
            raise ContractViolation(
                "other_interpretation_critical_refs must be unique"
            )
        if self.other_interpretation_critical_refs != canonical_refs:
            raise ContractViolation(
                "other_interpretation_critical_refs must use canonical sorted order"
            )
        if self.sealed_at.tzinfo is None:
            raise ContractViolation("sealed_at must be timezone-aware")

    @classmethod
    def seal(
        cls,
        *,
        subject_ref: SubjectRef,
        representation_snapshot_ref: RepresentationSnapshotRef,
        schema_registry_snapshot_ref: str,
        construct_registry_snapshot_ref: str,
        admission_policy_ref: str,
        epistemic_policy_ref: str,
        semantic_constraint_policy_ref: str,
        other_interpretation_critical_refs: tuple[str, ...] = (),
        sealed_at: datetime | None = None,
    ) -> SemanticBasisRef:
        if representation_snapshot_ref.subject_ref != subject_ref:
            raise ContractViolation(
                "semantic basis subject must match representation snapshot subject"
            )
        if len(other_interpretation_critical_refs) != len(
            set(other_interpretation_critical_refs)
        ):
            raise ContractViolation(
                "other_interpretation_critical_refs must be unique"
            )
        canonical_refs = tuple(sorted(other_interpretation_critical_refs))
        material = {
            "subject_id": subject_ref.subject_id,
            "representation_snapshot_id": representation_snapshot_ref.snapshot_id,
            "representation_sequence": representation_snapshot_ref.canonical_sequence,
            "representation_integrity": representation_snapshot_ref.integrity_ref,
            "schema_registry_snapshot_ref": schema_registry_snapshot_ref,
            "construct_registry_snapshot_ref": construct_registry_snapshot_ref,
            "admission_policy_ref": admission_policy_ref,
            "epistemic_policy_ref": epistemic_policy_ref,
            "semantic_constraint_policy_ref": semantic_constraint_policy_ref,
            "other_interpretation_critical_refs": list(canonical_refs),
        }
        return cls(
            semantic_basis_id=_stable_id("semantic-basis", material),
            subject_ref=subject_ref,
            representation_snapshot_ref=representation_snapshot_ref,
            schema_registry_snapshot_ref=schema_registry_snapshot_ref,
            construct_registry_snapshot_ref=construct_registry_snapshot_ref,
            admission_policy_ref=admission_policy_ref,
            epistemic_policy_ref=epistemic_policy_ref,
            semantic_constraint_policy_ref=semantic_constraint_policy_ref,
            other_interpretation_critical_refs=canonical_refs,
            integrity_ref=_stable_id("semantic-basis-integrity", material),
            sealed_at=sealed_at or datetime.now(UTC),
        )


class AdmissionStatus(StrEnum):
    ADMITTED = "admitted"
    REJECTED = "rejected"
    DEFERRED = "deferred"


class AdmissionDisposition(StrEnum):
    EFFECTS_APPLIED = "effects_applied"
    NO_OP_ALREADY_CANONICAL = "no_op_already_canonical"
    NO_OP_REDUNDANT = "no_op_redundant"


@dataclass(frozen=True, slots=True)
class CanonicalEffect:
    """Public semantic effect produced by canonical K2 admission."""

    effect_id: str
    effect_kind: str
    affected_semantic_ref: str
    source_request_id: str
    canonical_position: RepresentationSnapshotRef

    def __post_init__(self) -> None:
        _require(self.effect_id, "effect_id")
        _require(self.effect_kind, "effect_kind")
        _require(self.affected_semantic_ref, "affected_semantic_ref")
        _require(self.source_request_id, "source_request_id")


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    """Topology-independent canonical admission result.

    Admission reports its resulting canonical representation position. It does
    not require a SemanticBasisRef to be sealed.
    """

    request_id: str
    admission_id: str
    status: AdmissionStatus
    disposition: AdmissionDisposition | None
    canonical_effects: tuple[CanonicalEffect, ...]
    resulting_representation_state_ref: RepresentationSnapshotRef | None
    replayed: bool = False
    original_result_ref: str | None = None

    def __post_init__(self) -> None:
        _require(self.request_id, "request_id")
        _require(self.admission_id, "admission_id")
        if not isinstance(self.status, AdmissionStatus):
            raise ContractViolation("status must be an AdmissionStatus")

        if self.status is AdmissionStatus.ADMITTED:
            if self.disposition is None:
                raise ContractViolation(
                    "admitted results require an explicit disposition"
                )
            if self.resulting_representation_state_ref is None:
                raise ContractViolation(
                    "admitted results require the resulting canonical position"
                )
            if (
                self.disposition is AdmissionDisposition.EFFECTS_APPLIED
                and not self.canonical_effects
            ):
                raise ContractViolation(
                    "effects_applied requires at least one canonical effect"
                )
            if (
                self.disposition is not AdmissionDisposition.EFFECTS_APPLIED
                and self.canonical_effects
            ):
                raise ContractViolation(
                    "no-op dispositions require an empty canonical effect set"
                )
        else:
            if self.disposition is not None:
                raise ContractViolation(
                    "non-admitted results cannot carry an admission disposition"
                )
            if self.canonical_effects:
                raise ContractViolation(
                    "non-admitted results cannot carry canonical effects"
                )
            if self.resulting_representation_state_ref is not None:
                raise ContractViolation(
                    "non-admitted results cannot carry a resulting canonical position"
                )

        if (
            self.status is AdmissionStatus.ADMITTED
            and self.disposition is AdmissionDisposition.EFFECTS_APPLIED
        ):
            assert self.resulting_representation_state_ref is not None
            for effect in self.canonical_effects:
                if effect.source_request_id != self.request_id:
                    raise ContractViolation(
                        "canonical effect source_request_id must match admission request_id"
                    )
                if effect.canonical_position.subject_ref != (
                    self.resulting_representation_state_ref.subject_ref
                ):
                    raise ContractViolation(
                        "canonical effects cannot cross subjects within one admission"
                    )
                if effect.canonical_position != self.resulting_representation_state_ref:
                    raise ContractViolation(
                        "canonical effects must reference the admission final canonical position"
                    )

        if self.replayed and not self.original_result_ref:
            raise ContractViolation(
                "replayed results require original_result_ref"
            )
        if not self.replayed and self.original_result_ref is not None:
            raise ContractViolation(
                "original_result_ref is only valid for replayed results"
            )


@dataclass(frozen=True, slots=True)
class ReadinessResult:
    """Derived readiness assessment bound to, but not defining, a semantic basis."""

    readiness_result_id: str
    semantic_basis_ref: SemanticBasisRef
    readiness_profile_ref: str
    readiness_profile_hash: str
    result: str
    dependency_refs: tuple[str, ...] = ()
    evaluated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        _require(self.readiness_result_id, "readiness_result_id")
        _require(self.readiness_profile_ref, "readiness_profile_ref")
        _require(self.readiness_profile_hash, "readiness_profile_hash")
        _require(self.result, "result")
        if self.evaluated_at.tzinfo is None:
            raise ContractViolation("evaluated_at must be timezone-aware")
