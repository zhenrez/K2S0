"""Stable identifiers for the Digital Twin semantic compatibility contract."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class InvariantId(StrEnum):
    IDENTITY_SURFACES_DISTINCT = "DT-INV-001"
    ONE_SUBJECT_PER_TWIN = "DT-INV-002"
    ORDERED_HASH_CHAIN = "DT-INV-003"
    IDEMPOTENT_APPEND = "DT-INV-004"
    BITEMPORAL_STATE = "DT-INV-005"
    CLAIM_PROVENANCE = "DT-INV-006"
    EPISTEMIC_VECTOR_COMPLETE = "DT-INV-007"
    NO_SILENT_OVERWRITE = "DT-INV-008"
    SIMULATION_ISOLATION = "DT-INV-009"
    PROJECTION_BEFORE_DISCLOSURE = "DT-INV-010"
    DEFAULT_DENY_MINIMIZATION = "DT-INV-011"
    CONSENT_NOT_AUTHORITY = "DT-INV-012"
    EVENT_OWNER_ENFORCED = "DT-INV-013"
    TRANSITION_BEFORE_PERSISTENCE = "DT-INV-014"
    DELETION_INVALIDATES_DEPENDENCIES = "DT-INV-015"
    PROJECTION_REVOCABLE = "DT-INV-016"
    BOUNDED_REALTIME_BUFFER = "DT-INV-017"
    LOSS_REPORT_NONINTERFERENCE = "DT-INV-018"
    PRODUCER_MATCHES_ACTOR = "DT-INV-019"
    CANONICAL_SERIALIZATION = "DT-INV-020"


@dataclass(frozen=True, slots=True)
class InvariantSpec:
    invariant_id: InvariantId
    name: str
    owner: str
    evidence: tuple[str, ...]


INVARIANTS = (
    InvariantSpec(
        InvariantId.IDENTITY_SURFACES_DISTINCT,
        "Identity surfaces remain distinct",
        "identity",
        ("test_dt_invariants.TypeInvariantTests.test_four_identity_surfaces_are_distinct",),
    ),
    InvariantSpec(
        InvariantId.ONE_SUBJECT_PER_TWIN,
        "A twin binds exactly one subject",
        "aggregate",
        ("test_dt_invariants.ServiceTests.test_twin_cannot_mix_subjects",),
    ),
    InvariantSpec(
        InvariantId.ORDERED_HASH_CHAIN,
        "Events form an ordered tamper-evident chain",
        "event-store",
        (
            "test_dt_invariants.StoreTests.test_hash_chain_and_idempotency",
            "test_contract_conformance.GoldenReplayTests.test_golden_replay_is_stable",
        ),
    ),
    InvariantSpec(
        InvariantId.IDEMPOTENT_APPEND,
        "Retries return the original append",
        "event-store",
        (
            "test_dt_invariants.StoreTests.test_hash_chain_and_idempotency",
            "test_dt_invariants.ServiceTests.test_idempotent_retry_is_not_republished",
        ),
    ),
    InvariantSpec(
        InvariantId.BITEMPORAL_STATE,
        "Valid and recorded time remain independent",
        "aggregate",
        (
            "test_dt_invariants.TypeInvariantTests.test_valid_and_recorded_time_are_separate",
            "test_dt_invariants.ServiceTests.test_recorded_time_query_does_not_backport_review",
        ),
    ),
    InvariantSpec(
        InvariantId.CLAIM_PROVENANCE,
        "Claims retain verified evidence lineage",
        "adjudication",
        (
            "test_dt_invariants.ServiceTests."
            "test_unknown_provenance_is_rejected_before_persistence",
        ),
    ),
    InvariantSpec(
        InvariantId.EPISTEMIC_VECTOR_COMPLETE,
        "Claims carry seven bounded epistemic dimensions",
        "adjudication",
        ("test_dt_invariants.ServiceTests.test_incomplete_epistemic_vector_is_rejected",),
    ),
    InvariantSpec(
        InvariantId.NO_SILENT_OVERWRITE,
        "Corrections and supersession remain events",
        "aggregate",
        ("test_contract_conformance.GoldenReplayTests.test_golden_replay_is_stable",),
    ),
    InvariantSpec(
        InvariantId.SIMULATION_ISOLATION,
        "Simulation cannot mutate authoritative state",
        "simulation",
        (
            "test_dt_invariants.ServiceTests.test_simulation_is_isolated_from_authoritative_state",
            "test_dt_invariants.ServiceTests.test_event_plane_ownership_is_enforced",
        ),
    ),
    InvariantSpec(
        InvariantId.PROJECTION_BEFORE_DISCLOSURE,
        "Consumers receive compiled projections",
        "projection",
        ("test_dt_invariants.ServiceTests.test_projection_excludes_more_sensitive_claims",),
    ),
    InvariantSpec(
        InvariantId.DEFAULT_DENY_MINIMIZATION,
        "Projection is default-deny and sensitivity-minimized",
        "policy",
        (
            "test_dt_invariants.ServiceTests.test_projection_is_default_deny",
            "test_dt_invariants.ServiceTests."
            "test_projection_authorization_precedes_compilation",
            "test_dt_invariants.ServiceTests.test_projection_excludes_more_sensitive_claims",
        ),
    ),
    InvariantSpec(
        InvariantId.CONSENT_NOT_AUTHORITY,
        "Information consent never grants action authority",
        "policy",
        (
            "test_dt_invariants.AuthorityTests."
            "test_information_consent_does_not_authorize_action",
        ),
    ),
    InvariantSpec(
        InvariantId.EVENT_OWNER_ENFORCED,
        "Only registered producer roles own event types",
        "authorization",
        ("test_dt_invariants.ServiceTests.test_event_ownership_is_default_deny",),
    ),
    InvariantSpec(
        InvariantId.TRANSITION_BEFORE_PERSISTENCE,
        "Invalid transitions cannot poison replay",
        "service",
        (
            "test_dt_invariants.ServiceTests."
            "test_invalid_transition_is_rejected_before_persistence",
        ),
    ),
    InvariantSpec(
        InvariantId.DELETION_INVALIDATES_DEPENDENCIES,
        "Evidence deletion marks dependent claims stale",
        "aggregate",
        (
            "test_dt_invariants.ServiceTests."
            "test_evidence_deletion_invalidates_dependent_claim",
        ),
    ),
    InvariantSpec(
        InvariantId.PROJECTION_REVOCABLE,
        "Issued projections can be explicitly revoked",
        "projection",
        (
            "test_dt_invariants.ServiceTests.test_projection_excludes_more_sensitive_claims",
            "test_dt_invariants.ServiceTests.test_unknown_projection_cannot_be_revoked",
        ),
    ),
    InvariantSpec(
        InvariantId.BOUNDED_REALTIME_BUFFER,
        "Slow consumers cannot create unbounded memory",
        "synchronization",
        (
            "test_dt_invariants.ServiceTests."
            "test_bounded_stream_disconnects_slow_consumer",
        ),
    ),
    InvariantSpec(
        InvariantId.LOSS_REPORT_NONINTERFERENCE,
        "Projection loss reports do not reveal hidden identifiers",
        "projection",
        ("test_dt_invariants.ServiceTests.test_projection_excludes_more_sensitive_claims",),
    ),
    InvariantSpec(
        InvariantId.PRODUCER_MATCHES_ACTOR,
        "Persisted producer matches authenticated actor",
        "authorization",
        (
            "test_dt_invariants.ServiceTests."
            "test_event_producer_must_match_authenticated_actor",
        ),
    ),
    InvariantSpec(
        InvariantId.CANONICAL_SERIALIZATION,
        "Canonical hashes are stable across collection order",
        "contracts",
        (
            "test_contract_conformance.ContractRegistryTests."
            "test_set_serialization_is_deterministic",
        ),
    ),
)

INVARIANT_BY_ID = {spec.invariant_id: spec for spec in INVARIANTS}
