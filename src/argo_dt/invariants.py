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
    DURABLE_REPLAY_HANDOFF = "DT-INV-021"
    AUTHENTICATED_RESUME_CURSOR = "DT-INV-022"
    ACKNOWLEDGED_WINDOW_BOUNDED = "DT-INV-023"
    STATE_STREAM_MINIMIZED = "DT-INV-024"
    TRANSPORT_INPUT_BOUNDED = "DT-INV-025"
    TRANSPORT_SCOPE_VERIFIED = "DT-INV-026"
    CURSOR_ROTATION_BOUNDED = "DT-INV-027"
    BROKER_REDELIVERY_BOUNDED = "DT-INV-028"
    DEADLETTER_MINIMIZED = "DT-INV-029"
    OBSERVABILITY_MINIMIZED = "DT-INV-030"
    TELEMETRY_RESULT_EXPLICIT = "DT-INV-031"
    LATEST_STATE_CACHE_DERIVED = "DT-INV-032"


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
            "test_persistence.SnapshotIntegrityTests.test_snapshot_tampering_is_detected",
            "test_persistence.SnapshotIntegrityTests."
            "test_snapshot_must_link_to_exact_event_hash",
            "test_persistence.SQLiteConcurrencyTests."
            "test_two_connections_preserve_compare_and_swap",
        ),
    ),
    InvariantSpec(
        InvariantId.IDEMPOTENT_APPEND,
        "Retries return the original append",
        "event-store",
        (
            "test_dt_invariants.StoreTests.test_hash_chain_and_idempotency",
            "test_dt_invariants.ServiceTests.test_idempotent_retry_is_not_republished",
            "test_persistence.TransactionalOutboxTests.test_outbox_failure_rolls_back_event",
            "test_persistence.TransactionalOutboxTests."
            "test_failed_publish_remains_pending_and_retries",
            "test_persistence.SQLiteConcurrencyTests."
            "test_outbox_claim_is_exclusive_across_connections",
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
        (
            "test_contract_conformance.GoldenReplayTests.test_golden_replay_is_stable",
            "test_dt_invariants.ServiceTests."
            "test_source_record_change_requires_explicit_correction",
        ),
    ),
    InvariantSpec(
        InvariantId.SIMULATION_ISOLATION,
        "Simulation cannot mutate authoritative state",
        "simulation",
        (
            "test_dt_invariants.ServiceTests.test_simulation_is_isolated_from_authoritative_state",
            "test_dt_invariants.ServiceTests.test_event_plane_ownership_is_enforced",
            "test_persistence.SQLitePersistenceConformance."
            "test_authoritative_store_rejects_simulation_plane",
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
            "test_persistence.DependencyInvalidationTests."
            "test_evidence_deletion_queues_transitive_claim_and_projection",
            "test_persistence.DependencyInvalidationTests."
            "test_dependency_index_rebuilds_from_existing_events",
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
        "Projection payloads and loss reports do not reveal internal identifiers",
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
    InvariantSpec(
        InvariantId.DURABLE_REPLAY_HANDOFF,
        "Replay-to-live handoff is ordered and duplicate-safe",
        "synchronization",
        (
            "test_synchronization.DurableSynchronizationTests."
            "test_replay_live_handoff_is_ordered_deduplicated_and_paged",
            "test_synchronization.DurableSynchronizationTests."
            "test_resume_cursor_replays_only_unacknowledged_tail",
        ),
    ),
    InvariantSpec(
        InvariantId.AUTHENTICATED_RESUME_CURSOR,
        "Resume cursors are signed, twin-bound, and chain-bound",
        "synchronization",
        (
            "test_synchronization.DurableSynchronizationTests."
            "test_cursor_is_forgery_resistant_twin_bound_and_chain_bound",
        ),
    ),
    InvariantSpec(
        InvariantId.ACKNOWLEDGED_WINDOW_BOUNDED,
        "Delivery acknowledgements are cumulative and bounded",
        "synchronization",
        (
            "test_synchronization.DurableSynchronizationTests."
            "test_ack_window_is_cumulative_bounded_and_non_regressing",
        ),
    ),
    InvariantSpec(
        InvariantId.STATE_STREAM_MINIMIZED,
        "External state streams omit canonical payload and lineage",
        "synchronization",
        (
            "test_synchronization.DurableSynchronizationTests."
            "test_external_frame_is_payload_minimized_and_stream_is_subject_scoped",
        ),
    ),
    InvariantSpec(
        InvariantId.TRANSPORT_INPUT_BOUNDED,
        "Telemetry records and batches have explicit byte and count ceilings",
        "synchronization",
        (
            "test_synchronization.TransportPrimitiveTests."
            "test_transport_limits_reject_oversized_records_batches_and_counts",
        ),
    ),
    InvariantSpec(
        InvariantId.TRANSPORT_SCOPE_VERIFIED,
        "Network streams authenticate and authorize scope before processing",
        "transport",
        (
            "test_transport_adapters.WebSocketAdapterTests."
            "test_asgi_denies_before_accept_and_requires_subprotocol",
            "test_transport_adapters.GrpcAdapterTests."
            "test_grpc_rejects_missing_scope_before_processing",
        ),
    ),
    InvariantSpec(
        InvariantId.CURSOR_ROTATION_BOUNDED,
        "Cursor key rollover accepts only a bounded explicit overlap set",
        "security",
        (
            "test_transport_adapters.CursorRotationTests."
            "test_rotation_accepts_overlap_and_revokes_pruned_key",
            "test_transport_adapters.CursorRotationTests."
            "test_rotation_rejects_duplicate_ids_and_unbounded_key_sets",
        ),
    ),
    InvariantSpec(
        InvariantId.BROKER_REDELIVERY_BOUNDED,
        "JetStream derived delivery uses explicit ack and bounded retry",
        "synchronization",
        (
            "test_transport_adapters.NatsAdapterTests."
            "test_publisher_and_consumer_use_manual_ack_and_validated_subject",
            "test_transport_adapters.NatsAdapterTests."
            "test_bounded_redelivery_uses_payload_free_deadletter_marker",
        ),
    ),
    InvariantSpec(
        InvariantId.DEADLETTER_MINIMIZED,
        "Dead-letter markers never duplicate canonical event payloads",
        "privacy",
        (
            "test_transport_adapters.NatsAdapterTests."
            "test_bounded_redelivery_uses_payload_free_deadletter_marker",
        ),
    ),
    InvariantSpec(
        InvariantId.OBSERVABILITY_MINIMIZED,
        "Telemetry export rejects identity and high-cardinality attributes",
        "observability",
        (
            "test_transport_adapters.ObservabilityAdapterTests."
            "test_otel_export_is_delta_based_and_rejects_high_cardinality_attributes",
        ),
    ),
    InvariantSpec(
        InvariantId.TELEMETRY_RESULT_EXPLICIT,
        "Every telemetry record reports committed, duplicate, or rejected",
        "transport",
        (
            "test_transport_adapters.TelemetryProcessorTests."
            "test_batch_has_explicit_partial_commit_and_idempotent_retry",
        ),
    ),
    InvariantSpec(
        InvariantId.LATEST_STATE_CACHE_DERIVED,
        "Latest-state cache is bounded, copy-safe, and verified against SQLite head",
        "service",
        (
            "test_transport_adapters.StateCacheTests."
            "test_latest_cache_is_bounded_copy_safe_and_head_verified",
        ),
    ),
)

INVARIANT_BY_ID = {spec.invariant_id: spec for spec in INVARIANTS}
