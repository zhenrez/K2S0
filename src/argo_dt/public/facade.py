from __future__ import annotations

import sqlite3
import uuid

from seed_contracts import (
    AdmissionDisposition,
    AdmissionResult,
    AdmissionStatus,
    CanonicalEffect,
    RepresentationSnapshotRef,
    SubjectRef,
)

from ..epistemic import EpistemicNodeKind, LineageNode
from ..errors import (
    AuthorizationDenied,
    BackpressureExceeded,
    ConcurrencyConflict,
    IntegrityError,
    InvariantViolation,
    NotFound,
    PolicyDenied,
    ProtocolViolation,
)
from ..service import DigitalTwinService
from ..types import ActorContext, EventEnvelope, ProducerRole, Sensitivity
from .contracts import (
    AdmissionLookupRequest,
    AdmitEvidenceRequest,
    K2PublicError,
    LineageNodeRef,
    LineageQuery,
    LineageTraceView,
    PublicErrorCode,
    PublicLineageNodeKind,
    RepresentationDeficit,
    RepresentationQuery,
    RepresentationStateView,
)


class InProcessK2Facade:
    """Anti-corruption adapter over the existing K2 implementation."""

    def __init__(self, service: DigitalTwinService) -> None:
        self._service = service

    @staticmethod
    def _actor(request_authority: object) -> ActorContext:
        from .contracts import AuthorityContext

        if not isinstance(request_authority, AuthorityContext):
            raise K2PublicError(PublicErrorCode.INVALID_REQUEST, "invalid authority context")
        try:
            roles = frozenset(ProducerRole(role) for role in request_authority.roles)
        except ValueError as exc:
            raise K2PublicError(
                PublicErrorCode.INVALID_REQUEST,
                "authority contains an unsupported role",
            ) from exc
        return ActorContext(
            request_authority.identity_id,
            roles,
            subject_id=request_authority.subject_id,
        )

    @staticmethod
    def _translate_error(exc: Exception) -> K2PublicError:
        if isinstance(exc, K2PublicError):
            return exc
        if isinstance(exc, (AuthorizationDenied, PolicyDenied)):
            return K2PublicError(
                PublicErrorCode.AUTHORIZATION_DENIED,
                "authorization denied",
            )
        if isinstance(exc, ConcurrencyConflict):
            return K2PublicError(
                PublicErrorCode.STALE_STATE,
                "canonical state precondition is stale",
            )
        if isinstance(exc, NotFound):
            return K2PublicError(
                PublicErrorCode.NOT_FOUND,
                "requested canonical resource was not found",
            )
        if isinstance(exc, (InvariantViolation, ValueError, ProtocolViolation)):
            return K2PublicError(
                PublicErrorCode.INVALID_REQUEST,
                "request violates the public canonical contract",
            )
        if isinstance(exc, IntegrityError):
            return K2PublicError(
                PublicErrorCode.INVARIANT_VIOLATION,
                "canonical integrity invariant could not be satisfied",
            )
        if isinstance(exc, (sqlite3.OperationalError, OSError, BackpressureExceeded)):
            return K2PublicError(
                PublicErrorCode.TEMPORARILY_UNAVAILABLE,
                "canonical service is temporarily unavailable",
            )
        return K2PublicError(
            PublicErrorCode.TEMPORARILY_UNAVAILABLE,
            "canonical service is temporarily unavailable",
        )

    @staticmethod
    def _representation_item_ref(representation_id: str, claim_id: str) -> str:
        public_id = uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"k2-public:{representation_id}:representation-item:{claim_id}",
        )
        return f"representation-item:{public_id}"

    def _private_claim_id(self, representation_id: str, public_ref: str) -> str:
        state = self._service.state(representation_id)
        for claim_id in state.claims:
            if self._representation_item_ref(representation_id, claim_id) == public_ref:
                return claim_id
        raise K2PublicError(
            PublicErrorCode.NOT_FOUND,
            "requested public representation item was not found",
        )

    @staticmethod
    def _private_node_kind(kind: PublicLineageNodeKind) -> EpistemicNodeKind:
        mapping = {
            PublicLineageNodeKind.EVIDENCE: EpistemicNodeKind.EVIDENCE,
            PublicLineageNodeKind.REPRESENTATION_ENTITY: EpistemicNodeKind.ENTITY,
            PublicLineageNodeKind.REPRESENTATION_ITEM: EpistemicNodeKind.CLAIM,
            PublicLineageNodeKind.CONTRADICTION: EpistemicNodeKind.CONTRADICTION,
            PublicLineageNodeKind.CORRECTION: EpistemicNodeKind.CORRECTION,
            PublicLineageNodeKind.PROJECTION: EpistemicNodeKind.PROJECTION,
        }
        return mapping[kind]

    def _public_node_ref(
        self,
        representation_id: str,
        node: LineageNode,
    ) -> LineageNodeRef:
        mapping = {
            EpistemicNodeKind.EVIDENCE: PublicLineageNodeKind.EVIDENCE,
            EpistemicNodeKind.ENTITY: PublicLineageNodeKind.REPRESENTATION_ENTITY,
            EpistemicNodeKind.CLAIM: PublicLineageNodeKind.REPRESENTATION_ITEM,
            EpistemicNodeKind.CONTRADICTION: PublicLineageNodeKind.CONTRADICTION,
            EpistemicNodeKind.CORRECTION: PublicLineageNodeKind.CORRECTION,
            EpistemicNodeKind.PROJECTION: PublicLineageNodeKind.PROJECTION,
        }
        node_id = node.node_id
        if node.kind is EpistemicNodeKind.CLAIM:
            node_id = self._representation_item_ref(representation_id, node.node_id)
        return LineageNodeRef(mapping[node.kind], node_id)

    def _snapshot_ref(
        self,
        representation_id: str,
        sequence: int,
        *,
        event_hash: str | None = None,
    ) -> RepresentationSnapshotRef:
        state = self._service.state(representation_id, up_to_sequence=sequence)
        if state.subject_id is None:
            raise K2PublicError(
                PublicErrorCode.INVARIANT_VIOLATION,
                "canonical representation has no subject binding",
            )
        integrity = event_hash or state.last_event_hash
        if not integrity:
            raise K2PublicError(
                PublicErrorCode.INVARIANT_VIOLATION,
                "canonical representation has no integrity anchor",
            )
        return RepresentationSnapshotRef(
            subject_ref=SubjectRef(state.subject_id),
            snapshot_id=f"k2:{representation_id}:sequence:{sequence}",
            canonical_sequence=sequence,
            integrity_ref=f"event-chain:{integrity}",
        )

    def _event_to_result(
        self,
        event: EventEnvelope,
        *,
        request_id: str,
        replayed: bool,
    ) -> AdmissionResult:
        if event.event_type != "EvidenceIngested":
            raise K2PublicError(
                PublicErrorCode.INVARIANT_VIOLATION,
                "request does not resolve to an evidence admission",
            )
        snapshot = self._snapshot_ref(
            event.twin_id,
            event.sequence,
            event_hash=event.event_hash,
        )
        evidence_id = str(event.payload.get("evidence_id", ""))
        if not evidence_id:
            raise K2PublicError(
                PublicErrorCode.INVARIANT_VIOLATION,
                "canonical evidence event is missing evidence identity",
            )
        effect = CanonicalEffect(
            effect_id=str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"{event.event_id}:evidence_registered",
                )
            ),
            effect_kind="evidence_registered",
            affected_semantic_ref=evidence_id,
            source_request_id=request_id,
            canonical_position=snapshot,
        )
        return AdmissionResult(
            request_id=request_id,
            admission_id=event.event_id,
            status=AdmissionStatus.ADMITTED,
            disposition=AdmissionDisposition.EFFECTS_APPLIED,
            canonical_effects=(effect,),
            resulting_representation_state_ref=snapshot,
            replayed=replayed,
            original_result_ref=event.event_id if replayed else None,
        )

    async def admit_evidence(self, request: AdmitEvidenceRequest) -> AdmissionResult:
        try:
            actor = self._actor(request.authority)
            head_before, _ = self._service.store.head(request.representation_id)
            event = await self._service.ingest_evidence(
                twin_id=request.representation_id,
                subject_id=request.subject_ref.subject_id,
                source=request.source,
                source_record_id=request.source_record_id,
                payload=request.bounded_evidence,
                rights=request.rights,
                sensitivity=Sensitivity(request.sensitivity),
                valid_from=request.valid_from,
                valid_until=request.valid_until,
                independence_group=request.independence_group,
                expected_sequence=request.based_on_sequence,
                idempotency_key=request.request_id,
                actor=actor,
                connector_version=request.connector_version,
                media_type=request.media_type,
                source_content_hash=request.source_content_hash,
            )
        except Exception as exc:
            raise self._translate_error(exc) from exc
        replayed = event.sequence <= head_before
        return self._event_to_result(
            event,
            request_id=request.request_id,
            replayed=replayed,
        )

    async def get_admission_result(
        self, request: AdmissionLookupRequest
    ) -> AdmissionResult | None:
        actor = self._actor(request.authority)
        try:
            for event in self._service.store.load(request.representation_id):
                if event.idempotency_key != request.request_id:
                    continue
                if event.event_type != "EvidenceIngested":
                    return None
                state = self._service.state(
                    request.representation_id,
                    up_to_sequence=event.sequence,
                )
                self._service.ownership_policy.authorize_subject(actor, state.subject_id)
                return self._event_to_result(
                    event,
                    request_id=request.request_id,
                    replayed=False,
                )
        except Exception as exc:
            raise self._translate_error(exc) from exc
        return None

    async def read_representation_state(
        self, request: RepresentationQuery
    ) -> RepresentationStateView:
        actor = self._actor(request.authority)
        try:
            state = self._service.state(request.representation_id)
            self._service.ownership_policy.authorize_subject(actor, state.subject_id)
        except Exception as exc:
            raise self._translate_error(exc) from exc
        if state.subject_id is None or state.sequence < 1:
            raise K2PublicError(
                PublicErrorCode.NOT_FOUND,
                "canonical representation is not initialized",
            )
        snapshot = RepresentationSnapshotRef(
            subject_ref=SubjectRef(state.subject_id),
            snapshot_id=f"k2:{request.representation_id}:sequence:{state.sequence}",
            canonical_sequence=state.sequence,
            integrity_ref=f"event-chain:{state.last_event_hash}",
        )
        return RepresentationStateView(
            representation_id=request.representation_id,
            subject_ref=SubjectRef(state.subject_id),
            snapshot_ref=snapshot,
            evidence_refs=tuple(sorted(state.evidence)),
            representation_item_refs=tuple(
                self._representation_item_ref(request.representation_id, claim_id)
                for claim_id in sorted(state.claims)
            ),
            accepted_representation_item_refs=tuple(
                self._representation_item_ref(request.representation_id, claim_id)
                for claim_id in sorted(state.accepted_claim_ids)
            ),
            contested_representation_item_refs=tuple(
                self._representation_item_ref(request.representation_id, claim_id)
                for claim_id in sorted(state.contested_claim_ids)
            ),
            contradiction_refs=tuple(sorted(state.contradictions)),
            degraded_reasons=tuple(state.degraded_reasons),
        )

    async def read_representation_deficits(
        self, request: RepresentationQuery
    ) -> tuple[RepresentationDeficit, ...]:
        state_view = await self.read_representation_state(request)
        state = self._service.state(request.representation_id)
        deficits: list[RepresentationDeficit] = []
        if not state.claims:
            deficits.append(
                RepresentationDeficit(
                    deficit_id=f"{request.representation_id}:missing-representation",
                    kind="missing_representation",
                    target_refs=(),
                    rationale="No canonical claim currently represents the subject.",
                )
            )
        for claim_id in sorted(state.stale_claim_ids):
            deficits.append(
                RepresentationDeficit(
                    deficit_id=f"{request.representation_id}:stale:{claim_id}",
                    kind="stale_representation",
                    target_refs=(
                        self._representation_item_ref(
                            request.representation_id,
                            claim_id,
                        ),
                    ),
                    rationale="Canonical representation item depends on stale evidence.",
                )
            )
        for claim_id in sorted(state.contested_claim_ids):
            deficits.append(
                RepresentationDeficit(
                    deficit_id=f"{request.representation_id}:contested:{claim_id}",
                    kind="contested_representation",
                    target_refs=(
                        self._representation_item_ref(
                            request.representation_id,
                            claim_id,
                        ),
                    ),
                    rationale="Canonical representation item remains contested.",
                )
            )
        for contradiction_id in sorted(
            set(state.contradictions).difference(state.resolved_contradiction_ids)
        ):
            deficits.append(
                RepresentationDeficit(
                    deficit_id=f"{request.representation_id}:contradiction:{contradiction_id}",
                    kind="unresolved_contradiction",
                    target_refs=(contradiction_id,),
                    rationale="Canonical contradiction remains unresolved.",
                )
            )
        for index, reason in enumerate(state_view.degraded_reasons):
            deficits.append(
                RepresentationDeficit(
                    deficit_id=f"{request.representation_id}:degraded:{index}",
                    kind="degraded_state",
                    target_refs=(),
                    rationale=reason,
                )
            )
        return tuple(deficits)

    async def trace_lineage(self, request: LineageQuery) -> LineageTraceView:
        actor = self._actor(request.authority)
        try:
            state = self._service.state(request.representation_id)
            self._service.ownership_policy.authorize_subject(actor, state.subject_id)
            private_kind = self._private_node_kind(request.node_kind)
            private_node_id = request.node_id
            if request.node_kind is PublicLineageNodeKind.REPRESENTATION_ITEM:
                private_node_id = self._private_claim_id(
                    request.representation_id,
                    request.node_id,
                )
            trace = self._service.trace_lineage(
                request.representation_id,
                node_kind=private_kind,
                node_id=private_node_id,
            )
        except Exception as exc:
            raise self._translate_error(exc) from exc
        return LineageTraceView(
            representation_id=request.representation_id,
            root=self._public_node_ref(request.representation_id, trace.root),
            ancestors=tuple(
                self._public_node_ref(request.representation_id, node)
                for node in trace.ancestors
            ),
            dependents=tuple(
                self._public_node_ref(request.representation_id, node)
                for node in trace.dependents
            ),
            evidence_by_independence_group=trace.evidence_by_independence_group,
        )
