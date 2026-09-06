"""Application façade used by REST, gRPC, MCP, and local adapters."""

from __future__ import annotations

import copy
import uuid
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any

from .aggregate import TwinAggregate, TwinState
from .compiler import ProjectionCompiler
from .epistemic import (
    ElicitationPlan,
    ElicitationResponse,
    EpistemicNodeKind,
    GapDirectedElicitor,
    LineageNode,
    LineageTrace,
    LineageTracer,
)
from .errors import AuthorizationDenied, ConcurrencyConflict, InvariantViolation
from .ownership import EventOwnershipPolicy
from .ports import BronzeVault, DurableEventStore
from .simulation import SimulationBranch, SimulationEngine
from .sync import (
    BoundedEventBroker,
    CursorCodec,
    DurableSubscription,
    OutboxRelay,
    RelayBatch,
    StateStreamSession,
    StreamPosition,
    SyncLimits,
    SyncMetrics,
)
from .types import (
    ActorContext,
    ConsentGrant,
    EventEnvelope,
    EventPlane,
    ProducerRole,
    ProjectionReceipt,
    ProjectionRequest,
    Sensitivity,
    canonical_json,
    content_hash,
    utc_now,
)


class DigitalTwinService:
    """Coordinates invariant checks, persistence, and real-time publication."""

    def __init__(
        self,
        *,
        store: DurableEventStore,
        projection_compiler: ProjectionCompiler,
        broker: BoundedEventBroker | None = None,
        ownership_policy: EventOwnershipPolicy | None = None,
        snapshot_interval: int = 1000,
        snapshot_retention: int = 3,
        outbox_retention: timedelta = timedelta(days=7),
        cursor_signer: CursorCodec | None = None,
        sync_limits: SyncLimits | None = None,
        state_cache_entries: int = 128,
        bronze_vault: BronzeVault | None = None,
    ) -> None:
        if snapshot_interval < 0:
            raise ValueError("snapshot_interval cannot be negative")
        if snapshot_retention < 1:
            raise ValueError("snapshot_retention must be positive")
        if outbox_retention <= timedelta(0):
            raise ValueError("outbox_retention must be positive")
        if state_cache_entries < 0:
            raise ValueError("state_cache_entries cannot be negative")
        self.store = store
        self.projection_compiler = projection_compiler
        self.broker = broker or BoundedEventBroker()
        self.outbox_relay = OutboxRelay(store=store, publisher=self.broker)
        self.cursor_signer = cursor_signer
        self.sync_limits = sync_limits or SyncLimits()
        self.sync_metrics = SyncMetrics()
        self.ownership_policy = ownership_policy or EventOwnershipPolicy()
        self.simulations = SimulationEngine()
        self.snapshot_interval = snapshot_interval
        self.snapshot_retention = snapshot_retention
        self.outbox_retention = outbox_retention
        self._state_cache_entries = state_cache_entries
        self._state_cache: OrderedDict[str, TwinState] = OrderedDict()
        self.bronze_vault = bronze_vault
        self.elicitor = GapDirectedElicitor()
        self.lineage_tracer = LineageTracer(store)

    async def append(
        self,
        event: EventEnvelope,
        *,
        actor: ActorContext,
        expected_sequence: int,
    ) -> EventEnvelope:
        self.ownership_policy.authorize(event, actor)
        TwinAggregate.validate_event(event)
        if event.plane is EventPlane.SIMULATION:
            raise InvariantViolation(
                "simulation events belong to SimulationBranch, not the authoritative store"
            )
        head_sequence, head_hash = self.store.head(event.twin_id)
        current_state: TwinState | None = None
        if head_sequence == expected_sequence:
            current_state = self._latest_state(event.twin_id)
            if (
                current_state.sequence != head_sequence
                or current_state.last_event_hash != head_hash
            ):
                raise ConcurrencyConflict("stream changed during transition validation")
            preview = event.seal(
                sequence=expected_sequence + 1,
                previous_hash=head_hash,
                recorded_at=utc_now(),
            )
            TwinAggregate.apply(copy.deepcopy(current_state), preview)
        persisted = self.store.append(event, expected_sequence=expected_sequence)
        if persisted.sequence > head_sequence:
            if current_state is not None:
                try:
                    TwinAggregate.apply(current_state, persisted)
                except Exception:
                    self._state_cache.pop(persisted.twin_id, None)
                    raise
                self._cache_state(current_state)
            else:
                self._state_cache.pop(persisted.twin_id, None)
            if (
                self.snapshot_interval > 0
                and persisted.sequence % self.snapshot_interval == 0
            ):
                state = TwinAggregate.rebuild(self.store, persisted.twin_id)
                self.store.save_snapshot(state.to_snapshot())
                self.store.prune_snapshots(
                    persisted.twin_id,
                    keep=self.snapshot_retention,
                )
                self.store.prune_outbox(
                    published_before=utc_now() - self.outbox_retention
                )
        await self.outbox_relay.drain()
        return persisted

    async def flush_publications(self, *, limit: int = 100) -> RelayBatch:
        return await self.outbox_relay.drain(limit=limit)

    async def ingest_evidence(
        self,
        *,
        twin_id: str,
        subject_id: str,
        source: str,
        source_record_id: str,
        payload: Mapping[str, Any],
        rights: Mapping[str, Any],
        sensitivity: Sensitivity,
        valid_from: datetime,
        valid_until: datetime | None,
        independence_group: str,
        expected_sequence: int,
        idempotency_key: str,
        actor: ActorContext,
        connector_version: str | None = None,
        media_type: str | None = None,
        source_content_hash: str | None = None,
    ) -> EventEnvelope:
        self.ownership_policy.authorize_intent(
            "EvidenceIngested",
            ProducerRole.INGEST_SERVICE,
            actor,
        )
        self.ownership_policy.authorize_subject(actor, subject_id)
        state = self._latest_state(twin_id)
        if state.subject_id is not None and state.subject_id != subject_id:
            raise InvariantViolation("evidence subject does not match twin subject")
        evidence_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{twin_id}:{subject_id}:{source}:{source_record_id}",
            )
        )
        event_payload: dict[str, Any] = {
            "evidence_id": evidence_id,
            "subject_id": subject_id,
            "source": source,
            "source_record_id": source_record_id,
            "content_hash": content_hash(payload),
            "normalized_payload": dict(payload),
            "rights": dict(rights),
            "sensitivity": sensitivity.value,
            "valid_from": valid_from.isoformat(),
            "valid_until": valid_until.isoformat() if valid_until else None,
            "independence_group": independence_group,
        }
        if connector_version is not None:
            event_payload["connector_version"] = connector_version
        if media_type is not None:
            event_payload["media_type"] = media_type
        if source_content_hash is not None:
            event_payload["source_content_hash"] = source_content_hash
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="EvidenceIngested",
            plane=EventPlane.AUTHORITATIVE,
            producer=actor.identity_id,
            producer_role=ProducerRole.INGEST_SERVICE,
            idempotency_key=idempotency_key,
            occurred_at=valid_from,
            payload=event_payload,
        )
        return await self.append(event, actor=actor, expected_sequence=expected_sequence)

    async def delete_evidence(
        self,
        *,
        twin_id: str,
        evidence_id: str,
        reason: str,
        expected_sequence: int,
        idempotency_key: str,
        actor: ActorContext,
    ) -> EventEnvelope:
        self.ownership_policy.authorize_intent(
            "EvidenceDeleted",
            ProducerRole.INGEST_SERVICE,
            actor,
        )
        state = self.state(twin_id)
        self.ownership_policy.authorize_subject(actor, state.subject_id)
        if state.sequence != expected_sequence:
            raise ConcurrencyConflict(
                f"deletion expected {expected_sequence}, state is {state.sequence}"
            )
        if evidence_id not in state.evidence:
            raise InvariantViolation("cannot delete unknown evidence")
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="EvidenceDeleted",
            plane=EventPlane.AUTHORITATIVE,
            producer=actor.identity_id,
            producer_role=ProducerRole.INGEST_SERVICE,
            idempotency_key=idempotency_key,
            payload={
                "evidence_id": evidence_id,
                "reason": reason,
                "subject_id": state.subject_id,
            },
        )
        return await self.append(event, actor=actor, expected_sequence=expected_sequence)

    async def propose_claim(
        self,
        *,
        twin_id: str,
        subject_id: str,
        statement: str,
        kind: str,
        provenance: Sequence[Mapping[str, Any]],
        sensitivity: Sensitivity,
        valid_from: datetime,
        valid_until: datetime | None,
        epistemic: Mapping[str, float],
        expected_sequence: int,
        idempotency_key: str,
        actor: ActorContext,
        model_trace: Mapping[str, Any] | None = None,
    ) -> EventEnvelope:
        self.ownership_policy.authorize_intent(
            "ClaimProposed",
            ProducerRole.ADJUDICATION_WORKER,
            actor,
        )
        self.ownership_policy.authorize_subject(actor, subject_id)
        if not provenance:
            raise InvariantViolation("a claim cannot be proposed without provenance")
        TwinAggregate.validate_epistemic(dict(epistemic))
        claim_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                canonical_json(["claim", twin_id, idempotency_key]),
            )
        )
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="ClaimProposed",
            plane=EventPlane.AUTHORITATIVE,
            producer=actor.identity_id,
            producer_role=ProducerRole.ADJUDICATION_WORKER,
            idempotency_key=idempotency_key,
            occurred_at=valid_from,
            payload={
                "claim_id": claim_id,
                "subject_id": subject_id,
                "statement": statement,
                "kind": kind,
                "provenance": [dict(ref) for ref in provenance],
                "sensitivity": sensitivity.value,
                "valid_from": valid_from.isoformat(),
                "valid_until": valid_until.isoformat() if valid_until else None,
                "epistemic": dict(epistemic),
                "model_trace": dict(model_trace or {}),
                "review_state": "unreviewed",
            },
        )
        return await self.append(event, actor=actor, expected_sequence=expected_sequence)

    async def review_claim(
        self,
        *,
        twin_id: str,
        claim_id: str,
        accepted: bool,
        rationale: str,
        reviewed_at: datetime,
        expected_sequence: int,
        idempotency_key: str,
        actor: ActorContext,
    ) -> EventEnvelope:
        event_type = "ClaimAccepted" if accepted else "ClaimContested"
        self.ownership_policy.authorize_intent(
            event_type,
            ProducerRole.HUMAN_REVIEW,
            actor,
        )
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type=event_type,
            plane=EventPlane.AUTHORITATIVE,
            producer=actor.identity_id,
            producer_role=ProducerRole.HUMAN_REVIEW,
            idempotency_key=idempotency_key,
            occurred_at=reviewed_at,
            payload={
                "claim_id": claim_id,
                "reviewer_identity_id": actor.identity_id,
                "rationale": rationale,
            },
        )
        state = self.state(twin_id)
        self.ownership_policy.authorize_subject(actor, state.subject_id)
        if claim_id not in state.claims:
            raise InvariantViolation("cannot review an unknown claim")
        return await self.append(event, actor=actor, expected_sequence=expected_sequence)

    async def link_entity(
        self,
        *,
        twin_id: str,
        subject_id: str,
        namespace: str,
        entity_id: str,
        provenance: Sequence[Mapping[str, Any]],
        confidence: float,
        valid_from: datetime,
        valid_until: datetime | None,
        expected_sequence: int,
        idempotency_key: str,
        actor: ActorContext,
    ) -> EventEnvelope:
        self.ownership_policy.authorize_intent(
            "EntityLinked", ProducerRole.IDENTITY_WORKER, actor
        )
        self.ownership_policy.authorize_subject(actor, subject_id)
        link_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                canonical_json(
                    ["entity-link", twin_id, subject_id, namespace, entity_id]
                ),
            )
        )
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="EntityLinked",
            plane=EventPlane.AUTHORITATIVE,
            producer=actor.identity_id,
            producer_role=ProducerRole.IDENTITY_WORKER,
            idempotency_key=idempotency_key,
            occurred_at=valid_from,
            payload={
                "entity_link_id": link_id,
                "subject_id": subject_id,
                "namespace": namespace,
                "entity_id": entity_id,
                "provenance": [dict(reference) for reference in provenance],
                "confidence": confidence,
                "valid_from": valid_from.isoformat(),
                "valid_until": valid_until.isoformat() if valid_until else None,
            },
        )
        return await self.append(event, actor=actor, expected_sequence=expected_sequence)

    async def unlink_entity(
        self,
        *,
        twin_id: str,
        entity_link_id: str,
        reason: str,
        unlinked_at: datetime,
        expected_sequence: int,
        idempotency_key: str,
        actor: ActorContext,
    ) -> EventEnvelope:
        self.ownership_policy.authorize_intent(
            "EntityUnlinked", ProducerRole.IDENTITY_WORKER, actor
        )
        state = self.state(twin_id)
        self.ownership_policy.authorize_subject(actor, state.subject_id)
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="EntityUnlinked",
            plane=EventPlane.AUTHORITATIVE,
            producer=actor.identity_id,
            producer_role=ProducerRole.IDENTITY_WORKER,
            idempotency_key=idempotency_key,
            occurred_at=unlinked_at,
            payload={
                "entity_link_id": entity_link_id,
                "subject_id": state.subject_id,
                "reason": reason,
            },
        )
        return await self.append(event, actor=actor, expected_sequence=expected_sequence)

    async def detect_contradiction(
        self,
        *,
        twin_id: str,
        claim_ids: Sequence[str],
        basis: str,
        detected_at: datetime,
        expected_sequence: int,
        idempotency_key: str,
        actor: ActorContext,
    ) -> EventEnvelope:
        self.ownership_policy.authorize_intent(
            "ContradictionDetected", ProducerRole.ADJUDICATION_WORKER, actor
        )
        state = self.state(twin_id)
        self.ownership_policy.authorize_subject(actor, state.subject_id)
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="ContradictionDetected",
            plane=EventPlane.AUTHORITATIVE,
            producer=actor.identity_id,
            producer_role=ProducerRole.ADJUDICATION_WORKER,
            idempotency_key=idempotency_key,
            occurred_at=detected_at,
            payload={
                "contradiction_id": str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        canonical_json(["contradiction", twin_id, idempotency_key]),
                    )
                ),
                "subject_id": state.subject_id,
                "claim_ids": list(claim_ids),
                "basis": basis,
            },
        )
        return await self.append(event, actor=actor, expected_sequence=expected_sequence)

    async def adjudicate_contradiction(
        self,
        *,
        twin_id: str,
        contradiction_id: str,
        resolution: str,
        preferred_claim_ids: Sequence[str],
        rationale: str,
        adjudicated_at: datetime,
        expected_sequence: int,
        idempotency_key: str,
        actor: ActorContext,
    ) -> EventEnvelope:
        self.ownership_policy.authorize_intent(
            "ContradictionAdjudicated", ProducerRole.HUMAN_REVIEW, actor
        )
        state = self.state(twin_id)
        self.ownership_policy.authorize_subject(actor, state.subject_id)
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="ContradictionAdjudicated",
            plane=EventPlane.AUTHORITATIVE,
            producer=actor.identity_id,
            producer_role=ProducerRole.HUMAN_REVIEW,
            idempotency_key=idempotency_key,
            occurred_at=adjudicated_at,
            payload={
                "contradiction_id": contradiction_id,
                "subject_id": state.subject_id,
                "resolution": resolution,
                "preferred_claim_ids": list(preferred_claim_ids),
                "reviewer_identity_id": actor.identity_id,
                "rationale": rationale,
            },
        )
        return await self.append(event, actor=actor, expected_sequence=expected_sequence)

    async def record_correction(
        self,
        *,
        twin_id: str,
        target_kind: str,
        target_id: str,
        replacement_id: str,
        rationale: str,
        corrected_at: datetime,
        expected_sequence: int,
        idempotency_key: str,
        actor: ActorContext,
    ) -> EventEnvelope:
        self.ownership_policy.authorize_intent(
            "CorrectionRecorded", ProducerRole.HUMAN_REVIEW, actor
        )
        state = self.state(twin_id)
        self.ownership_policy.authorize_subject(actor, state.subject_id)
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="CorrectionRecorded",
            plane=EventPlane.AUTHORITATIVE,
            producer=actor.identity_id,
            producer_role=ProducerRole.HUMAN_REVIEW,
            idempotency_key=idempotency_key,
            occurred_at=corrected_at,
            payload={
                "correction_id": str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        canonical_json(["correction", twin_id, idempotency_key]),
                    )
                ),
                "subject_id": state.subject_id,
                "target_kind": target_kind,
                "target_id": target_id,
                "replacement_id": replacement_id,
                "reviewer_identity_id": actor.identity_id,
                "rationale": rationale,
            },
        )
        return await self.append(event, actor=actor, expected_sequence=expected_sequence)

    def trace_lineage(
        self,
        twin_id: str,
        *,
        node_kind: EpistemicNodeKind,
        node_id: str,
        as_of_recorded_time: datetime | None = None,
    ) -> LineageTrace:
        state = self.state(twin_id, as_of_recorded_time=as_of_recorded_time)
        return self.lineage_tracer.trace(state, LineageNode(node_kind, node_id))

    def plan_elicitation(
        self,
        twin_id: str,
        *,
        objective: str,
        claim_ids: tuple[str, ...] | None = None,
    ) -> ElicitationPlan:
        return self.elicitor.plan(
            self.state(twin_id),
            objective=objective,
            claim_ids=claim_ids,
        )

    async def record_elicitation_response(
        self,
        *,
        twin_id: str,
        subject_id: str,
        plan: ElicitationPlan,
        response: ElicitationResponse,
        rights: Mapping[str, Any],
        expected_sequence: int,
        idempotency_key: str,
        actor: ActorContext,
    ) -> EventEnvelope:
        self.ownership_policy.authorize_intent(
            "EvidenceIngested", ProducerRole.INGEST_SERVICE, actor
        )
        self.ownership_policy.authorize_subject(actor, subject_id)
        if self.bronze_vault is None:
            raise InvariantViolation("elicitation requires a configured Bronze vault")
        if plan.twin_id != twin_id:
            raise InvariantViolation("elicitation plan belongs to a different twin")
        source_state = self.state(twin_id, up_to_sequence=plan.source_sequence)
        self.elicitor.verify_plan(source_state, plan)
        question = plan.question(response.question_id)
        state = self.state(twin_id)
        if state.subject_id is not None and state.subject_id != subject_id:
            raise InvariantViolation("elicitation subject does not match twin subject")
        content = canonical_json(
            {
                "schema_version": "argo.dt.elicitation-response/v1",
                "plan_id": plan.plan_id,
                "question_id": response.question_id,
                "answer": response.answer,
                "answered_at": response.answered_at,
            }
        ).encode("utf-8")
        object_uri, source_hash = self.bronze_vault.put(
            subject_id=subject_id,
            media_type="application/json",
            content=content,
            metadata={
                "connector_id": "argo.dt.elicitation",
                "source_record_id": response.response_id,
                "plan_id": plan.plan_id,
                "question_id": response.question_id,
            },
        )
        evidence_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                canonical_json(
                    ["elicitation", twin_id, subject_id, response.response_id]
                ),
            )
        )
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="EvidenceIngested",
            plane=EventPlane.AUTHORITATIVE,
            producer=actor.identity_id,
            producer_role=ProducerRole.INGEST_SERVICE,
            idempotency_key=idempotency_key,
            occurred_at=response.answered_at,
            payload={
                "evidence_id": evidence_id,
                "subject_id": subject_id,
                "source": "elicitation",
                "source_record_id": response.response_id,
                "content_hash": source_hash,
                "bronze_uri": object_uri,
                "normalized_payload": {
                    "plan_id": plan.plan_id,
                    "plan_source_sequence": plan.source_sequence,
                    "gap_id": question.gap_id,
                    "question_id": question.question_id,
                },
                "rights": dict(rights),
                "sensitivity": response.sensitivity.value,
                "valid_from": response.answered_at.isoformat(),
                "valid_until": None,
                "independence_group": f"elicitation:{plan.plan_id}",
            },
        )
        return await self.append(event, actor=actor, expected_sequence=expected_sequence)

    def state(
        self,
        twin_id: str,
        *,
        up_to_sequence: int | None = None,
        as_of_valid_time: datetime | None = None,
        as_of_recorded_time: datetime | None = None,
    ) -> TwinState:
        if up_to_sequence is None and as_of_recorded_time is None:
            state = copy.deepcopy(self._latest_state(twin_id))
        else:
            state = TwinAggregate.rebuild(
                self.store,
                twin_id,
                snapshot_store=self.store,
                up_to_sequence=up_to_sequence,
                as_of_recorded_time=as_of_recorded_time,
            )
        return state.select_valid_at(as_of_valid_time) if as_of_valid_time else state

    @property
    def cached_twin_count(self) -> int:
        return len(self._state_cache)

    def _latest_state(self, twin_id: str) -> TwinState:
        head_sequence, head_hash = self.store.head(twin_id)
        cached = self._state_cache.get(twin_id)
        if (
            cached is not None
            and cached.sequence == head_sequence
            and cached.last_event_hash == head_hash
        ):
            self._state_cache.move_to_end(twin_id)
            return cached
        if cached is not None:
            self._state_cache.pop(twin_id, None)
        state = TwinAggregate.rebuild(
            self.store,
            twin_id,
            snapshot_store=self.store,
        )
        self._cache_state(state)
        return state

    def _cache_state(self, state: TwinState) -> None:
        if self._state_cache_entries == 0:
            return
        self._state_cache[state.twin_id] = state
        self._state_cache.move_to_end(state.twin_id)
        while len(self._state_cache) > self._state_cache_entries:
            self._state_cache.popitem(last=False)

    async def issue_projection(
        self,
        *,
        request: ProjectionRequest,
        consent: ConsentGrant | None,
        expected_sequence: int,
        idempotency_key: str,
        actor: ActorContext,
    ) -> tuple[dict[str, Any], ProjectionReceipt, EventEnvelope]:
        self.ownership_policy.authorize_intent(
            "ProjectionIssued",
            ProducerRole.PROJECTION_SERVICE,
            actor,
        )
        self.ownership_policy.authorize_subject(actor, request.subject_id)
        head_sequence, _ = self.store.head(request.twin_id)
        if head_sequence != expected_sequence:
            raise ConcurrencyConflict(
                f"projection expected {expected_sequence}, stream head is {head_sequence}"
            )
        state = self.state(
            request.twin_id,
            as_of_recorded_time=request.as_of_recorded_time,
        )
        if state.subject_id is None or state.subject_id != request.subject_id:
            raise InvariantViolation("projection subject does not match twin subject")
        projection, receipt = self.projection_compiler.compile(
            state=state,
            request=request,
            consent=consent,
        )
        audit_event = EventEnvelope.new(
            twin_id=request.twin_id,
            event_type="ProjectionIssued",
            plane=EventPlane.PROJECTION,
            producer=actor.identity_id,
            producer_role=ProducerRole.PROJECTION_SERVICE,
            idempotency_key=idempotency_key,
            correlation_id=request.request_id,
            payload={
                "projection_id": receipt.projection_id,
                "purpose": request.purpose,
                "recipient_id": request.recipient_id,
                "receipt_hash": content_hash(receipt),
                "disclosed_fields": list(receipt.disclosed_fields),
                "source_sequence": receipt.source_sequence,
                "source_claim_ids": list(receipt.source_claim_ids),
            },
        )
        persisted = await self.append(
            audit_event,
            actor=actor,
            expected_sequence=expected_sequence,
        )
        return projection, receipt, persisted

    async def revoke_projection(
        self,
        *,
        twin_id: str,
        projection_id: str,
        reason: str,
        expected_sequence: int,
        idempotency_key: str,
        actor: ActorContext,
    ) -> EventEnvelope:
        self.ownership_policy.authorize_intent(
            "ProjectionRevoked",
            ProducerRole.PROJECTION_SERVICE,
            actor,
        )
        state = self.state(twin_id)
        self.ownership_policy.authorize_subject(actor, state.subject_id)
        if state.sequence != expected_sequence:
            raise ConcurrencyConflict(
                f"revocation expected {expected_sequence}, state is {state.sequence}"
            )
        if projection_id not in state.issued_projections:
            raise InvariantViolation("cannot revoke an unknown projection")
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="ProjectionRevoked",
            plane=EventPlane.PROJECTION,
            producer=actor.identity_id,
            producer_role=ProducerRole.PROJECTION_SERVICE,
            idempotency_key=idempotency_key,
            payload={
                "projection_id": projection_id,
                "reason": reason,
                "actor_identity_id": actor.identity_id,
            },
        )
        return await self.append(event, actor=actor, expected_sequence=expected_sequence)

    def fork_simulation(
        self,
        twin_id: str,
        *,
        scenario: str,
        actor: ActorContext,
    ) -> SimulationBranch:
        self.ownership_policy.authorize_intent(
            "ScenarioStatePredicted",
            ProducerRole.SIMULATION_SERVICE,
            actor,
        )
        state = self.state(twin_id)
        self.ownership_policy.authorize_subject(actor, state.subject_id)
        return self.simulations.fork(state, scenario=scenario)

    async def subscribe(
        self,
        twin_id: str,
        *,
        actor: ActorContext,
        after_sequence: int = 0,
    ) -> DurableSubscription:
        """Open an operations-only raw event stream with durable replay."""

        if ProducerRole.OPERATIONS_SERVICE not in actor.roles:
            raise AuthorizationDenied("raw event subscriptions require operations role")
        if actor.subject_id is not None:
            state = self.state(twin_id)
            self.ownership_policy.authorize_subject(actor, state.subject_id)
        start = self._stream_position(twin_id, after_sequence)
        return await DurableSubscription.open(
            store=self.store,
            broker=self.broker,
            start=start,
            limits=self.sync_limits,
            metrics=self.sync_metrics,
        )

    async def open_state_stream(
        self,
        twin_id: str,
        *,
        actor: ActorContext,
        resume_token: str | None = None,
        event_types: Sequence[str] = (),
        include_projection_events: bool = False,
        max_in_flight: int | None = None,
    ) -> StateStreamSession:
        """Open an authorized payload-minimized state notification stream."""

        trusted_global_roles = {
            ProducerRole.OPERATIONS_SERVICE,
            ProducerRole.PROJECTION_SERVICE,
        }
        if actor.subject_id is None:
            if actor.roles.isdisjoint(trusted_global_roles):
                raise AuthorizationDenied(
                    "global state streams require operations or projection role"
                )
        else:
            state = self.state(twin_id)
            self.ownership_policy.authorize_subject(actor, state.subject_id)
        cursor_signer = self.cursor_signer
        if cursor_signer is None:
            raise InvariantViolation(
                "state streams require an explicitly configured cursor signer"
            )
        if resume_token is None:
            start = StreamPosition(twin_id, 0, "")
        else:
            decoded = cursor_signer.verify(
                resume_token,
                twin_id=twin_id,
            )
            start = cursor_signer.verify_chain_binding(
                decoded,
                self._stream_position(twin_id, decoded.sequence),
            )
        planes = {EventPlane.AUTHORITATIVE}
        if include_projection_events:
            planes.add(EventPlane.PROJECTION)
        subscription = await DurableSubscription.open(
            store=self.store,
            broker=self.broker,
            start=start,
            limits=self.sync_limits.negotiate(max_in_flight=max_in_flight),
            event_types=event_types,
            planes=planes,
            metrics=self.sync_metrics,
        )
        return StateStreamSession(
            subscription=subscription,
            cursor_signer=cursor_signer,
            metrics=self.sync_metrics,
        )

    def _stream_position(self, twin_id: str, sequence: int) -> StreamPosition:
        if sequence < 0:
            raise InvariantViolation("after_sequence cannot be negative")
        if sequence == 0:
            return StreamPosition(twin_id, 0, "")
        events = self.store.load(
            twin_id,
            after_sequence=sequence - 1,
            up_to_sequence=sequence,
            limit=1,
        )
        if not events or events[0].sequence != sequence:
            raise InvariantViolation("after_sequence is not present in the event stream")
        return StreamPosition(twin_id, sequence, events[0].event_hash)
