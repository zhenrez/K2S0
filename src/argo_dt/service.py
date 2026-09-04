"""Application façade used by REST, gRPC, MCP, and local adapters."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Mapping, Sequence

from .aggregate import TwinAggregate, TwinState
from .compiler import ProjectionCompiler
from .errors import ConcurrencyConflict, InvariantViolation
from .ownership import EventOwnershipPolicy
from .ports import EventStore
from .simulation import SimulationBranch, SimulationEngine
from .sync import BoundedEventBroker, Subscription
from .types import (
    ActorContext,
    ConsentGrant,
    EventEnvelope,
    EventPlane,
    ProducerRole,
    ProjectionReceipt,
    ProjectionRequest,
    Sensitivity,
    content_hash,
    utc_now,
)


class DigitalTwinService:
    """Coordinates invariant checks, persistence, and real-time publication."""

    def __init__(
        self,
        *,
        store: EventStore,
        projection_compiler: ProjectionCompiler,
        broker: BoundedEventBroker | None = None,
        ownership_policy: EventOwnershipPolicy | None = None,
    ) -> None:
        self.store = store
        self.projection_compiler = projection_compiler
        self.broker = broker or BoundedEventBroker()
        self.ownership_policy = ownership_policy or EventOwnershipPolicy()
        self.simulations = SimulationEngine()

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
        if head_sequence == expected_sequence:
            preview = event.seal(
                sequence=expected_sequence + 1,
                previous_hash=head_hash,
                recorded_at=utc_now(),
            )
            TwinAggregate.apply(self.state(event.twin_id), preview)
        persisted = self.store.append(event, expected_sequence=expected_sequence)
        if persisted.sequence > head_sequence:
            await self.broker.publish(persisted)
        return persisted

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
    ) -> EventEnvelope:
        self.ownership_policy.authorize_intent(
            "EvidenceIngested",
            ProducerRole.INGEST_SERVICE,
            actor,
        )
        self.ownership_policy.authorize_subject(actor, subject_id)
        state = self.state(twin_id)
        if state.subject_id is not None and state.subject_id != subject_id:
            raise InvariantViolation("evidence subject does not match twin subject")
        evidence_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{twin_id}:{subject_id}:{source}:{source_record_id}",
            )
        )
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="EvidenceIngested",
            plane=EventPlane.AUTHORITATIVE,
            producer=actor.identity_id,
            producer_role=ProducerRole.INGEST_SERVICE,
            idempotency_key=idempotency_key,
            occurred_at=valid_from,
            payload={
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
        state = self.state(twin_id)
        if state.sequence != expected_sequence:
            raise ConcurrencyConflict(
                f"claim expected {expected_sequence}, state is {state.sequence}"
            )
        if state.subject_id is None or state.subject_id != subject_id:
            raise InvariantViolation("claim subject does not match twin subject")
        for ref in provenance:
            evidence_id = str(ref.get("evidence_id", ""))
            evidence = state.evidence.get(evidence_id)
            if evidence is None:
                raise InvariantViolation("claim provenance references unknown evidence")
            if ref.get("independence_group") != evidence.get("independence_group"):
                raise InvariantViolation(
                    "claim provenance independence group does not match evidence"
                )
        TwinAggregate.validate_epistemic(dict(epistemic))
        claim_id = str(uuid.uuid4())
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="ClaimProposed",
            plane=EventPlane.AUTHORITATIVE,
            producer=actor.identity_id,
            producer_role=ProducerRole.ADJUDICATION_WORKER,
            idempotency_key=idempotency_key,
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
            payload={
                "claim_id": claim_id,
                "reviewer_identity_id": actor.identity_id,
                "rationale": rationale,
            },
        )
        state = self.state(twin_id)
        self.ownership_policy.authorize_subject(actor, state.subject_id)
        if state.sequence != expected_sequence:
            raise ConcurrencyConflict(
                f"review expected {expected_sequence}, state is {state.sequence}"
            )
        if claim_id not in state.claims:
            raise InvariantViolation("cannot review an unknown claim")
        return await self.append(event, actor=actor, expected_sequence=expected_sequence)

    def state(
        self,
        twin_id: str,
        *,
        up_to_sequence: int | None = None,
        as_of_recorded_time: datetime | None = None,
    ) -> TwinState:
        return TwinAggregate.rebuild(
            self.store,
            twin_id,
            up_to_sequence=up_to_sequence,
            as_of_recorded_time=as_of_recorded_time,
        )

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
        after_sequence: int = 0,
    ) -> Subscription:
        return await self.broker.subscribe(twin_id, after_sequence=after_sequence)
