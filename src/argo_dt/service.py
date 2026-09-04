"""Application façade used by REST, gRPC, MCP, and local adapters."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Mapping, Sequence

from .aggregate import TwinAggregate, TwinState
from .compiler import ProjectionCompiler
from .errors import ConcurrencyConflict, InvariantViolation
from .ports import EventStore
from .simulation import SimulationBranch, SimulationEngine
from .sync import BoundedEventBroker, Subscription
from .types import (
    ConsentGrant,
    EventEnvelope,
    EventPlane,
    ProjectionReceipt,
    ProjectionRequest,
    Sensitivity,
    content_hash,
)


class DigitalTwinService:
    """Coordinates invariant checks, persistence, and real-time publication."""

    def __init__(
        self,
        *,
        store: EventStore,
        projection_compiler: ProjectionCompiler,
        broker: BoundedEventBroker | None = None,
        producer: str = "argo-dt-reference",
    ) -> None:
        self.store = store
        self.projection_compiler = projection_compiler
        self.broker = broker or BoundedEventBroker()
        self.producer = producer
        self.simulations = SimulationEngine()

    async def append(
        self,
        event: EventEnvelope,
        *,
        expected_sequence: int,
    ) -> EventEnvelope:
        TwinAggregate.validate_event(event)
        persisted = self.store.append(event, expected_sequence=expected_sequence)
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
    ) -> EventEnvelope:
        evidence_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source}:{source_record_id}"))
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="EvidenceIngested",
            plane=EventPlane.AUTHORITATIVE,
            producer=self.producer,
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
        return await self.append(event, expected_sequence=expected_sequence)

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
        model_trace: Mapping[str, Any] | None = None,
    ) -> EventEnvelope:
        if not provenance:
            raise InvariantViolation("a claim cannot be proposed without provenance")
        claim_id = str(uuid.uuid4())
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="ClaimProposed",
            plane=EventPlane.AUTHORITATIVE,
            producer=self.producer,
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
        return await self.append(event, expected_sequence=expected_sequence)

    async def review_claim(
        self,
        *,
        twin_id: str,
        claim_id: str,
        accepted: bool,
        reviewer_identity_id: str,
        rationale: str,
        expected_sequence: int,
        idempotency_key: str,
    ) -> EventEnvelope:
        event = EventEnvelope.new(
            twin_id=twin_id,
            event_type="ClaimAccepted" if accepted else "ClaimContested",
            plane=EventPlane.AUTHORITATIVE,
            producer=reviewer_identity_id,
            idempotency_key=idempotency_key,
            payload={
                "claim_id": claim_id,
                "reviewer_identity_id": reviewer_identity_id,
                "rationale": rationale,
            },
        )
        state = self.state(twin_id)
        if claim_id not in state.claims:
            raise InvariantViolation("cannot review an unknown claim")
        return await self.append(event, expected_sequence=expected_sequence)

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
    ) -> tuple[dict[str, Any], ProjectionReceipt, EventEnvelope]:
        head_sequence, _ = self.store.head(request.twin_id)
        if head_sequence != expected_sequence:
            raise ConcurrencyConflict(
                f"projection expected {expected_sequence}, stream head is {head_sequence}"
            )
        state = self.state(
            request.twin_id,
            as_of_recorded_time=request.as_of_recorded_time,
        )
        projection, receipt = self.projection_compiler.compile(
            state=state,
            request=request,
            consent=consent,
        )
        audit_event = EventEnvelope.new(
            twin_id=request.twin_id,
            event_type="ProjectionIssued",
            plane=EventPlane.PROJECTION,
            producer=self.producer,
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
        persisted = await self.append(audit_event, expected_sequence=expected_sequence)
        return projection, receipt, persisted

    def fork_simulation(self, twin_id: str, *, scenario: str) -> SimulationBranch:
        return self.simulations.fork(self.state(twin_id), scenario=scenario)

    async def subscribe(
        self,
        twin_id: str,
        *,
        after_sequence: int = 0,
    ) -> Subscription:
        return await self.broker.subscribe(twin_id, after_sequence=after_sequence)
