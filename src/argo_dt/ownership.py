"""Default-deny event ownership at the authenticated service boundary."""

from __future__ import annotations

from collections.abc import Mapping

from .errors import AuthorizationDenied, InvariantViolation
from .types import ActorContext, EventEnvelope, ProducerRole

EVENT_OWNERS: Mapping[str, frozenset[ProducerRole]] = {
    "EvidenceIngested": frozenset({ProducerRole.INGEST_SERVICE}),
    "EvidenceDeleted": frozenset({ProducerRole.INGEST_SERVICE}),
    "AcquisitionCompleted": frozenset({ProducerRole.INGEST_SERVICE}),
    "EntityLinked": frozenset({ProducerRole.IDENTITY_WORKER}),
    "EntityUnlinked": frozenset({ProducerRole.IDENTITY_WORKER}),
    "ClaimProposed": frozenset({ProducerRole.ADJUDICATION_WORKER}),
    "ContradictionDetected": frozenset({ProducerRole.ADJUDICATION_WORKER}),
    "ContradictionAdjudicated": frozenset({ProducerRole.HUMAN_REVIEW}),
    "ReferralCreated": frozenset({ProducerRole.ADJUDICATION_WORKER}),
    "ClaimAccepted": frozenset({ProducerRole.HUMAN_REVIEW}),
    "ClaimContested": frozenset({ProducerRole.HUMAN_REVIEW}),
    "ClaimRetired": frozenset({ProducerRole.HUMAN_REVIEW}),
    "ClaimSuperseded": frozenset({ProducerRole.HUMAN_REVIEW}),
    "CorrectionRecorded": frozenset({ProducerRole.HUMAN_REVIEW}),
    "KernelCompiled": frozenset({ProducerRole.COMPILER}),
    "EvaluationCompleted": frozenset({ProducerRole.COMPILER}),
    "ReadinessChanged": frozenset({ProducerRole.COMPILER}),
    "ProjectionIssued": frozenset({ProducerRole.PROJECTION_SERVICE}),
    "ProjectionRevoked": frozenset({ProducerRole.PROJECTION_SERVICE}),
    "ScenarioStatePredicted": frozenset({ProducerRole.SIMULATION_SERVICE}),
    "AgentDecisionObserved": frozenset({ProducerRole.DOWNSTREAM_AGENT}),
    "OutcomeSubmitted": frozenset({ProducerRole.DOWNSTREAM_AGENT}),
    "DegradationDeclared": frozenset({ProducerRole.OPERATIONS_SERVICE}),
    "DegradationCleared": frozenset({ProducerRole.OPERATIONS_SERVICE}),
}


def sole_event_owner(event_type: str) -> ProducerRole:
    """Resolve the unique owner used to upcast historical v1 envelopes."""

    roles = EVENT_OWNERS.get(event_type)
    if roles is None or len(roles) != 1:
        raise InvariantViolation(
            f"legacy event type {event_type!r} has no unambiguous producer role"
        )
    return next(iter(roles))


class EventOwnershipPolicy:
    """Authorize event creation from verified identity claims, never payload claims."""

    def authorize_intent(
        self,
        event_type: str,
        producer_role: ProducerRole,
        actor: ActorContext,
    ) -> None:
        if producer_role not in actor.roles:
            raise AuthorizationDenied("producer role is not present in actor claims")
        allowed_roles = EVENT_OWNERS.get(event_type)
        if allowed_roles is None:
            raise AuthorizationDenied("unregistered event type is denied by default")
        if producer_role not in allowed_roles:
            raise AuthorizationDenied("producer role does not own this event type")

    def authorize_subject(self, actor: ActorContext, subject_id: str | None) -> None:
        if (
            actor.subject_id is not None
            and subject_id is not None
            and actor.subject_id != subject_id
        ):
            raise AuthorizationDenied("actor subject scope does not match target subject")

    def authorize(self, event: EventEnvelope, actor: ActorContext) -> None:
        if event.producer != actor.identity_id:
            raise AuthorizationDenied("event producer does not match authenticated actor")
        self.authorize_intent(event.event_type, event.producer_role, actor)
        payload_subject = event.payload.get("subject_id")
        self.authorize_subject(
            actor,
            str(payload_subject) if payload_subject is not None else None,
        )
