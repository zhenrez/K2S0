from __future__ import annotations

import unittest
import uuid
from datetime import UTC, datetime, timedelta

from argo_dt.compiler import ProjectionCompiler
from argo_dt.errors import (
    BackpressureExceeded,
    ConcurrencyConflict,
    InvariantViolation,
    PolicyDenied,
)
from argo_dt.event_store import SQLiteEventStore
from argo_dt.policy import DefaultDenyPolicy
from argo_dt.service import DigitalTwinService
from argo_dt.sync import BoundedEventBroker
from argo_dt.types import (
    ARGOCell,
    ActionEnvelope,
    AuthorityGrant,
    BitemporalInterval,
    ConsentGrant,
    EpistemicVector,
    EventEnvelope,
    EventPlane,
    IdentityKind,
    ProjectionRequest,
    Sensitivity,
    TemporalState,
)


def now() -> datetime:
    return datetime.now(UTC)


class TypeInvariantTests(unittest.TestCase):
    def test_four_identity_surfaces_are_distinct(self) -> None:
        self.assertEqual(4, len(set(IdentityKind)))

    def test_claim_cell_requires_provenance(self) -> None:
        instant = now()
        with self.assertRaises(InvariantViolation):
            ARGOCell(
                cell_id="claim-1",
                cell_type="claim",
                subject_id="human-1",
                identity_kind=IdentityKind.COGNITIVE_TWIN,
                temporal=BitemporalInterval(instant, None, instant),
                temporal_state=TemporalState.UNKNOWN,
                observed_state={"statement": "unknown is represented, not coerced"},
            )

    def test_epistemic_dimensions_are_independent_and_bounded(self) -> None:
        vector = EpistemicVector(0.9, 0.5, 0.8, 0.2, 1.0, 0.4, 0.7)
        self.assertNotEqual(vector.evidence_quality, vector.confidence)
        with self.assertRaises(InvariantViolation):
            EpistemicVector(1.1, 0.5, 0.8, 0.2, 1.0, 0.4, 0.7)

    def test_valid_and_recorded_time_are_separate(self) -> None:
        valid = now() - timedelta(days=365)
        recorded = now()
        interval = BitemporalInterval(valid, None, recorded)
        self.assertTrue(interval.valid_at(recorded))
        self.assertFalse(interval.known_at(recorded - timedelta(seconds=1)))


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = SQLiteEventStore()

    def tearDown(self) -> None:
        self.store.close()

    def event(self, key: str) -> EventEnvelope:
        return EventEnvelope.new(
            twin_id="twin-1",
            event_type="DegradationDeclared",
            plane=EventPlane.AUTHORITATIVE,
            payload={"reason": "test"},
            producer="test",
            idempotency_key=key,
        )

    def test_hash_chain_and_idempotency(self) -> None:
        first = self.store.append(self.event("same-key"), expected_sequence=0)
        duplicate = self.store.append(self.event("same-key"), expected_sequence=0)
        self.assertEqual(first.event_id, duplicate.event_id)
        self.assertEqual(1, self.store.head("twin-1")[0])
        self.assertTrue(self.store.verify_chain("twin-1"))

    def test_idempotency_key_cannot_mask_different_request(self) -> None:
        self.store.append(self.event("same-key"), expected_sequence=0)
        changed = EventEnvelope.new(
            twin_id="twin-1",
            event_type="DegradationDeclared",
            plane=EventPlane.AUTHORITATIVE,
            payload={"reason": "different"},
            producer="test",
            idempotency_key="same-key",
        )
        with self.assertRaises(InvariantViolation):
            self.store.append(changed, expected_sequence=0)

    def test_optimistic_concurrency(self) -> None:
        self.store.append(self.event("one"), expected_sequence=0)
        with self.assertRaises(ConcurrencyConflict):
            self.store.append(self.event("two"), expected_sequence=0)


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.store = SQLiteEventStore()
        self.service = DigitalTwinService(
            store=self.store,
            projection_compiler=ProjectionCompiler(DefaultDenyPolicy()),
        )
        self.instant = now()

    async def asyncTearDown(self) -> None:
        self.store.close()

    async def ingest(self, sequence: int = 0) -> EventEnvelope:
        return await self.service.ingest_evidence(
            twin_id="twin-1",
            subject_id="human-1",
            source="unit-test",
            source_record_id=f"record-{sequence}",
            payload={"text": "observed"},
            rights={"processing": ["modeling"]},
            sensitivity=Sensitivity.INTERNAL,
            valid_from=self.instant,
            valid_until=None,
            independence_group=f"session-{sequence}",
            expected_sequence=sequence,
            idempotency_key=f"evidence-{sequence}",
        )

    async def claim(
        self,
        evidence: EventEnvelope,
        sequence: int,
        *,
        sensitivity: Sensitivity = Sensitivity.INTERNAL,
        kind: str = "decision_heuristic",
    ) -> EventEnvelope:
        return await self.service.propose_claim(
            twin_id="twin-1",
            subject_id="human-1",
            statement=f"claim at {sequence}",
            kind=kind,
            provenance=[
                {
                    "evidence_id": evidence.payload["evidence_id"],
                    "relation": "supports",
                    "independence_group": evidence.payload["independence_group"],
                }
            ],
            sensitivity=sensitivity,
            valid_from=self.instant,
            valid_until=None,
            epistemic={"confidence": 0.7},
            expected_sequence=sequence,
            idempotency_key=f"claim-{sequence}",
        )

    async def accept(self, claim: EventEnvelope, sequence: int) -> EventEnvelope:
        return await self.service.review_claim(
            twin_id="twin-1",
            claim_id=str(claim.payload["claim_id"]),
            accepted=True,
            reviewer_identity_id="human-1",
            rationale="verified",
            expected_sequence=sequence,
            idempotency_key=f"accept-{sequence}",
        )

    async def test_claim_without_provenance_is_rejected(self) -> None:
        with self.assertRaises(InvariantViolation):
            await self.service.propose_claim(
                twin_id="twin-1",
                subject_id="human-1",
                statement="unsupported",
                kind="preference",
                provenance=[],
                sensitivity=Sensitivity.INTERNAL,
                valid_from=self.instant,
                valid_until=None,
                epistemic={"confidence": 0.1},
                expected_sequence=0,
                idempotency_key="unsupported",
            )

    async def test_evidence_deletion_invalidates_dependent_claim(self) -> None:
        evidence = await self.ingest()
        claim = await self.claim(evidence, 1)
        await self.accept(claim, 2)
        deletion = EventEnvelope.new(
            twin_id="twin-1",
            event_type="EvidenceDeleted",
            plane=EventPlane.AUTHORITATIVE,
            payload={"evidence_id": evidence.payload["evidence_id"]},
            producer="human-1",
            idempotency_key="delete-1",
        )
        await self.service.append(deletion, expected_sequence=3)
        state = self.service.state("twin-1")
        self.assertIn(str(claim.payload["claim_id"]), state.stale_claim_ids)
        self.assertEqual([], state.accepted_claims())

    async def test_recorded_time_query_does_not_backport_review(self) -> None:
        evidence = await self.ingest()
        claim = await self.claim(evidence, 1)
        await self.accept(claim, 2)
        before_review = self.service.state(
            "twin-1",
            as_of_recorded_time=claim.recorded_at,
        )
        current = self.service.state("twin-1")
        self.assertEqual([], before_review.accepted_claims())
        self.assertEqual(
            [claim.payload["claim_id"]],
            [item["claim_id"] for item in current.accepted_claims()],
        )

    async def test_simulation_is_isolated_from_authoritative_state(self) -> None:
        evidence = await self.ingest()
        state_before = self.service.state("twin-1")
        branch = self.service.fork_simulation(
            "twin-1",
            scenario="What if the schedule changes?",
        )
        branch.append(
            EventEnvelope.new(
                twin_id="twin-1",
                event_type="ScenarioStatePredicted",
                plane=EventPlane.SIMULATION,
                payload={"predicted_state": {"stress": "higher"}},
                producer="simulation-worker",
                idempotency_key="sim-1",
            )
        )
        state_after = self.service.state("twin-1")
        self.assertEqual(state_before.sequence, state_after.sequence)
        self.assertFalse(hasattr(state_after, "predicted_state"))
        proposal = self.service.simulations.proposal_payload(branch)
        self.assertTrue(proposal["requires_human_review"])
        self.assertEqual(evidence.sequence, branch.base_sequence)

    async def test_projection_is_default_deny(self) -> None:
        request = ProjectionRequest(
            request_id=str(uuid.uuid4()),
            twin_id="twin-1",
            subject_id="human-1",
            recipient_id="questn",
            purpose="decision-support",
            requested_fields=frozenset({"decision_model"}),
            maximum_sensitivity=Sensitivity.INTERNAL,
            as_of_valid_time=self.instant,
            as_of_recorded_time=now(),
        )
        with self.assertRaises(PolicyDenied):
            await self.service.issue_projection(
                request=request,
                consent=None,
                expected_sequence=0,
                idempotency_key="projection-denied",
            )

    async def test_projection_excludes_more_sensitive_claims(self) -> None:
        evidence_one = await self.ingest()
        internal = await self.claim(evidence_one, 1)
        await self.accept(internal, 2)
        evidence_two = await self.ingest(3)
        restricted = await self.claim(
            evidence_two,
            4,
            sensitivity=Sensitivity.RESTRICTED,
        )
        await self.accept(restricted, 5)

        request = ProjectionRequest(
            request_id=str(uuid.uuid4()),
            twin_id="twin-1",
            subject_id="human-1",
            recipient_id="questn",
            purpose="decision-support",
            requested_fields=frozenset({"decision_model", "readiness"}),
            maximum_sensitivity=Sensitivity.INTERNAL,
            as_of_valid_time=self.instant,
            as_of_recorded_time=now() + timedelta(seconds=1),
        )
        consent = ConsentGrant(
            consent_id=str(uuid.uuid4()),
            subject_id="human-1",
            recipient_id="questn",
            purposes=frozenset({"decision-support"}),
            allowed_fields=frozenset({"decision_model", "readiness"}),
            max_sensitivity=Sensitivity.INTERNAL,
            valid_from=now() - timedelta(minutes=1),
            valid_until=now() + timedelta(hours=1),
            policy_version="constitution/v1",
        )
        projection, receipt, event = await self.service.issue_projection(
            request=request,
            consent=consent,
            expected_sequence=6,
            idempotency_key="projection-allowed",
        )
        claims = projection["artifacts"]["decision_model"]["payload"]["claims"]
        self.assertEqual([internal.payload["claim_id"]], [item["claim_id"] for item in claims])
        self.assertNotIn(str(restricted.payload["claim_id"]), receipt.source_claim_ids)
        self.assertEqual(EventPlane.PROJECTION, event.plane)

    async def test_bounded_stream_disconnects_slow_consumer(self) -> None:
        broker = BoundedEventBroker(queue_capacity=1)
        subscription = await broker.subscribe("twin-1")
        first = EventEnvelope.new(
            twin_id="twin-1",
            event_type="DegradationDeclared",
            plane=EventPlane.AUTHORITATIVE,
            payload={"reason": "one"},
            producer="test",
            idempotency_key="stream-1",
        ).seal(sequence=1, previous_hash="", recorded_at=now())
        second = EventEnvelope.new(
            twin_id="twin-1",
            event_type="DegradationDeclared",
            plane=EventPlane.AUTHORITATIVE,
            payload={"reason": "two"},
            producer="test",
            idempotency_key="stream-2",
        ).seal(sequence=2, previous_hash=first.event_hash, recorded_at=now())
        await broker.publish(first)
        await broker.publish(second)
        with self.assertRaises(BackpressureExceeded):
            await subscription.__anext__()


class AuthorityTests(unittest.TestCase):
    def test_information_consent_does_not_authorize_action(self) -> None:
        instant = now()
        action = ActionEnvelope(
            action_id="action-1",
            principal_id="human-1",
            actor_identity_id="agent-1",
            purpose="scheduling",
            target="calendar",
            requested_capabilities=frozenset({"calendar.write"}),
            impact="moderate",
            reversible=True,
            evidence_ids=("claim-1",),
            constraint_version="constitution/v1",
            idempotency_key="action-1",
            issued_at=instant,
            expires_at=instant + timedelta(minutes=5),
        )
        allowed, reasons = DefaultDenyPolicy().evaluate_action(action, None)
        self.assertFalse(allowed)
        self.assertIn("no_authority_grant", reasons)

        grant = AuthorityGrant(
            grant_id="grant-1",
            principal_id="human-1",
            actor_identity_id="agent-1",
            purposes=frozenset({"scheduling"}),
            capabilities=frozenset({"calendar.write"}),
            maximum_impact="moderate",
            require_reversible=True,
            valid_from=instant - timedelta(minutes=1),
            valid_until=instant + timedelta(hours=1),
        )
        allowed, _ = DefaultDenyPolicy().evaluate_action(action, grant)
        self.assertTrue(allowed)


if __name__ == "__main__":
    unittest.main()
