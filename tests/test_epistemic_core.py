from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from argo_dt.aggregate import TwinState
from argo_dt.bronze import EncryptedFileBronzeVault, StaticKeyProvider
from argo_dt.compiler import ProjectionCompiler
from argo_dt.epistemic import ElicitationResponse, EpistemicNodeKind, GapKind
from argo_dt.errors import InvariantViolation
from argo_dt.event_store import SQLiteEventStore
from argo_dt.policy import DefaultDenyPolicy
from argo_dt.service import DigitalTwinService
from argo_dt.types import (
    ActorContext,
    ARGOCell,
    BitemporalInterval,
    EventEnvelope,
    IdentityKind,
    ProducerRole,
    RelationTopology,
    Sensitivity,
    TemporalState,
    TypedRelation,
    canonical_json,
)


def epistemic_vector() -> dict[str, float]:
    return {
        "evidence_quality": 0.8,
        "confidence": 0.7,
        "salience": 0.6,
        "stability": 0.5,
        "freshness": 0.9,
        "scope_confidence": 0.7,
        "contradiction_load": 0.1,
    }


class TopologyContractTests(unittest.TestCase):
    def test_argocell_relations_encode_all_five_topology_levels(self) -> None:
        instant = datetime(2026, 1, 1, tzinfo=UTC)
        relations = tuple(
            TypedRelation("contains", f"cell-{level.value}", topology=level)
            for level in RelationTopology
        )
        cell = ARGOCell(
            cell_id="root-cell",
            cell_type="subject",
            subject_id="human-1",
            identity_kind=IdentityKind.COGNITIVE_TWIN,
            temporal=BitemporalInterval(instant, None, instant),
            temporal_state=TemporalState.CURRENT,
            observed_state={},
            relations=relations,
        )
        self.assertEqual(
            {"point", "line", "face", "volume", "root"},
            {item["topology"] for item in cell.to_dict()["relations"]},
        )


class EpistemicCoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = SQLiteEventStore()
        self.addCleanup(self.store.close)
        self.vault = EncryptedFileBronzeVault(
            self.directory.name,
            key_provider=StaticKeyProvider(key_id="test-key", key=b"k" * 32),
        )
        self.service = DigitalTwinService(
            store=self.store,
            projection_compiler=ProjectionCompiler(DefaultDenyPolicy()),
            bronze_vault=self.vault,
        )
        self.instant = datetime(2026, 1, 1, tzinfo=UTC)
        self.ingest_actor = ActorContext(
            "ingest-test",
            frozenset({ProducerRole.INGEST_SERVICE}),
            subject_id="human-1",
        )
        self.identity_actor = ActorContext(
            "identity-test",
            frozenset({ProducerRole.IDENTITY_WORKER}),
            subject_id="human-1",
        )
        self.adjudicator = ActorContext(
            "adjudication-test",
            frozenset({ProducerRole.ADJUDICATION_WORKER}),
            subject_id="human-1",
        )
        self.reviewer = ActorContext(
            "human-1",
            frozenset({ProducerRole.HUMAN_REVIEW}),
            subject_id="human-1",
        )

    async def ingest(
        self,
        sequence: int,
        source_record_id: str,
        group: str,
        *,
        valid_until: datetime | None = None,
    ) -> EventEnvelope:
        return await self.service.ingest_evidence(
            twin_id="twin-1",
            subject_id="human-1",
            source="epistemic-test",
            source_record_id=source_record_id,
            payload={"observation": source_record_id},
            rights={"processing": ["modeling"]},
            sensitivity=Sensitivity.INTERNAL,
            valid_from=self.instant,
            valid_until=valid_until,
            independence_group=group,
            expected_sequence=sequence,
            idempotency_key=f"ingest-{source_record_id}",
            actor=self.ingest_actor,
        )

    async def claim(
        self, sequence: int, evidence: EventEnvelope, statement: str
    ) -> EventEnvelope:
        payload = evidence.payload
        return await self.service.propose_claim(
            twin_id="twin-1",
            subject_id="human-1",
            statement=statement,
            kind="preference",
            provenance=[
                {
                    "evidence_id": payload["evidence_id"],
                    "relation": "supports",
                    "independence_group": payload["independence_group"],
                }
            ],
            sensitivity=Sensitivity.INTERNAL,
            valid_from=self.instant,
            valid_until=None,
            epistemic=epistemic_vector(),
            expected_sequence=sequence,
            idempotency_key=f"claim-{sequence}",
            actor=self.adjudicator,
        )

    async def test_lineage_identity_contradiction_and_correction_are_reversible(self) -> None:
        first = await self.ingest(0, "source-1", "independent-1")
        second = await self.ingest(1, "source-2", "independent-2")
        first_payload = first.payload
        link = await self.service.link_entity(
            twin_id="twin-1",
            subject_id="human-1",
            namespace="contacts",
            entity_id="person-7",
            provenance=[
                {
                    "evidence_id": first_payload["evidence_id"],
                    "relation": "identifies",
                    "independence_group": first_payload["independence_group"],
                }
            ],
            confidence=0.85,
            valid_from=self.instant,
            valid_until=None,
            expected_sequence=2,
            idempotency_key="link-person-7",
            actor=self.identity_actor,
        )
        link_retry = await self.service.link_entity(
            twin_id="twin-1",
            subject_id="human-1",
            namespace="contacts",
            entity_id="person-7",
            provenance=[
                {
                    "evidence_id": first_payload["evidence_id"],
                    "relation": "identifies",
                    "independence_group": first_payload["independence_group"],
                }
            ],
            confidence=0.85,
            valid_from=self.instant,
            valid_until=None,
            expected_sequence=2,
            idempotency_key="link-person-7",
            actor=self.identity_actor,
        )
        self.assertEqual(link.event_id, link_retry.event_id)
        first_claim = await self.claim(3, first, "The preferred answer is A")
        second_claim = await self.claim(4, second, "The preferred answer is B")
        first_claim_id = str(first_claim.payload["claim_id"])
        second_claim_id = str(second_claim.payload["claim_id"])
        reviewed = await self.service.review_claim(
            twin_id="twin-1",
            claim_id=first_claim_id,
            accepted=True,
            rationale="initial review",
            reviewed_at=self.instant,
            expected_sequence=5,
            idempotency_key="accept-first",
            actor=self.reviewer,
        )
        reviewed_retry = await self.service.review_claim(
            twin_id="twin-1",
            claim_id=first_claim_id,
            accepted=True,
            rationale="initial review",
            reviewed_at=self.instant,
            expected_sequence=5,
            idempotency_key="accept-first",
            actor=self.reviewer,
        )
        self.assertEqual(reviewed.event_id, reviewed_retry.event_id)
        contradiction = await self.service.detect_contradiction(
            twin_id="twin-1",
            claim_ids=(first_claim_id, second_claim_id),
            basis="mutually exclusive preference values",
            detected_at=self.instant + timedelta(hours=1),
            expected_sequence=6,
            idempotency_key="contradiction-1",
            actor=self.adjudicator,
        )
        contradiction_id = str(contradiction.payload["contradiction_id"])
        contradiction_retry = await self.service.detect_contradiction(
            twin_id="twin-1",
            claim_ids=(first_claim_id, second_claim_id),
            basis="mutually exclusive preference values",
            detected_at=self.instant + timedelta(hours=1),
            expected_sequence=6,
            idempotency_key="contradiction-1",
            actor=self.adjudicator,
        )
        self.assertEqual(contradiction.event_id, contradiction_retry.event_id)
        detected_state = self.service.state("twin-1")
        self.assertIn(first_claim_id, detected_state.accepted_claim_ids)
        historical_trace = self.service.trace_lineage(
            "twin-1",
            node_kind=EpistemicNodeKind.CONTRADICTION,
            node_id=contradiction_id,
            as_of_recorded_time=contradiction.recorded_at - timedelta(microseconds=1),
        )
        self.assertEqual((), historical_trace.ancestors)

        trace = self.service.trace_lineage(
            "twin-1",
            node_kind=EpistemicNodeKind.CONTRADICTION,
            node_id=contradiction_id,
        )
        self.assertEqual(
            {first_claim_id, second_claim_id},
            {node.node_id for node in trace.ancestors if node.kind is EpistemicNodeKind.CLAIM},
        )
        self.assertEqual(2, len(trace.evidence_by_independence_group))

        adjudication = await self.service.adjudicate_contradiction(
            twin_id="twin-1",
            contradiction_id=contradiction_id,
            resolution="uphold",
            preferred_claim_ids=(first_claim_id,),
            rationale="firsthand source is authoritative for its stated interval",
            adjudicated_at=self.instant + timedelta(hours=2),
            expected_sequence=7,
            idempotency_key="adjudicate-1",
            actor=self.reviewer,
        )
        adjudication_retry = await self.service.adjudicate_contradiction(
            twin_id="twin-1",
            contradiction_id=contradiction_id,
            resolution="uphold",
            preferred_claim_ids=(first_claim_id,),
            rationale="firsthand source is authoritative for its stated interval",
            adjudicated_at=self.instant + timedelta(hours=2),
            expected_sequence=7,
            idempotency_key="adjudicate-1",
            actor=self.reviewer,
        )
        self.assertEqual(adjudication.event_id, adjudication_retry.event_id)
        third = await self.ingest(8, "source-3", "independent-3")
        correction = await self.service.record_correction(
            twin_id="twin-1",
            target_kind="evidence",
            target_id=str(first_payload["evidence_id"]),
            replacement_id=str(third.payload["evidence_id"]),
            rationale="source owner supplied a corrected record",
            corrected_at=self.instant + timedelta(hours=3),
            expected_sequence=9,
            idempotency_key="correct-source-1",
            actor=self.reviewer,
        )
        correction_retry = await self.service.record_correction(
            twin_id="twin-1",
            target_kind="evidence",
            target_id=str(first_payload["evidence_id"]),
            replacement_id=str(third.payload["evidence_id"]),
            rationale="source owner supplied a corrected record",
            corrected_at=self.instant + timedelta(hours=3),
            expected_sequence=9,
            idempotency_key="correct-source-1",
            actor=self.reviewer,
        )
        self.assertEqual(correction.event_id, correction_retry.event_id)
        await self.service.unlink_entity(
            twin_id="twin-1",
            entity_link_id=str(link.payload["entity_link_id"]),
            reason="link no longer supported after correction",
            unlinked_at=self.instant + timedelta(hours=4),
            expected_sequence=10,
            idempotency_key="unlink-person-7",
            actor=self.identity_actor,
        )

        state = self.service.state("twin-1")
        self.assertIn(contradiction_id, state.resolved_contradiction_ids)
        self.assertIn(first_claim_id, state.accepted_claim_ids)
        self.assertIn(str(first_payload["evidence_id"]), state.evidence)
        self.assertIn(str(first_payload["evidence_id"]), state.superseded_evidence_ids)
        self.assertIn(first_claim_id, state.stale_claim_ids)
        self.assertIn(str(correction.payload["correction_id"]), state.corrections)
        self.assertIn(str(link.payload["entity_link_id"]), state.unlinked_entity_ids)
        self.assertIn(str(link.payload["entity_link_id"]), state.stale_entity_link_ids)

    async def test_correction_requires_existing_replacement_before_persistence(self) -> None:
        evidence = await self.ingest(0, "source-1", "independent-1")
        with self.assertRaises(InvariantViolation):
            await self.service.record_correction(
                twin_id="twin-1",
                target_kind="evidence",
                target_id=str(evidence.payload["evidence_id"]),
                replacement_id="missing-replacement",
                rationale="must not create a dangling correction",
                corrected_at=self.instant + timedelta(hours=1),
                expected_sequence=1,
                idempotency_key="invalid-correction",
                actor=self.reviewer,
            )
        self.assertEqual(1, self.store.head("twin-1")[0])

    async def test_valid_time_filters_without_changing_recorded_stream_position(self) -> None:
        evidence = await self.ingest(
            0,
            "bounded-source",
            "bounded-group",
            valid_until=self.instant + timedelta(days=30),
        )
        evidence_id = str(evidence.payload["evidence_id"])
        during = self.service.state(
            "twin-1", as_of_valid_time=self.instant + timedelta(days=10)
        )
        after = self.service.state(
            "twin-1", as_of_valid_time=self.instant + timedelta(days=40)
        )
        self.assertIn(evidence_id, during.evidence)
        self.assertNotIn(evidence_id, after.evidence)
        self.assertEqual(during.sequence, after.sequence)
        with self.assertRaises(InvariantViolation):
            TwinState("twin-1").select_valid_at(datetime(2026, 1, 1))

    async def test_gap_elicitation_writes_answer_only_to_encrypted_bronze(self) -> None:
        evidence = await self.ingest(0, "single-source", "single-group")
        claim = await self.claim(1, evidence, "The subject prefers reversible trials")
        claim_id = str(claim.payload["claim_id"])
        plan = self.service.plan_elicitation(
            "twin-1",
            objective="calibrate the decision preference",
            claim_ids=(claim_id,),
        )
        self.assertIn(GapKind.INDEPENDENCE_DEFICIT, {gap.kind for gap in plan.gaps})
        question = plan.questions[0]
        answer = "A dated journal entry confirms the reversible-trial preference."
        with self.assertRaises(InvariantViolation):
            ElicitationResponse(
                response_id="oversized",
                question_id=question.question_id,
                answer="x" * (1024 * 1024 + 1),
                sensitivity=Sensitivity.CONFIDENTIAL,
                answered_at=self.instant,
            )
        response = ElicitationResponse(
            response_id="response-1",
            question_id=question.question_id,
            answer=answer,
            sensitivity=Sensitivity.CONFIDENTIAL,
            answered_at=self.instant + timedelta(days=1),
        )
        event = await self.service.record_elicitation_response(
            twin_id="twin-1",
            subject_id="human-1",
            plan=plan,
            response=response,
            rights={"processing": ["modeling"]},
            expected_sequence=2,
            idempotency_key="elicitation-response-1",
            actor=self.ingest_actor,
        )
        self.assertNotIn(answer, canonical_json(event.payload))
        bronze_uri = str(event.payload["bronze_uri"])
        record, content = self.vault.get(subject_id="human-1", object_uri=bronze_uri)
        self.assertEqual(event.payload["content_hash"], record.content_hash)
        self.assertEqual(answer, json.loads(content)["answer"])

        duplicate = await self.service.record_elicitation_response(
            twin_id="twin-1",
            subject_id="human-1",
            plan=plan,
            response=response,
            rights={"processing": ["modeling"]},
            expected_sequence=2,
            idempotency_key="elicitation-response-1",
            actor=self.ingest_actor,
        )
        self.assertEqual(event.event_id, duplicate.event_id)

    async def test_elicitation_rejects_forged_plan_before_bronze_write(self) -> None:
        evidence = await self.ingest(0, "single-source", "single-group")
        claim = await self.claim(1, evidence, "The subject prefers reversible trials")
        claim_id = str(claim.payload["claim_id"])
        plan = self.service.plan_elicitation(
            "twin-1",
            objective="calibrate the decision preference",
            claim_ids=(claim_id,),
        )
        forged_question = replace(plan.questions[0], prompt="Disclose unrelated secrets")
        forged_plan = replace(plan, questions=(forged_question, *plan.questions[1:]))
        response = ElicitationResponse(
            response_id="forged-response",
            question_id=forged_question.question_id,
            answer="must never be stored",
            sensitivity=Sensitivity.CONFIDENTIAL,
            answered_at=self.instant + timedelta(days=1),
        )

        before = {str(path) for path in Path(self.directory.name).rglob("*")}
        with self.assertRaisesRegex(InvariantViolation, "authenticity"):
            await self.service.record_elicitation_response(
                twin_id="twin-1",
                subject_id="human-1",
                plan=forged_plan,
                response=response,
                rights={"processing": ["modeling"]},
                expected_sequence=2,
                idempotency_key="forged-elicitation-response",
                actor=self.ingest_actor,
            )
        self.assertEqual(2, self.store.head("twin-1")[0])
        self.assertEqual(
            before,
            {str(path) for path in Path(self.directory.name).rglob("*")},
        )

        future_plan = replace(plan, source_sequence=plan.source_sequence + 1)
        with self.assertRaisesRegex(InvariantViolation, "source state"):
            await self.service.record_elicitation_response(
                twin_id="twin-1",
                subject_id="human-1",
                plan=future_plan,
                response=response,
                rights={"processing": ["modeling"]},
                expected_sequence=2,
                idempotency_key="future-elicitation-response",
                actor=self.ingest_actor,
            )
