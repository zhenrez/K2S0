from __future__ import annotations

import ast
import unittest
from datetime import UTC, datetime
from pathlib import Path

from argo_dt.compiler import ProjectionCompiler
from argo_dt.event_store import SQLiteEventStore
from argo_dt.policy import DefaultDenyPolicy
from argo_dt.public import (
    AdmissionLookupRequest,
    AdmitEvidenceRequest,
    AuthorityContext,
    InProcessK2Facade,
    LineageQuery,
    RepresentationQuery,
    TransportSimulatedK2Client,
)
from argo_dt.service import DigitalTwinService
from seed_contracts import AdmissionDisposition, AdmissionStatus, SubjectRef


def request() -> AdmitEvidenceRequest:
    return AdmitEvidenceRequest(
        request_id="request-ai-history-1",
        representation_id="k2-human-1",
        subject_ref=SubjectRef("human-1"),
        source="chat-history",
        source_record_id="message-1",
        payload={"text": "I prefer concise technical explanations."},
        rights={"basis": "owner_import"},
        sensitivity="internal",
        valid_from=datetime(2026, 9, 19, 10, 0, tzinfo=UTC),
        valid_until=None,
        independence_group="chat-history:conversation-1",
        based_on_sequence=0,
        authority=AuthorityContext(
            identity_id="r2d2-ingest",
            roles=("ingest_service",),
            subject_id="human-1",
        ),
        connector_version="fixture-v1",
        media_type="application/json",
        source_content_hash="sha256:source-fixture",
    )


def semantic_signature(result: object) -> tuple[object, ...]:
    from seed_contracts import AdmissionResult

    assert isinstance(result, AdmissionResult)
    effect = result.canonical_effects[0]
    assert result.resulting_representation_state_ref is not None
    return (
        result.status,
        result.disposition,
        effect.effect_kind,
        effect.affected_semantic_ref,
        effect.source_request_id,
        effect.canonical_position.subject_ref,
        effect.canonical_position.canonical_sequence,
        result.resulting_representation_state_ref.subject_ref,
        result.resulting_representation_state_ref.canonical_sequence,
    )


class PublicFacadeTests(unittest.IsolatedAsyncioTestCase):
    def make_facade(self) -> tuple[SQLiteEventStore, InProcessK2Facade]:
        store = SQLiteEventStore()
        service = DigitalTwinService(
            store=store,
            projection_compiler=ProjectionCompiler(DefaultDenyPolicy()),
        )
        return store, InProcessK2Facade(service)

    async def test_public_admission_reaches_existing_k2_and_maps_effects(self) -> None:
        store, facade = self.make_facade()
        self.addCleanup(store.close)

        result = await facade.admit_evidence(request())

        self.assertEqual(result.status, AdmissionStatus.ADMITTED)
        self.assertEqual(result.disposition, AdmissionDisposition.EFFECTS_APPLIED)
        self.assertFalse(result.replayed)
        self.assertEqual(1, len(result.canonical_effects))
        effect = result.canonical_effects[0]
        self.assertEqual("evidence_registered", effect.effect_kind)
        self.assertEqual("request-ai-history-1", effect.source_request_id)
        self.assertEqual(1, effect.canonical_position.canonical_sequence)
        self.assertEqual(
            effect.canonical_position,
            result.resulting_representation_state_ref,
        )
        self.assertEqual(1, store.head("k2-human-1")[0])

    async def test_idempotent_replay_returns_original_public_admission(self) -> None:
        store, facade = self.make_facade()
        self.addCleanup(store.close)
        admission_request = request()

        first = await facade.admit_evidence(admission_request)
        replay = await facade.admit_evidence(admission_request)

        self.assertFalse(first.replayed)
        self.assertTrue(replay.replayed)
        self.assertEqual(first.admission_id, replay.admission_id)
        self.assertEqual(first.canonical_effects, replay.canonical_effects)
        self.assertEqual(first.admission_id, replay.original_result_ref)
        self.assertEqual(1, store.head("k2-human-1")[0])

        recovered = await facade.get_admission_result(
            AdmissionLookupRequest(
                representation_id=admission_request.representation_id,
                request_id=admission_request.request_id,
                authority=admission_request.authority,
            )
        )
        self.assertIsNotNone(recovered)
        assert recovered is not None
        self.assertFalse(recovered.replayed)
        self.assertEqual(first.admission_id, recovered.admission_id)
        self.assertEqual(
            semantic_signature(first),
            semantic_signature(recovered),
        )

    async def test_public_reads_do_not_return_private_k2_objects(self) -> None:
        store, facade = self.make_facade()
        self.addCleanup(store.close)
        admission_request = request()
        result = await facade.admit_evidence(admission_request)

        state = await facade.read_representation_state(
            RepresentationQuery(
                representation_id=admission_request.representation_id,
                authority=admission_request.authority,
            )
        )
        deficits = await facade.read_representation_deficits(
            RepresentationQuery(
                representation_id=admission_request.representation_id,
                authority=admission_request.authority,
            )
        )
        trace = await facade.trace_lineage(
            LineageQuery(
                representation_id=admission_request.representation_id,
                node_kind="evidence",
                node_id=result.canonical_effects[0].affected_semantic_ref,
                authority=admission_request.authority,
            )
        )

        self.assertEqual("seed_contracts.semantic", type(result).__module__)
        self.assertEqual("argo_dt.public.contracts", type(state).__module__)
        self.assertTrue(
            all(type(item).__module__ == "argo_dt.public.contracts" for item in deficits)
        )
        self.assertEqual("argo_dt.public.contracts", type(trace).__module__)
        self.assertEqual(("missing_representation",), tuple(item.kind for item in deficits))

    async def test_same_caller_logic_works_in_process_and_transport_simulated(self) -> None:
        direct_store, direct = self.make_facade()
        transport_store, transport_backend = self.make_facade()
        self.addCleanup(direct_store.close)
        self.addCleanup(transport_store.close)
        transported = TransportSimulatedK2Client(transport_backend)

        async def caller(api: object) -> tuple[tuple[object, ...], tuple[object, ...]]:
            from argo_dt.public import K2PublicAPI

            assert isinstance(api, K2PublicAPI)
            admission_request = request()
            admitted = await api.admit_evidence(admission_request)
            recovered = await api.get_admission_result(
                AdmissionLookupRequest(
                    representation_id=admission_request.representation_id,
                    request_id=admission_request.request_id,
                    authority=admission_request.authority,
                )
            )
            assert recovered is not None
            return semantic_signature(admitted), semantic_signature(recovered)

        direct_signature = await caller(direct)
        transported_signature = await caller(transported)
        self.assertEqual(direct_signature, transported_signature)

    def test_r2d2_cannot_import_private_argo_dt_modules(self) -> None:
        for path in Path("src/r2d2").rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module:
                    if node.module == "argo_dt.public":
                        continue
                    self.assertFalse(
                        node.module == "argo_dt"
                        or node.module.startswith("argo_dt."),
                        f"{path} imports private K2 module {node.module}",
                    )
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        self.assertFalse(
                            alias.name == "argo_dt"
                            or (
                                alias.name.startswith("argo_dt.")
                                and not alias.name.startswith("argo_dt.public")
                            ),
                            f"{path} imports private K2 module {alias.name}",
                        )


if __name__ == "__main__":
    unittest.main()
