from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from argo_dt.compiler import ProjectionCompiler
from argo_dt.conformance import load_golden_replay, replay_events
from argo_dt.errors import InvariantViolation
from argo_dt.event_store import SQLiteEventStore
from argo_dt.invariants import INVARIANT_BY_ID, INVARIANTS, InvariantId
from argo_dt.policy import DefaultDenyPolicy
from argo_dt.service import DigitalTwinService
from argo_dt.types import (
    ActorContext,
    ConsentGrant,
    EventEnvelope,
    EventPlane,
    ProducerRole,
    ProjectionRequest,
    Sensitivity,
    canonical_json,
    content_hash,
    parse_time,
)

ROOT = Path(__file__).resolve().parents[1]


def discovered_test_ids() -> set[str]:
    suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"))
    found: set[str] = set()

    def visit(item: unittest.TestSuite | unittest.TestCase) -> None:
        if isinstance(item, unittest.TestSuite):
            for child in item:
                visit(child)
        else:
            found.add(item.id().removeprefix("tests."))

    visit(suite)
    return found


class ContractRegistryTests(unittest.TestCase):
    def test_every_invariant_has_unique_id_owner_and_executable_evidence(self) -> None:
        self.assertEqual(len(INVARIANTS), len(INVARIANT_BY_ID))
        self.assertEqual(set(InvariantId), set(INVARIANT_BY_ID))
        available = discovered_test_ids()
        for spec in INVARIANTS:
            with self.subTest(invariant_id=spec.invariant_id):
                self.assertTrue(spec.owner)
                self.assertTrue(spec.evidence)
                self.assertTrue(set(spec.evidence).issubset(available))

    def test_set_serialization_is_deterministic(self) -> None:
        left = {"fields": frozenset({"voice", "kernel", "readiness"})}
        right = {"fields": frozenset({"readiness", "voice", "kernel"})}
        self.assertEqual(canonical_json(left), canonical_json(right))
        self.assertEqual(content_hash(left), content_hash(right))


class GoldenReplayTests(unittest.TestCase):
    def test_golden_replay_is_stable(self) -> None:
        events, expected = load_golden_replay(
            ROOT / "fixtures" / "v1" / "authoritative-replay.json"
        )
        state = replay_events(events)
        self.assertEqual(expected["twin_id"], state.twin_id)
        self.assertEqual(expected["subject_id"], state.subject_id)
        self.assertEqual(expected["sequence"], state.sequence)
        self.assertEqual(expected["last_event_hash"], state.last_event_hash)
        self.assertEqual(set(expected["accepted_claim_ids"]), state.accepted_claim_ids)
        self.assertEqual(set(expected["contested_claim_ids"]), state.contested_claim_ids)
        self.assertEqual(set(expected["stale_claim_ids"]), state.stale_claim_ids)
        self.assertEqual(
            set(expected["revoked_projection_ids"]),
            state.revoked_projection_ids,
        )

    def test_legacy_v1_sqlite_ledger_upcasts_without_changing_hash(self) -> None:
        stamp = datetime(2026, 1, 1, tzinfo=UTC)
        legacy = EventEnvelope(
            event_id="99999999-9999-4999-8999-999999999999",
            twin_id="legacy-twin",
            event_type="DegradationDeclared",
            plane=EventPlane.AUTHORITATIVE,
            payload={"reason": "legacy-fixture"},
            occurred_at=stamp,
            recorded_at=stamp,
            producer="legacy-operations",
            producer_role=ProducerRole.OPERATIONS_SERVICE,
            idempotency_key="legacy-event-1",
            schema_version="argo.dt.event/v1",
        ).seal(sequence=1, previous_hash="", recorded_at=stamp)
        with tempfile.NamedTemporaryFile(suffix=".db") as database:
            connection = sqlite3.connect(database.name)
            connection.execute(
                """
                CREATE TABLE dt_events (
                    twin_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    plane TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    producer TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    causation_id TEXT,
                    correlation_id TEXT,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    PRIMARY KEY (twin_id, sequence),
                    UNIQUE (twin_id, idempotency_key)
                )
                """
            )
            connection.execute(
                """
                INSERT INTO dt_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    legacy.twin_id,
                    legacy.sequence,
                    legacy.event_id,
                    legacy.event_type,
                    legacy.plane.value,
                    legacy.schema_version,
                    canonical_json(legacy.payload),
                    legacy.occurred_at.isoformat(),
                    legacy.recorded_at.isoformat(),
                    legacy.producer,
                    legacy.idempotency_key,
                    legacy.causation_id,
                    legacy.correlation_id,
                    legacy.previous_hash,
                    legacy.event_hash,
                ),
            )
            connection.commit()
            connection.close()

            store = SQLiteEventStore(database.name)
            try:
                loaded = store.load("legacy-twin")
                self.assertEqual(ProducerRole.OPERATIONS_SERVICE, loaded[0].producer_role)
                self.assertEqual(legacy.event_hash, loaded[0].event_hash)
                self.assertTrue(store.verify_chain("legacy-twin"))
                with self.assertRaisesRegex(
                    InvariantViolation,
                    "new event-store writes require EventEnvelope v2",
                ):
                    store.append(legacy, expected_sequence=1)
            finally:
                store.close()


class NegativeFixtureTests(unittest.IsolatedAsyncioTestCase):
    def fixture(self) -> dict[str, object]:
        return json.loads(
            (ROOT / "fixtures" / "v1" / "negative-boundaries.json").read_text()
        )

    async def test_revoked_consent_is_denied(self) -> None:
        raw = self.fixture()["revoked_consent"]
        assert isinstance(raw, dict)
        consent = ConsentGrant(
            consent_id=str(raw["consent_id"]),
            subject_id=str(raw["subject_id"]),
            recipient_id=str(raw["recipient_id"]),
            purposes=frozenset(raw["purposes"]),
            allowed_fields=frozenset(raw["allowed_fields"]),
            max_sensitivity=Sensitivity(str(raw["max_sensitivity"])),
            valid_from=parse_time(str(raw["valid_from"])),
            valid_until=parse_time(str(raw["valid_until"])),
            policy_version=str(raw["policy_version"]),
            revoked_at=parse_time(str(raw["revoked_at"])),
        )
        request = ProjectionRequest(
            request_id="88888888-8888-4888-8888-888888888888",
            twin_id="11111111-1111-4111-8111-111111111111",
            subject_id=consent.subject_id,
            recipient_id=consent.recipient_id,
            purpose="decision-support",
            requested_fields=frozenset({"decision_model"}),
            maximum_sensitivity=Sensitivity.INTERNAL,
            as_of_valid_time=datetime.now(UTC),
            as_of_recorded_time=datetime.now(UTC),
        )
        allowed, reasons, fields = DefaultDenyPolicy().evaluate_projection(
            request,
            consent,
            available_fields=frozenset({"decision_model"}),
        )
        self.assertFalse(allowed)
        self.assertIn("consent_inactive", reasons)
        self.assertFalse(fields)

    async def test_simulation_fixture_cannot_enter_authoritative_store(self) -> None:
        raw = self.fixture()["simulation_store_attempt"]
        assert isinstance(raw, dict)
        store = SQLiteEventStore()
        self.addCleanup(store.close)
        service = DigitalTwinService(
            store=store,
            projection_compiler=ProjectionCompiler(DefaultDenyPolicy()),
        )
        role = ProducerRole(str(raw["producer_role"]))
        actor = ActorContext(
            str(raw["producer"]),
            frozenset({role}),
            subject_id=str(raw["subject_id"]),
        )
        event = EventEnvelope.new(
            twin_id=str(raw["twin_id"]),
            event_type=str(raw["event_type"]),
            plane=EventPlane.SIMULATION,
            payload={
                "subject_id": raw["subject_id"],
                "predicted_state": raw["predicted_state"],
            },
            producer=actor.identity_id,
            producer_role=role,
            idempotency_key=str(raw["idempotency_key"]),
        )
        with self.assertRaisesRegex(
            InvariantViolation,
            str(raw["expected_error"]),
        ):
            await service.append(event, actor=actor, expected_sequence=0)
