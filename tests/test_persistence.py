from __future__ import annotations

import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

from argo_dt.aggregate import TwinAggregate
from argo_dt.bronze import EncryptedFileBronzeVault, StaticKeyProvider
from argo_dt.errors import (
    ConcurrencyConflict,
    IntegrityError,
    InvariantViolation,
    NotFound,
)
from argo_dt.event_store import SQLiteEventStore
from argo_dt.ports import DurableEventStore
from argo_dt.sync import OutboxRelay
from argo_dt.types import EventEnvelope, EventPlane, ProducerRole


def event(key: str, *, twin_id: str = "twin-persistence") -> EventEnvelope:
    return EventEnvelope.new(
        twin_id=twin_id,
        event_type="DegradationDeclared",
        plane=EventPlane.AUTHORITATIVE,
        payload={"reason": key},
        producer="operations-test",
        producer_role=ProducerRole.OPERATIONS_SERVICE,
        idempotency_key=key,
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


class EventStoreContractMixin:
    """Reusable behavioral contract for every authoritative ledger adapter."""

    store: DurableEventStore

    def test_append_contract_cas_idempotency_chain_and_outbox(self) -> None:
        first = self.store.append(event("one"), expected_sequence=0)
        retried = self.store.append(event("one"), expected_sequence=0)
        self.assertEqual(first.event_id, retried.event_id)  # type: ignore[attr-defined]
        self.assertEqual(  # type: ignore[attr-defined]
            (1, first.event_hash),
            self.store.head(first.twin_id),
        )
        self.assertTrue(self.store.verify_chain(first.twin_id))  # type: ignore[attr-defined]
        self.assertEqual(1, self.store.outbox_backlog())  # type: ignore[attr-defined]

    def test_snapshot_contract_preserves_tail_replay(self) -> None:
        for sequence in range(5):
            self.store.append(event(f"event-{sequence}"), expected_sequence=sequence)
        state_at_three = TwinAggregate.rebuild(
            self.store,
            "twin-persistence",
            up_to_sequence=3,
        )
        self.store.save_snapshot(state_at_three.to_snapshot())
        from_snapshot = TwinAggregate.rebuild(
            self.store,
            "twin-persistence",
            snapshot_store=self.store,
        )
        from_origin = TwinAggregate.rebuild(self.store, "twin-persistence")
        self.assertEqual(  # type: ignore[attr-defined]
            from_origin.to_dict(),
            from_snapshot.to_dict(),
        )

    def test_authoritative_store_rejects_simulation_plane(self) -> None:
        simulation = EventEnvelope.new(
            twin_id="twin-persistence",
            event_type="ScenarioStatePredicted",
            plane=EventPlane.SIMULATION,
            payload={"predicted_state": {}},
            producer="simulation-test",
            producer_role=ProducerRole.SIMULATION_SERVICE,
            idempotency_key="simulation",
        )
        with self.assertRaisesRegex(  # type: ignore[attr-defined]
            InvariantViolation,
            "isolated branch",
        ):
            self.store.append(simulation, expected_sequence=0)


class SQLitePersistenceConformance(EventStoreContractMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.store = SQLiteEventStore()

    def tearDown(self) -> None:
        self.store.close()


class FailingOutboxStore(SQLiteEventStore):
    @staticmethod
    def _insert_outbox(cursor: object, event: EventEnvelope) -> None:
        raise RuntimeError("injected outbox write failure")


class RecordingPublisher:
    def __init__(self, *, failures: int = 0) -> None:
        self.failures = failures
        self.events: list[EventEnvelope] = []

    async def publish(self, value: EventEnvelope) -> None:
        if self.failures:
            self.failures -= 1
            raise ConnectionError("injected broker outage")
        self.events.append(value)


class TransactionalOutboxTests(unittest.IsolatedAsyncioTestCase):
    async def test_outbox_failure_rolls_back_event(self) -> None:
        store = FailingOutboxStore()
        self.addCleanup(store.close)
        with self.assertRaisesRegex(RuntimeError, "injected outbox"):
            store.append(event("atomic"), expected_sequence=0)
        self.assertEqual((0, ""), store.head("twin-persistence"))
        self.assertEqual(0, store.outbox_backlog())

    async def test_failed_publish_remains_pending_and_retries(self) -> None:
        store = SQLiteEventStore()
        self.addCleanup(store.close)
        persisted = store.append(event("retry"), expected_sequence=0)
        publisher = RecordingPublisher(failures=1)
        relay = OutboxRelay(
            store=store,
            publisher=publisher,
            lease_owner="test-relay",
        )

        first = await relay.drain()
        self.assertEqual((1, 0, 1), (first.claimed, first.published, first.failed))
        self.assertEqual(1, store.outbox_backlog())

        second = await relay.drain()
        self.assertEqual((1, 1, 0), (second.claimed, second.published, second.failed))
        self.assertEqual([persisted.event_id], [item.event_id for item in publisher.events])
        self.assertEqual(0, store.outbox_backlog())
        self.assertEqual(
            1,
            store.prune_outbox(
                published_before=datetime.now(UTC) + timedelta(seconds=1)
            ),
        )

    async def test_pending_publication_survives_process_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "ledger.db"
            first_store = SQLiteEventStore(database)
            persisted = first_store.append(event("recover"), expected_sequence=0)
            first_store.close()

            recovered_store = SQLiteEventStore(database)
            self.addCleanup(recovered_store.close)
            publisher = RecordingPublisher()
            result = await OutboxRelay(
                store=recovered_store,
                publisher=publisher,
                lease_owner="recovery-relay",
            ).drain()
            self.assertEqual(1, result.published)
            self.assertEqual(persisted.event_hash, publisher.events[0].event_hash)


class SQLiteConcurrencyTests(unittest.TestCase):
    def test_two_connections_preserve_compare_and_swap(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "ledger.db"
            stores = [SQLiteEventStore(database), SQLiteEventStore(database)]
            self.addCleanup(stores[0].close)
            self.addCleanup(stores[1].close)
            barrier = threading.Barrier(2)

            def append_from(index: int) -> EventEnvelope | Exception:
                barrier.wait()
                try:
                    return stores[index].append(
                        event(f"writer-{index}"),
                        expected_sequence=0,
                    )
                except Exception as exc:
                    return exc

            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(append_from, range(2)))
            self.assertEqual(1, sum(isinstance(item, EventEnvelope) for item in results))
            self.assertEqual(
                1,
                sum(isinstance(item, ConcurrencyConflict) for item in results),
            )
            self.assertEqual(
                (1, stores[0].head("twin-persistence")[1]),
                stores[1].head("twin-persistence"),
            )

    def test_outbox_claim_is_exclusive_across_connections(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "ledger.db"
            first = SQLiteEventStore(database)
            second = SQLiteEventStore(database)
            self.addCleanup(first.close)
            self.addCleanup(second.close)
            first.append(event("claim-exclusive"), expected_sequence=0)
            claimed = first.claim_outbox(lease_owner="relay-one")
            self.assertEqual(1, len(claimed))
            self.assertEqual([], second.claim_outbox(lease_owner="relay-two"))
            self.assertTrue(first.release_outbox(claimed[0], error="retry"))
            self.assertEqual(
                1,
                len(second.claim_outbox(lease_owner="relay-two")),
            )


class SnapshotIntegrityTests(unittest.TestCase):
    def test_snapshot_retention_keeps_newest_checkpoints(self) -> None:
        store = SQLiteEventStore()
        self.addCleanup(store.close)
        for sequence in range(5):
            stored = store.append(
                event(f"retention-{sequence}"),
                expected_sequence=sequence,
            )
            state = TwinAggregate.rebuild(store, stored.twin_id)
            store.save_snapshot(state.to_snapshot())
        self.assertEqual(3, store.prune_snapshots("twin-persistence", keep=2))
        newest = store.load_snapshot("twin-persistence")
        self.assertIsNotNone(newest)
        assert newest is not None
        self.assertEqual(5, newest.sequence)

    def test_snapshot_tampering_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "ledger.db"
            store = SQLiteEventStore(database)
            stored = store.append(event("snapshot"), expected_sequence=0)
            state = TwinAggregate.rebuild(store, stored.twin_id)
            store.save_snapshot(state.to_snapshot())
            store._connection.execute(  # noqa: SLF001 - deliberate corruption injection
                "UPDATE dt_snapshots SET state_hash = 'sha256:tampered'"
            )
            with self.assertRaisesRegex(IntegrityError, "snapshot state hash"):
                store.load_snapshot(stored.twin_id)
            store.close()

    def test_snapshot_must_link_to_exact_event_hash(self) -> None:
        store = SQLiteEventStore()
        self.addCleanup(store.close)
        stored = store.append(event("snapshot-link"), expected_sequence=0)
        state = TwinAggregate.rebuild(store, stored.twin_id)
        snapshot = state.to_snapshot()
        object.__setattr__(snapshot, "last_event_hash", "sha256:wrong")
        with self.assertRaises(IntegrityError):
            store.save_snapshot(snapshot)


class DependencyInvalidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = SQLiteEventStore()

    def tearDown(self) -> None:
        self.store.close()

    def append(
        self,
        event_type: str,
        payload: dict[str, object],
        sequence: int,
        role: ProducerRole,
    ) -> EventEnvelope:
        value = EventEnvelope.new(
            twin_id="twin-dependency",
            event_type=event_type,
            plane=(
                EventPlane.PROJECTION
                if event_type == "ProjectionIssued"
                else EventPlane.AUTHORITATIVE
            ),
            payload=payload,
            producer=f"{role.value}-test",
            producer_role=role,
            idempotency_key=f"{event_type}-{sequence}",
        )
        return self.store.append(value, expected_sequence=sequence)

    def test_evidence_deletion_queues_transitive_claim_and_projection(self) -> None:
        self.append(
            "EvidenceIngested",
            {
                "evidence_id": "evidence-1",
                "subject_id": "subject-1",
                "content_hash": "sha256:evidence",
                "source": "test",
                "rights": {},
                "sensitivity": "internal",
                "independence_group": "source-1",
            },
            0,
            ProducerRole.INGEST_SERVICE,
        )
        self.append(
            "ClaimProposed",
            {
                "claim_id": "claim-1",
                "subject_id": "subject-1",
                "statement": "test",
                "provenance": [
                    {
                        "evidence_id": "evidence-1",
                        "relation": "supports",
                        "independence_group": "source-1",
                    }
                ],
                "epistemic": {
                    "evidence_quality": 1.0,
                    "confidence": 1.0,
                    "salience": 1.0,
                    "stability": 1.0,
                    "freshness": 1.0,
                    "scope_confidence": 1.0,
                    "contradiction_load": 0.0,
                },
            },
            1,
            ProducerRole.ADJUDICATION_WORKER,
        )
        self.append(
            "ProjectionIssued",
            {
                "projection_id": "projection-1",
                "purpose": "test",
                "recipient_id": "recipient-1",
                "receipt_hash": "sha256:receipt",
                "source_claim_ids": ["claim-1"],
            },
            2,
            ProducerRole.PROJECTION_SERVICE,
        )
        self.append(
            "EvidenceDeleted",
            {"evidence_id": "evidence-1", "reason": "erasure"},
            3,
            ProducerRole.INGEST_SERVICE,
        )

        self.assertEqual(
            (("claim", "claim-1"), ("projection", "projection-1")),
            self.store.dependents(
                "twin-dependency",
                "evidence",
                "evidence-1",
            ),
        )
        pending = self.store.pending_invalidations()
        self.assertEqual(
            {("claim", "claim-1"), ("projection", "projection-1")},
            {(item.dependent_kind, item.dependent_id) for item in pending},
        )
        self.assertTrue(self.store.mark_invalidation_processed(pending[0]))
        self.assertEqual(1, len(self.store.pending_invalidations()))

        state = TwinAggregate.rebuild(self.store, "twin-dependency")
        self.assertEqual({"claim-1"}, state.stale_claim_ids)
        self.assertEqual({"projection-1"}, state.stale_projection_ids)

    def test_dependency_index_rebuilds_from_existing_events(self) -> None:
        self.append(
            "EvidenceIngested",
            {
                "evidence_id": "evidence-rebuild",
                "subject_id": "subject-1",
                "content_hash": "sha256:evidence",
                "source": "test",
                "rights": {},
                "sensitivity": "internal",
                "independence_group": "source-1",
            },
            0,
            ProducerRole.INGEST_SERVICE,
        )
        self.append(
            "ClaimProposed",
            {
                "claim_id": "claim-rebuild",
                "subject_id": "subject-1",
                "statement": "test",
                "provenance": [
                    {
                        "evidence_id": "evidence-rebuild",
                        "relation": "supports",
                        "independence_group": "source-1",
                    }
                ],
                "epistemic": {
                    "evidence_quality": 1.0,
                    "confidence": 1.0,
                    "salience": 1.0,
                    "stability": 1.0,
                    "freshness": 1.0,
                    "scope_confidence": 1.0,
                    "contradiction_load": 0.0,
                },
            },
            1,
            ProducerRole.ADJUDICATION_WORKER,
        )
        self.store._connection.execute("DELETE FROM dt_dependencies")  # noqa: SLF001
        self.store._connection.execute(  # noqa: SLF001
            "DELETE FROM dt_adapter_metadata WHERE key = 'dependency_index'"
        )
        self.store._migrate_dependency_index()  # noqa: SLF001 - upgrade drill
        self.assertEqual(
            (("claim", "claim-rebuild"),),
            self.store.dependents(
                "twin-dependency",
                "evidence",
                "evidence-rebuild",
            ),
        )


class BronzeVaultTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.vault = EncryptedFileBronzeVault(
            self.directory.name,
            key_provider=StaticKeyProvider(key_id="local-test-key", key=b"k" * 32),
        )
        self.metadata = {
            "connector_id": "fixture",
            "connector_version": "1.0.0",
            "source_record_id": "record-1",
            "acquired_at": datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
            "rights": {"processing": ["modeling"]},
            "sensitivity": "restricted",
        }

    def test_round_trip_is_encrypted_and_source_identity_is_deterministic(self) -> None:
        content = b"highly-sensitive-source-content"
        uri, value_hash = self.vault.put(
            subject_id="subject-1",
            media_type="text/plain",
            content=content,
            metadata=self.metadata,
        )
        duplicate_uri, duplicate_hash = self.vault.put(
            subject_id="subject-1",
            media_type="text/plain",
            content=content,
            metadata=self.metadata,
        )
        self.assertEqual((uri, value_hash), (duplicate_uri, duplicate_hash))
        encrypted = next(Path(self.directory.name).rglob("*.dtb")).read_bytes()
        self.assertNotIn(content, encrypted)
        self.assertNotIn(b"restricted", encrypted)

        record, restored = self.vault.get(subject_id="subject-1", object_uri=uri)
        self.assertEqual(content, restored)
        self.assertEqual(value_hash, record.content_hash)
        self.assertEqual(self.metadata, record.metadata)

    def test_same_source_identity_cannot_silently_change_content(self) -> None:
        self.vault.put(
            subject_id="subject-1",
            media_type="text/plain",
            content=b"original",
            metadata=self.metadata,
        )
        with self.assertRaisesRegex(InvariantViolation, "changed acquisition"):
            self.vault.put(
                subject_id="subject-1",
                media_type="text/plain",
                content=b"replacement",
                metadata=self.metadata,
            )

    def test_ciphertext_tampering_and_cross_subject_reads_fail_closed(self) -> None:
        uri, _ = self.vault.put(
            subject_id="subject-1",
            media_type="application/octet-stream",
            content=b"payload",
            metadata=self.metadata,
        )
        with self.assertRaises(NotFound):
            self.vault.get(subject_id="subject-2", object_uri=uri)

        path = next(Path(self.directory.name).rglob("*.dtb"))
        ciphertext = bytearray(path.read_bytes())
        ciphertext[-1] ^= 1
        path.write_bytes(ciphertext)
        with self.assertRaisesRegex(IntegrityError, "authentication failed"):
            self.vault.get(subject_id="subject-1", object_uri=uri)

    def test_delete_is_subject_scoped_and_idempotent(self) -> None:
        uri, _ = self.vault.put(
            subject_id="subject-1",
            media_type="text/plain",
            content=b"delete-me",
            metadata=self.metadata,
        )
        self.assertTrue(self.vault.delete(subject_id="subject-1", object_uri=uri))
        self.assertFalse(self.vault.delete(subject_id="subject-1", object_uri=uri))


if __name__ == "__main__":
    unittest.main()
