from __future__ import annotations

import asyncio
import base64
import json
import unittest
from datetime import UTC, datetime, timedelta

from argo_dt.compiler import ProjectionCompiler
from argo_dt.errors import (
    AuthorizationDenied,
    BackpressureExceeded,
    InvariantViolation,
    MessageTooLarge,
    ProtocolViolation,
    ResumeCursorRejected,
)
from argo_dt.event_store import SQLiteEventStore
from argo_dt.policy import DefaultDenyPolicy
from argo_dt.service import DigitalTwinService
from argo_dt.sync import (
    CursorSigner,
    NatsSubjectTopology,
    StreamPosition,
    SyncLimits,
    TelemetryRecordAck,
    TelemetryRecordStatus,
    TelemetryStreamAck,
)
from argo_dt.types import (
    ActorContext,
    EventEnvelope,
    EventPlane,
    ProducerRole,
    Sensitivity,
)


class TrackingSQLiteEventStore(SQLiteEventStore):
    def __init__(self) -> None:
        super().__init__()
        self.load_limits: list[int | None] = []

    def load(
        self,
        twin_id: str,
        *,
        after_sequence: int = 0,
        up_to_sequence: int | None = None,
        planes: frozenset[EventPlane] | None = None,
        limit: int | None = None,
    ) -> list[EventEnvelope]:
        self.load_limits.append(limit)
        return super().load(
            twin_id,
            after_sequence=after_sequence,
            up_to_sequence=up_to_sequence,
            planes=planes,
            limit=limit,
        )


class DurableSynchronizationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.store = TrackingSQLiteEventStore()
        self.signer = CursorSigner(b"cursor-test-key-32-bytes-minimum!!")
        self.limits = SyncLimits(replay_page_size=2, max_in_flight=3)
        self.service = DigitalTwinService(
            store=self.store,
            projection_compiler=ProjectionCompiler(DefaultDenyPolicy()),
            cursor_signer=self.signer,
            sync_limits=self.limits,
        )
        self.operations = ActorContext(
            "operations-test",
            frozenset({ProducerRole.OPERATIONS_SERVICE}),
        )
        self.ingest_actor = ActorContext(
            "ingest-test",
            frozenset({ProducerRole.INGEST_SERVICE}),
            subject_id="human-1",
        )
        self.subject_actor = ActorContext(
            "human-1",
            frozenset({ProducerRole.HUMAN_REVIEW}),
            subject_id="human-1",
        )

    async def asyncTearDown(self) -> None:
        self.store.close()

    async def append_degradation(self, sequence: int) -> EventEnvelope:
        event = self.degradation_event(sequence)
        return await self.service.append(
            event,
            actor=self.operations,
            expected_sequence=sequence - 1,
        )

    def degradation_event(self, sequence: int) -> EventEnvelope:
        return EventEnvelope.new(
            twin_id="twin-1",
            event_type="DegradationDeclared",
            plane=EventPlane.AUTHORITATIVE,
            payload={"reason": f"reason-{sequence}"},
            producer=self.operations.identity_id,
            producer_role=ProducerRole.OPERATIONS_SERVICE,
            idempotency_key=f"sync-{sequence}",
            occurred_at=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=sequence),
        )

    async def ingest_private_evidence(self) -> EventEnvelope:
        return await self.service.ingest_evidence(
            twin_id="twin-1",
            subject_id="human-1",
            source="screen-capture",
            source_record_id="private-record",
            payload={"secret": "must never enter a state-change frame"},
            rights={"processing": ["modeling"]},
            sensitivity=Sensitivity.RESTRICTED,
            valid_from=datetime(2026, 1, 1, tzinfo=UTC),
            valid_until=None,
            independence_group="private-session",
            expected_sequence=0,
            idempotency_key="private-evidence",
            actor=self.ingest_actor,
        )

    async def test_replay_live_handoff_is_ordered_deduplicated_and_paged(self) -> None:
        await self.append_degradation(1)
        second = await self.append_degradation(2)
        stream = await self.service.open_state_stream(
            "twin-1",
            actor=self.operations,
        )

        # Simulate an at-least-once relay delivery racing with the captured head.
        await self.service.broker.publish(second)
        first_frame = await stream.__anext__()
        second_frame = await stream.__anext__()
        stream.acknowledge(second_frame.resume_token)
        third = await self.append_degradation(3)
        third_frame = await asyncio.wait_for(stream.__anext__(), timeout=0.2)

        self.assertEqual(
            [1, 2, 3],
            [first_frame.sequence, second_frame.sequence, third_frame.sequence],
        )
        self.assertEqual(third.sequence, third_frame.sequence)
        self.assertGreaterEqual(self.service.sync_metrics.duplicate_events, 1)
        replay_limits = [limit for limit in self.store.load_limits if limit is not None]
        self.assertTrue(replay_limits)
        self.assertLessEqual(max(replay_limits), self.limits.replay_page_size)
        await stream.close()
        self.assertEqual(0, self.service.broker.subscription_count)

    async def test_resume_cursor_replays_only_unacknowledged_tail(self) -> None:
        for sequence in range(1, 4):
            await self.append_degradation(sequence)
        stream = await self.service.open_state_stream("twin-1", actor=self.operations)
        first = await stream.__anext__()
        second = await stream.__anext__()
        stream.acknowledge(second.resume_token)
        close = await stream.close(reason="client restart")

        resumed = await self.service.open_state_stream(
            "twin-1",
            actor=self.operations,
            resume_token=close.resume_token,
        )
        tail = await resumed.__anext__()
        self.assertEqual(1, first.sequence)
        self.assertEqual(3, tail.sequence)
        await resumed.close()

    async def test_live_sequence_gap_is_recovered_from_sqlite(self) -> None:
        first = await self.append_degradation(1)
        cursor = self.signer.issue(
            StreamPosition(first.twin_id, first.sequence, first.event_hash)
        )
        stream = await self.service.open_state_stream(
            "twin-1",
            actor=self.operations,
            resume_token=cursor,
        )
        self.store.append(self.degradation_event(2), expected_sequence=1)
        third = self.store.append(self.degradation_event(3), expected_sequence=2)
        await self.service.broker.publish(third)

        recovered = [await stream.__anext__(), await stream.__anext__()]
        self.assertEqual([2, 3], [frame.sequence for frame in recovered])
        await stream.close()

    async def test_cursor_is_forgery_resistant_twin_bound_and_chain_bound(self) -> None:
        event = await self.append_degradation(1)
        valid = self.signer.issue(StreamPosition("twin-1", 1, event.event_hash))
        forged = valid[:-1] + ("A" if valid[-1] != "A" else "B")
        with self.assertRaises(ResumeCursorRejected):
            self.signer.verify(forged, twin_id="twin-1")
        with self.assertRaises(ResumeCursorRejected):
            self.signer.verify(valid, twin_id="twin-2")
        cursor_payload = json.loads(
            base64.urlsafe_b64decode(valid.split(".")[0] + "==")
        )
        self.assertNotIn("event_hash", cursor_payload)
        issued_at = datetime(2026, 1, 1, tzinfo=UTC)
        issued_signer = CursorSigner(b"cursor-expiry-test-key-32-bytes!!", clock=lambda: issued_at)
        expired = issued_signer.issue(StreamPosition("twin-1", 1, event.event_hash))
        expired_signer = CursorSigner(
            b"cursor-expiry-test-key-32-bytes!!",
            clock=lambda: issued_at + timedelta(days=8),
        )
        with self.assertRaises(ResumeCursorRejected):
            expired_signer.verify(expired, twin_id="twin-1")

        nonexistent = self.signer.issue(
            StreamPosition("twin-1", 1, "sha256:" + "0" * 64)
        )
        with self.assertRaises(ResumeCursorRejected):
            await self.service.open_state_stream(
                "twin-1",
                actor=self.operations,
                resume_token=nonexistent,
            )
        self.assertEqual(0, self.service.broker.subscription_count)

    async def test_ack_window_is_cumulative_bounded_and_non_regressing(self) -> None:
        for sequence in range(1, 4):
            await self.append_degradation(sequence)
        stream = await self.service.open_state_stream("twin-1", actor=self.operations)
        first = await stream.__anext__()
        second = await stream.__anext__()
        self.assertEqual(2, stream.outstanding_count)
        stream.acknowledge(second.resume_token)
        self.assertEqual(0, stream.outstanding_count)
        with self.assertRaises(ProtocolViolation):
            stream.acknowledge(first.resume_token)

        third_event = self.store.load("twin-1", after_sequence=2, limit=1)[0]
        undelivered = self.signer.issue(
            StreamPosition("twin-1", third_event.sequence, third_event.event_hash)
        )
        with self.assertRaises(ProtocolViolation):
            stream.acknowledge(undelivered)
        await stream.close()

        constrained_service = DigitalTwinService(
            store=self.store,
            projection_compiler=ProjectionCompiler(DefaultDenyPolicy()),
            cursor_signer=self.signer,
            sync_limits=SyncLimits(replay_page_size=1, max_in_flight=2),
        )
        constrained = await constrained_service.open_state_stream(
            "twin-1",
            actor=self.operations,
        )
        await constrained.__anext__()
        await constrained.__anext__()
        with self.assertRaises(BackpressureExceeded):
            await constrained.__anext__()
        self.assertEqual(0, constrained_service.broker.subscription_count)

    async def test_external_frame_is_payload_minimized_and_stream_is_subject_scoped(self) -> None:
        await self.ingest_private_evidence()
        stream = await self.service.open_state_stream(
            "twin-1",
            actor=self.subject_actor,
        )
        frame = await stream.__anext__()
        public = frame.to_dict()
        self.assertEqual(
            {
                "type",
                "schema_version",
                "twin_id",
                "sequence",
                "event_type",
                "plane",
                "occurred_at",
                "recorded_at",
                "resume_token",
            },
            set(public),
        )
        serialized = str(public)
        for forbidden in (
            "secret",
            "payload",
            "event_id",
            "event_hash",
            "producer",
            "source_record_id",
        ):
            self.assertNotIn(forbidden, serialized)
        await stream.close()

        wrong_subject = ActorContext(
            "human-2",
            frozenset({ProducerRole.HUMAN_REVIEW}),
            subject_id="human-2",
        )
        with self.assertRaises(AuthorizationDenied):
            await self.service.open_state_stream("twin-1", actor=wrong_subject)
        with self.assertRaises(AuthorizationDenied):
            await self.service.subscribe("twin-1", actor=self.subject_actor)
        unscoped_ingest = ActorContext(
            "global-ingest",
            frozenset({ProducerRole.INGEST_SERVICE}),
        )
        with self.assertRaises(AuthorizationDenied):
            await self.service.open_state_stream("twin-1", actor=unscoped_ingest)
        unsigned_service = DigitalTwinService(
            store=self.store,
            projection_compiler=ProjectionCompiler(DefaultDenyPolicy()),
        )
        with self.assertRaises(InvariantViolation):
            await unsigned_service.open_state_stream("twin-1", actor=self.operations)

    async def test_heartbeat_and_close_report_last_acknowledged_cursor(self) -> None:
        await self.append_degradation(1)
        stream = await self.service.open_state_stream("twin-1", actor=self.operations)
        event = await stream.__anext__()
        stream.acknowledge(event.resume_token)
        heartbeat = stream.heartbeat()
        self.assertEqual(1, heartbeat.acknowledged_sequence)
        self.assertEqual(
            1,
            self.signer.verify(heartbeat.resume_token, twin_id="twin-1").sequence,
        )
        close = await stream.close(code=1000, reason="complete")
        self.assertEqual(1, close.acknowledged_sequence)
        with self.assertRaises(StopAsyncIteration):
            await stream.__anext__()

    async def test_server_filter_advances_cursor_when_no_frame_is_outstanding(self) -> None:
        await self.append_degradation(1)
        stream = await self.service.open_state_stream(
            "twin-1",
            actor=self.operations,
            event_types=("ClaimAccepted",),
        )
        waiting = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(0)
        heartbeat = stream.heartbeat()
        self.assertEqual(1, heartbeat.acknowledged_sequence)
        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiting
        await stream.close()


class TransportPrimitiveTests(unittest.TestCase):
    def test_transport_limits_reject_oversized_records_batches_and_counts(self) -> None:
        limits = SyncLimits(
            max_record_bytes=10,
            max_batch_bytes=20,
            max_batch_records=2,
        )
        limits.validate_batch(record_sizes=(10, 10), serialized_batch_bytes=20)
        with self.assertRaises(MessageTooLarge):
            limits.validate_batch(record_sizes=(11,), serialized_batch_bytes=11)
        with self.assertRaises(MessageTooLarge):
            limits.validate_batch(record_sizes=(10, 10), serialized_batch_bytes=21)
        with self.assertRaises(MessageTooLarge):
            limits.validate_batch(record_sizes=(1, 1, 1), serialized_batch_bytes=3)
        with self.assertRaises(ProtocolViolation):
            limits.validate_batch(record_sizes=(10, 10), serialized_batch_bytes=19)

    def test_record_and_stream_acknowledgements_are_explicit(self) -> None:
        committed = TelemetryRecordAck(
            record_index=0,
            source_record_id="source-1",
            status=TelemetryRecordStatus.COMMITTED,
            event_id="event-1",
            sequence=4,
        )
        rejected = TelemetryRecordAck(
            record_index=1,
            source_record_id="source-2",
            status=TelemetryRecordStatus.REJECTED,
            error_code="INVALID_RECORD",
            retryable=False,
        )
        ack = TelemetryStreamAck(
            twin_id="twin-1",
            idempotency_key="batch-1",
            committed_sequence=4,
            records=(committed, rejected),
            resume_token="signed-token",
        )
        self.assertEqual(2, len(ack.records))
        with self.assertRaises(ProtocolViolation):
            TelemetryStreamAck(
                twin_id="twin-1",
                idempotency_key="batch-1",
                committed_sequence=4,
                records=(committed, committed),
                resume_token="signed-token",
            )
        with self.assertRaises(ProtocolViolation):
            TelemetryRecordAck(
                record_index=2,
                source_record_id="source-3",
                status="unknown",  # type: ignore[arg-type]
                error_code="INVALID_STATUS",
            )

    def test_nats_subject_topology_rejects_wildcards(self) -> None:
        event = EventEnvelope.new(
            twin_id="twin-1",
            event_type="DegradationDeclared",
            plane=EventPlane.AUTHORITATIVE,
            payload={"reason": "test"},
            producer="operations",
            producer_role=ProducerRole.OPERATIONS_SERVICE,
            idempotency_key="subject-test",
        )
        self.assertEqual(
            "argo.dt.tenant-1.twin-1.authoritative.DegradationDeclared",
            NatsSubjectTopology.event(tenant="tenant-1", event=event),
        )
        with self.assertRaises(ProtocolViolation):
            NatsSubjectTopology.event(tenant="tenant.*", event=event)
        self.assertEqual(
            "argo.dt.tenant-1.twin-1.authoritative.*",
            NatsSubjectTopology.consumer_filter(
                tenant="tenant-1",
                twin_id="twin-1",
                plane="authoritative",
            ),
        )
        with self.assertRaises(ProtocolViolation):
            NatsSubjectTopology.consumer_filter(
                tenant="tenant-1",
                twin_id="caller.*",
            )


if __name__ == "__main__":
    unittest.main()
