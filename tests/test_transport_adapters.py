from __future__ import annotations

import asyncio
import hashlib
import json
import unittest
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, AsyncIterator, Mapping

from argo_dt.adapters.auth import (
    AuthenticatedPrincipal,
    TransportCredentials,
)
from argo_dt.adapters.grpc import (
    DigitalTwinGrpcAdapter,
    ProtobufCodec,
    register_grpc_service,
)
from argo_dt.adapters.nats import JetStreamConsumer, JetStreamPublisher
from argo_dt.adapters.observability import OpenTelemetrySyncExporter
from argo_dt.adapters.telemetry import (
    TelemetryBatchProcessor,
    TelemetryInputBatch,
    TelemetryInputRecord,
)
from argo_dt.adapters.websocket import StateWebSocketApp, StateWebSocketCodec
from argo_dt.compiler import ProjectionCompiler
from argo_dt.errors import AuthorizationDenied, MessageTooLarge, ProtocolViolation
from argo_dt.event_store import SQLiteEventStore
from argo_dt.policy import DefaultDenyPolicy
from argo_dt.service import DigitalTwinService
from argo_dt.sync import CursorSigner, RotatingCursorSigner, StreamPosition, SyncLimits
from argo_dt.types import ActorContext, EventEnvelope, EventPlane, ProducerRole, utc_now


class FixedAuthenticator:
    def __init__(self, principal: AuthenticatedPrincipal | None) -> None:
        self.principal = principal
        self.credentials: list[TransportCredentials] = []

    async def authenticate(
        self,
        credentials: TransportCredentials,
    ) -> AuthenticatedPrincipal:
        self.credentials.append(credentials)
        if self.principal is None:
            raise AuthorizationDenied("denied")
        return self.principal


class TransportTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.store = SQLiteEventStore()
        self.signer = CursorSigner(
            b"transport-test-key-is-at-least-32-bytes",
            key_id="transport-v1",
        )
        self.service = DigitalTwinService(
            store=self.store,
            projection_compiler=ProjectionCompiler(DefaultDenyPolicy()),
            cursor_signer=self.signer,
            sync_limits=SyncLimits(
                max_record_bytes=1024,
                max_batch_bytes=4096,
                max_batch_records=8,
                replay_page_size=2,
                max_in_flight=4,
                heartbeat_seconds=1,
            ),
        )
        self.operations = ActorContext(
            "operations",
            frozenset({ProducerRole.OPERATIONS_SERVICE}),
        )
        self.ingest = ActorContext(
            "ingest",
            frozenset({ProducerRole.INGEST_SERVICE}),
            subject_id="human-1",
        )

    async def asyncTearDown(self) -> None:
        self.store.close()

    async def append_event(self, sequence: int = 1) -> EventEnvelope:
        return await self.service.append(
            EventEnvelope.new(
                twin_id="twin-1",
                event_type="DegradationDeclared",
                plane=EventPlane.AUTHORITATIVE,
                payload={"reason": f"reason-{sequence}"},
                producer="operations",
                producer_role=ProducerRole.OPERATIONS_SERVICE,
                idempotency_key=f"transport-event-{sequence}",
            ),
            actor=self.operations,
            expected_sequence=sequence - 1,
        )


class CursorRotationTests(unittest.TestCase):
    def test_rotation_accepts_overlap_and_revokes_pruned_key(self) -> None:
        position = StreamPosition("twin-1", 1, "sha256:" + "a" * 64)
        old = CursorSigner(b"old-key-material-is-long-enough-000", key_id="old")
        current = CursorSigner(b"new-key-material-is-long-enough-000", key_id="current")
        ring = RotatingCursorSigner(current, retiring=(old,))

        old_token = old.issue(position)
        decoded = ring.verify(old_token, twin_id="twin-1")
        self.assertEqual("old", decoded.signing_key_id)
        self.assertEqual(position, ring.verify_chain_binding(decoded, position))
        self.assertEqual(
            "current",
            ring.verify(ring.issue(position), twin_id="twin-1").signing_key_id,
        )

        future = CursorSigner(b"future-key-material-is-long-enough!!", key_id="future")
        rotated = ring.rotate(future, retain=1)
        self.assertEqual(("future", "current"), rotated.accepted_key_ids)
        with self.assertRaises(ProtocolViolation):
            rotated.verify(old_token, twin_id="twin-1")

    def test_rotation_rejects_duplicate_ids_and_unbounded_key_sets(self) -> None:
        active = CursorSigner(b"active-key-material-is-long-enough!!", key_id="same")
        duplicate = CursorSigner(b"other-key-material-is-long-enough!!!", key_id="same")
        with self.assertRaises(ValueError):
            RotatingCursorSigner(active, retiring=(duplicate,))
        keys = tuple(
            CursorSigner(bytes([index]) * 32, key_id=f"key-{index}")
            for index in range(1, 9)
        )
        with self.assertRaises(ValueError):
            RotatingCursorSigner(active, retiring=keys)


class TelemetryProcessorTests(TransportTestCase):
    @staticmethod
    def record(
        source_id: str,
        payload: bytes,
        *,
        content_hash: str | None = None,
    ) -> TelemetryInputRecord:
        return TelemetryInputRecord(
            source_record_id=source_id,
            valid_from=datetime(2026, 1, 1, tzinfo=UTC),
            valid_until=None,
            media_type="application/json",
            payload=payload,
            content_hash=content_hash
            or "sha256:" + hashlib.sha256(payload).hexdigest(),
            independence_group="sensor-a",
            sensitivity="internal",
            rights={"processing": ["modeling"]},
        )

    async def test_batch_has_explicit_partial_commit_and_idempotent_retry(self) -> None:
        records = (
            self.record("one", b'{"value":1}'),
            self.record("bad", b'{"value":2}', content_hash="sha256:" + "0" * 64),
            self.record("three", b'{"value":3}'),
        )
        batch = TelemetryInputBatch(
            twin_id="twin-1",
            subject_id="human-1",
            source="sensor",
            connector_version="1.2.3",
            idempotency_key="batch-12345678",
            expected_sequence=0,
            records=records,
        )
        processor = TelemetryBatchProcessor(self.service)
        first = await processor.process(
            batch,
            actor=self.ingest,
            record_sizes=tuple(len(record.payload) for record in records),
            serialized_batch_bytes=100,
        )
        self.assertEqual(["committed", "rejected", "committed"], [r.status for r in first.records])
        self.assertEqual("CONTENT_HASH_MISMATCH", first.records[1].error_code)
        self.assertEqual(2, first.committed_sequence)
        persisted = self.store.load("twin-1")
        self.assertEqual("1.2.3", persisted[0].payload["connector_version"])
        self.assertNotIn(records[0].payload.decode(), json.dumps(persisted[0].payload))

        retried = await processor.process(
            batch,
            actor=self.ingest,
            record_sizes=tuple(len(record.payload) for record in records),
            serialized_batch_bytes=100,
        )
        self.assertEqual(
            ["duplicate", "rejected", "duplicate"],
            [record.status for record in retried.records],
        )
        self.assertEqual(2, self.store.head("twin-1")[0])

    async def test_batch_rejects_ambiguous_or_oversized_input_before_writes(self) -> None:
        record = self.record("same", b'{"value":1}')
        batch = TelemetryInputBatch(
            "twin-1",
            "human-1",
            "sensor",
            "1",
            "batch-12345678",
            0,
            (record, record),
        )
        processor = TelemetryBatchProcessor(self.service)
        with self.assertRaises(ProtocolViolation):
            await processor.process(
                batch,
                actor=self.ingest,
                record_sizes=(10, 10),
                serialized_batch_bytes=20,
            )
        with self.assertRaises(MessageTooLarge):
            await processor.process(
                TelemetryInputBatch(
                    "twin-1",
                    "human-1",
                    "sensor",
                    "1",
                    "batch-abcdefgh",
                    0,
                    (record,),
                ),
                actor=self.ingest,
                record_sizes=(2048,),
                serialized_batch_bytes=2048,
            )
        with self.assertRaises(ProtocolViolation):
            await processor.process(
                TelemetryInputBatch(
                    "twin-1",
                    "human-1",
                    "sensor",
                    "1",
                    "batch-size-vector",
                    0,
                    (record,),
                ),
                actor=self.ingest,
                record_sizes=(10, 10),
                serialized_batch_bytes=20,
            )
        self.assertEqual(0, self.store.head("twin-1")[0])

    async def test_json_duplicate_keys_are_rejected_without_silent_overwrite(self) -> None:
        payload = b'{"value":1,"value":2}'
        record = self.record("duplicate-json", payload)
        batch = TelemetryInputBatch(
            "twin-1",
            "human-1",
            "sensor",
            "1",
            "batch-json-duplicate",
            0,
            (record,),
        )
        ack = await TelemetryBatchProcessor(self.service).process(
            batch,
            actor=self.ingest,
            record_sizes=(len(payload),),
            serialized_batch_bytes=len(payload),
        )
        self.assertEqual("INVALID_JSON_PAYLOAD", ack.records[0].error_code)
        self.assertEqual(0, self.store.head("twin-1")[0])

    async def test_non_finite_json_numbers_are_rejected(self) -> None:
        payload = b'{"value":NaN}'
        record = self.record("non-finite", payload)
        batch = TelemetryInputBatch(
            "twin-1",
            "human-1",
            "sensor",
            "1",
            "batch-non-finite",
            0,
            (record,),
        )
        ack = await TelemetryBatchProcessor(self.service).process(
            batch,
            actor=self.ingest,
            record_sizes=(len(payload),),
            serialized_batch_bytes=len(payload),
        )
        self.assertEqual("INVALID_JSON_PAYLOAD", ack.records[0].error_code)
        self.assertEqual(0, self.store.head("twin-1")[0])


class StateCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_latest_cache_is_bounded_copy_safe_and_head_verified(self) -> None:
        store = SQLiteEventStore()
        service = DigitalTwinService(
            store=store,
            projection_compiler=ProjectionCompiler(DefaultDenyPolicy()),
            state_cache_entries=1,
        )
        actor = ActorContext(
            "operations",
            frozenset({ProducerRole.OPERATIONS_SERVICE}),
        )

        def event(twin_id: str, key: str) -> EventEnvelope:
            return EventEnvelope.new(
                twin_id=twin_id,
                event_type="DegradationDeclared",
                plane=EventPlane.AUTHORITATIVE,
                payload={"reason": key},
                producer=actor.identity_id,
                producer_role=ProducerRole.OPERATIONS_SERVICE,
                idempotency_key=key,
            )

        try:
            await service.append(event("twin-1", "one"), actor=actor, expected_sequence=0)
            caller_copy = service.state("twin-1")
            caller_copy.degraded_reasons.append("caller mutation")
            self.assertNotIn("caller mutation", service.state("twin-1").degraded_reasons)

            store.append(event("twin-1", "two"), expected_sequence=1)
            refreshed = service.state("twin-1")
            self.assertEqual(2, refreshed.sequence)
            self.assertIn("two", refreshed.degraded_reasons)

            await service.append(event("twin-2", "other"), actor=actor, expected_sequence=0)
            self.assertEqual(1, service.cached_twin_count)
        finally:
            store.close()


class WebSocketAdapterTests(TransportTestCase):
    async def test_asgi_stream_authenticates_emits_minimized_frame_and_acks(self) -> None:
        await self.append_event()
        principal = AuthenticatedPrincipal(self.operations, frozenset({"dt.stream"}))
        authenticator = FixedAuthenticator(principal)
        app = StateWebSocketApp(service=self.service, authenticator=authenticator)
        incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        await incoming.put({"type": "websocket.connect"})
        await incoming.put(
            {
                "type": "websocket.receive",
                "text": json.dumps({"type": "subscribe", "max_in_flight": 2}),
            }
        )
        outgoing: list[dict[str, Any]] = []

        async def receive() -> dict[str, Any]:
            return await incoming.get()

        async def send(message: dict[str, Any]) -> None:
            outgoing.append(message)
            if message.get("type") == "websocket.send":
                body = json.loads(message["text"])
                if body["type"] == "event":
                    await incoming.put(
                        {
                            "type": "websocket.receive",
                            "text": json.dumps(
                                {
                                    "type": "acknowledge",
                                    "resume_token": body["resume_token"],
                                }
                            ),
                        }
                    )
                    await incoming.put(
                        {
                            "type": "websocket.receive",
                            "text": json.dumps(
                                {"type": "close", "code": 1000, "reason": "done"}
                            ),
                        }
                    )

        await asyncio.wait_for(
            app(
                {
                    "type": "websocket",
                    "path": "/v1/twins/twin-1/events",
                    "headers": [(b"authorization", b"Bearer opaque")],
                    "subprotocols": ["argo.dt.state-stream.v1"],
                    "client": ("127.0.0.1", 1234),
                },
                receive,
                send,
            ),
            timeout=1,
        )
        accepted = [item for item in outgoing if item["type"] == "websocket.accept"]
        self.assertEqual("argo.dt.state-stream.v1", accepted[0]["subprotocol"])
        frames = [json.loads(item["text"]) for item in outgoing if item["type"] == "websocket.send"]
        event = next(frame for frame in frames if frame["type"] == "event")
        self.assertEqual(1, event["sequence"])
        self.assertTrue({"payload", "event_hash", "event_id"}.isdisjoint(event))
        self.assertEqual(1, self.service.sync_metrics.acknowledgements)
        self.assertEqual(0, self.service.broker.subscription_count)
        self.assertEqual("Bearer opaque", authenticator.credentials[0].authorization)

    async def test_asgi_denies_before_accept_and_requires_subprotocol(self) -> None:
        incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        await incoming.put({"type": "websocket.connect"})
        await incoming.put(
            {"type": "websocket.receive", "text": '{"type":"subscribe"}'}
        )
        received = 0

        async def receive() -> dict[str, Any]:
            nonlocal received
            received += 1
            return await incoming.get()

        denied: list[dict[str, Any]] = []
        app = StateWebSocketApp(
            service=self.service,
            authenticator=FixedAuthenticator(None),
        )
        async def send_denied(message: dict[str, Any]) -> None:
            denied.append(message)

        await app(
            {
                "type": "websocket",
                "path": "/v1/twins/twin-1/events",
                "headers": [],
                "subprotocols": ["argo.dt.state-stream.v1"],
            },
            receive,
            send_denied,
        )
        self.assertEqual([4403], [item["code"] for item in denied])
        self.assertFalse(any(item["type"] == "websocket.accept" for item in denied))
        self.assertEqual(1, received)
        self.assertEqual(1, incoming.qsize())

        missing_protocol: list[dict[str, Any]] = []
        protocol_incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        await protocol_incoming.put({"type": "websocket.connect"})

        async def receive_missing_protocol() -> dict[str, Any]:
            return await protocol_incoming.get()

        async def send_missing_protocol(message: dict[str, Any]) -> None:
            missing_protocol.append(message)

        await app(
            {
                "type": "websocket",
                "path": "/v1/twins/twin-1/events",
                "headers": [],
                "subprotocols": [],
            },
            receive_missing_protocol,
            send_missing_protocol,
        )
        self.assertEqual(4406, missing_protocol[0]["code"])

    def test_websocket_codec_rejects_duplicates_unknown_fields_and_large_frames(self) -> None:
        codec = StateWebSocketCodec(max_frame_bytes=256)
        with self.assertRaises(ProtocolViolation):
            codec.decode('{"type":"subscribe","type":"close"}')
        with self.assertRaises(ProtocolViolation):
            codec.decode('{"type":"subscribe","unknown":true}')
        with self.assertRaises(MessageTooLarge):
            codec.decode("{" + " " * 300 + "}")


class FakeJetStream:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, dict[str, Any]]] = []
        self.subscriptions: list[tuple[str, dict[str, Any]]] = []

    async def publish(self, subject: str, payload: bytes, **kwargs: Any) -> object:
        self.published.append((subject, payload, kwargs))
        return object()

    async def subscribe(self, subject: str, **kwargs: Any) -> object:
        self.subscriptions.append((subject, kwargs))
        return object()


class FakeNatsMessage:
    def __init__(self, subject: str, data: bytes, deliveries: int = 1) -> None:
        self.subject = subject
        self.data = data
        self.metadata = SimpleNamespace(num_delivered=deliveries)
        self.acked = False
        self.termed = False
        self.nak_delay: float | None = None

    async def ack(self) -> None:
        self.acked = True

    async def nak(self, *, delay: float | None = None) -> None:
        self.nak_delay = delay

    async def term(self) -> None:
        self.termed = True


class NatsAdapterTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def event() -> EventEnvelope:
        return EventEnvelope.new(
            twin_id="twin-1",
            event_type="DegradationDeclared",
            plane=EventPlane.AUTHORITATIVE,
            payload={"secret": "internal-only"},
            producer="operations",
            producer_role=ProducerRole.OPERATIONS_SERVICE,
            idempotency_key="nats-event",
        ).seal(sequence=1, previous_hash="", recorded_at=utc_now())

    async def test_publisher_and_consumer_use_manual_ack_and_validated_subject(self) -> None:
        jetstream = FakeJetStream()
        event = self.event()
        await JetStreamPublisher(jetstream=jetstream, tenant="tenant-1").publish(event)
        subject, payload, options = jetstream.published[0]
        self.assertEqual(
            "argo.dt.tenant-1.twin-1.authoritative.DegradationDeclared",
            subject,
        )
        self.assertEqual(event.event_id, options["headers"]["Nats-Msg-Id"])
        handled: list[EventEnvelope] = []

        async def handler(value: EventEnvelope) -> None:
            handled.append(value)

        message = FakeNatsMessage(subject, payload)
        consumer = JetStreamConsumer(
            jetstream=jetstream,
            tenant="tenant-1",
            stage="projection",
            handler=handler,
        )
        await consumer.process(message)
        self.assertTrue(message.acked)
        self.assertEqual([event.event_hash], [item.event_hash for item in handled])
        await consumer.bind(
            durable="projection-v1",
            stream="dt-events",
            twin_id="twin-1",
            plane="authoritative",
        )
        filter_subject, options = jetstream.subscriptions[0]
        self.assertEqual("argo.dt.tenant-1.twin-1.authoritative.*", filter_subject)
        self.assertTrue(options["manual_ack"])

    async def test_bounded_redelivery_uses_payload_free_deadletter_marker(self) -> None:
        jetstream = FakeJetStream()
        event = self.event()
        publisher = JetStreamPublisher(jetstream=jetstream, tenant="tenant-1")
        await publisher.publish(event)
        subject, payload, _ = jetstream.published.pop()

        async def failing_handler(_event: EventEnvelope) -> None:
            raise RuntimeError("private failure detail")

        consumer = JetStreamConsumer(
            jetstream=jetstream,
            tenant="tenant-1",
            stage="projection",
            handler=failing_handler,
            max_deliver=2,
        )
        retry = FakeNatsMessage(subject, payload, deliveries=1)
        await consumer.process(retry)
        self.assertEqual(1.0, retry.nak_delay)
        terminal = FakeNatsMessage(subject, payload, deliveries=2)
        await consumer.process(terminal)
        self.assertTrue(terminal.termed)
        marker = json.loads(jetstream.published[-1][1])
        self.assertEqual("MAX_DELIVER_EXCEEDED", marker["reason"])
        rendered = json.dumps(marker)
        for forbidden in (
            "payload",
            "event_id",
            "event_hash",
            "internal-only",
            "private failure detail",
        ):
            self.assertNotIn(forbidden, rendered)


class FakeCounter:
    def __init__(self) -> None:
        self.calls: list[tuple[int, Mapping[str, object] | None]] = []

    def add(
        self,
        amount: int,
        attributes: Mapping[str, object] | None = None,
    ) -> None:
        self.calls.append((amount, attributes))


class FakeMeter:
    def __init__(self) -> None:
        self.counters: dict[str, FakeCounter] = {}

    def create_counter(self, name: str, **_kwargs: Any) -> FakeCounter:
        counter = FakeCounter()
        self.counters[name] = counter
        return counter


class ObservabilityAdapterTests(unittest.TestCase):
    def test_otel_export_is_delta_based_and_rejects_high_cardinality_attributes(self) -> None:
        from argo_dt.sync import SyncMetrics

        metrics = SyncMetrics(replayed_events=3, acknowledgements=2)
        meter = FakeMeter()
        exporter = OpenTelemetrySyncExporter(
            metrics=metrics,
            meter=meter,
            attributes={"service.name": "argo-dt", "network.transport": "grpc"},
        )
        first = exporter.export()
        self.assertEqual(3, first["replayed_events"])
        metrics.replayed_events += 2
        second = exporter.export()
        self.assertEqual(2, second["replayed_events"])
        for counter in meter.counters.values():
            for _amount, attributes in counter.calls:
                self.assertEqual(
                    {"service.name": "argo-dt", "network.transport": "grpc"},
                    attributes,
                )
        with self.assertRaises(ValueError):
            OpenTelemetrySyncExporter(
                metrics=metrics,
                meter=meter,
                attributes={"twin_id": "forbidden"},
            )
        with self.assertRaises(ValueError):
            OpenTelemetrySyncExporter(
                metrics=metrics,
                meter=meter,
                attributes={"service.name": {"unbounded": "shape"}},
            )


class FakeTimestamp:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def ToDatetime(self, *, tzinfo: Any) -> datetime:
        return self.value.astimezone(tzinfo)


class FakeProtoRecord:
    def __init__(self, payload: bytes) -> None:
        self.source_record_id = "record-1"
        self.valid_from = FakeTimestamp(datetime(2026, 1, 1, tzinfo=UTC))
        self.valid_until = None
        self.media_type = "application/json"
        self.payload = payload
        self.content_hash = "sha256:" + hashlib.sha256(payload).hexdigest()
        self.independence_group = "sensor-a"
        self.sensitivity = "internal"
        self.rights = {"processing": ["modeling"]}

    def HasField(self, name: str) -> bool:
        return getattr(self, name) is not None

    def ByteSize(self) -> int:
        return len(self.payload) + 64


class FakeProtoBatch:
    def __init__(self, record: FakeProtoRecord) -> None:
        self.twin_id = "twin-1"
        self.subject_id = "human-1"
        self.source = "sensor"
        self.connector_version = "1"
        self.idempotency_key = "grpc-batch-12345678"
        self.expected_sequence = 0
        self.records = [record]

    def ByteSize(self) -> int:
        return self.records[0].ByteSize() + 64


class FakeStateRequest:
    def __init__(self, kind: str, value: object) -> None:
        self._kind = kind
        setattr(self, kind, value)

    def WhichOneof(self, _name: str) -> str:
        return self._kind

    def ByteSize(self) -> int:
        return 128


class FakeMessages:
    def __getattr__(self, name: str) -> Any:
        def construct(**kwargs: Any) -> Any:
            return SimpleNamespace(_message_type=name, **kwargs)

        return construct


class FakeStatusCodes:
    PERMISSION_DENIED = "PERMISSION_DENIED"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    FAILED_PRECONDITION = "FAILED_PRECONDITION"
    DATA_LOSS = "DATA_LOSS"
    UNIMPLEMENTED = "UNIMPLEMENTED"


class FakeGrpcContext:
    def __init__(self) -> None:
        self.aborted: tuple[str, str] | None = None

    def invocation_metadata(self) -> tuple[tuple[str, str], ...]:
        return (("authorization", "Bearer opaque"),)

    def peer(self) -> str:
        return "ipv4:127.0.0.1:50000"

    async def abort(self, code: str, reason: str) -> None:
        self.aborted = (code, reason)
        raise RuntimeError(f"aborted:{code}")


class GrpcAdapterTests(TransportTestCase):
    def codec(self) -> ProtobufCodec:
        return ProtobufCodec(
            messages=FakeMessages(),
            timestamp_factory=lambda value: value,
            struct_decoder=lambda value: value,
        )

    async def test_grpc_telemetry_maps_generated_messages_and_exact_sizes(self) -> None:
        principal = AuthenticatedPrincipal(self.ingest, frozenset({"dt.ingest"}))
        adapter = DigitalTwinGrpcAdapter(
            service=self.service,
            authenticator=FixedAuthenticator(principal),
            codec=self.codec(),
            status_codes=FakeStatusCodes,
        )
        batch = FakeProtoBatch(FakeProtoRecord(b'{"temperature":21}'))

        async def requests() -> AsyncIterator[FakeProtoBatch]:
            yield batch

        responses = [item async for item in adapter.StreamTelemetry(requests(), FakeGrpcContext())]
        self.assertEqual(1, len(responses))
        self.assertEqual("committed", responses[0].stream_status)
        self.assertEqual(1, responses[0].committed_sequence)
        self.assertEqual("committed", responses[0].record_acks[0].status)

    async def test_grpc_rejects_missing_scope_before_processing(self) -> None:
        principal = AuthenticatedPrincipal(self.ingest, frozenset({"dt.stream"}))
        adapter = DigitalTwinGrpcAdapter(
            service=self.service,
            authenticator=FixedAuthenticator(principal),
            codec=self.codec(),
            status_codes=FakeStatusCodes,
        )

        async def requests() -> AsyncIterator[FakeProtoBatch]:
            raise AssertionError("request stream must not be consumed")
            yield FakeProtoBatch(FakeProtoRecord(b"{}"))

        context = FakeGrpcContext()
        with self.assertRaisesRegex(RuntimeError, "PERMISSION_DENIED"):
            _ = [item async for item in adapter.StreamTelemetry(requests(), context)]
        self.assertEqual(("PERMISSION_DENIED", "access denied"), context.aborted)

    async def test_grpc_state_stream_replays_acknowledges_and_closes(self) -> None:
        await self.append_event()
        principal = AuthenticatedPrincipal(self.operations, frozenset({"dt.stream"}))
        adapter = DigitalTwinGrpcAdapter(
            service=self.service,
            authenticator=FixedAuthenticator(principal),
            codec=self.codec(),
            status_codes=FakeStatusCodes,
        )
        controls: asyncio.Queue[FakeStateRequest | None] = asyncio.Queue()
        subscribe = FakeStateRequest(
            "subscribe",
            SimpleNamespace(
                twin_id="twin-1",
                after_sequence=0,
                event_types=[],
                include_projection_events=False,
                resume_token="",
                max_in_flight=2,
            ),
        )

        async def requests() -> AsyncIterator[FakeStateRequest]:
            yield subscribe
            while True:
                item = await controls.get()
                if item is None:
                    return
                yield item

        responses = adapter.SubscribeState(requests(), FakeGrpcContext())
        event_frame = await asyncio.wait_for(anext(responses), timeout=1)
        self.assertEqual(1, event_frame.event.sequence)
        self.assertFalse(hasattr(event_frame.event, "payload"))
        await controls.put(
            FakeStateRequest(
                "acknowledge",
                SimpleNamespace(resume_token=event_frame.event.resume_token),
            )
        )
        await controls.put(
            FakeStateRequest(
                "close",
                SimpleNamespace(code=1000, reason="done"),
            )
        )
        close_frame = await asyncio.wait_for(anext(responses), timeout=1)
        self.assertEqual(1, close_frame.close.acknowledged_sequence)
        self.assertEqual(1, self.service.sync_metrics.acknowledgements)
        with self.assertRaises(StopAsyncIteration):
            await anext(responses)

    def test_registration_uses_injected_generated_registration_function(self) -> None:
        registered: list[tuple[object, object]] = []
        adapter = object()
        server = object()
        register_grpc_service(
            server,
            adapter,  # type: ignore[arg-type]
            add_servicer=lambda value, target: registered.append((value, target)),
        )
        self.assertEqual([(adapter, server)], registered)


if __name__ == "__main__":
    unittest.main()
