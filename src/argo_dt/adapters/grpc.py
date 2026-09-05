"""gRPC synchronization adapter over host-generated protobuf modules.

The repository owns the `.proto`; the host build owns generated code. This
module imports neither grpcio nor protobuf at module load, keeping the embedded
SQLite profile dependency-light while still providing executable handlers.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from ..errors import (
    AuthorizationDenied,
    BackpressureExceeded,
    IntegrityError,
    InvariantViolation,
    MessageTooLarge,
    ProtocolViolation,
)
from ..service import DigitalTwinService
from ..sync import CloseFrame, HeartbeatFrame, StateChangeFrame
from .auth import AuthenticatedPrincipal, Authenticator, TransportCredentials
from .telemetry import (
    TelemetryBatchProcessor,
    TelemetryInputBatch,
    TelemetryInputRecord,
)
from .websocket import (
    AcknowledgeControl,
    ClientCloseControl,
    HeartbeatControl,
    SubscribeControl,
)


class ProtobufCodec:
    """Map generated protobuf messages without coupling the kernel to protobuf."""

    def __init__(
        self,
        *,
        messages: Any,
        timestamp_factory: Callable[[datetime], Any],
        struct_decoder: Callable[[Any], Mapping[str, object]],
        max_control_frame_bytes: int = 16 * 1024,
    ) -> None:
        if max_control_frame_bytes < 256:
            raise ValueError("max_control_frame_bytes must be at least 256")
        self.messages = messages
        self._timestamp_factory = timestamp_factory
        self._struct_decoder = struct_decoder
        self._max_control_frame_bytes = max_control_frame_bytes

    @classmethod
    def from_generated(cls, messages: Any) -> ProtobufCodec:
        try:
            from google.protobuf.json_format import MessageToDict
            from google.protobuf.timestamp_pb2 import Timestamp
        except ImportError as exc:  # pragma: no cover - optional production extra
            raise RuntimeError("install the 'grpc' extra to use protobuf transport") from exc

        def timestamp_factory(value: datetime) -> Timestamp:
            result = Timestamp()
            result.FromDatetime(value.astimezone(UTC))
            return result

        def struct_decoder(value: Any) -> Mapping[str, object]:
            return cast(
                Mapping[str, object],
                MessageToDict(value, preserving_proto_field_name=True),
            )

        return cls(
            messages=messages,
            timestamp_factory=timestamp_factory,
            struct_decoder=struct_decoder,
        )

    def telemetry_batch(
        self,
        message: Any,
    ) -> tuple[TelemetryInputBatch, tuple[int, ...], int]:
        records = tuple(self._telemetry_record(record) for record in message.records)
        record_sizes = tuple(self._byte_size(record) for record in message.records)
        batch = TelemetryInputBatch(
            twin_id=str(message.twin_id),
            subject_id=str(message.subject_id),
            source=str(message.source),
            connector_version=str(message.connector_version),
            idempotency_key=str(message.idempotency_key),
            expected_sequence=int(message.expected_sequence),
            records=records,
        )
        return batch, record_sizes, self._byte_size(message)

    def ingest_ack(self, ack: Any) -> Any:
        record_acks = [
            self.messages.TelemetryRecordAck(
                record_index=record.record_index,
                source_record_id=record.source_record_id,
                status=record.status.value,
                event_id=record.event_id or "",
                sequence=record.sequence or 0,
                error_code=record.error_code or "",
                retryable=record.retryable,
            )
            for record in ack.records
        ]
        rejected = [record for record in ack.records if record.error_code]
        errors = [
            self.messages.RecordError(
                record_index=record.record_index,
                code=record.error_code,
                message="record rejected",
                retryable=record.retryable,
            )
            for record in rejected
        ]
        successful = [record for record in ack.records if record.sequence is not None]
        sequences = [
            record.sequence
            for record in ack.records
            if record.sequence is not None
        ]
        event_ids = [record.event_id for record in successful if record.event_id]
        if rejected and successful:
            stream_status = "partial"
        elif rejected:
            stream_status = "rejected"
        else:
            stream_status = "committed"
        return self.messages.IngestAck(
            twin_id=ack.twin_id,
            idempotency_key=ack.idempotency_key,
            first_sequence=min(sequences) if sequences else 0,
            last_sequence=max(sequences) if sequences else 0,
            event_ids=event_ids,
            errors=errors,
            resume_token=ack.resume_token,
            record_acks=record_acks,
            committed_sequence=ack.committed_sequence,
            stream_status=stream_status,
        )

    def state_control(self, message: Any) -> Any:
        if self._byte_size(message) > self._max_control_frame_bytes:
            raise MessageTooLarge("gRPC state control frame exceeds its byte limit")
        kind = message.WhichOneof("frame")
        if kind == "subscribe":
            value = message.subscribe
            if int(value.after_sequence) != 0:
                raise ProtocolViolation("gRPC public streams require a resume token")
            max_in_flight = int(value.max_in_flight) or None
            token = str(value.resume_token) or None
            if token is not None and not 32 <= len(token) <= 2048:
                raise ProtocolViolation("gRPC resume token is invalid")
            event_types = tuple(str(item) for item in value.event_types)
            if len(event_types) > 64 or len(event_types) != len(set(event_types)):
                raise ProtocolViolation("gRPC event_types is invalid")
            if any(not 1 <= len(item) <= 128 for item in event_types):
                raise ProtocolViolation("gRPC event_types is invalid")
            return SubscribeControl(
                resume_token=token,
                event_types=event_types,
                include_projection_events=bool(value.include_projection_events),
                max_in_flight=max_in_flight,
            )
        if kind == "acknowledge":
            token = str(message.acknowledge.resume_token)
            if not 32 <= len(token) <= 2048:
                raise ProtocolViolation("gRPC acknowledgement token is invalid")
            return AcknowledgeControl(token)
        if kind == "heartbeat":
            value = message.heartbeat
            if not self._has_field(value, "sent_at"):
                raise ProtocolViolation("gRPC heartbeat timestamp is required")
            sent_at = self._timestamp(value.sent_at, required=True)
            if sent_at > datetime.now(UTC) + timedelta(minutes=5):
                raise ProtocolViolation("gRPC heartbeat is too far in the future")
            return HeartbeatControl(sent_at)
        if kind == "close":
            value = message.close
            code = int(value.code)
            reason = str(value.reason)
            if not 1000 <= code <= 4999 or len(reason) > 256:
                raise ProtocolViolation("gRPC close frame is invalid")
            return ClientCloseControl(code, reason)
        raise ProtocolViolation("gRPC state stream frame is missing")

    def state_frame(self, frame: StateChangeFrame | HeartbeatFrame | CloseFrame) -> Any:
        if isinstance(frame, StateChangeFrame):
            return self.messages.StateStreamFrame(
                event=self.messages.StateChange(
                    schema_version=frame.schema_version,
                    twin_id=frame.twin_id,
                    sequence=frame.sequence,
                    event_type=frame.event_type,
                    plane=frame.plane,
                    occurred_at=self._timestamp_factory(
                        datetime.fromisoformat(frame.occurred_at.replace("Z", "+00:00"))
                    ),
                    recorded_at=self._timestamp_factory(
                        datetime.fromisoformat(frame.recorded_at.replace("Z", "+00:00"))
                    ),
                    resume_token=frame.resume_token,
                )
            )
        if isinstance(frame, HeartbeatFrame):
            return self.messages.StateStreamFrame(
                heartbeat=self.messages.ServerHeartbeat(
                    twin_id=frame.twin_id,
                    acknowledged_sequence=frame.acknowledged_sequence,
                    resume_token=frame.resume_token,
                    sent_at=self._timestamp_factory(
                        datetime.fromisoformat(frame.sent_at.replace("Z", "+00:00"))
                    ),
                )
            )
        return self.messages.StateStreamFrame(
            close=self.messages.ServerClose(
                twin_id=frame.twin_id,
                code=frame.code,
                reason=frame.reason,
                acknowledged_sequence=frame.acknowledged_sequence,
                resume_token=frame.resume_token,
            )
        )

    def health(self, *, backlog: int) -> Any:
        return self.messages.HealthResponse(
            status="serving",
            version="0.5.0",
            event_store_lag=backlog,
            degraded_dependencies=[],
        )

    def _telemetry_record(self, record: Any) -> TelemetryInputRecord:
        if not self._has_field(record, "valid_from"):
            raise ProtocolViolation("telemetry valid_from is required")
        valid_until = (
            self._timestamp(record.valid_until, required=True)
            if self._has_field(record, "valid_until")
            else None
        )
        rights = (
            self._struct_decoder(record.rights)
            if self._has_field(record, "rights")
            else {}
        )
        return TelemetryInputRecord(
            source_record_id=str(record.source_record_id),
            valid_from=self._timestamp(record.valid_from, required=True),
            valid_until=valid_until,
            media_type=str(record.media_type),
            payload=bytes(record.payload),
            content_hash=str(record.content_hash),
            independence_group=str(record.independence_group),
            sensitivity=str(record.sensitivity),
            rights=rights,
        )

    @staticmethod
    def _timestamp(value: Any, *, required: bool) -> datetime:
        try:
            result = cast(datetime, value.ToDatetime(tzinfo=UTC))
        except Exception as exc:
            if required:
                raise ProtocolViolation("protobuf timestamp is invalid") from exc
            raise
        if result.tzinfo is None:
            raise ProtocolViolation("protobuf timestamp must be timezone-aware")
        return result.astimezone(UTC)

    @staticmethod
    def _has_field(message: Any, name: str) -> bool:
        try:
            return bool(message.HasField(name))
        except (AttributeError, ValueError):
            return getattr(message, name, None) is not None

    @staticmethod
    def _byte_size(message: Any) -> int:
        try:
            size = message.ByteSize()
        except Exception as exc:
            raise ProtocolViolation("protobuf message size is unavailable") from exc
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ProtocolViolation("protobuf message size is invalid")
        return size


class DigitalTwinGrpcAdapter:
    """Executable gRPC handlers for the two synchronization RPCs."""

    def __init__(
        self,
        *,
        service: DigitalTwinService,
        authenticator: Authenticator,
        codec: ProtobufCodec,
        status_codes: Any | None = None,
    ) -> None:
        if status_codes is None:
            try:
                import grpc
            except ImportError as exc:  # pragma: no cover - optional production extra
                raise RuntimeError("install the 'grpc' extra to run the gRPC adapter") from exc
            status_codes = grpc.StatusCode
        self._service = service
        self._authenticator = authenticator
        self._codec = codec
        self._status = status_codes
        self._telemetry = TelemetryBatchProcessor(service)

    async def StreamTelemetry(
        self,
        request_iterator: AsyncIterator[Any],
        context: Any,
    ) -> AsyncIterator[Any]:
        try:
            principal = await self._authenticate(context, scope="dt.ingest")
        except AuthorizationDenied:
            await self._abort(context, "PERMISSION_DENIED", "access denied")
            return
        async for message in request_iterator:
            try:
                batch, record_sizes, batch_size = self._codec.telemetry_batch(message)
                ack = await self._telemetry.process(
                    batch,
                    actor=principal.actor,
                    record_sizes=record_sizes,
                    serialized_batch_bytes=batch_size,
                )
            except MessageTooLarge:
                await self._abort(context, "RESOURCE_EXHAUSTED", "telemetry limit exceeded")
                return
            except AuthorizationDenied:
                await self._abort(context, "PERMISSION_DENIED", "access denied")
                return
            except ProtocolViolation:
                await self._abort(context, "INVALID_ARGUMENT", "invalid telemetry batch")
                return
            except InvariantViolation:
                await self._abort(context, "FAILED_PRECONDITION", "telemetry unavailable")
                return
            yield self._codec.ingest_ack(ack)

    async def SubscribeState(
        self,
        request_iterator: AsyncIterator[Any],
        context: Any,
    ) -> AsyncIterator[Any]:
        session = None
        try:
            principal = await self._authenticate(context, scope="dt.stream")
            try:
                first_message = await anext(request_iterator)
            except StopAsyncIteration as exc:
                raise ProtocolViolation("state stream requires a subscribe frame") from exc
            control = self._codec.state_control(first_message)
            if not isinstance(control, SubscribeControl):
                raise ProtocolViolation("first gRPC state frame must subscribe")
            session = await self._service.open_state_stream(
                str(first_message.subscribe.twin_id),
                actor=principal.actor,
                resume_token=control.resume_token,
                event_types=control.event_types,
                include_projection_events=control.include_projection_events,
                max_in_flight=control.max_in_flight,
            )
            receiver = asyncio.create_task(self._receive_controls(session, request_iterator))
            next_event = asyncio.create_task(session.__anext__())
            try:
                while True:
                    done, _ = await asyncio.wait(
                        {receiver, next_event},
                        timeout=self._service.sync_limits.heartbeat_seconds,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if receiver in done:
                        close = receiver.result()
                        if close is not None:
                            yield self._codec.state_frame(
                                await session.close(code=close.code, reason=close.reason)
                            )
                        return
                    if next_event in done:
                        try:
                            frame = next_event.result()
                        except StopAsyncIteration:
                            return
                        yield self._codec.state_frame(frame)
                        next_event = asyncio.create_task(session.__anext__())
                    else:
                        yield self._codec.state_frame(session.heartbeat())
            finally:
                for task in (receiver, next_event):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(receiver, next_event, return_exceptions=True)
        except AuthorizationDenied:
            await self._abort(context, "PERMISSION_DENIED", "access denied")
        except MessageTooLarge:
            await self._abort(context, "RESOURCE_EXHAUSTED", "state frame too large")
        except ProtocolViolation:
            await self._abort(context, "INVALID_ARGUMENT", "invalid state stream frame")
        except BackpressureExceeded:
            await self._abort(context, "RESOURCE_EXHAUSTED", "acknowledgement required")
        except IntegrityError:
            await self._abort(context, "DATA_LOSS", "state stream integrity failure")
        except InvariantViolation:
            await self._abort(context, "FAILED_PRECONDITION", "state stream unavailable")
        finally:
            if session is not None:
                await session.close()

    async def Health(self, _request: Any, _context: Any) -> Any:
        return self._codec.health(backlog=self._service.store.outbox_backlog())

    async def GetState(self, _request: Any, context: Any) -> Any:
        return await self._unimplemented(context)

    async def CompileProjection(self, _request: Any, context: Any) -> Any:
        return await self._unimplemented(context)

    async def RevokeProjection(self, _request: Any, context: Any) -> Any:
        return await self._unimplemented(context)

    async def ForkSimulation(self, _request: Any, context: Any) -> Any:
        return await self._unimplemented(context)

    async def AppendSimulation(self, _request: Any, context: Any) -> Any:
        return await self._unimplemented(context)

    async def ProposeSimulationPromotion(self, _request: Any, context: Any) -> Any:
        return await self._unimplemented(context)

    async def CheckAction(self, _request: Any, context: Any) -> Any:
        return await self._unimplemented(context)

    async def _receive_controls(
        self,
        session: Any,
        request_iterator: AsyncIterator[Any],
    ) -> ClientCloseControl | None:
        async for message in request_iterator:
            control = self._codec.state_control(message)
            if isinstance(control, SubscribeControl):
                raise ProtocolViolation("a gRPC stream can subscribe only once")
            if isinstance(control, AcknowledgeControl):
                session.acknowledge(control.resume_token)
            elif isinstance(control, ClientCloseControl):
                return control
        return None

    async def _authenticate(
        self,
        context: Any,
        *,
        scope: str,
    ) -> AuthenticatedPrincipal:
        metadata: dict[str, str] = {}
        for item in context.invocation_metadata():
            if hasattr(item, "key") and hasattr(item, "value"):
                name = str(item.key).lower()
                value = str(item.value)
            else:
                name = str(item[0]).lower()
                value = str(item[1])
            if name in metadata:
                raise AuthorizationDenied("duplicate gRPC metadata")
            metadata[name] = value
        peer_value = context.peer()
        principal = await self._authenticator.authenticate(
            TransportCredentials(
                transport="grpc",
                metadata=metadata,
                peer=str(peer_value) if peer_value else None,
            )
        )
        principal.require_scope(scope)
        return principal

    async def _abort(self, context: Any, status_name: str, reason: str) -> None:
        result = context.abort(getattr(self._status, status_name), reason)
        if inspect.isawaitable(result):
            await result

    async def _unimplemented(self, context: Any) -> Any:
        await self._abort(
            context,
            "UNIMPLEMENTED",
            "RPC belongs to a later host adapter package",
        )
        raise RuntimeError("gRPC context.abort returned unexpectedly")


def register_grpc_service(
    server: Any,
    adapter: DigitalTwinGrpcAdapter,
    *,
    add_servicer: Callable[[Any, Any], None] | None = None,
) -> None:
    """Register against generated code supplied by the host build."""

    if add_servicer is None:
        try:
            from argo.dt.v1.twin_pb2_grpc import add_DigitalTwinServicer_to_server
        except ImportError as exc:  # pragma: no cover - host generation boundary
            raise RuntimeError(
                "generate twin_pb2_grpc.py and place it on PYTHONPATH"
            ) from exc
        add_servicer = add_DigitalTwinServicer_to_server
    add_servicer(adapter, server)
