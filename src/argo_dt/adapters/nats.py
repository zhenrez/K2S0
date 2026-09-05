"""Optional NATS JetStream adapters; SQLite remains authoritative."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Protocol

from ..conformance import event_from_primitive
from ..errors import IntegrityError, MessageTooLarge, ProtocolViolation
from ..sync import NatsSubjectTopology
from ..types import EventEnvelope, canonical_json, to_primitive, utc_now


class JetStreamContext(Protocol):
    async def publish(
        self,
        subject: str,
        payload: bytes,
        *,
        timeout: float | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> object: ...

    async def subscribe(self, subject: str, **kwargs: Any) -> object: ...


class JetStreamMessage(Protocol):
    subject: str
    data: bytes
    metadata: object

    async def ack(self) -> None: ...

    async def nak(self, *, delay: float | None = None) -> None: ...

    async def term(self) -> None: ...


class RetryableConsumerError(Exception):
    """A derived consumer failure that may succeed on bounded redelivery."""


class PermanentConsumerError(Exception):
    """A derived consumer failure that must terminate without redelivery."""


class JetStreamPublisher:
    """Publish canonical events from the transactional outbox to JetStream."""

    def __init__(
        self,
        *,
        jetstream: JetStreamContext,
        tenant: str,
        timeout_seconds: float = 2.0,
        max_event_bytes: int = 2 * 1024 * 1024,
    ) -> None:
        if timeout_seconds <= 0 or max_event_bytes < 1024:
            raise ValueError("NATS publisher limits are invalid")
        self._jetstream = jetstream
        self._tenant = tenant
        self._timeout = timeout_seconds
        self._max_event_bytes = max_event_bytes

    async def publish(self, event: EventEnvelope) -> None:
        if event.sequence < 1 or not event.event_hash:
            raise IntegrityError("NATS publisher requires a persisted event")
        event.verify(event.previous_hash)
        payload = canonical_json(event).encode("utf-8")
        if len(payload) > self._max_event_bytes:
            raise MessageTooLarge("canonical NATS event exceeds its byte limit")
        await self._jetstream.publish(
            NatsSubjectTopology.event(tenant=self._tenant, event=event),
            payload,
            timeout=self._timeout,
            headers={
                "Content-Type": "application/json",
                "Argo-Schema": event.schema_version,
                "Nats-Msg-Id": event.event_id,
            },
        )


@dataclass(slots=True)
class NatsConsumerStats:
    received: int = 0
    acknowledged: int = 0
    retried: int = 0
    deadlettered: int = 0
    malformed: int = 0

    def snapshot(self) -> dict[str, int]:
        return {
            "received": self.received,
            "acknowledged": self.acknowledged,
            "retried": self.retried,
            "deadlettered": self.deadlettered,
            "malformed": self.malformed,
        }


class JetStreamConsumer:
    """Process a manually acknowledged message with bounded redelivery.

    The dead-letter lane contains only replay coordinates and a reason code.
    The canonical payload remains recoverable from SQLite and is not copied.
    """

    def __init__(
        self,
        *,
        jetstream: JetStreamContext,
        tenant: str,
        stage: str,
        handler: Callable[[EventEnvelope], Awaitable[None]],
        max_deliver: int = 5,
        retry_delay_seconds: float = 1.0,
        max_event_bytes: int = 2 * 1024 * 1024,
        stats: NatsConsumerStats | None = None,
    ) -> None:
        if max_deliver < 1 or retry_delay_seconds < 0 or max_event_bytes < 1024:
            raise ValueError("NATS consumer limits are invalid")
        NatsSubjectTopology.deadletter(
            tenant=tenant,
            twin_id="validation",
            stage=stage,
        )
        self._jetstream = jetstream
        self._tenant = tenant
        self._stage = stage
        self._handler = handler
        self._max_deliver = max_deliver
        self._retry_delay = retry_delay_seconds
        self._max_event_bytes = max_event_bytes
        self.stats = stats or NatsConsumerStats()

    async def bind(
        self,
        *,
        durable: str,
        stream: str,
        twin_id: str | None = None,
        plane: str | None = None,
        event_type: str | None = None,
        pending_messages: int = 1024,
        pending_bytes: int = 8 * 1024 * 1024,
    ) -> object:
        """Bind the callback to a pre-provisioned explicit-ack durable consumer."""

        if pending_messages < 1 or pending_bytes < 1024:
            raise ValueError("NATS pending limits are invalid")
        durable_name = NatsSubjectTopology.token(durable, name="durable")
        stream_name = NatsSubjectTopology.token(stream, name="stream")
        subject = NatsSubjectTopology.consumer_filter(
            tenant=self._tenant,
            twin_id=twin_id,
            plane=plane,
            event_type=event_type,
        )
        return await self._jetstream.subscribe(
            subject,
            durable=durable_name,
            stream=stream_name,
            cb=self.process,
            manual_ack=True,
            pending_msgs_limit=pending_messages,
            pending_bytes_limit=pending_bytes,
        )

    async def process(self, message: JetStreamMessage) -> None:
        self.stats.received += 1
        event: EventEnvelope | None = None
        try:
            event = self._decode(message.data)
            expected_subject = NatsSubjectTopology.event(
                tenant=self._tenant,
                event=event,
            )
            if message.subject != expected_subject:
                raise PermanentConsumerError("SUBJECT_EVENT_MISMATCH")
            await self._handler(event)
        except (PermanentConsumerError, ProtocolViolation, IntegrityError):
            self.stats.malformed += 1
            await self._deadletter(message, event, reason="INVALID_EVENT")
            return
        except Exception:
            if self._delivery_count(message) >= self._max_deliver:
                await self._deadletter(message, event, reason="MAX_DELIVER_EXCEEDED")
            else:
                await message.nak(delay=self._retry_delay)
                self.stats.retried += 1
            return
        await message.ack()
        self.stats.acknowledged += 1

    def _decode(self, payload: bytes) -> EventEnvelope:
        if len(payload) > self._max_event_bytes:
            raise ProtocolViolation("NATS event exceeds its byte limit")
        try:
            value = json.loads(payload.decode("utf-8"), object_pairs_hook=self._unique)
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise ProtocolViolation("NATS event is invalid JSON") from exc
        if not isinstance(value, dict):
            raise ProtocolViolation("NATS event must be an object")
        try:
            event = event_from_primitive(value)
        except Exception as exc:
            raise ProtocolViolation("NATS event envelope is invalid") from exc
        if event.schema_version != "argo.dt.event/v2":
            raise ProtocolViolation("NATS accepts only EventEnvelope v2")
        event.verify(event.previous_hash)
        return event

    async def _deadletter(
        self,
        message: JetStreamMessage,
        event: EventEnvelope | None,
        *,
        reason: str,
    ) -> None:
        twin_id = event.twin_id if event is not None else "unknown"
        marker = {
            "schema_version": "argo.dt.deadletter/v1",
            "twin_id": twin_id,
            "sequence": event.sequence if event is not None else 0,
            "stage": self._stage,
            "reason": reason,
            "delivery_count": self._delivery_count(message),
            "observed_at": to_primitive(utc_now()),
        }
        await self._jetstream.publish(
            NatsSubjectTopology.deadletter(
                tenant=self._tenant,
                twin_id=twin_id,
                stage=self._stage,
            ),
            canonical_json(marker).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        await message.term()
        self.stats.deadlettered += 1

    @staticmethod
    def _delivery_count(message: JetStreamMessage) -> int:
        value = getattr(message.metadata, "num_delivered", 1)
        valid = isinstance(value, int) and not isinstance(value, bool) and value > 0
        return value if valid else 1

    @staticmethod
    def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate JSON key")
            value[key] = item
        return value
