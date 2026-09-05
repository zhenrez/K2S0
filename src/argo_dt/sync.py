"""Durable, bounded synchronization primitives for transport adapters.

The module deliberately contains no WebSocket, gRPC, NATS, or OpenTelemetry
runtime dependency. It implements the semantics those adapters must preserve:
authenticated cursors, bounded replay/live handoff, explicit acknowledgements,
payload-minimized state frames, byte budgets, and observable counters.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import re
import secrets
import uuid
from collections import deque
from collections.abc import AsyncIterator, Callable, Collection, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol, cast

from .errors import (
    BackpressureExceeded,
    IntegrityError,
    MessageTooLarge,
    ProtocolViolation,
    ResumeCursorRejected,
)
from .ports import EventStore, OutboxStore, TelemetryPublisher
from .types import EventEnvelope, EventPlane, canonical_json, parse_time, to_primitive, utc_now

_CLOSED = object()
_CURSOR_SCHEMA = "argo.dt.cursor/v1"
_STREAM_SCHEMA = "argo.dt.state-stream/v1"
_HASH_PATTERN = re.compile(r"sha256:[a-f0-9]{64}\Z")
_SUBJECT_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    if not value or "=" in value:
        raise ResumeCursorRejected("resume cursor encoding is invalid")
    try:
        padding = "=" * (-len(value) % 4)
        decoded = base64.b64decode(value + padding, altchars=b"-_", validate=True)
    except (ValueError, UnicodeEncodeError) as exc:
        raise ResumeCursorRejected("resume cursor encoding is invalid") from exc
    if _b64url_encode(decoded) != value:
        raise ResumeCursorRejected("resume cursor encoding is not canonical")
    return decoded


@dataclass(frozen=True, slots=True)
class StreamPosition:
    """One verified position in a twin's ordered event chain."""

    twin_id: str
    sequence: int
    event_hash: str

    def __post_init__(self) -> None:
        if not self.twin_id:
            raise ResumeCursorRejected("resume cursor twin_id is required")
        if self.sequence < 0:
            raise ResumeCursorRejected("resume cursor sequence cannot be negative")
        if self.sequence == 0 and self.event_hash:
            raise ResumeCursorRejected("stream origin cannot carry an event hash")
        if self.sequence > 0 and not _HASH_PATTERN.fullmatch(self.event_hash):
            raise ResumeCursorRejected("resume cursor event hash is invalid")


@dataclass(frozen=True, slots=True)
class DecodedCursor:
    twin_id: str
    sequence: int
    chain_tag: str
    issued_at: datetime
    signing_key_id: str = "default"


class CursorCodec(Protocol):
    """Common cursor contract implemented by single-key and rotating signers."""

    def issue(self, position: StreamPosition) -> str: ...

    def verify(self, token: str, *, twin_id: str) -> DecodedCursor: ...

    def verify_chain_binding(
        self,
        cursor: DecodedCursor,
        position: StreamPosition,
    ) -> StreamPosition: ...


class CursorSigner:
    """Issue and verify opaque, HMAC-SHA256 authenticated resume cursors."""

    def __init__(
        self,
        key: bytes,
        *,
        key_id: str = "default",
        max_age: timedelta | None = timedelta(days=7),
        clock: Callable[[], datetime] = utc_now,
        allowed_clock_skew: timedelta = timedelta(minutes=1),
    ) -> None:
        if len(key) < 32:
            raise ValueError("cursor signing key must contain at least 32 bytes")
        if not _SUBJECT_TOKEN_PATTERN.fullmatch(key_id):
            raise ValueError("cursor signing key_id must be a URL-safe token")
        if max_age is not None and max_age <= timedelta(0):
            raise ValueError("cursor max_age must be positive")
        if allowed_clock_skew < timedelta(0):
            raise ValueError("allowed_clock_skew cannot be negative")
        self._key = bytes(key)
        self.key_id = key_id
        self._max_age = max_age
        self._clock = clock
        self._allowed_clock_skew = allowed_clock_skew

    @classmethod
    def ephemeral(cls, *, key_id: str = "ephemeral") -> CursorSigner:
        """Create a process-local signer; production adapters inject durable key material."""

        return cls(secrets.token_bytes(32), key_id=key_id)

    def issue(self, position: StreamPosition) -> str:
        material = {
            "schema_version": _CURSOR_SCHEMA,
            "twin_id": position.twin_id,
            "sequence": position.sequence,
            "chain_tag": self._chain_tag(position),
            "issued_at": to_primitive(self._clock()),
        }
        payload = canonical_json(material).encode("utf-8")
        signature = hmac.new(self._key, payload, hashlib.sha256).digest()
        return f"{_b64url_encode(payload)}.{_b64url_encode(signature)}"

    def verify(self, token: str, *, twin_id: str) -> DecodedCursor:
        parts = token.split(".")
        if len(parts) != 2:
            raise ResumeCursorRejected("resume cursor structure is invalid")
        payload = _b64url_decode(parts[0])
        supplied_signature = _b64url_decode(parts[1])
        expected_signature = hmac.new(self._key, payload, hashlib.sha256).digest()
        if not hmac.compare_digest(supplied_signature, expected_signature):
            raise ResumeCursorRejected("resume cursor signature is invalid")
        try:
            material = json.loads(payload)
        except (UnicodeDecodeError, ValueError) as exc:
            raise ResumeCursorRejected("resume cursor payload is invalid") from exc
        expected_keys = {
            "schema_version",
            "twin_id",
            "sequence",
            "chain_tag",
            "issued_at",
        }
        if not isinstance(material, dict) or set(material) != expected_keys:
            raise ResumeCursorRejected("resume cursor payload shape is invalid")
        if material["schema_version"] != _CURSOR_SCHEMA:
            raise ResumeCursorRejected("resume cursor version is unsupported")
        if material["twin_id"] != twin_id:
            raise ResumeCursorRejected("resume cursor is bound to another twin")
        if isinstance(material["sequence"], bool) or not isinstance(
            material["sequence"], int
        ):
            raise ResumeCursorRejected("resume cursor sequence is invalid")
        if not isinstance(material["chain_tag"], str):
            raise ResumeCursorRejected("resume cursor chain tag is invalid")
        if not isinstance(material["issued_at"], str):
            raise ResumeCursorRejected("resume cursor issued_at is invalid")
        try:
            issued_at = parse_time(material["issued_at"])
        except Exception as exc:
            raise ResumeCursorRejected("resume cursor issued_at is invalid") from exc
        now = self._clock().astimezone(UTC)
        if issued_at > now + self._allowed_clock_skew:
            raise ResumeCursorRejected("resume cursor was issued in the future")
        if self._max_age is not None and now - issued_at > self._max_age:
            raise ResumeCursorRejected("resume cursor has expired")
        return DecodedCursor(
            twin_id=twin_id,
            sequence=material["sequence"],
            chain_tag=material["chain_tag"],
            issued_at=issued_at,
            signing_key_id=self.key_id,
        )

    def matches_signature(self, token: str) -> bool:
        """Check only the MAC so a bounded key ring can select the signer."""

        parts = token.split(".")
        if len(parts) != 2:
            return False
        try:
            payload = _b64url_decode(parts[0])
            supplied_signature = _b64url_decode(parts[1])
        except ResumeCursorRejected:
            return False
        expected_signature = hmac.new(self._key, payload, hashlib.sha256).digest()
        return hmac.compare_digest(supplied_signature, expected_signature)

    def verify_chain_binding(
        self,
        cursor: DecodedCursor,
        position: StreamPosition,
    ) -> StreamPosition:
        if cursor.twin_id != position.twin_id or cursor.sequence != position.sequence:
            raise ResumeCursorRejected("resume cursor position does not match the stream")
        if not hmac.compare_digest(cursor.chain_tag, self._chain_tag(position)):
            raise ResumeCursorRejected(
                "resume cursor does not match the authoritative event chain"
            )
        return position

    def _chain_tag(self, position: StreamPosition) -> str:
        material = canonical_json(
            {
                "twin_id": position.twin_id,
                "sequence": position.sequence,
                "event_hash": position.event_hash,
            }
        ).encode("utf-8")
        return _b64url_encode(hmac.new(self._key, material, hashlib.sha256).digest())


class RotatingCursorSigner:
    """Issue with one active key while accepting a bounded overlap key set.

    The v1 cursor wire shape remains unchanged. Verification first identifies
    the signing key by its MAC, then applies that key's age and chain checks.
    Removing a key immediately revokes every cursor it signed.
    """

    MAX_KEYS = 8

    def __init__(
        self,
        active: CursorSigner,
        *,
        retiring: Sequence[CursorSigner] = (),
    ) -> None:
        signers = (active, *retiring)
        if len(signers) > self.MAX_KEYS:
            raise ValueError(f"cursor key ring cannot exceed {self.MAX_KEYS} keys")
        key_ids = [signer.key_id for signer in signers]
        if len(key_ids) != len(set(key_ids)):
            raise ValueError("cursor key IDs must be unique")
        self._active = active
        self._by_id = {signer.key_id: signer for signer in signers}
        self._verification_order = signers

    @property
    def active_key_id(self) -> str:
        return self._active.key_id

    @property
    def accepted_key_ids(self) -> tuple[str, ...]:
        return tuple(signer.key_id for signer in self._verification_order)

    def issue(self, position: StreamPosition) -> str:
        return self._active.issue(position)

    def verify(self, token: str, *, twin_id: str) -> DecodedCursor:
        for signer in self._verification_order:
            if signer.matches_signature(token):
                return signer.verify(token, twin_id=twin_id)
        raise ResumeCursorRejected("resume cursor signature is invalid")

    def verify_chain_binding(
        self,
        cursor: DecodedCursor,
        position: StreamPosition,
    ) -> StreamPosition:
        signer = self._by_id.get(cursor.signing_key_id)
        if signer is None:
            raise ResumeCursorRejected("resume cursor signing key is no longer accepted")
        return signer.verify_chain_binding(cursor, position)

    def rotate(
        self,
        new_active: CursorSigner,
        *,
        retain: int = 1,
    ) -> RotatingCursorSigner:
        """Return a new key ring; callers atomically swap the configured ring."""

        if retain < 0 or retain >= self.MAX_KEYS:
            raise ValueError(f"retain must be in [0, {self.MAX_KEYS - 1}]")
        previous = [
            signer
            for signer in self._verification_order
            if signer.key_id != new_active.key_id
        ]
        return RotatingCursorSigner(new_active, retiring=previous[:retain])


@dataclass(frozen=True, slots=True)
class SyncLimits:
    """Server-side ceilings; adapters may negotiate only stricter values."""

    max_record_bytes: int = 1 * 1024 * 1024
    max_batch_bytes: int = 4 * 1024 * 1024
    max_batch_records: int = 500
    replay_page_size: int = 256
    max_in_flight: int = 128
    heartbeat_seconds: int = 30

    def __post_init__(self) -> None:
        for name in (
            "max_record_bytes",
            "max_batch_bytes",
            "max_batch_records",
            "replay_page_size",
            "max_in_flight",
            "heartbeat_seconds",
        ):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")
        if self.max_record_bytes > self.max_batch_bytes:
            raise ValueError("max_record_bytes cannot exceed max_batch_bytes")

    def validate_batch(
        self,
        *,
        record_sizes: Sequence[int],
        serialized_batch_bytes: int,
    ) -> None:
        """Validate exact sizes measured by the concrete transport serializer."""

        if not record_sizes:
            raise ProtocolViolation("telemetry batch must contain at least one record")
        if len(record_sizes) > self.max_batch_records:
            raise MessageTooLarge("telemetry batch exceeds the record-count limit")
        if isinstance(serialized_batch_bytes, bool) or not isinstance(
            serialized_batch_bytes, int
        ):
            raise ProtocolViolation("serialized batch size must be an integer")
        if any(isinstance(size, bool) or not isinstance(size, int) for size in record_sizes):
            raise ProtocolViolation("serialized record sizes must be integers")
        if serialized_batch_bytes < 0 or any(size < 0 for size in record_sizes):
            raise ProtocolViolation("serialized byte sizes cannot be negative")
        if serialized_batch_bytes < sum(record_sizes):
            raise ProtocolViolation("batch size cannot be smaller than its records")
        if any(size > self.max_record_bytes for size in record_sizes):
            raise MessageTooLarge("telemetry record exceeds the byte limit")
        if serialized_batch_bytes > self.max_batch_bytes:
            raise MessageTooLarge("telemetry batch exceeds the byte limit")

    def negotiate(self, *, max_in_flight: int | None = None) -> SyncLimits:
        """Apply a client-requested limit only when it is at least as strict."""

        if max_in_flight is None or max_in_flight == 0:
            return self
        if isinstance(max_in_flight, bool) or not isinstance(max_in_flight, int):
            raise ProtocolViolation("max_in_flight must be an integer")
        if max_in_flight < 1 or max_in_flight > self.max_in_flight:
            raise ProtocolViolation("max_in_flight exceeds the server limit")
        return replace(self, max_in_flight=max_in_flight)


class TelemetryRecordStatus(StrEnum):
    COMMITTED = "committed"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class TelemetryRecordAck:
    record_index: int
    source_record_id: str
    status: TelemetryRecordStatus
    event_id: str | None = None
    sequence: int | None = None
    error_code: str | None = None
    retryable: bool = False

    def __post_init__(self) -> None:
        if self.record_index < 0 or not self.source_record_id:
            raise ProtocolViolation("record acknowledgement identity is invalid")
        if not isinstance(self.status, TelemetryRecordStatus):
            raise ProtocolViolation("record acknowledgement status is invalid")
        if self.status in {
            TelemetryRecordStatus.COMMITTED,
            TelemetryRecordStatus.DUPLICATE,
        }:
            if not self.event_id or self.sequence is None or self.sequence < 1:
                raise ProtocolViolation("committed record acknowledgement lacks position")
            if self.error_code is not None:
                raise ProtocolViolation("successful acknowledgement cannot carry an error")
        elif not self.error_code:
            raise ProtocolViolation("rejected record acknowledgement requires an error code")


@dataclass(frozen=True, slots=True)
class TelemetryStreamAck:
    twin_id: str
    idempotency_key: str
    committed_sequence: int
    records: tuple[TelemetryRecordAck, ...]
    resume_token: str

    def __post_init__(self) -> None:
        if not self.twin_id or not self.idempotency_key or not self.resume_token:
            raise ProtocolViolation("stream acknowledgement identity is incomplete")
        if self.committed_sequence < 0:
            raise ProtocolViolation("committed sequence cannot be negative")
        indices = [record.record_index for record in self.records]
        if len(indices) != len(set(indices)):
            raise ProtocolViolation("record acknowledgement indices must be unique")


@dataclass(eq=False, slots=True)
class Subscription:
    """Bounded live-broker subscription used beneath durable replay."""

    twin_id: str
    after_sequence: int
    queue: asyncio.Queue[EventEnvelope | object]
    _closed: bool = False
    _close_reason: str | None = None

    def __aiter__(self) -> AsyncIterator[EventEnvelope]:
        return self

    async def __anext__(self) -> EventEnvelope:
        if self._closed:
            if self._close_reason:
                raise BackpressureExceeded(self._close_reason)
            raise StopAsyncIteration
        item = await self.queue.get()
        if item is _CLOSED:
            self._closed = True
            if self._close_reason:
                raise BackpressureExceeded(self._close_reason)
            raise StopAsyncIteration
        assert isinstance(item, EventEnvelope)
        self.after_sequence = item.sequence
        return item

    def close(self, reason: str | None = None) -> None:
        if self._closed:
            return
        self._closed = True
        self._close_reason = reason
        if self.queue.full():
            self.queue.get_nowait()
        self.queue.put_nowait(_CLOSED)


@dataclass(slots=True)
class BrokerStats:
    published: int = 0
    delivered: int = 0
    disconnected_slow_consumers: int = 0


@dataclass(slots=True)
class SyncMetrics:
    replayed_events: int = 0
    live_events: int = 0
    duplicate_events: int = 0
    emitted_frames: int = 0
    acknowledgements: int = 0
    heartbeats: int = 0
    closed_streams: int = 0
    integrity_failures: int = 0
    backpressure_closes: int = 0

    def snapshot(self) -> dict[str, int]:
        """Return redaction-safe values suitable for an OpenTelemetry adapter."""

        return {
            "replayed_events": self.replayed_events,
            "live_events": self.live_events,
            "duplicate_events": self.duplicate_events,
            "emitted_frames": self.emitted_frames,
            "acknowledgements": self.acknowledgements,
            "heartbeats": self.heartbeats,
            "closed_streams": self.closed_streams,
            "integrity_failures": self.integrity_failures,
            "backpressure_closes": self.backpressure_closes,
        }


@dataclass(frozen=True, slots=True)
class RelayBatch:
    claimed: int
    published: int
    failed: int


class OutboxRelay:
    """At-least-once relay; consumers deduplicate by twin ID and sequence."""

    def __init__(
        self,
        *,
        store: OutboxStore,
        publisher: TelemetryPublisher,
        lease_owner: str | None = None,
        lease_seconds: int = 30,
    ) -> None:
        self.store = store
        self.publisher = publisher
        self.lease_owner = lease_owner or f"relay-{uuid.uuid4()}"
        self.lease_seconds = lease_seconds

    async def drain(self, *, limit: int = 100) -> RelayBatch:
        records = self.store.claim_outbox(
            lease_owner=self.lease_owner,
            limit=limit,
            lease_seconds=self.lease_seconds,
        )
        published = 0
        failed = 0
        for record in records:
            try:
                await self.publisher.publish(record.event)
            except Exception as exc:  # publisher boundaries are intentionally isolated
                if not self.store.release_outbox(record, error=type(exc).__name__):
                    raise IntegrityError("failed publication lost its outbox lease") from exc
                failed += 1
                continue
            if not self.store.mark_outbox_published(record):
                raise IntegrityError("published event lost its outbox lease")
            published += 1
            # In-process publishers can complete without suspending. Yield once
            # per record so a backlog cannot starve stream readers on the loop.
            await asyncio.sleep(0)
        return RelayBatch(len(records), published, failed)


class BoundedEventBroker:
    """In-process fan-out with explicit slow-consumer disconnection."""

    def __init__(self, queue_capacity: int = 1024) -> None:
        if queue_capacity < 1:
            raise ValueError("queue_capacity must be positive")
        self._capacity = queue_capacity
        self._subscriptions: set[Subscription] = set()
        self._lock = asyncio.Lock()
        self.stats = BrokerStats()

    @property
    def subscription_count(self) -> int:
        return len(self._subscriptions)

    async def subscribe(self, twin_id: str, *, after_sequence: int = 0) -> Subscription:
        subscription = Subscription(
            twin_id=twin_id,
            after_sequence=after_sequence,
            queue=asyncio.Queue(maxsize=self._capacity),
        )
        async with self._lock:
            self._subscriptions.add(subscription)
        return subscription

    async def unsubscribe(self, subscription: Subscription) -> None:
        async with self._lock:
            self._subscriptions.discard(subscription)
        subscription.close()

    async def publish(self, event: EventEnvelope) -> None:
        async with self._lock:
            self.stats.published += 1
            stale: list[Subscription] = []
            for subscription in self._subscriptions:
                if subscription.twin_id != event.twin_id:
                    continue
                if event.sequence <= subscription.after_sequence:
                    continue
                try:
                    subscription.queue.put_nowait(event)
                    self.stats.delivered += 1
                except asyncio.QueueFull:
                    stale.append(subscription)
            for subscription in stale:
                self._subscriptions.discard(subscription)
                subscription.close(
                    "subscriber exceeded bounded queue; reconnect with last cursor"
                )
                self.stats.disconnected_slow_consumers += 1


class DurableSubscription:
    """Bounded snapshot-at-head replay followed by a deduplicated live handoff."""

    def __init__(
        self,
        *,
        store: EventStore,
        broker: BoundedEventBroker,
        live: Subscription,
        start: StreamPosition,
        replay_head: int,
        limits: SyncLimits,
        event_types: Collection[str] | None,
        planes: Collection[EventPlane] | None,
        metrics: SyncMetrics,
    ) -> None:
        self._store = store
        self._broker = broker
        self._live = live
        self._position = start
        self._acknowledged = start
        self._replay_head = replay_head
        self._limits = limits
        self._event_types = frozenset(event_types) if event_types else None
        self._planes = frozenset(planes) if planes else None
        self._metrics = metrics
        self._replay_buffer: deque[EventEnvelope] = deque()
        self._outstanding: deque[StreamPosition] = deque()
        self._deferred_live: EventEnvelope | None = None
        self._closed = False

    @classmethod
    async def open(
        cls,
        *,
        store: EventStore,
        broker: BoundedEventBroker,
        start: StreamPosition,
        limits: SyncLimits,
        event_types: Collection[str] | None = None,
        planes: Collection[EventPlane] | None = None,
        metrics: SyncMetrics | None = None,
    ) -> DurableSubscription:
        # Subscribe before reading the head. Events committed across this handoff
        # are either replayed, queued live, or both; sequence dedup handles both.
        live = await broker.subscribe(start.twin_id, after_sequence=start.sequence)
        try:
            replay_head, _ = store.head(start.twin_id)
            if start.sequence > replay_head:
                raise ResumeCursorRejected("resume cursor is beyond the stream head")
            if start.sequence > 0:
                event = store.load(
                    start.twin_id,
                    after_sequence=start.sequence - 1,
                    up_to_sequence=start.sequence,
                    limit=1,
                )
                if not event or event[0].event_hash != start.event_hash:
                    raise ResumeCursorRejected(
                        "resume cursor does not match the authoritative event chain"
                    )
            return cls(
                store=store,
                broker=broker,
                live=live,
                start=start,
                replay_head=replay_head,
                limits=limits,
                event_types=event_types,
                planes=planes,
                metrics=metrics or SyncMetrics(),
            )
        except Exception:
            await broker.unsubscribe(live)
            raise

    @property
    def queue(self) -> asyncio.Queue[EventEnvelope | object]:
        """Expose the bounded live queue for operational inspection."""

        return self._live.queue

    @property
    def position(self) -> StreamPosition:
        return self._position

    @property
    def acknowledged_position(self) -> StreamPosition:
        return self._acknowledged

    @property
    def outstanding_count(self) -> int:
        return len(self._outstanding)

    def __aiter__(self) -> AsyncIterator[EventEnvelope]:
        return self

    async def __aenter__(self) -> DurableSubscription:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def __anext__(self) -> EventEnvelope:
        if self._closed:
            raise StopAsyncIteration
        if len(self._outstanding) >= self._limits.max_in_flight:
            self._metrics.backpressure_closes += 1
            await self.aclose()
            raise BackpressureExceeded(
                "unacknowledged delivery window exceeded; reconnect with last cursor"
            )
        while True:
            try:
                event, source = await self._next_candidate()
            except IntegrityError:
                await self.aclose()
                raise
            if self._event_types is not None and event.event_type not in self._event_types:
                self._advance_ack_over_filtered_event()
                continue
            if self._planes is not None and event.plane not in self._planes:
                self._advance_ack_over_filtered_event()
                continue
            position = StreamPosition(event.twin_id, event.sequence, event.event_hash)
            self._outstanding.append(position)
            if source == "replay":
                self._metrics.replayed_events += 1
            else:
                self._metrics.live_events += 1
            return event

    def _advance_ack_over_filtered_event(self) -> None:
        # A filter is part of the subscription contract. If no client-visible
        # frame remains outstanding, excluded events are safe to accept locally.
        if not self._outstanding:
            self._acknowledged = self._position

    async def _next_candidate(self) -> tuple[EventEnvelope, str]:
        while True:
            if self._replay_buffer:
                event = self._replay_buffer.popleft()
                self._advance_verified(event)
                return event, "replay"
            if self._position.sequence < self._replay_head:
                batch = self._store.load(
                    self._position.twin_id,
                    after_sequence=self._position.sequence,
                    up_to_sequence=self._replay_head,
                    limit=self._limits.replay_page_size,
                )
                if not batch:
                    self._metrics.integrity_failures += 1
                    raise IntegrityError("event replay ended before the captured stream head")
                self._replay_buffer.extend(batch)
                continue
            if self._deferred_live is not None:
                event = self._deferred_live
                self._deferred_live = None
            else:
                try:
                    event = await self._live.__anext__()
                except BackpressureExceeded:
                    self._closed = True
                    self._metrics.backpressure_closes += 1
                    raise
            if event.sequence <= self._position.sequence:
                self._metrics.duplicate_events += 1
                continue
            if event.sequence > self._position.sequence + 1:
                # Recover a publication gap from SQLite without growing memory.
                persisted_head, _ = self._store.head(self._position.twin_id)
                if persisted_head < event.sequence:
                    self._metrics.integrity_failures += 1
                    raise IntegrityError("live stream contains a non-persisted sequence gap")
                self._deferred_live = event
                self._replay_head = event.sequence
                continue
            self._advance_verified(event)
            return event, "live"

    def _advance_verified(self, event: EventEnvelope) -> None:
        expected = self._position.sequence + 1
        if event.twin_id != self._position.twin_id or event.sequence != expected:
            self._metrics.integrity_failures += 1
            raise IntegrityError(
                f"stream continuity failed: expected sequence {expected}, "
                f"found {event.sequence}"
            )
        try:
            event.verify(self._position.event_hash)
        except IntegrityError:
            self._metrics.integrity_failures += 1
            raise
        self._position = StreamPosition(event.twin_id, event.sequence, event.event_hash)

    def acknowledge(self, position: StreamPosition) -> StreamPosition:
        if position.twin_id != self._position.twin_id:
            raise ProtocolViolation("acknowledgement is bound to another twin")
        if position.sequence < self._acknowledged.sequence:
            raise ProtocolViolation("acknowledgement sequence cannot regress")
        if position.sequence == self._acknowledged.sequence:
            if position.event_hash != self._acknowledged.event_hash:
                raise ProtocolViolation("acknowledgement hash does not match its sequence")
            return self._acknowledged
        matched = next(
            (item for item in self._outstanding if item.sequence == position.sequence),
            None,
        )
        if matched is None or matched.event_hash != position.event_hash:
            raise ProtocolViolation("acknowledgement was not issued by this stream")
        while self._outstanding and self._outstanding[0].sequence <= position.sequence:
            self._outstanding.popleft()
        self._acknowledged = position
        return position

    def acknowledgement_candidate(self, sequence: int) -> StreamPosition:
        if sequence == self._acknowledged.sequence:
            return self._acknowledged
        matched = next(
            (item for item in self._outstanding if item.sequence == sequence),
            None,
        )
        if matched is None:
            raise ProtocolViolation("acknowledgement was not issued by this stream")
        return matched

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._broker.unsubscribe(self._live)
        self._metrics.closed_streams += 1


@dataclass(frozen=True, slots=True)
class StateChangeFrame:
    twin_id: str
    sequence: int
    event_type: str
    plane: str
    occurred_at: str
    recorded_at: str
    resume_token: str
    type: str = "event"
    schema_version: str = _STREAM_SCHEMA

    @classmethod
    def from_event(cls, event: EventEnvelope, *, resume_token: str) -> StateChangeFrame:
        return cls(
            twin_id=event.twin_id,
            sequence=event.sequence,
            event_type=event.event_type,
            plane=event.plane.value,
            occurred_at=str(to_primitive(event.occurred_at)),
            recorded_at=str(to_primitive(event.recorded_at)),
            resume_token=resume_token,
        )

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], to_primitive(self))


@dataclass(frozen=True, slots=True)
class HeartbeatFrame:
    twin_id: str
    acknowledged_sequence: int
    resume_token: str
    sent_at: str
    type: str = "heartbeat"
    schema_version: str = _STREAM_SCHEMA

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], to_primitive(self))


@dataclass(frozen=True, slots=True)
class CloseFrame:
    twin_id: str
    code: int
    reason: str
    acknowledged_sequence: int
    resume_token: str
    type: str = "close"
    schema_version: str = _STREAM_SCHEMA

    def __post_init__(self) -> None:
        if not 1000 <= self.code <= 4999:
            raise ProtocolViolation("stream close code must be in [1000, 4999]")
        if not self.twin_id or not self.resume_token:
            raise ProtocolViolation("stream close identity is incomplete")
        if len(self.reason) > 256:
            raise ProtocolViolation("stream close reason exceeds 256 characters")

    def to_dict(self) -> dict[str, object]:
        return cast(dict[str, object], to_primitive(self))


class StateStreamSession:
    """Transport-neutral state notification session with explicit client acks."""

    def __init__(
        self,
        *,
        subscription: DurableSubscription,
        cursor_signer: CursorCodec,
        metrics: SyncMetrics,
    ) -> None:
        self._subscription = subscription
        self._cursor_signer = cursor_signer
        self._metrics = metrics

    def __aiter__(self) -> AsyncIterator[StateChangeFrame]:
        return self

    async def __aenter__(self) -> StateStreamSession:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self._subscription.aclose()

    async def __anext__(self) -> StateChangeFrame:
        event = await self._subscription.__anext__()
        position = StreamPosition(event.twin_id, event.sequence, event.event_hash)
        self._metrics.emitted_frames += 1
        return StateChangeFrame.from_event(
            event,
            resume_token=self._cursor_signer.issue(position),
        )

    @property
    def outstanding_count(self) -> int:
        return self._subscription.outstanding_count

    @property
    def acknowledged_sequence(self) -> int:
        return self._subscription.acknowledged_position.sequence

    def acknowledge(self, resume_token: str) -> StreamPosition:
        decoded = self._cursor_signer.verify(
            resume_token,
            twin_id=self._subscription.position.twin_id,
        )
        candidate = self._subscription.acknowledgement_candidate(decoded.sequence)
        position = self._subscription.acknowledge(
            self._cursor_signer.verify_chain_binding(decoded, candidate)
        )
        self._metrics.acknowledgements += 1
        return position

    def heartbeat(self) -> HeartbeatFrame:
        position = self._subscription.acknowledged_position
        self._metrics.heartbeats += 1
        return HeartbeatFrame(
            twin_id=position.twin_id,
            acknowledged_sequence=position.sequence,
            resume_token=self._cursor_signer.issue(position),
            sent_at=str(to_primitive(utc_now())),
        )

    async def close(self, *, code: int = 1000, reason: str = "normal") -> CloseFrame:
        position = self._subscription.acknowledged_position
        frame = CloseFrame(
            twin_id=position.twin_id,
            code=code,
            reason=reason,
            acknowledged_sequence=position.sequence,
            resume_token=self._cursor_signer.issue(position),
        )
        await self._subscription.aclose()
        return frame


class NatsSubjectTopology:
    """Validated subject construction; routing metadata never grants access."""

    @staticmethod
    def token(value: str, *, name: str) -> str:
        if not _SUBJECT_TOKEN_PATTERN.fullmatch(value):
            raise ProtocolViolation(
                f"{name} must be 1-64 URL-safe characters without NATS wildcards"
            )
        return value

    @classmethod
    def event(cls, *, tenant: str, event: EventEnvelope) -> str:
        subject = ".".join(
            (
                "argo",
                "dt",
                cls.token(tenant, name="tenant"),
                cls.token(event.twin_id, name="twin_id"),
                cls.token(event.plane.value, name="plane"),
                cls.token(event.event_type, name="event_type"),
            )
        )
        if len(subject.encode("utf-8")) > 255:
            raise ProtocolViolation("NATS subject exceeds the 255-byte project limit")
        return subject

    @classmethod
    def deadletter(cls, *, tenant: str, twin_id: str, stage: str) -> str:
        subject = ".".join(
            (
                "argo",
                "dt",
                cls.token(tenant, name="tenant"),
                cls.token(twin_id, name="twin_id"),
                "deadletter",
                cls.token(stage, name="stage"),
            )
        )
        if len(subject.encode("utf-8")) > 255:
            raise ProtocolViolation("NATS dead-letter subject exceeds project limit")
        return subject

    @classmethod
    def consumer_filter(
        cls,
        *,
        tenant: str,
        twin_id: str | None = None,
        plane: str | None = None,
        event_type: str | None = None,
    ) -> str:
        """Build a single-token wildcard filter without accepting raw wildcards."""

        tokens = (
            "argo",
            "dt",
            cls.token(tenant, name="tenant"),
            cls.token(twin_id, name="twin_id") if twin_id is not None else "*",
            cls.token(plane, name="plane") if plane is not None else "*",
            cls.token(event_type, name="event_type")
            if event_type is not None
            else "*",
        )
        subject = ".".join(tokens)
        if len(subject.encode("utf-8")) > 255:
            raise ProtocolViolation("NATS consumer filter exceeds project limit")
        return subject
