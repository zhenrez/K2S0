"""Bounded in-process reference broker for low-latency state/event delivery."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import AsyncIterator

from .errors import BackpressureExceeded, IntegrityError
from .ports import OutboxStore, TelemetryPublisher
from .types import EventEnvelope

_CLOSED = object()


@dataclass(eq=False, slots=True)
class Subscription:
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
        return RelayBatch(len(records), published, failed)


class BoundedEventBroker:
    """Fan-out with explicit slow-consumer disconnection.

    Production WebSocket adapters should resume from the subscriber's last
    sequence through EventStore.load rather than buffering without bounds.
    """

    def __init__(self, queue_capacity: int = 1024) -> None:
        if queue_capacity < 1:
            raise ValueError("queue_capacity must be positive")
        self._capacity = queue_capacity
        self._subscriptions: set[Subscription] = set()
        self._lock = asyncio.Lock()
        self.stats = BrokerStats()

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
                    "subscriber exceeded bounded queue; reconnect with last sequence"
                )
                self.stats.disconnected_slow_consumers += 1
