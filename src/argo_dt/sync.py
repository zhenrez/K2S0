"""Bounded in-process reference broker for low-latency state/event delivery."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import AsyncIterator

from .errors import BackpressureExceeded
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
