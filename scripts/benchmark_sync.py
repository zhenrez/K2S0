"""Local DT-2 replay/live smoke benchmark; not production capacity evidence."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
import tracemalloc

from argo_dt.event_store import SQLiteEventStore
from argo_dt.sync import (
    BoundedEventBroker,
    DurableSubscription,
    StreamPosition,
    SyncLimits,
    SyncMetrics,
)
from argo_dt.types import EventEnvelope, EventPlane, ProducerRole


def append_event(store: SQLiteEventStore, sequence: int) -> EventEnvelope:
    event = EventEnvelope.new(
        twin_id="benchmark-twin",
        event_type="DegradationDeclared",
        plane=EventPlane.AUTHORITATIVE,
        payload={"reason": "sync-smoke"},
        producer="benchmark-operations",
        producer_role=ProducerRole.OPERATIONS_SERVICE,
        idempotency_key=f"sync-benchmark-{sequence}",
    )
    return store.append(event, expected_sequence=sequence - 1)


async def benchmark(event_count: int, page_size: int) -> dict[str, object]:
    store = SQLiteEventStore()
    broker = BoundedEventBroker(queue_capacity=1024)
    metrics = SyncMetrics()
    try:
        for sequence in range(1, event_count + 1):
            append_event(store, sequence)
        subscription = await DurableSubscription.open(
            store=store,
            broker=broker,
            start=StreamPosition("benchmark-twin", 0, ""),
            limits=SyncLimits(replay_page_size=page_size, max_in_flight=1),
            metrics=metrics,
        )
        replay_start = time.perf_counter()
        for _ in range(event_count):
            event = await subscription.__anext__()
            subscription.acknowledge(
                StreamPosition(event.twin_id, event.sequence, event.event_hash)
            )
        replay_seconds = time.perf_counter() - replay_start

        live = append_event(store, event_count + 1)
        live_start = time.perf_counter()
        await broker.publish(live)
        delivered = await subscription.__anext__()
        live_seconds = time.perf_counter() - live_start
        subscription.acknowledge(
            StreamPosition(delivered.twin_id, delivered.sequence, delivered.event_hash)
        )
        await subscription.aclose()

        memory_subscription = await DurableSubscription.open(
            store=store,
            broker=broker,
            start=StreamPosition("benchmark-twin", 0, ""),
            limits=SyncLimits(replay_page_size=page_size, max_in_flight=1),
        )
        tracemalloc.start()
        for _ in range(event_count + 1):
            event = await memory_subscription.__anext__()
            memory_subscription.acknowledge(
                StreamPosition(event.twin_id, event.sequence, event.event_hash)
            )
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        await memory_subscription.aclose()
        return {
            "events": event_count,
            "replay_page_size": page_size,
            "replay_seconds": round(replay_seconds, 6),
            "replay_events_per_second": round(event_count / replay_seconds, 2),
            "peak_traced_bytes": peak_bytes,
            "live_publish_to_consume_ms": round(live_seconds * 1000, 3),
            "metrics": metrics.snapshot(),
            "warning": "local smoke benchmark; not production SLO evidence",
        }
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument("--page-size", type=int, default=256)
    args = parser.parse_args()
    if args.events < 1 or args.page_size < 1:
        parser.error("--events and --page-size must be positive")
    print(json.dumps(asyncio.run(benchmark(args.events, args.page_size)), indent=2))


if __name__ == "__main__":
    main()
