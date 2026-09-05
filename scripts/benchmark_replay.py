"""Compare full replay with hash-verified snapshot-plus-tail replay."""

from __future__ import annotations

import argparse
import statistics
import tempfile
import time
from collections.abc import Callable

from argo_dt.aggregate import TwinAggregate
from argo_dt.event_store import SQLiteEventStore
from argo_dt.types import EventEnvelope, EventPlane, ProducerRole


def elapsed(callable_: Callable[[], object]) -> float:
    started = time.perf_counter()
    callable_()
    return time.perf_counter() - started


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument("--tail", type=int, default=100)
    parser.add_argument("--iterations", type=int, default=10)
    args = parser.parse_args()
    if args.events < 2 or not 0 < args.tail < args.events:
        raise SystemExit("require events >= 2 and 0 < tail < events")
    if args.iterations < 1:
        raise SystemExit("iterations must be positive")

    with tempfile.NamedTemporaryFile(suffix=".db") as database:
        store = SQLiteEventStore(database.name)
        for sequence in range(args.events):
            store.append(
                EventEnvelope.new(
                    twin_id="replay-benchmark",
                    event_type=(
                        "DegradationDeclared"
                        if sequence % 2 == 0
                        else "DegradationCleared"
                    ),
                    plane=EventPlane.AUTHORITATIVE,
                    payload={"reason": "benchmark"},
                    producer="benchmark",
                    producer_role=ProducerRole.OPERATIONS_SERVICE,
                    idempotency_key=f"replay-{sequence}",
                ),
                expected_sequence=sequence,
            )

        snapshot_sequence = args.events - args.tail
        snapshot_state = TwinAggregate.rebuild(
            store,
            "replay-benchmark",
            up_to_sequence=snapshot_sequence,
        )
        store.save_snapshot(snapshot_state.to_snapshot())
        expected = TwinAggregate.rebuild(store, "replay-benchmark").to_dict()
        actual = TwinAggregate.rebuild(
            store,
            "replay-benchmark",
            snapshot_store=store,
        ).to_dict()
        if actual != expected:
            raise SystemExit("snapshot replay diverged from full replay")

        full_samples = [
            elapsed(lambda: TwinAggregate.rebuild(store, "replay-benchmark"))
            for _ in range(args.iterations)
        ]
        snapshot_samples = [
            elapsed(
                lambda: TwinAggregate.rebuild(
                    store,
                    "replay-benchmark",
                    snapshot_store=store,
                )
            )
            for _ in range(args.iterations)
        ]
        full_median = statistics.median(full_samples)
        snapshot_median = statistics.median(snapshot_samples)
        print(
            {
                "events": args.events,
                "tail_events": args.tail,
                "iterations": args.iterations,
                "full_replay_median_ms": round(full_median * 1000, 3),
                "snapshot_tail_median_ms": round(snapshot_median * 1000, 3),
                "speedup": round(full_median / snapshot_median, 2),
                "states_equal": True,
            }
        )
        store.close()


if __name__ == "__main__":
    main()
