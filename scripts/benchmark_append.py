"""Local smoke benchmark; use a production load generator for capacity claims."""

from __future__ import annotations

import argparse
import tempfile
import time

from argo_dt.event_store import SQLiteEventStore
from argo_dt.types import EventEnvelope, EventPlane, ProducerRole


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=10_000)
    args = parser.parse_args()
    with tempfile.NamedTemporaryFile(suffix=".db") as database:
        store = SQLiteEventStore(database.name)
        started = time.perf_counter()
        for sequence in range(args.events):
            store.append(
                EventEnvelope.new(
                    twin_id="benchmark",
                    event_type="DegradationDeclared",
                    plane=EventPlane.AUTHORITATIVE,
                    payload={"reason": "benchmark"},
                    producer="benchmark",
                    producer_role=ProducerRole.OPERATIONS_SERVICE,
                    idempotency_key=f"benchmark-{sequence}",
                ),
                expected_sequence=sequence,
            )
        elapsed = time.perf_counter() - started
        print(
            {
                "events": args.events,
                "seconds": round(elapsed, 3),
                "events_per_second": round(args.events / elapsed, 1),
                "chain_valid": store.verify_chain("benchmark"),
            }
        )
        store.close()


if __name__ == "__main__":
    main()
