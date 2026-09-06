"""Sustained SQLite-to-state-stream load harness with machine-readable gates."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import tempfile
import time
import tracemalloc
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from argo_dt.compiler import ProjectionCompiler
from argo_dt.event_store import SQLiteEventStore
from argo_dt.policy import DefaultDenyPolicy
from argo_dt.service import DigitalTwinService
from argo_dt.sync import CursorSigner, SyncLimits
from argo_dt.types import ActorContext, EventEnvelope, EventPlane, ProducerRole


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=float, default=60.0)
    parser.add_argument("--events-per-second", type=float, default=100.0)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--max-p99-ms", type=float, default=100.0)
    parser.add_argument("--minimum-throughput", type=float)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        raise ValueError("cannot calculate a percentile without observations")
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


async def run(args: argparse.Namespace, database: Path) -> dict[str, Any]:
    if args.duration_seconds <= 0 or args.events_per_second <= 0:
        raise ValueError("duration and event rate must be positive")
    if args.payload_bytes < 1 or args.max_p99_ms <= 0:
        raise ValueError("payload size and p99 gate must be positive")
    target = max(1, int(args.duration_seconds * args.events_per_second))
    minimum_throughput = (
        args.minimum_throughput
        if args.minimum_throughput is not None
        else args.events_per_second * 0.9
    )
    if minimum_throughput <= 0:
        raise ValueError("minimum throughput must be positive")

    store = SQLiteEventStore(database)
    actor = ActorContext(
        "soak-operations",
        frozenset({ProducerRole.OPERATIONS_SERVICE}),
    )
    service = DigitalTwinService(
        store=store,
        projection_compiler=ProjectionCompiler(DefaultDenyPolicy()),
        cursor_signer=CursorSigner(
            b"soak-only-cursor-key-material-32-bytes",
            key_id="soak",
        ),
        sync_limits=SyncLimits(max_in_flight=128, replay_page_size=256),
        snapshot_interval=0,
    )
    stream = await service.open_state_stream("soak-twin", actor=actor)
    latencies_ms: list[float] = []
    errors: list[str] = []
    stop = asyncio.Event()
    maximum_outstanding = 0
    filler = "x" * args.payload_bytes

    async def writer(started: float) -> None:
        for index in range(target):
            if stop.is_set():
                return
            due = started + index / args.events_per_second
            remaining = due - time.perf_counter()
            if remaining > 0:
                await asyncio.sleep(remaining)
            try:
                await service.append(
                    EventEnvelope.new(
                        twin_id="soak-twin",
                        event_type="DegradationDeclared",
                        plane=EventPlane.AUTHORITATIVE,
                        payload={"reason": filler, "sample": index},
                        producer=actor.identity_id,
                        producer_role=ProducerRole.OPERATIONS_SERVICE,
                        idempotency_key=f"soak-{index}",
                    ),
                    actor=actor,
                    expected_sequence=index,
                )
            except Exception as exc:
                errors.append(type(exc).__name__)
                stop.set()
                await stream.close(code=1011, reason="writer failure")
                return

    async def reader() -> None:
        nonlocal maximum_outstanding
        for _ in range(target):
            try:
                frame = await stream.__anext__()
                recorded_at = datetime.fromisoformat(
                    frame.recorded_at.replace("Z", "+00:00")
                ).astimezone(UTC)
                latency = (datetime.now(UTC) - recorded_at).total_seconds() * 1000
                latencies_ms.append(max(latency, 0.0))
                maximum_outstanding = max(maximum_outstanding, stream.outstanding_count)
                stream.acknowledge(frame.resume_token)
            except Exception as exc:
                errors.append(type(exc).__name__)
                stop.set()
                return

    tracemalloc.start()
    started = time.perf_counter()
    try:
        try:
            await asyncio.wait_for(
                asyncio.gather(writer(started), reader()),
                timeout=args.duration_seconds + 30,
            )
        except TimeoutError:
            errors.append("TimeoutError")
        elapsed = time.perf_counter() - started
        _current, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        await stream.close()
        store.close()

    throughput = len(latencies_ms) / elapsed
    p50 = percentile(latencies_ms, 0.50) if latencies_ms else float("inf")
    p95 = percentile(latencies_ms, 0.95) if latencies_ms else float("inf")
    p99 = percentile(latencies_ms, 0.99) if latencies_ms else float("inf")
    passed = (
        not errors
        and len(latencies_ms) == target
        and p99 <= args.max_p99_ms
        and throughput >= minimum_throughput
        and maximum_outstanding <= service.sync_limits.max_in_flight
    )
    return {
        "schema_version": "argo.dt.slo-report/v1",
        "passed": passed,
        "duration_seconds": round(elapsed, 6),
        "target_events": target,
        "delivered_events": len(latencies_ms),
        "requested_events_per_second": args.events_per_second,
        "throughput_events_per_second": round(throughput, 2),
        "visibility_latency_ms": {
            "p50": round(p50, 3),
            "p95": round(p95, 3),
            "p99": round(p99, 3),
            "maximum": round(max(latencies_ms), 3) if latencies_ms else None,
        },
        "gates": {
            "max_p99_ms": args.max_p99_ms,
            "minimum_throughput": minimum_throughput,
        },
        "peak_traced_bytes": peak_bytes,
        "maximum_unacknowledged_frames": maximum_outstanding,
        "broker": asdict(service.broker.stats),
        "errors": errors,
    }


def main() -> None:
    args = parse_args()
    if args.database is not None:
        report = asyncio.run(run(args, args.database))
    else:
        with tempfile.TemporaryDirectory(prefix="argo-dt-soak-") as directory:
            report = asyncio.run(run(args, Path(directory) / "soak.db"))
    rendered = json.dumps(report, sort_keys=True, separators=(",", ":"))
    if args.json:
        print(rendered)
    else:
        print(json.dumps(report, indent=2, sort_keys=True))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
