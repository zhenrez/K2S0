"""Measure SQLite lineage-index construction and bidirectional traversal."""

from __future__ import annotations

import argparse
import json
import statistics
import time

from argo_dt.event_store import SQLiteEventStore
from argo_dt.types import EventEnvelope, EventPlane, ProducerRole


def event(
    event_type: str,
    payload: dict[str, object],
    role: ProducerRole,
    sequence: int,
) -> EventEnvelope:
    return EventEnvelope.new(
        twin_id="lineage-benchmark",
        event_type=event_type,
        plane=(
            EventPlane.PROJECTION
            if event_type == "ProjectionIssued"
            else EventPlane.AUTHORITATIVE
        ),
        payload=payload,
        producer=f"benchmark-{role.value}",
        producer_role=role,
        idempotency_key=f"benchmark-{sequence}",
    )


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]


def run(claims: int, iterations: int) -> dict[str, float | int]:
    if claims < 1 or iterations < 1:
        raise ValueError("claims and iterations must be positive")
    store = SQLiteEventStore()
    sequence = 0
    started = time.perf_counter()
    try:
        for index in range(claims):
            evidence_id = f"evidence-{index}"
            claim_id = f"claim-{index}"
            store.append(
                event(
                    "EvidenceIngested",
                    {
                        "evidence_id": evidence_id,
                        "subject_id": "benchmark-subject",
                        "content_hash": f"sha256:{index:064x}",
                        "source": "benchmark",
                        "rights": {},
                        "sensitivity": "internal",
                        "independence_group": f"source-{index}",
                    },
                    ProducerRole.INGEST_SERVICE,
                    sequence,
                ),
                expected_sequence=sequence,
            )
            sequence += 1
            store.append(
                event(
                    "ClaimProposed",
                    {
                        "claim_id": claim_id,
                        "subject_id": "benchmark-subject",
                        "statement": f"benchmark claim {index}",
                        "provenance": [
                            {
                                "evidence_id": evidence_id,
                                "relation": "supports",
                                "independence_group": f"source-{index}",
                            }
                        ],
                        "epistemic": {},
                    },
                    ProducerRole.ADJUDICATION_WORKER,
                    sequence,
                ),
                expected_sequence=sequence,
            )
            sequence += 1
        store.append(
            event(
                "ProjectionIssued",
                {
                    "projection_id": "projection-all",
                    "purpose": "benchmark",
                    "recipient_id": "benchmark",
                    "receipt_hash": "sha256:" + "0" * 64,
                    "source_claim_ids": [f"claim-{index}" for index in range(claims)],
                },
                ProducerRole.PROJECTION_SERVICE,
                sequence,
            ),
            expected_sequence=sequence,
        )
        seeded_seconds = time.perf_counter() - started

        ancestor_ms: list[float] = []
        dependent_ms: list[float] = []
        for _ in range(iterations):
            query_started = time.perf_counter()
            ancestors = store.ancestors(
                "lineage-benchmark", "projection", "projection-all"
            )
            ancestor_ms.append((time.perf_counter() - query_started) * 1000)
            query_started = time.perf_counter()
            dependents = store.dependents(
                "lineage-benchmark", "evidence", "evidence-0"
            )
            dependent_ms.append((time.perf_counter() - query_started) * 1000)
        if len(ancestors) != claims * 2 or len(dependents) != 2:
            raise RuntimeError("lineage benchmark returned an incomplete traversal")
        return {
            "claims": claims,
            "edges": claims * 2,
            "seed_events_per_second": round((claims * 2 + 1) / seeded_seconds, 2),
            "ancestor_p50_ms": round(statistics.median(ancestor_ms), 3),
            "ancestor_p95_ms": round(percentile(ancestor_ms, 0.95), 3),
            "ancestor_p99_ms": round(percentile(ancestor_ms, 0.99), 3),
            "dependent_p50_ms": round(statistics.median(dependent_ms), 3),
            "dependent_p95_ms": round(percentile(dependent_ms, 0.95), 3),
            "dependent_p99_ms": round(percentile(dependent_ms, 0.99), 3),
        }
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--claims", type=int, default=1000)
    parser.add_argument("--iterations", type=int, default=30)
    parser.add_argument("--max-ancestor-p99-ms", type=float)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    result = run(args.claims, args.iterations)
    if (
        args.max_ancestor_p99_ms is not None
        and result["ancestor_p99_ms"] > args.max_ancestor_p99_ms
    ):
        raise SystemExit(
            f"ancestor p99 {result['ancestor_p99_ms']} ms exceeds "
            f"{args.max_ancestor_p99_ms} ms"
        )
    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        for key, value in result.items():
            print(f"{key}: {value}")


if __name__ == "__main__":
    main()
