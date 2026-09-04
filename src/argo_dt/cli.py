"""Small local demonstration; production servers bind the declared contracts."""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
import uuid
from datetime import UTC, datetime, timedelta

from .compiler import ProjectionCompiler
from .event_store import SQLiteEventStore
from .policy import DefaultDenyPolicy
from .service import DigitalTwinService
from .types import ConsentGrant, ProjectionRequest, Sensitivity, to_primitive


async def _demo() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db") as database:
        store = SQLiteEventStore(database.name)
        service = DigitalTwinService(
            store=store,
            projection_compiler=ProjectionCompiler(DefaultDenyPolicy()),
        )
        now = datetime.now(UTC)
        evidence = await service.ingest_evidence(
            twin_id="twin-demo",
            subject_id="human-demo",
            source="explicit-self-report",
            source_record_id="demo-1",
            payload={"text": "I prefer reversible experiments under uncertainty."},
            rights={"owner": "human-demo", "processing": ["modeling"]},
            sensitivity=Sensitivity.INTERNAL,
            valid_from=now,
            valid_until=None,
            independence_group="demo-session-1",
            expected_sequence=0,
            idempotency_key="demo-evidence-1",
        )
        claim = await service.propose_claim(
            twin_id="twin-demo",
            subject_id="human-demo",
            statement="Under uncertainty, the subject often prefers reversible experiments.",
            kind="decision_heuristic",
            provenance=[
                {
                    "evidence_id": evidence.payload["evidence_id"],
                    "relation": "supports",
                    "independence_group": "demo-session-1",
                }
            ],
            sensitivity=Sensitivity.INTERNAL,
            valid_from=now,
            valid_until=None,
            epistemic={
                "evidence_quality": 0.7,
                "confidence": 0.65,
                "salience": 0.8,
                "stability": 0.5,
                "freshness": 1.0,
                "scope_confidence": 0.5,
                "contradiction_load": 0.0,
            },
            expected_sequence=1,
            idempotency_key="demo-claim-1",
        )
        await service.review_claim(
            twin_id="twin-demo",
            claim_id=str(claim.payload["claim_id"]),
            accepted=True,
            reviewer_identity_id="human-demo",
            rationale="Accurate but intentionally scoped.",
            expected_sequence=2,
            idempotency_key="demo-review-1",
        )
        consent = ConsentGrant(
            consent_id=str(uuid.uuid4()),
            subject_id="human-demo",
            recipient_id="questn-demo",
            purposes=frozenset({"decision-support"}),
            allowed_fields=frozenset({"decision_model", "readiness"}),
            max_sensitivity=Sensitivity.INTERNAL,
            valid_from=now - timedelta(minutes=1),
            valid_until=now + timedelta(hours=1),
            policy_version="constitution/v1",
        )
        request = ProjectionRequest(
            request_id=str(uuid.uuid4()),
            twin_id="twin-demo",
            subject_id="human-demo",
            recipient_id="questn-demo",
            purpose="decision-support",
            requested_fields=frozenset({"decision_model", "readiness"}),
            maximum_sensitivity=Sensitivity.INTERNAL,
            as_of_valid_time=now,
            as_of_recorded_time=now + timedelta(seconds=1),
        )
        projection, receipt, _ = await service.issue_projection(
            request=request,
            consent=consent,
            expected_sequence=3,
            idempotency_key="demo-projection-1",
        )
        print(
            json.dumps(
                {"projection": projection, "receipt": to_primitive(receipt)},
                indent=2,
                sort_keys=True,
            )
        )
        assert store.verify_chain("twin-demo")
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="argo-dt")
    parser.add_argument("command", choices=["demo"])
    args = parser.parse_args()
    if args.command == "demo":
        asyncio.run(_demo())


if __name__ == "__main__":
    main()

