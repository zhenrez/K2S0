"""Deterministic kernel and minimum-necessary projection compilers."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any

from .aggregate import TwinState
from .errors import InvariantViolation, PolicyDenied
from .ports import PolicyEvaluator
from .types import (
    ConsentGrant,
    KernelArtifact,
    ProjectionReceipt,
    ProjectionRequest,
    Sensitivity,
    content_hash,
    parse_time,
    utc_now,
)

ARTIFACT_FIELDS = frozenset(
    {
        "kernel",
        "now",
        "counterweights",
        "voice",
        "people",
        "capabilities",
        "decision_model",
        "boundaries",
        "history",
        "readiness",
    }
)

_CLAIM_KIND_TO_ARTIFACT = {
    "current_state": "now",
    "communication": "voice",
    "relationship": "people",
    "capability": "capabilities",
    "decision_heuristic": "decision_model",
    "boundary": "boundaries",
    "event": "history",
}


class KernelCompiler:
    """Builds inspectable artifacts only from accepted, non-stale claims."""

    compiler_version = "argo.dt.kernel-compiler/v1"

    def compile(
        self,
        state: TwinState,
        artifact_type: str,
        *,
        valid_at: datetime | None = None,
        known_at: datetime | None = None,
        max_sensitivity: Sensitivity | None = None,
    ) -> KernelArtifact:
        if artifact_type not in ARTIFACT_FIELDS:
            raise InvariantViolation(f"unsupported artifact type: {artifact_type}")
        accepted = state.accepted_claims(
            valid_at=valid_at,
            known_at=known_at,
            max_sensitivity=max_sensitivity,
        )
        if artifact_type == "kernel":
            selected = accepted
        elif artifact_type == "counterweights":
            def visible(claim: dict[str, Any]) -> bool:
                if max_sensitivity is not None:
                    sensitivity = Sensitivity(claim.get("sensitivity", "internal"))
                    if sensitivity.rank > max_sensitivity.rank:
                        return False
                if valid_at is not None:
                    start = parse_time(claim["valid_from"])
                    end_raw = claim.get("valid_until")
                    end = parse_time(end_raw) if end_raw else None
                    if valid_at < start or (end is not None and valid_at >= end):
                        return False
                if known_at is not None and claim.get("recorded_at"):
                    if parse_time(claim["recorded_at"]) > known_at:
                        return False
                return True

            selected = [
                state.claims[claim_id]
                for claim_id in sorted(state.contested_claim_ids)
                if claim_id in state.claims and visible(state.claims[claim_id])
            ]
        elif artifact_type == "readiness":
            selected = []
        else:
            selected = [
                claim
                for claim in accepted
                if _CLAIM_KIND_TO_ARTIFACT.get(str(claim.get("kind"))) == artifact_type
            ]

        claim_ids = tuple(str(claim["claim_id"]) for claim in selected)
        status = "degraded" if state.degraded_reasons else "ready"
        payload: dict[str, Any]
        if artifact_type == "readiness":
            payload = {
                "visible_accepted_claims": len(accepted),
                "degraded_reasons": list(state.degraded_reasons),
            }
        else:
            payload = {"claims": selected}
        loss_report = {
            "excluded_stale_claim_ids": sorted(state.stale_claim_ids),
            "unresolved_contradiction_ids": sorted(state.contradictions),
            "degraded_reasons": list(state.degraded_reasons),
        }
        artifact_material = {
            "twin_id": state.twin_id,
            "artifact_type": artifact_type,
            "source_sequence": state.sequence,
            "source_claim_ids": claim_ids,
            "compiler_version": self.compiler_version,
            "status": status,
            "payload": payload,
            "loss_report": loss_report,
        }
        return KernelArtifact(
            artifact_id=str(uuid.uuid4()),
            twin_id=state.twin_id,
            artifact_type=artifact_type,
            source_sequence=state.sequence,
            source_claim_ids=claim_ids,
            compiler_version=self.compiler_version,
            status=status,
            payload=payload,
            loss_report=loss_report,
            compiled_at=utc_now(),
            artifact_hash=content_hash(artifact_material),
        )


class ProjectionCompiler:
    """Compiles at the consent boundary; it does not filter after retrieval."""

    def __init__(
        self,
        policy: PolicyEvaluator,
        kernel_compiler: KernelCompiler | None = None,
    ) -> None:
        self._policy = policy
        self._kernel = kernel_compiler or KernelCompiler()

    def compile(
        self,
        *,
        state: TwinState,
        request: ProjectionRequest,
        consent: ConsentGrant | None,
    ) -> tuple[dict[str, Any], ProjectionReceipt]:
        if request.twin_id != state.twin_id:
            raise InvariantViolation("projection request twin does not match state")
        allowed, reasons, fields = self._policy.evaluate_projection(
            request,
            consent,
            available_fields=ARTIFACT_FIELDS,
        )
        if not allowed or consent is None:
            raise PolicyDenied(",".join(reasons))

        artifacts: dict[str, Any] = {}
        source_claim_ids: set[str] = set()
        for field_name in sorted(fields):
            artifact = self._kernel.compile(
                state,
                field_name,
                valid_at=request.as_of_valid_time,
                known_at=request.as_of_recorded_time,
                max_sensitivity=request.maximum_sensitivity,
            )
            artifacts[field_name] = {
                "artifact_id": artifact.artifact_id,
                "status": artifact.status,
                "source_sequence": artifact.source_sequence,
                "payload": artifact.payload,
                "loss_report": {
                    "omitted_stale_claim_count": len(
                        artifact.loss_report["excluded_stale_claim_ids"]
                    ),
                    "unresolved_contradiction_count": len(
                        artifact.loss_report["unresolved_contradiction_ids"]
                    ),
                    "degraded": bool(artifact.loss_report["degraded_reasons"]),
                },
                "artifact_hash": artifact.artifact_hash,
            }
            source_claim_ids.update(artifact.source_claim_ids)

        projection = {
            "schema_version": "argo.dt.projection/v1",
            "twin_id": state.twin_id,
            "subject_id": request.subject_id,
            "recipient_id": request.recipient_id,
            "purpose": request.purpose,
            "as_of_valid_time": request.as_of_valid_time.isoformat(),
            "as_of_recorded_time": request.as_of_recorded_time.isoformat(),
            "artifacts": artifacts,
        }
        issued_at = utc_now()
        expires_at = min(consent.valid_until, issued_at + timedelta(hours=1))
        receipt = ProjectionReceipt(
            projection_id=str(uuid.uuid4()),
            request_id=request.request_id,
            consent_id=consent.consent_id,
            policy_version=consent.policy_version,
            issued_at=issued_at,
            expires_at=expires_at,
            source_sequence=state.sequence,
            disclosed_fields=tuple(sorted(fields)),
            source_claim_ids=tuple(sorted(source_claim_ids)),
            artifact_hash=content_hash(projection),
            decision_reasons=reasons,
        )
        return projection, receipt
