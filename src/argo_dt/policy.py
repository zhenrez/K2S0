"""Deterministic default-deny policy reference.

Production deployments may delegate evaluation to OPA/Rego, but transport
adapters must return the same allow/reasons/field-set contract.
"""

from __future__ import annotations

from .types import (
    ActionEnvelope,
    AuthorityGrant,
    ConsentGrant,
    ProjectionRequest,
    utc_now,
)

_IMPACT_ORDER = {"none": 0, "low": 1, "moderate": 2, "high": 3, "critical": 4}


class DefaultDenyPolicy:
    def evaluate_projection(
        self,
        request: ProjectionRequest,
        consent: ConsentGrant | None,
        *,
        available_fields: frozenset[str],
    ) -> tuple[bool, tuple[str, ...], frozenset[str]]:
        reasons: list[str] = []
        now = utc_now()
        if consent is None:
            return False, ("no_active_consent",), frozenset()
        if not consent.active_at(now):
            reasons.append("consent_inactive")
        if consent.subject_id != request.subject_id:
            reasons.append("subject_mismatch")
        if consent.recipient_id != request.recipient_id:
            reasons.append("recipient_mismatch")
        if request.purpose not in consent.purposes:
            reasons.append("purpose_not_granted")
        if request.maximum_sensitivity.rank > consent.max_sensitivity.rank:
            reasons.append("sensitivity_exceeds_grant")

        unknown_fields = request.requested_fields.difference(available_fields)
        ungranted_fields = request.requested_fields.difference(consent.allowed_fields)
        if unknown_fields:
            reasons.append("unsupported_field_request")
        if ungranted_fields:
            reasons.append("field_not_granted")

        if reasons:
            return False, tuple(reasons), frozenset()
        allowed = request.requested_fields & consent.allowed_fields & available_fields
        if not allowed:
            return False, ("empty_projection",), frozenset()
        return True, ("consent_and_policy_satisfied",), allowed

    def evaluate_action(
        self,
        action: ActionEnvelope,
        grant: AuthorityGrant | None,
    ) -> tuple[bool, tuple[str, ...]]:
        now = utc_now()
        reasons: list[str] = []
        if grant is None:
            return False, ("no_authority_grant",)
        if not grant.active_at(now):
            reasons.append("authority_inactive")
        if action.expires_at <= now or action.issued_at > now:
            reasons.append("action_envelope_inactive")
        if grant.principal_id != action.principal_id:
            reasons.append("principal_mismatch")
        if grant.actor_identity_id != action.actor_identity_id:
            reasons.append("actor_mismatch")
        if action.purpose not in grant.purposes:
            reasons.append("purpose_not_authorized")
        if not action.requested_capabilities.issubset(grant.capabilities):
            reasons.append("capability_not_authorized")
        if _IMPACT_ORDER.get(action.impact, 99) > _IMPACT_ORDER.get(grant.maximum_impact, -1):
            reasons.append("impact_exceeds_grant")
        if grant.require_reversible and not action.reversible:
            reasons.append("irreversible_action_denied")
        if not action.evidence_ids:
            reasons.append("action_has_no_evidence")
        if reasons:
            return False, tuple(reasons)
        return True, ("authority_and_constraints_satisfied",)

