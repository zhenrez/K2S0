package argo.dt.action

import rego.v1

default allow := false

impact_rank := {
    "none": 0,
    "low": 1,
    "moderate": 2,
    "high": 3,
    "critical": 4,
}

active_grant if {
    input.grant.revoked_at == null
    time.parse_rfc3339_ns(input.grant.valid_from) <= time.now_ns()
    time.now_ns() < time.parse_rfc3339_ns(input.grant.valid_until)
}

envelope_active if {
    time.parse_rfc3339_ns(input.action.issued_at) <= time.now_ns()
    time.now_ns() < time.parse_rfc3339_ns(input.action.expires_at)
}

capabilities_granted if {
    every capability in input.action.requested_capabilities {
        capability in input.grant.capabilities
    }
}

reversibility_satisfied if {
    not input.grant.require_reversible
}

reversibility_satisfied if {
    input.grant.require_reversible
    input.action.reversible
}

allow if {
    input.default_deny
    active_grant
    envelope_active
    input.action.principal_id == input.grant.principal_id
    input.action.actor_identity_id == input.grant.actor_identity_id
    input.action.purpose in input.grant.purposes
    capabilities_granted
    impact_rank[input.action.impact] <= impact_rank[input.grant.maximum_impact]
    reversibility_satisfied
    count(input.action.evidence_ids) > 0
}

