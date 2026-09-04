package argo.dt.projection

import rego.v1

default allow := false

sensitivity_rank := {
    "public": 0,
    "internal": 1,
    "confidential": 2,
    "restricted": 3,
}

active_consent if {
    input.consent.revoked_at == null
    time.parse_rfc3339_ns(input.consent.valid_from) <= time.now_ns()
    time.now_ns() < time.parse_rfc3339_ns(input.consent.valid_until)
}

identity_matches if {
    input.request.subject_id == input.consent.subject_id
    input.request.recipient_id == input.consent.recipient_id
}

purpose_granted if {
    input.request.purpose in input.consent.purposes
}

sensitivity_granted if {
    sensitivity_rank[input.request.maximum_sensitivity] <=
        sensitivity_rank[input.consent.max_sensitivity]
}

all_fields_granted if {
    every field in input.request.requested_fields {
        field in input.consent.allowed_fields
        field in input.available_fields
        field != "raw_evidence"
        field != "personhood_ceremony"
        field != "secret"
    }
}

allow if {
    input.default_deny
    active_consent
    identity_matches
    purpose_granted
    sensitivity_granted
    all_fields_granted
    count(input.request.requested_fields) > 0
}

decision := {
    "allow": allow,
    "policy_version": "argo.dt.projection/v1",
    "disclosed_fields": {field |
        allow
        field := input.request.requested_fields[_]
    },
}

