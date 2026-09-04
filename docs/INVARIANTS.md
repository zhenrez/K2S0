# Digital Twin invariant registry

These identifiers are the compatibility boundary for adapters and generated
implementations. Implementations may change mechanics, but must preserve every
invariant or record an explicit, reviewed divergence.

| ID | Invariant | Owner | Primary executable evidence |
| --- | --- | --- | --- |
| DT-INV-001 | Human, cognitive-twin, service-agent, and avatar identities remain distinct | identity | identity-surface test |
| DT-INV-002 | One twin binds exactly one subject | aggregate | cross-subject isolation test |
| DT-INV-003 | Each twin stream is contiguous and hash chained | event store | chain and golden replay tests |
| DT-INV-004 | An idempotent retry returns the original append and is not republished | event store | store and broker retry tests |
| DT-INV-005 | Valid time and recorded time remain independent | aggregate | bitemporal time-travel tests |
| DT-INV-006 | Every claim has resolvable evidence provenance | adjudication | unknown-provenance rejection test |
| DT-INV-007 | Every claim has seven independently bounded epistemic dimensions | adjudication | epistemic completeness test |
| DT-INV-008 | Corrections and supersession append history; they never overwrite it | aggregate | golden correction replay |
| DT-INV-009 | Simulation cannot mutate authoritative state | simulation | contamination mutation tests |
| DT-INV-010 | External consumers receive projections, never unrestricted canonical state | projection | minimized projection test |
| DT-INV-011 | Disclosure is default-deny and sensitivity-minimized | policy | denial and field filtering tests |
| DT-INV-012 | Consent to information never grants authority to act | policy | ActionEnvelope separation test |
| DT-INV-013 | Only the registered producer role may create each event type | authorization | ownership matrix tests |
| DT-INV-014 | Invalid transitions are rejected before persistence | service | replay-poisoning prevention test |
| DT-INV-015 | Evidence deletion invalidates dependent claims and artifacts | aggregate | deletion dependency test |
| DT-INV-016 | Projection issuance and revocation remain auditable | projection | revocation state tests |
| DT-INV-017 | Real-time subscribers cannot create unbounded buffering | synchronization | slow-consumer test |
| DT-INV-018 | Loss reports do not reveal hidden identifiers | projection | non-interference assertion |
| DT-INV-019 | Persisted producer identity matches the authenticated actor | authorization | spoofed-producer test |
| DT-INV-020 | Canonical hashes are stable across unordered collection iteration | contracts | deterministic serialization test |

The executable registry is `src/argo_dt/invariants.py`. CI verifies that every
entry has an owner and points to discoverable executable evidence. The v1
fixtures under `fixtures/v1/` are immutable compatibility inputs; update them
only as an explicit schema-version change, never to make a regression pass.

## Event ownership matrix

| Producer role | Owned event families |
| --- | --- |
| ingest_service | evidence ingest/deletion and acquisition completion |
| identity_worker | entity link/unlink proposals |
| adjudication_worker | claim proposals, contradictions, referrals |
| human_review | claim decisions, corrections, supersession |
| compiler | kernel, evaluation, and readiness outputs |
| projection_service | projection issuance and revocation |
| simulation_service | simulation-plane predictions only |
| downstream_agent | observed decisions and submitted outcomes only |
| operations_service | explicit degradation state only |

An event type absent from the matrix is denied. EventEnvelope v2 persists the
role as hash-covered material. Historical v1 envelopes infer their sole owner
only during replay; new v1 writes are refused. An event's persisted `producer`
and `producer_role` must match identity claims supplied by a trusted
transport adapter; values asserted only by the event payload are not trusted.
