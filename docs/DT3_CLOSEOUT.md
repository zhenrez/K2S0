# DT-3 closeout and DT-4 entry record

Date: 2026-09-08 UTC  
Release: 0.6.1  
Profile: embedded Python 3.12 + SQLite/WAL

## Closure decision

DT-3 is closed for the embedded reference profile. Its executable scope is the
epistemic and admission foundation used to build a K2S0 DT-Seed; it is not a
claim that K2S0 already has its final portable container, that R2D2 interviewing
is implemented, or that the complete Human Digital Twin is finished.

## Accepted capability

- evidence-backed reversible entity links;
- provenance-required seven-dimension claims;
- explicit contradiction findings and separate human adjudication;
- append-only corrections and supersession;
- recorded-time replay combined with valid-time selection;
- sequence-bounded bidirectional SQLite lineage and independence grouping;
- typed Point/Line/Face/Volume/Root relations;
- deterministic gap discovery;
- source-state-authenticated elicitation plans;
- encrypted, bounded, idempotent Bronze response capture;
- stale/deletion propagation without historical erasure;
- REST, gRPC, JSON Schema, migration, invariant, performance, and conformance
  coverage for the implemented boundary.

## Security closure

Response admission reconstructs the plan from its exact recorded source state.
Altered, fabricated, cross-twin, and future-sequence plans fail before Bronze
writes. Plaintext answers and prompts remain outside the event ledger, outbox,
NATS payloads, and state notifications. Deterministic identifiers use canonical
structured material; exact retries remain valid after later source deletion.

## Validation baseline

The closing 0.6.0 implementation passed 88 tests with generated protobuf
modules, all nine JSON Schemas, OpenAPI 3.1 and SQLite migration validation,
Ruff, strict mypy, compileall, the CLI demonstration, the 1,000-claim lineage
regression, and the synchronization soak. PR and post-merge CI passed on the
merged tree. The 0.6.1 closeout added one boundary-regression test and passed
the same CI-equivalent suite with 89 tests.

## Residual environment gates

The following remain production/deployment gates and are not reclassified as
DT-3 implementation defects:

- live OIDC/mTLS and policy-engine binding;
- host HTTP/gRPC control/query handler binding;
- deployed NATS and OpenTelemetry collector verification;
- recovery reconciliation for orphaned encrypted Bronze objects;
- numeric retention and deployment SLO baselines;
- privacy review of elicitation prompts;
- optional Neo4j projection validation;
- 24-hour soak at twice forecast peak.

These gates must be satisfied before the corresponding production claim, but
they do not prevent reference work from advancing to DT-4.

## DT-4 entry conditions

DT-4 may open when:

1. DT-3 source, contracts, tests, and closeout documentation are merged and CI
   is green.
2. R2D2 is recorded as the active interviewer/acquirer and K2S0 as the passive
   DT-Seed/representation artifact.
3. SQLite/WAL remains authoritative; no server database is introduced as a
   hidden prerequisite.
4. Frozen v1 event compatibility remains unchanged.
5. DT-4 accepts an explicit ledger source sequence and does not read raw Bronze
   material without purpose- and field-level authorization.

DT-4's first deliverable is the canonical K2S0 seed manifest/container and a
deterministic compiler/reader round trip. Voice, visual/embodied, health, and
R2D2 conversational completeness remain separately qualified capability tracks.
