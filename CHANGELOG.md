# Changelog

## 0.3.0 — 2026-09-05

- Selected SQLite/WAL as the authoritative embedded ledger; Neo4j remains an
  optional rebuildable graph read model and is not required.
- Added hash-verified snapshots and deterministic snapshot-plus-tail replay.
- Added atomic event/outbox commits, leased at-least-once publication, durable
  retry state, process-reopen recovery, pointer-only rows, and bounded pruning.
- Added a durable recursive evidence → claim → projection dependency index and
  transactional deletion invalidation queue.
- Added an AES-256-GCM local Bronze vault with injected keys, encrypted
  metadata, deterministic source identities, tamper detection, and atomic
  non-overwriting writes.
- Added a reusable persistence adapter contract, recovery/fault-injection
  tests, executable SQLite migration validation, and replay benchmark.
- Removed internal claim, evidence, model-trace, sensitivity, and lineage IDs
  from public projection payloads and receipts.

## 0.2.0 — 2026-09-04

- Added authenticated actor contexts and a default-deny event ownership
  matrix, with producer identity and role persisted in EventEnvelope v2.
- Preserved EventEnvelope v1 hash/replay compatibility through unique-owner
  inference while refusing security-downgrade writes using the legacy schema.
- Added 20 stable invariant identifiers with machine-verified ownership and
  executable evidence links.
- Added immutable v1 replay and negative-boundary fixtures covering temporal
  correction, supersession, contradiction, deletion, revoked consent,
  projection revocation, and simulation contamination attempts.
- Made canonical serialization deterministic for set-like values.
- Added cross-contract CI validation for JSON Schema, OpenAPI, protobuf, and
  SQL definitions.

## 0.1.0 — 2026-09-04

- Added K2S0 Digital Twin reference kernel.
- Added ARGOCell, event, bitemporal, epistemic, identity, consent, projection,
  authority, and action contracts.
- Added SQLite hash-chained event adapter, deterministic state projector,
  isolated simulations, bounded real-time broker, and projection compiler.
- Added gRPC, REST, JSON Schema, SQL, and Rego production contracts.
- Added architecture, integration, repository-pattern, performance/security,
  roadmap, and host merge documentation.
- Added executable demo and invariant tests.
- Hardened subject binding, provenance referential integrity, epistemic vector
  validation, and event-plane ownership after pre-merge review.
- Added transition validation before append, projection revocation state,
  disclosure-safe projection loss reports, and duplicate-publication defense.
- Aligned the OpenAPI projection, revocation, and reference-state contracts with
  the executable service surface.
