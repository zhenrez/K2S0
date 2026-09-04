# Changelog

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
  PostgreSQL definitions.

## 0.1.0 — 2026-09-04

- Added K2S0 Digital Twin reference kernel.
- Added ARGOCell, event, bitemporal, epistemic, identity, consent, projection,
  authority, and action contracts.
- Added SQLite hash-chained event adapter, deterministic state projector,
  isolated simulations, bounded real-time broker, and projection compiler.
- Added gRPC, REST, JSON Schema, PostgreSQL, and Rego production contracts.
- Added architecture, integration, repository-pattern, performance/security,
  roadmap, and host merge documentation.
- Added executable demo and invariant tests.
- Hardened subject binding, provenance referential integrity, epistemic vector
  validation, and event-plane ownership after pre-merge review.
- Added transition validation before append, projection revocation state,
  disclosure-safe projection loss reports, and duplicate-publication defense.
- Aligned the OpenAPI projection, revocation, and reference-state contracts with
  the executable service surface.
