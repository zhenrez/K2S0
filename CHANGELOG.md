# Changelog

## 0.5.0 — 2026-09-05

- Added an authenticated raw-ASGI WebSocket runtime for the state protocol,
  including required subprotocol, strict JSON decoding, bounded control frames,
  negotiated in-flight limits, heartbeat, acknowledgement, and close handling.
- Added executable gRPC telemetry/state handlers over host-generated protobuf
  modules, exact serialized-size enforcement, and generated-contract CI.
- Added per-record JSON telemetry validation with duplicate-key rejection,
  explicit partial commits, stable idempotent retry, and source metadata.
- Added optional NATS JetStream publication and manual-ack consumption with
  bounded redelivery and payload-free dead-letter replay markers.
- Added bounded cursor-key overlap/rotation and low-cardinality OpenTelemetry
  delta export without changing the cursor v1 wire shape.
- Added a sustained SQLite commit-to-stream load harness. Its first run exposed
  and fixed event-loop starvation in the in-process outbox path.
- Added a bounded, caller-copy-safe latest-state cache that verifies SQLite
  sequence/hash before reuse and never caches historical/as-of queries.
- Made the executable demo emit only the public projection-receipt shape.
- Enabled repository-wide Ruff and strict-mypy CI gates and cleared the prior
  lint baseline so new violations fail builds.
- Kept SQLite as the only authoritative/default database and Neo4j optional.

## 0.4.0 — 2026-09-05

- Added bounded, hash-verified SQLite replay with a race-safe, duplicate-safe
  handoff to live transactional-outbox publication.
- Added HMAC-SHA256 resume cursors with expiry, twin binding, and private keyed
  chain binding; raw event hashes are not disclosed in cursor payloads.
- Added cumulative delivery acknowledgements, bounded in-flight windows,
  explicit heartbeat/close frames, and redaction-safe synchronization metrics.
- Added payload-minimized state notifications and subject-scoped stream access;
  unrestricted raw event subscriptions now require an operations role.
- Added exact transport byte/count guards, per-record telemetry acknowledgement
  types, and validated NATS subject construction without adding a service.
- Hardened protobuf and OpenAPI stream contracts while retaining SQLite as the
  only required database. gRPC/WebSocket/NATS/OpenTelemetry runtime adapters
  remain explicit deployment gates.

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
