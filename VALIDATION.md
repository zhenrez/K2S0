# Validation record

Date: 2026-09-05 UTC

## Passed

| Check | Result |
| --- | --- |
| Python compile | src, tests, and scripts compiled successfully under Python 3.12.13 |
| Invariant, persistence, and synchronization tests | 62/62 passed |
| Demo | Evidence → claim → human review → consented projection → receipt completed |
| JSON contracts | All six JSON Schema files parsed successfully |
| OpenAPI syntax | YAML parsed successfully with the available YAML parser |
| Event integrity | Hash-chain verification passed |
| SQLite migration | Executed successfully in an in-memory SQLite database |
| Snapshot replay | Full replay and snapshot + 100-event tail produced equal state |
| Transactional outbox | Atomic rollback, retry, lease, and process-reopen recovery passed |
| Deletion propagation | Evidence → claim → projection invalidation queue passed |
| Bronze encryption | AES-GCM round trip, tamper rejection, subject isolation, and no plaintext-at-rest assertion passed |
| Projection non-interference | Public payload and receipt omit claim, evidence, model-trace, and sensitivity identifiers |
| Durable synchronization | Paged replay, replay/live race deduplication, gap recovery, resume, and unsubscribe passed |
| Cursor security | Forgery, expiry, wrong-twin, wrong-chain, and raw-hash non-disclosure checks passed |
| Stream controls | Cumulative ack, bounded in-flight window, heartbeat, close, and subject-scope checks passed |
| Transport contracts | Record/batch ceilings, record-level acks, WebSocket frames, and NATS subject validation passed |
| Package metadata | setuptools 84.0.0 is available; source-layout metadata is defined |

Covered tests:

- four identity kinds remain distinct;
- bitemporal valid/recorded time;
- bounded multidimensional epistemic values;
- claim-cell provenance;
- event idempotency and key-reuse rejection;
- optimistic concurrency;
- event hash chain;
- claim provenance;
- evidence deletion and dependent-claim invalidation;
- recorded-time review isolation;
- simulation isolation;
- default-deny projection;
- sensitivity-minimized projection;
- bounded stream backpressure;
- information consent separate from action authority.
- one-subject-per-twin binding;
- provenance referential integrity and independence-group matching;
- strict seven-dimension claim epistemics;
- authoritative, projection, and simulation plane ownership;
- invalid-transition rejection before persistence;
- projection subject binding and revocation;
- projection loss-report privacy;
- idempotent retries without duplicate event publication;
- terminal slow-consumer behavior without a blocking retry.
- default-deny event ownership and authenticated producer matching;
- deterministic canonical serialization of unordered collections;
- golden replay compatibility across correction, contradiction, deletion,
  supersession, and projection revocation;
- negative fixtures for revoked consent and simulation contamination;
- invariant registry ownership and executable-evidence completeness.
- hash-linked snapshots and tail replay equivalence;
- atomic event/outbox rollback and at-least-once retry;
- pending publication recovery after database reopen;
- cross-connection compare-and-swap and exclusive outbox leases;
- recursive deletion dependency queuing through projections;
- deterministic encrypted Bronze writes, source collision rejection, and
  authenticated decryption failure on tampering.
- ordered, bounded replay-to-live handoff with at-least-once duplicate removal;
- HMAC-authenticated, expiry-checked, twin- and chain-bound cursor resume;
- cumulative acknowledgement and slow/unacknowledged consumer termination;
- payload-minimized, subject-scoped external state-change frames;
- exact telemetry record/batch byte ceilings and explicit record statuses;
- wildcard-safe NATS subject construction.

## Local smoke benchmark

The SQLite/WAL adapter appended and verified 10,000 events, including an
outbox pointer per event, at approximately 8,596 events/second in this ephemeral
environment. Full replay of 10,000 events had a 531.380 ms median; replay from
a hash-verified snapshot plus 100 tail events had a 5.140 ms median (103.38×)
across 10 iterations. Both replay paths produced identical state.

The DT-2 synchronization smoke tool replayed and hash-verified 10,000 events in
0.565 seconds (17,713 events/second), with approximately 424 KB peak memory
traced during a separate bounded-page replay pass. In-process live publish to
consume measured 0.095 ms. The page size was 256 and the acknowledgement window
was one event during the measurement.

This is not a production capacity result. It excludes network, authentication,
external publication, concurrent writers, multi-tenant load, disk-failure
recovery, and sustained soak. Production SLOs in docs/PERFORMANCE_SECURITY.md
remain targets until the specified load and soak tests pass.

## Not executable in this workspace

| Check | Reason / required environment |
| --- | --- |
| Protobuf generation | protoc/buf toolchain is not present |
| OPA policy tests | OPA toolchain is not present |
| NATS/WebSocket/gRPC/OTel/Restate integration | No service topology or adapter runtime was supplied |
| Host-repository integration | Host source tree was not supplied |
| Ruff/mypy | Optional development tools are not installed |
| Wheel build | Package command triggered a blocked dependency/network workflow; no network override was attempted |

These are open verification items, not passed checks.
