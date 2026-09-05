# Validation record

Date: 2026-09-05 UTC

## Passed

| Check | Result |
| --- | --- |
| Python compile | src, tests, and scripts compiled successfully under Python 3.12.13 |
| Invariant and persistence tests | 51/51 passed |
| Demo | Evidence → claim → human review → consented projection → receipt completed |
| JSON contracts | All five JSON Schema files parsed successfully |
| OpenAPI syntax | YAML parsed successfully with the available YAML parser |
| Event integrity | Hash-chain verification passed |
| SQLite migration | Executed successfully in an in-memory SQLite database |
| Snapshot replay | Full replay and snapshot + 100-event tail produced equal state |
| Transactional outbox | Atomic rollback, retry, lease, and process-reopen recovery passed |
| Deletion propagation | Evidence → claim → projection invalidation queue passed |
| Bronze encryption | AES-GCM round trip, tamper rejection, subject isolation, and no plaintext-at-rest assertion passed |
| Projection non-interference | Public payload and receipt omit claim, evidence, model-trace, and sensitivity identifiers |
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

## Local smoke benchmark

The SQLite/WAL adapter appended and verified 10,000 events, including an
outbox pointer per event, at approximately 8,596 events/second in this ephemeral
environment. Full replay of 10,000 events had a 531.380 ms median; replay from
a hash-verified snapshot plus 100 tail events had a 5.140 ms median (103.38×)
across 10 iterations. Both replay paths produced identical state.

This is not a production capacity result. It excludes network, authentication,
external publication, concurrent writers, multi-tenant load, disk-failure
recovery, and sustained soak. Production SLOs in docs/PERFORMANCE_SECURITY.md
remain targets until the specified load and soak tests pass.

## Not executable in this workspace

| Check | Reason / required environment |
| --- | --- |
| Protobuf generation | protoc/buf toolchain is not present |
| OPA policy tests | OPA toolchain is not present |
| NATS/Restate integration | No service topology was supplied |
| Host-repository integration | Host source tree was not supplied |
| Ruff/mypy | Optional development tools are not installed |
| Wheel build | Package command triggered a blocked dependency/network workflow; no network override was attempted |

These are open verification items, not passed checks.
