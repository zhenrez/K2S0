# Validation record

Date: 2026-09-04 UTC

## Passed

| Check | Result |
| --- | --- |
| Python compile | src, tests, and scripts compiled successfully under Python 3.12.13 |
| Invariant tests | 24/24 passed |
| Demo | Evidence → claim → human review → consented projection → receipt completed |
| JSON contracts | All four JSON Schema files parsed successfully |
| OpenAPI syntax | YAML parsed successfully with the available YAML parser |
| Event integrity | Hash-chain verification passed |
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

## Local smoke benchmark

The SQLite reference adapter appended and verified 10,000 events at
approximately 9,795 events/second in this ephemeral environment.

This is not a production capacity result. It excludes network, authentication,
Bronze object I/O, PostgreSQL replication, outbox publication, contention,
multi-tenant load, and failure injection. Production SLOs in
docs/PERFORMANCE_SECURITY.md remain targets until the specified load and soak
tests pass.

## Not executable in this workspace

| Check | Reason / required environment |
| --- | --- |
| Rust hot-path build | Rust toolchain is not installed |
| Protobuf generation | protoc/buf toolchain is not present |
| OPA policy tests | OPA toolchain is not present |
| PostgreSQL migration | No PostgreSQL service was supplied |
| NATS/Restate integration | No service topology was supplied |
| Host-repository integration | Host source tree was not supplied |
| Ruff/mypy | Optional development tools are not installed |
| Wheel build | Package command triggered a blocked dependency/network workflow; no network override was attempted |

These are open verification items, not passed checks.
