# Host merge contract

## 1. Current status

This repository is the standalone K2S0 implementation. At inspection, its
default branch contained only a twelve-byte README and the Boost Software
License, so the implementation can occupy the repository root without
displacing existing code.

This is not yet a completed merge into the broader ARGO ecosystem. No host
package graph, deployment configuration, or CHIP/MARC/Wausauk33/MorphIQ/QuestN
source APIs were available.

Initial implementation branch:

~~~text
dt/k2s0-core-v1
~~~

Standalone repository root:

~~~text
/
~~~

If this repository is later vendored into a monorepo, use **modules/dt/** and
move the entire module without flattening its protocol/schema/policy
boundaries.

## 2. Required host inputs

| Input | Why it is needed | Can the scaffold proceed without it? |
| --- | --- | --- |
| Broader ecosystem source tree + current branch | Real imports, dependency graph, and merge safety | No for ecosystem integration |
| Repository instructions/CI | Formatting, testing, generated-code policy | No |
| Existing ARGOCell/UKIR schema | Prevent competing canonical types | No |
| Identity and tenancy model | Subject isolation and service claims | No |
| Current data/event infrastructure | Select adapters instead of duplicating them | No |
| Deployment topology | Trust zones, network policy, HA | No |
| Existing CHIP/MARC APIs | Implement real adapter signatures | No |
| Traffic/retention/SLO baselines | Capacity and partition sizing | No |

## 3. Stable surfaces

The host may replace implementations, but these semantics are merge
preconditions:

- four distinct identity kinds;
- EventEnvelope ordering/idempotency/hash/plane fields;
- dual valid/recorded time;
- provenance-required claim/pattern/artifact cells;
- observed/desired/predicted state separation;
- simulation namespace isolation;
- projection before disclosure;
- consent separate from action authority;
- immutable/root-signed constitutional constraint precedence.

## 4. Adapter mapping template

Complete this table during the host merge:

| DT port | Existing host component | Adapter owner | Gap |
| --- | --- | --- | --- |
| EventStore | UNKNOWN | UNKNOWN | expected-sequence + idempotency + replay |
| BronzeVault | UNKNOWN | UNKNOWN | encryption + rights + deletion |
| ConsentStore | UNKNOWN | UNKNOWN | purpose/recipient/field/TTL |
| PolicyEvaluator | UNKNOWN | UNKNOWN | default deny + audit |
| TelemetryPublisher | UNKNOWN | UNKNOWN | replay + backpressure |
| ModelWorker | UNKNOWN | UNKNOWN | prompt/model/version receipts |
| Connector registry | UNKNOWN | UNKNOWN | manifests + cursors |
| Identity verifier | UNKNOWN | UNKNOWN | principal/agent/avatar distinction |
| ProjectionSink | UNKNOWN | UNKNOWN | signed delivery + revocation |

Unknown is deliberately represented as unknown; it is not a negative
assessment of the host.

## 5. Merge sequence

1. Create branch from the host's current integration base.
2. Add protocol/schema/policy files and run compatibility checks.
3. Map or alias existing canonical types; do not create a second ARGOCell.
4. Import reference tests as black-box conformance tests.
5. Implement host adapters one at a time.
6. Wire a single evidence→claim→review→projection vertical slice.
7. Add simulation isolation and action authority checks.
8. Run repository-wide tests, load tests, and security checks.
9. Produce an explicit divergence ledger for every changed invariant.
10. Merge only after a canary projection is byte/semantically equivalent
    across reference and production implementations.

## 6. Conflict rules

- Host infrastructure wins on deployment mechanics when it preserves DT
  semantics.
- Existing canonical ARGOCell/UKIR definitions win after a field-by-field
  compatibility audit.
- K2S0 invariants win over convenience or repository-specific patterns.
- Constitution wins over MorphIQ-generated changes.
- Human-reviewed evidence wins over simulation output as an evidence class;
  this does not automatically make every self-report factually superior.
- No conflict is resolved by silently deleting an alternative. Record it in a
  divergence ledger as defect, ambiguity, stronger/over/under-conformance, or
  implementation detail.

## 7. Definition of integrated

The branch is integrated only when:

- host CI builds and tests it;
- host identity reaches DT policy decisions;
- one real historical and one real-time connector ingest;
- state replay survives restart;
- QuestN can inspect/correct/revoke;
- MARC-1 receives only a purpose-bound projection;
- Wausauk33/MorphIQ cannot mutate canonical state directly;
- an ActionEnvelope without a separate grant is denied;
- a simulation contamination attempt fails;
- deletion invalidates affected artifacts;
- operational dashboards and alerts exist;
- rollback and recovery are tested.
