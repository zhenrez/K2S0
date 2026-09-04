# Implementation roadmap and acceptance gates

## 1. Delivery strategy

Build a narrow vertical slice first, but do not collapse the retained
architecture. Each phase adds executable capability behind stable ports.

| Phase | Scope | Exit gate |
| --- | --- | --- |
| DT-0 — contracts | ARGOCell, events, consent, action, projection, identity kinds, schema registry | golden schemas; compatibility tests; invariant owners assigned |
| DT-1 — authoritative ledger | PostgreSQL stream head/event/outbox, Bronze object adapter, evidence ingest | idempotency/CAS/chain/deletion tests; restore drill |
| DT-2 — synchronization | gRPC stream, NATS publish/replay, WebSocket cursors | p99/throughput target at 2× expected peak; bounded-memory soak |
| DT-3 — epistemic core | normalized events, claim/counterevidence, contradictions, review, time travel | all source→claim→evidence traces reversible; no silent overwrite |
| DT-4 — compiler/projection | kernel artifacts, loss manifests, OPA consent and receipts | field non-interference; revocation race; generic denials |
| DT-5 — simulation | branch/fork, deterministic scenario inputs, evaluation, review referral | mutation test proves zero automatic evidence contamination |
| DT-6 — ecosystem adapters | CHIP, MARC-1, Wausauk33, MorphIQ, QuestN, ContextForge | contract tests in the actual host repository |
| DT-7 — hardening | Rust hot path, HA, operational automation, security review | SLO soak, chaos suite, audit, recovery exercise |

## 2. Work packages

### DT-0

Status: complete in 0.2.0 for the standalone reference boundary. Host-language
generation and host-specific invariant ownership remain conditional on the
actual ecosystem repository becoming available.

- Freeze ARGOCell v1 and secured EventEnvelope v2 semantics; preserve v1
  replay/hash compatibility through the explicit upcaster.
- Convert every invariant in the K2S0 packet into a test identifier.
- Add JSON Schema, protobuf, OpenAPI, SQL migration, and policy bundle checks.
- Define event ownership matrix and identity/capability claims.
- Create fixture corpus with corrections, contradictions, temporal changes,
  deletion, revoked consent, and simulation attempts.

### DT-1

- Implement Rust EventStore port with SERIALIZABLE append transaction.
- Implement encrypted Bronze object adapter and acquisition manifest.
- Add deterministic IDs for connector/source records.
- Implement snapshots, replay, integrity verification, and deletion dependency
  queue.
- Add transactional outbox worker with idempotent publisher.

### DT-2

- Generate gRPC stubs and enforce message/record byte limits.
- Add stream-level and record-level acknowledgements.
- Implement NATS subject topology and durable consumers.
- Add WebSocket cursor/heartbeat/close protocol.
- Expose OpenTelemetry metrics, traces, and structured redacted logs.

### DT-3

- Implement canonical event types for evidence, identity, claims,
  contradictions, adjudication, and corrections.
- Add lineage ancestry and independent-evidence grouping.
- Build valid-time and recorded-time queries.
- Implement Point/Line/Face/Volume/Root as typed ARGOCell relations, not a
  hard dependency on a graph database.
- Add gap-directed elicitation as a workflow producing Bronze evidence.

### DT-4

- Implement all kernel artifact families.
- Store compiler/model/prompt/tool versions and loss accounting.
- Compile minimal input packages before external model calls.
- Evaluate consent and policy before selecting source material.
- Sign receipts, cache by full authorization/version key, and invalidate on
  source/policy/consent change.

### DT-5

- Store simulation branches in an isolated namespace with immutable base
  sequence.
- Label counterfactual and causal assumptions.
- Integrate optional Z3/Pyomo/HiGHS/DoWhy workers through typed ports.
- Create review referrals; require new human observation/correction before
  authoritative promotion.

### DT-6

- CHIP: implement deterministic ARGOCell ID and ontology adapters.
- MARC-1: add read-only projection resource and write-only outcome submission.
- Wausauk33: map world observations to evidence and world-role projections.
- MorphIQ: accept model change proposals only; canary, evaluate, and require
  constitutional promotion.
- QuestN: add review, correction, consent, approval, time-travel, and
  uncertainty interfaces.
- ContextForge: register projection/readiness/action-check tools; never expose
  Bronze, direct claim mutation, or secrets.

### DT-7

- Port reference hash/event/policy/projection semantics to Rust.
- Add multi-AZ PostgreSQL/object store/NATS deployment.
- Add key rotation, SBOM, artifact signing, SAST/DAST, image scanning.
- Run performance, failover, disaster recovery, privacy, and security gates.
- Freeze a production compatibility baseline.

## 3. Verification matrix

| Requirement | Mechanical evidence |
| --- | --- |
| Provenance | schema constraint + property test + reverse lineage query |
| No silent overwrite | event replay fixture with supersession |
| Temporal correctness | bitemporal model-based tests |
| Contradiction preservation | competing-claim fixture and review workflow |
| Simulation isolation | mutation tests attempting every promotion path |
| Consent minimization | non-interference/differential projection tests |
| Authority separation | consent-only requests fail ActionEnvelope policy |
| Fault tolerance | process/network/database failure injection |
| Low latency | reproducible load-test report with p50/p95/p99 |
| Deletion | transitive invalidation and verified deletion receipt |
| Model independence | same source fixture through two workers, canonical data unchanged |
| Schema evolution | old event fixture replay after every release |

## 4. Decision gates

Do not advance from reference scaffold to production until:

1. The actual host repository and its AGENTS/contribution rules are available.
2. The internal meaning of any literal **dt** repository prefix is resolved.
3. Database, event bus, durable runtime, identity provider, object store, and
   policy-engine standards are reconciled with existing infrastructure.
4. Every direct third-party dependency has a pinned snapshot and license,
   maintenance, security, and runtime assessment.
5. Traffic, retention, privacy region, recovery, and offline-device
   requirements have numeric baselines.

These gates can change adapter choices; they do not change the K2S0 semantic
invariants.

## 5. First sprint

1. Import this folder under the host's module boundary.
2. Run the reference tests unchanged.
3. Generate host-language types from protobuf/JSON Schema.
4. Implement PostgreSQL EventStore and BronzeVault adapters.
5. Run the same invariant suite against SQLite and PostgreSQL.
6. Connect one HPI-style export source and one Screenpipe-style live source.
7. Expose one QuestN review flow and one MARC-1 read-only projection.
8. Measure and record baseline latency/throughput before optimization.
