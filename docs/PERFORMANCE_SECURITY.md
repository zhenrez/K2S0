# Performance, scalability, resilience, and security

## 1. Service objectives

These are proposed acceptance targets, not measured claims. The included
SQLite benchmark is a local smoke tool and must not be presented as production
capacity evidence.

| Signal | Initial SLO target | Measurement boundary |
| --- | --- | --- |
| Single telemetry append | p99 ≤ 25 ms | authenticated ingest receipt to durable commit, same region |
| Batched telemetry append | ≥ 25,000 records/s/node | 100–500 record batches, payloads externalized |
| State delta visibility | p99 ≤ 100 ms | commit to subscribed in-region consumer |
| Projection compile, cached/no model | p99 ≤ 75 ms | request to signed receipt |
| Projection compile with model | p95 ≤ 2 s or declared async | excludes provider queue beyond timeout budget |
| Availability | 99.95% control/query, 99.9% ingest | monthly, excluding declared maintenance |
| Recovery point | 0 committed ledger events | synchronous database quorum |
| Recovery time | ≤ 15 minutes regional service | tested restore/failover |
| Stream replay | ≥ 7 days hot | durable ledger remains canonical |

## 2. Performance design

### Write path

- Partition by twin ID; never serialize unrelated twins.
- Batch 100–500 small records or 1–5 MB, whichever comes first.
- Store large binary evidence in the Bronze object store before ledger commit;
  event payload carries content hash and object reference.
- Use prepared statements, connection pooling, and one transaction for stream
  head + event + outbox.
- Return after durable commit, not after all derived indexes update.
- Idempotency removes retry duplicates; it does not merge distinct evidence.

### Read path

- Rebuild from snapshot + tail events.
- Snapshot every 1,000 events initially, then adapt by replay cost.
- Cache compiled artifacts by source sequence + bitemporal instant + purpose +
  recipient + policy version + compiler version.
- Invalidate on source correction/deletion, consent change, policy change, or
  schema/compiler version.
- Use PostgreSQL FTS/vector or dedicated indexes only as candidate retrieval;
  rehydrate canonical claims by ID.

### Backpressure

- gRPC flow control limits in-flight batches.
- NATS consumers use explicit ack, bounded max-deliver, and dead-letter lanes.
- WebSocket queues are bounded; slow clients receive a close reason and resume
  from the last sequence.
- Heavy workers advertise capacity and pause partition consumption.
- No unbounded Python/JavaScript queue exists in the production path.

### Hot-path implementation

Port EventEnvelope canonicalization, validation, batching, hash chaining,
projection minimization, and WebSocket fan-out to Rust. Keep Python for source
adapters, evaluation, research, and model orchestration. Cross-language
boundaries use protobuf, not shared in-process objects.

## 3. Scaling model

| Axis | Partition/shard | Coordination |
| --- | --- | --- |
| Ledger writes | twin ID | one stream head CAS |
| Bronze bytes | subject ID/content hash | object store |
| Normalization | connector/source type | event sequence checkpoint |
| Claim/model work | subject + domain | durable workflow |
| Vector/search | tenant + subject | rebuildable index cursor |
| Projection | subject + recipient + purpose | policy/consent version |
| Simulation | branch ID | immutable base sequence |

Avoid multi-master ledger writes. Edge devices queue locally and synchronize
through expected-sequence retries. A future region split assigns a twin a home
region; reads may replicate, writes route home.

## 4. Failure model

| Failure | Behavior | Data consequence |
| --- | --- | --- |
| Duplicate ingest | Return original committed result | none |
| Stale expected sequence | 409/ABORTED with current head | caller reloads/rebases |
| Event bus outage | Outbox accumulates and retries | commit remains durable |
| Projector crash | Replay from last checkpoint | no canonical loss |
| Model provider outage | deterministic fallback or degraded artifact | no fabricated fields |
| Index corruption | rebuild from ledger | slower retrieval only |
| Bronze unavailable | reject evidence commit or declare explicit pending state | never create false provenance |
| Policy engine unavailable | deny projection/action | availability sacrificed for safety |
| Consent revoked | append revocation, invalidate caches, deny future access | prior receipt retained for audit |
| Evidence deleted | tombstone + dependency walk | claims/artifacts marked stale |
| Slow real-time client | disconnect and resume | bounded memory |

Every retrying side effect needs one of: idempotency key, deduplication record,
or compensating action. “Exactly once” is an end-to-end property, not a broker
marketing setting.

## 5. Trust zones

~~~mermaid
flowchart LR
  Z0["Z0: raw vault + keys"]
  Z1["Z1: ledger + normalizer"]
  Z2["Z2: compiler/model workers"]
  Z3["Z3: projection/policy"]
  Z4["Z4: agents/UI/worlds"]
  Z5["Z5: external services"]

  Z0 --> Z1 --> Z2 --> Z3 --> Z4 --> Z5
~~~

Data is minimized at every outward transition. Reverse calls are authenticated
requests or new evidence submissions; outer zones cannot directly query inner
stores.

## 6. Security controls

### Identity and transport

- Workload identity: SPIFFE-compatible service identity or equivalent.
- User identity: OIDC with short-lived tokens and phishing-resistant MFA.
- Service transport: TLS 1.3; mTLS inside the trust boundary.
- Agent identity: map Agent Identity Protocol credentials to a local service
  identity and explicit capability grants.
- Never infer human-principal identity from an avatar, model, wallet, or voice.

### Authorization

- Deny by default at database, API, policy, and connector layers.
- PostgreSQL RLS scopes subject data; app roles cannot disable RLS.
- Consent grants control knowledge disclosure.
- Authority grants control external actions.
- Purpose, recipient, sensitivity, field set, TTL, reversibility, impact, and
  domain are independently checked.
- Policy inputs and decision outputs are audit events.

### Secrets and plugins

- Infisical stores service secrets and encryption-key references.
- Agent Vault proxies credential use so agents do not receive reusable secrets.
- Untrusted connectors/tools run as Wasmtime components with explicit
  filesystem, network, clock, memory, fuel, and timeout capabilities.
- ContextForge exposes only projection tools/resources and policy-approved
  external capabilities.

### Data protection

- Per-subject envelope encryption for Bronze and sensitive compiled artifacts.
- KMS/HSM master keys; rotation changes data keys without rewriting semantics.
- Separate key custody from database administration.
- Content hashes are over canonical plaintext metadata/payload before
  encryption; hashes are themselves treated as sensitive correlation data.
- Backups include event ledger, Bronze bytes, policy/consent versions, schema
  registry, key metadata, and dependency indexes.
- Restore drills verify hash chain and projection revocation state.

### Audit

- Append-only action attempt, policy decision, outcome receipt, projection,
  correction, consent, and administration events.
- Hash chaining is tamper-evident, not magic immutability; periodically anchor
  Merkle roots in an independently controlled log.
- Never log raw evidence, prompts with private content, access tokens, or
  excluded projection fields.
- Outcome verification is separate from recording an allowed attempt.

## 7. Threats and mitigations

| Threat | Primary mitigation |
| --- | --- |
| Prompt injection in captured content | Treat content as data; sandbox workers; policy outside LLM |
| Cross-subject retrieval | RLS + explicit subject scope + negative isolation tests |
| Membership inference through errors | Generic denial; no excluded-field metadata |
| Model self-contamination | source ancestry graph; generated outputs marked derived |
| Simulation laundering | distinct event plane and review-only promotion |
| Replay/duplicate action | TTL + idempotency + outcome receipt |
| Privilege accumulation | short-lived scoped grants; no consent-to-authority conversion |
| Credential exfiltration | credential proxy and egress policy |
| Malicious connector | WASI sandbox, signed package, capability manifest |
| Supply-chain compromise | pinned commits/digests, SBOM, provenance, signature verification |
| Silent model/prompt drift | version/hash every material transformation |
| Deletion incompleteness | dependency graph, invalidation queue, verified deletion report |

## 8. Required validation before production

- 24-hour soak at 2× forecast peak.
- Per-twin hot-key test and multi-tenant isolation test.
- Kill -9 and network-partition fault injection during every pipeline stage.
- Event/outbox atomicity and replay determinism tests.
- Backup restore plus hash-chain verification.
- Consent revocation race tests.
- Projection non-interference and excluded-field leakage tests.
- Simulation contamination mutation tests.
- Property-based schema/upcaster tests.
- Red-team prompt injection, SSRF, path traversal, deserialization, and
  capability-escalation tests.
- Independent privacy and security review.

