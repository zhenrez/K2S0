# Performance, scalability, resilience, and security

## 1. Service objectives

These are proposed acceptance targets, not measured claims. The included
SQLite benchmarks are local smoke tools and must not be presented as production
capacity evidence.

| Signal | Initial SLO target | Measurement boundary |
| --- | --- | --- |
| Single telemetry append | p99 ≤ 25 ms | authenticated ingest receipt to durable commit, same region |
| Batched telemetry append | ≥ 25,000 records/s/node | 100–500 record batches, payloads externalized |
| State delta visibility | p99 ≤ 100 ms | commit to subscribed in-region consumer |
| Projection compile, cached/no model | p99 ≤ 75 ms | request to signed receipt |
| Projection compile with model | p95 ≤ 2 s or declared async | excludes provider queue beyond timeout budget |
| Availability | 99.95% control/query, 99.9% ingest | monthly, excluding declared maintenance |
| Recovery point | 0 acknowledged events for process crash | FULL-sync SQLite commit |
| Recovery time | ≤ 15 minutes | tested file + Bronze restore procedure |
| Stream replay | ≥ 7 days hot | durable ledger remains canonical |

## 2. Performance design

### Write path

- Serialize writes per SQLite database; shard by tenant/profile only after
  measurement proves one file insufficient.
- Batch 100–500 small records or 1–5 MB, whichever comes first.
- Store large binary evidence in the Bronze object store before ledger commit;
  event payload carries content hash and object reference.
- Use prepared statements and one `BEGIN IMMEDIATE` transaction for event,
  dependency index, invalidation queue, and outbox.
- Return after durable commit, not after all derived indexes update.
- Idempotency removes retry duplicates; it does not merge distinct evidence.

### Read path

- Rebuild from snapshot + tail events.
- Cache only the latest state in a bounded, per-process LRU. Verify its sequence
  and hash against the SQLite head before reuse; return caller-safe copies and
  never cache historical/as-of queries.
- Snapshot every 1,000 events initially, then adapt by replay cost.
- Retain the newest three snapshots per twin by default; snapshots are caches,
  while the event stream remains canonical.
- Cache compiled artifacts by source sequence + bitemporal instant + purpose +
  recipient + policy version + compiler version.
- Invalidate on source correction/deletion, consent change, policy change, or
  schema/compiler version.
- Use SQLite FTS5/vector extensions or a derived Neo4j/search adapter only for
  candidate retrieval; rehydrate canonical claims by ID.

### Backpressure

- gRPC adapters reject more than 1 MiB/record, 4 MiB/batch, or 500
  records/batch using the concrete serializer's exact byte count.
- State replay reads at most 256 events/page and permits at most 128
  unacknowledged notification frames by default.
- NATS consumers use explicit ack, bounded max-deliver, and dead-letter lanes.
- WebSocket queues are bounded; slow clients receive a close reason and resume
  from the last acknowledged authenticated cursor.
- Heavy workers advertise capacity and pause partition consumption.
- No unbounded Python/JavaScript queue exists in the production path.

The replay/live handoff subscribes first, captures the SQLite stream head,
pages and hash-verifies through that position, then drains live delivery while
discarding duplicate sequences. A detected live gap is recovered from SQLite
in bounded pages. Missing or chain-invalid positions fail closed.

`SyncMetrics.snapshot()` exposes only aggregate replay, live, duplicate,
frame, acknowledgement, heartbeat, close, integrity-failure, and backpressure
counters. OpenTelemetry adapters must not attach twin IDs, subject IDs, cursor
tokens, event IDs, payloads, or hashes as attributes.

`OpenTelemetrySyncExporter` enforces this with a fixed low-cardinality
attribute allow-list and exports only new counter deltas. The embedded outbox
relay explicitly yields between immediately completed in-process publications,
preventing a caught-up SQLite writer from starving stream readers.

### Disk footprint

- Outbox rows store an event reference and delivery metadata, not a second copy
  of the event payload.
- Prune delivered outbox rows after the configured audit window (seven days in
  the embedded example); never prune pending rows.
- Use incremental vacuum and a WAL truncate checkpoint during a quiet
  maintenance window. Neither operation deletes canonical events.
- Bronze deletion unlinks the encrypted object but cannot promise physical
  overwrite on SSD/copy-on-write media; cryptographic erasure requires a
  per-subject key provider with key destruction.

### Hot-path implementation

Profile before adding another runtime. Preserve EventEnvelope canonicalization,
hash chaining, and projection semantics if a measured hotspot is later moved
to native code. Cross-language boundaries use protobuf, not shared in-process
objects.

## 3. Scaling model

| Axis | Partition/shard | Coordination |
| --- | --- | --- |
| Ledger writes | SQLite file, then twin ID | one writer transaction + stream CAS |
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
- Resume cursors: HMAC-SHA256 with an injected, durable 256-bit-or-stronger
  secret reference; rotate with an overlap window no longer than cursor TTL.
- Cursor rotation: one active key and at most seven explicit overlap keys;
  atomically replace the immutable ring and remove retired keys after TTL plus
  allowed clock skew. Key material never appears in cursor or telemetry output.
- Agent identity: map Agent Identity Protocol credentials to a local service
  identity and explicit capability grants.
- Never infer human-principal identity from an avatar, model, wallet, or voice.

### Authorization

- Deny by default at database, API, policy, and connector layers.
- Separate SQLite files or encrypted volumes scope tenants; every query and
  service transition also carries explicit subject scope.
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
| Cross-subject retrieval | database-per-tenant + explicit subject scope + negative isolation tests |
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

The `scripts/soak_sync.py` harness measures sustained SQLite commit-to-stream
visibility, achieved throughput, bounded unacknowledged frames, traced peak
memory, broker delivery, and errors. CI runs a two-second regression smoke only;
the 24-hour/2× gate still requires deployment traffic and hardware baselines.
