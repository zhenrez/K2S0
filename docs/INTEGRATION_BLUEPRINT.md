# Integration blueprint

## 1. Binding to the ARGO cascade

| Ecosystem layer | Digital Twin binding | Direction | Contract |
| --- | --- | --- | --- |
| MARC-0 | Supplies invariant-generating substrate and schema grammar | into DT | schema registry + signed invariant bundle |
| CHIP | Resolves deterministic IDs, patterns, ontology, and ARGOCell compilation | bidirectional | ARGOCell v1 + event lineage |
| Constitution | Signs root constraints, consent policy, authority policy | into DT | versioned policy bundle |
| MARC-1 | Consumes projections; submits decisions/outcomes as new evidence | both | Projection + ActionEnvelope + OutcomeReceipt |
| Wausauk33 | Consumes world/role projections and publishes environment observations | both | world-state adapter |
| MorphIQ | Proposes model/compiler changes and evaluates canaries | both, gated | ModelChangeProposal; never direct root mutation |
| QuestN | Review, correction, consent, approval, time travel, readiness | both | REST + WebSocket |

## 2. Ports and adapters

The package defines ports in **src/argo_dt/ports.py**. Production adapters:

| Port | Default adapter | Alternatives |
| --- | --- | --- |
| EventStore | PostgreSQL + transactional outbox | Restate state; SQLite edge |
| BronzeVault | S3-compatible object store with envelope encryption | local encrypted filesystem |
| TelemetryPublisher | NATS JetStream | Kafka/Redpanda |
| ConsentStore | PostgreSQL with RLS | external policy registry |
| PolicyEvaluator | OPA/Rego sidecar or embedded WASM | Cedar/custom deterministic engine |
| ModelWorker | Portkey-routed provider or local model | any versioned worker |
| Connector | HPI-style provider module | Screenpipe, message/export adapters |
| ProjectionSink | signed object store / direct response | MCP resource, agent runtime |

Adapters may be replaced independently. They may not weaken idempotency,
ordering, temporal, provenance, minimization, or policy semantics.

## 3. Transport selection

| Path | Protocol | Why | Required semantics |
| --- | --- | --- | --- |
| Device/capture → ingest | gRPC bidirectional stream | binary framing, multiplexing, flow control, typed acks | mTLS, batches, per-record errors, resume token |
| Internal event distribution | NATS JetStream | low latency, subject partitioning, replay | at-least-once + consumer dedup |
| Browser/mobile updates | WebSocket | low-overhead bidirectional review/update channel | cursor resume, bounded queue, heartbeat |
| Admin/query/control | REST/JSON | ecosystem compatibility and inspectability | OIDC, idempotency header, ETag/sequence |
| Agent/tool discovery | MCP through ContextForge | governed tool/resource discovery | projection-only resources |
| Agent-to-agent | A2A through gateway | external agent interoperability | signed agent identity and scoped capability |
| Long-running pipelines | Restate service calls/workflows | durable retries and entity state | deterministic IDs, idempotent effects |
| Bulk historical import | object manifest + REST job control | resumability and checksum validation | immutable manifest, cursor, loss report |

REST is not used for unbounded telemetry. WebSocket is not the source of truth.
The event bus does not bypass the policy boundary.

## 4. Public API surfaces

### Data plane: gRPC

Defined in **proto/argo/dt/v1/twin.proto**.

- StreamTelemetry
- SubscribeState
- GetState
- CompileProjection
- RevokeProjection
- ForkSimulation
- AppendSimulation
- ProposeSimulationPromotion
- CheckAction

### Control plane: REST

Defined in **openapi/dt-v1.yaml**.

- GET /v1/twins/{twin_id}/state
- POST /v1/twins/{twin_id}/projections
- POST /v1/twins/{twin_id}/projections/{projection_id}/revoke
- POST /v1/twins/{twin_id}/simulations
- POST /v1/actions/check
- GET /healthz
- GET /readyz

The K2S0 design packet's evidence, claim, correction, referral, interview,
compile, evaluation, readiness, and audit surfaces should be added as the next
API slice. They are intentionally not faked by generic CRUD routes.

### WebSocket

Endpoint:

~~~text
wss://host/v1/twins/{twin_id}/events?after_sequence=1234
~~~

Server frames:

~~~json
{
  "type": "event",
  "twin_id": "…",
  "sequence": 1235,
  "event_type": "ClaimAccepted",
  "recorded_at": "2026-09-04T00:00:00Z",
  "payload": {}
}
~~~

Control frames are limited to subscribe, acknowledge, heartbeat, and close.
Domain mutations use REST/gRPC so idempotency and concurrency preconditions are
not ambiguous. A slow consumer is disconnected with its last acknowledged
sequence and must replay.

## 5. Event subjects and ownership

~~~text
argo.dt.<tenant>.<twin>.authoritative.<event_type>
argo.dt.<tenant>.<twin>.projection.<event_type>
argo.dt.<tenant>.<twin>.simulation.<branch>.<event_type>
argo.dt.<tenant>.<twin>.deadletter.<stage>
~~~

The subject name is routing metadata, not authorization. Every payload carries
identity, purpose, sensitivity, schema version, trace context, and event hash.

| Producer | Permitted events |
| --- | --- |
| ingest service | EvidenceIngested, EvidenceDeleted, AcquisitionCompleted |
| identity worker | EntityLinked/Unlinked proposals |
| adjudication worker | ClaimProposed, ContradictionDetected, ReferralCreated |
| human review service | ClaimAccepted/Contested/Retired, CorrectionRecorded |
| compiler | KernelCompiled, EvaluationCompleted, ReadinessChanged |
| projection service | ProjectionIssued/Revoked |
| simulation service | simulation-plane events only |
| downstream agent | AgentDecisionObserved and OutcomeSubmitted only |

The service identity policy rejects every producer/event combination not
explicitly listed.

## 6. Canonical schemas

### EventEnvelope

| Field | Rule |
| --- | --- |
| twin_id + sequence | Per-twin total order |
| event_id | Unique event identity |
| idempotency_key | Unique within twin; retries return original result |
| occurred_at | Source/valid occurrence time |
| recorded_at | Ledger commit time |
| plane | authoritative, projection, or simulation |
| previous_hash + event_hash | Tamper-evident chain |
| causation_id + correlation_id | Workflow and trace lineage |
| schema_version | Exact decoder contract |
| payload | Event-specific versioned object |

### ARGOCell

Machine-readable JSON Schema: **schemas/argocell-v1.schema.json**.

### ProjectionRequest

Required dimensions are subject, twin, recipient, purpose, requested fields,
maximum sensitivity, valid-time view, and recorded-time view. Policy refuses
partial implicit grants; the caller must request a known surface.

### ProjectionReceipt

Stores consent and policy versions, source sequence, disclosed fields, private
source claim references, artifact hash, issue/expiry time, and revocation.
The external recipient need not receive private claim IDs.

### ActionEnvelope

Carries principal and agent identities, purpose, target, capabilities, impact,
reversibility, evidence, constraint version, TTL, and idempotency key. It never
derives authority from a ProjectionReceipt.

## 7. Synchronization protocol

### Online write

~~~mermaid
sequenceDiagram
  participant D as Device
  participant I as DT ingest
  participant E as Event store
  participant B as Stream bus
  participant P as Projector

  D->>I: batch + expected sequence + idempotency
  I->>I: authenticate, validate, classify
  I->>E: append event + outbox atomically
  E-->>I: committed sequence + hash
  I-->>D: ack + resume token
  E->>B: outbox publish
  B->>P: at-least-once event
  P->>P: deduplicate and apply
~~~

### Offline/multi-device merge

- No last-write-wins for claims or evidence.
- Independent observations append concurrently after resolving the current
  stream head.
- Deterministic source IDs and idempotency keys collapse retransmission, not
  distinct evidence.
- Conflicting claims remain separate and create a contradiction object.
- Entity merge is an event and can be reversed.
- UI edits generate corrections/supersession; they do not rewrite history.

CRDTs are suitable for non-authoritative annotations and draft UI state, not
for collapsing epistemic contradictions.

## 8. Connector contract

Every connector emits an acquisition manifest:

~~~text
connector_id, connector_version, source_system, source_account
capture_method, authorization, source_time_range
records_seen, ingested, rejected, errors, warnings
checksums, cursor, known_lossiness, permissions
~~~

Records require:

- stable source record ID;
- content hash;
- authorship tier;
- valid-time interval and uncertainty;
- independence/session group;
- media type and encoding;
- sensitivity and third-party rights;
- source cursor and transform lineage.

HPI provider modules are the adapter pattern. Screenpipe is a capture source,
not the canonical store. WeClone is a communication/voice dataset producer,
not the person model.

## 9. Versioning and compatibility

- Package/API versions use semantic versioning.
- Event and ARGOCell schemas use explicit identifiers.
- Consumers must ignore unknown optional fields but reject unknown required
  schema major versions.
- Event upcasters create a new view; they never rewrite historical payloads.
- Prompt/model/compiler changes produce a new artifact version.
- CI maintains golden fixtures for previous two schema minors and migration
  tests for every supported major.

