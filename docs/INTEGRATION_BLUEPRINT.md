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
| EventStore | SQLite/WAL + transactional outbox | sharded SQLite files; Restate state |
| BronzeVault | S3-compatible object store with envelope encryption | local encrypted filesystem |
| TelemetryPublisher | bounded in-process broker | optional NATS JetStream; Kafka/Redpanda |
| ConsentStore | SQLite in the same tenant boundary | external policy registry |
| PolicyEvaluator | OPA/Rego sidecar or embedded WASM | Cedar/custom deterministic engine |
| ModelWorker | Portkey-routed provider or local model | any versioned worker |
| Connector | HPI-style provider module | Screenpipe, message/export adapters |
| ProjectionSink | signed object store / direct response | MCP resource, agent runtime |

Adapters may be replaced independently. They may not weaken idempotency,
ordering, temporal, provenance, minimization, or policy semantics.

The embedded profile supplies durable synchronization directly over the SQLite
ledger and in-process outbox. `DurableSubscription` subscribes to live delivery
before capturing the SQLite head, pages replay to that head, then deduplicates
the live handoff by verified sequence and chain position.

Executable adapters are under `src/argo_dt/adapters/`:

| Adapter | Binding | Required deployment input |
| --- | --- | --- |
| `StateWebSocketApp` | ASGI 3 WebSocket state stream | OIDC/mTLS authenticator + ASGI server |
| `DigitalTwinGrpcAdapter` | telemetry/state bidi RPCs + health | generated protobuf + grpcio server |
| `JetStreamPublisher` | transactional-outbox publication | nats.py JetStream context + tenant |
| `JetStreamConsumer` | manual ack, bounded retry, payload-free DLQ | durable consumer + derived handler |
| `OpenTelemetrySyncExporter` | sync counters → OTel deltas | configured MeterProvider/export pipeline |

Every network adapter requires an injected `Authenticator`. It receives raw
transport credentials and returns a verified `ActorContext` plus scopes;
transport code never manufactures roles from caller-controlled payloads.

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

The 0.5.0 reference adapter executes `StreamTelemetry`, `SubscribeState`, and
`Health`. Remaining RPCs fail closed as `UNIMPLEMENTED` until a host package
binds its identity, consent, policy, and simulation services. Generate Python
modules with `make grpc-generate`; CI imports the generated contract.

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
wss://host/v1/twins/{twin_id}/events
~~~

Required subprotocol: `argo.dt.state-stream.v1`. Authentication and the
`dt.stream` scope are checked before ASGI acceptance. The initial subscribe
frame must arrive within 10 seconds; control frames are capped at 16 KiB and
client silence at 90 seconds by default.

Server frames:

~~~json
{
  "type": "event",
  "twin_id": "…",
  "sequence": 1235,
  "event_type": "ClaimAccepted",
  "plane": "authoritative",
  "occurred_at": "2026-09-04T00:00:00Z",
  "recorded_at": "2026-09-04T00:00:00Z",
  "resume_token": "authenticated-resume-token"
}
~~~

Control frames are limited to subscribe, acknowledge, heartbeat, and close.
Domain mutations use REST/gRPC so idempotency and concurrency preconditions are
not ambiguous. A slow consumer is disconnected with its last acknowledged
cursor and must replay. Public frames omit payload, event and producer identity,
provenance, and raw chain hashes; consumers fetch a newly authorized projection
after a relevant change notification. Cursor tokens are HMAC-authenticated,
twin-bound, expiry-checked, and chain-bound through a keyed tag. They are
resumability capabilities, never authentication credentials. Unsigned
`after_sequence` is operations-only inside the trusted service boundary. The
optional resume cursor is sent in the initial subscribe frame, not the URL, to
avoid routine proxy/access-log capture.

## 5. Event subjects and ownership

~~~text
argo.dt.<tenant>.<twin>.authoritative.<event_type>
argo.dt.<tenant>.<twin>.projection.<event_type>
argo.dt.<tenant>.<twin>.simulation.<branch>.<event_type>
argo.dt.<tenant>.<twin>.deadletter.<stage>
~~~

The subject name is routing metadata, not authorization. Every payload carries
the canonical EventEnvelope v2 identity, owner role, ordering, lineage, schema,
and hash material. Event-specific purpose and sensitivity remain inside the
payload only when that event schema defines them.

JetStream is an acceleration and work-distribution layer, never canonical
storage. Failed derived work is replayed from SQLite by twin and sequence.
Dead-letter messages contain only those coordinates, stage, delivery count,
time, and a bounded reason code—not the event payload, event ID, hash, or cursor.

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
| producer + producer_role | Authenticated identity and hash-covered owner role |
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

Disclosed claim payloads use an explicit allowlist: statement, kind, validity
interval, and epistemic vector. Claim IDs, subject IDs, provenance/evidence
IDs, model traces, recorded time, review state, and source sensitivity remain
inside the authoritative boundary. The public artifact hash covers only the
disclosed form.

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

### Cursor key rotation

`RotatingCursorSigner` issues with one active key and accepts at most seven
explicit overlap keys. Existing v1 cursor wire data remains compatible; the
matching key is identified by MAC verification, never a caller-trusted key ID.
Deployment constructs a new immutable ring and atomically swaps it into the
service. Removing a retired key revokes its outstanding cursors. The overlap
must cover the configured cursor TTL plus clock skew.

### Observability

`OpenTelemetrySyncExporter` converts cumulative synchronization counters into
monotonic deltas. Its attribute allow-list is limited to service name/version,
deployment environment, and network transport. Twin, subject, event, evidence,
payload, hash, and resume-token dimensions are structurally unavailable.

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
