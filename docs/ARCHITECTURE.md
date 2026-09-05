# Digital Twin architecture

## 1. Scope and interpretation

The supplied list contains no repository literally prefixed with **dt**.
This design therefore treats section 1.6, “Digital Twin / Personal Continuity
Model,” as the authoritative DT reference family. If “dt” referred to a
separate internal prefix, that source tree was not present and remains an
integration unknown.

K2S0 is the canonical person-modeling boundary. It compiles multiple
purpose-specific twins; it is not itself an agent, avatar, fine-tune, or
single master persona.

### Goals

- Preserve evidence, provenance, contradiction, correction, and time.
- Synchronize observed, desired, and predicted state with deterministic order.
- Process real-time telemetry without putting raw evidence on agent-facing
  surfaces.
- Compile purpose- and recipient-specific projections before disclosure.
- Fork simulations without contaminating authoritative evidence.
- Make every consequential downstream action independently governable.
- Remain model-, vector-store-, graph-store-, and workflow-engine-neutral.

### Non-goals

- K2S0 does not autonomously act on external systems.
- It does not declare one permanent or context-free persona.
- It does not use embeddings or generated summaries as canonical truth.
- It does not promote simulation output into evidence.
- It does not merge human, agent, avatar, and legal/principal identity.

## 2. Layered architecture

~~~mermaid
flowchart TB
  subgraph P["Producer plane"]
    HPI["Historical providers"]
    CAP["Ambient capture"]
    MSG["Communications"]
    ASK["Interviews + corrections"]
  end
  subgraph A["Authoritative plane"]
    BRZ["Bronze object vault"]
    NRM["Normalization + sessionization"]
    IDS["Identity/entity resolution"]
    ELS["Evidence + claim ledger"]
    CEL["ARGOCell temporal state"]
  end
  subgraph D["Derived plane"]
    IDX["Search/vector/graph indexes"]
    CMP["Kernel compiler"]
    EVA["Evaluation + readiness"]
    SIM["Simulation branches"]
  end
  subgraph G["Governance plane"]
    CST["Root-signed constraints"]
    CON["Consent grants"]
    AIP["Identity + capability policy"]
    PRJ["Projection compiler"]
    AUD["Receipts + audit"]
  end
  subgraph C["Consumer plane"]
    M1["MARC-1 / agent runtime"]
    WV["Wausauk33 / MorphIQ"]
    QN["QuestN"]
    AV["Avatar / voice / external"]
  end

  HPI --> BRZ
  CAP --> BRZ
  MSG --> BRZ
  ASK --> BRZ
  BRZ --> NRM --> IDS --> ELS --> CEL
  CEL --> IDX
  CEL --> CMP --> EVA
  CEL --> SIM
  CST --> PRJ
  CON --> PRJ
  AIP --> PRJ
  CMP --> PRJ --> AUD
  PRJ --> M1
  PRJ --> WV
  PRJ --> QN
  PRJ --> AV
  M1 -. "verified outcome" .-> BRZ
  SIM -. "review referral only" .-> ELS
~~~

## 3. Logical components

| Component | Responsibility | Authoritative? | Scaling key |
| --- | --- | --- | --- |
| Connector supervisor | Runs resumable source adapters and acquisition manifests | No | connector + account |
| Bronze vault | Encrypted original bytes, content hashes, rights, deletion tombstones | Yes | subject |
| Normalizer | Reversible parsing, authorship classification, media extraction | Derived from Bronze | source type |
| Identity resolver | Aliases and reversible entity merge proposals | Yes through events | subject |
| Event ledger | Ordered, append-only, hash-chained changes | Yes | twin ID |
| Claim ledger | Evidence, counterevidence, alternatives, review, supersession | Yes | subject/domain |
| ARGOCell projector | Recursive observed/desired/predicted state with typed relations | Rebuildable | twin ID |
| Index pipeline | FTS, vector, graph, and materialized read models | No | index shard |
| Adjudicator | Deduplication, contradiction, scope, salience, and referral proposals | Proposal only | claim/domain |
| Elicitation engine | Gap-directed questions, sealed holdouts, burden management | Answers enter Bronze | subject |
| Kernel compiler | KERNEL, NOW, COUNTERWEIGHTS, VOICE, PEOPLE, CAPABILITIES, DECISION MODEL, BOUNDARIES, HISTORY | Rebuildable | artifact |
| Evaluation lab | Coverage, calibration, drift, prediction, regression, readiness | Rebuildable; results audited | suite/model |
| Simulation engine | Branches from an explicit sequence and holds predicted state | No | branch ID |
| Projection compiler | Builds minimum-necessary recipient/purpose artifact | Issuance receipt is authoritative | recipient/purpose |
| Action policy port | Checks ActionEnvelope against delegated authority | Decision is audited | actor/domain |
| Sync gateway | gRPC ingest, NATS events, WebSocket deltas, resume cursors | No | twin ID |

The executable sync core is transport-neutral: SQLite is the replay source,
the transactional outbox supplies at-least-once live publication, and bounded
sessions perform cumulative acknowledgement. Public state frames are change
notifications only; private canonical payloads stay behind the projection and
authorization boundaries.

Logical separation does not imply one service or database per row. The initial
deployment should be a modular monolith plus separate heavy workers; split only
where load, trust, or failure isolation requires it.

## 4. Canonical state and consistency

### 4.1 Event order

Each twin is one ordered stream:

1. Caller supplies an idempotency key and expected sequence.
2. The service serializes the append for that twin.
3. The event receives the next sequence and previous event hash.
4. Event and transactional outbox record commit together.
5. The outbox publishes to the stream bus.
6. Projections consume at least once and deduplicate by twin ID + sequence.

This gives per-twin linearizability and cross-twin eventual consistency.
Global ordering is neither required nor desirable.

### 4.2 Dual temporal plane

Every authoritative object carries:

- **valid time**: when it describes the subject or world;
- **recorded/system time**: when K2S0 learned or accepted it.

Queries may ask “what was true at T?” and “what did K2S0 know at T?” separately.
Current state never overwrites historical state.

### 4.3 State planes

| Plane | May affect authoritative state? | May be disclosed? | Promotion path |
| --- | --- | --- | --- |
| Authoritative | Yes | Through projection only | Normal review/adjudication |
| Projection | Audit/revocation only | It is the disclosure artifact | Recompile |
| Simulation | No | Only when explicitly labeled | Human review → new independent evidence or claim proposal |

### 4.4 ARGOCell

ARGOCell is the universal recursive IR for subject, domain, project, task,
agent, tool, hypothesis, resource, and repository state. It contains:

- one of four identity kinds;
- typed constituent and arbitrary relations;
- observed, desired, and predicted state;
- requirements, constraints, and intents;
- bitemporal coordinates;
- provenance and a multidimensional EpistemicVector;
- sensitivity and projection handles;
- an explicit schema version.

Claims, patterns, and compiled artifacts mechanically require provenance.

## 5. Data lifecycle

~~~mermaid
stateDiagram-v2
  [*] --> Captured
  Captured --> Normalized: reversible transform
  Normalized --> Proposed: claim extraction
  Proposed --> Accepted: review/adjudication
  Proposed --> Contested: counterevidence
  Accepted --> Superseded: temporal change
  Accepted --> Stale: source deletion/correction
  Contested --> Accepted: scoped resolution
  Stale --> Proposed: re-evaluation
  Superseded --> [*]
~~~

No transition deletes the historical fact that a state once existed. Physical
evidence deletion creates tombstones and invalidates dependent artifacts.

## 6. Kernel compilation

The compiler is a pure, replayable transformation from accepted claims at an
explicit stream sequence and bitemporal instant. A compilation manifest stores:

- compiler, prompt, model, tool, and schema versions;
- source claim IDs and lineage root;
- included/excluded scopes;
- degraded stages and last-good artifact;
- loss report;
- artifact hash and evaluation result.

External model calls receive a compiled minimal evidence package, never the
entire private archive. Model output remains a proposal until accepted.

## 7. Deployment topology

### Initial production configuration

| Runtime unit | Contents |
| --- | --- |
| dt-api | REST control plane, gRPC query, WebSocket/SSE egress |
| dt-ingest | gRPC telemetry, validation, Bronze writes, event append |
| dt-worker | normalization, identity, claim, compiler, evaluation workers |
| dt-policy | OPA bundle and identity/capability verification |
| dt-runtime | Restate durable objects/workflows |
| SQLite/WAL | event ledger, snapshots, dependencies, consents, receipts, outbox |
| Encrypted filesystem vault | AES-GCM Bronze evidence; object storage remains optional |
| In-process relay | transactional-outbox delivery for the embedded profile |
| Sync sessions | bounded SQLite replay/live handoff and signed cursor acks |
| Index adapters | SQLite FTS5 initially; optional Neo4j/vector read models |

SQLite is authoritative and runs without a database service. Deploy one file
per tenant or equivalent security boundary. Neo4j must never become the claim
ledger: if introduced, it is rebuilt from the event stream and dependency
tables. Delivered outbox pointers and superseded snapshots have bounded
retention; authoritative history is never silently compacted.

Heavy capture, OCR, speech, embedding, model, and simulation workers are
separate processes. The control API remains available when those workers are
degraded.

### Split triggers

Split a module into an independent service only when one is observed:

- different data classification or key custody;
- independent horizontal scaling requirement;
- incompatible runtime/GPU requirement;
- repeated failure-domain coupling;
- separately owned release cadence;
- measurable latency contention.

## 8. Constitutional invariants

The reference tests cover a first subset; production conformance must cover all:

1. No claim without provenance.
2. No derived artifact without source claim IDs.
3. No circular source counted as independent evidence.
4. Raw evidence is never overwritten by interpretation.
5. Counterevidence and contradiction are preserved.
6. Historical state is never overwritten by current state.
7. Unknown is distinct from false; private is distinct from absent.
8. Inferred hard boundaries require human confirmation.
9. Simulations do not become evidence automatically.
10. Evaluation holdouts never enter training.
11. Embeddings are never canonical truth.
12. Downstream agents cannot mutate K2S0 state directly.
13. Every projection is recipient- and purpose-bound.
14. Information access and action authority are independent.
15. Readiness does not imply authorization.
16. Deletion invalidates dependent artifacts.
17. Every material model transformation is version-identifiable.
18. Degraded artifacts identify their missing stages.
19. Every high-level assertion traces to evidence.
20. Root-signed constitutional constraints outrank MorphIQ proposals.
