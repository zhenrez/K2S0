# K2S0 Digital Twin Core

This repository is an integration-ready Digital Twin branch for the
MARC-0 → CHIP → Constitution → MARC-1 → Wausauk33 → MorphIQ → QuestN cascade.
It implements the K2S0 boundary as a human epistemic operating layer rather
than a single persona, embedding, agent, or avatar.

The K2S0 repository initially contained only its README and Boost Software
License. This package establishes the standalone implementation boundary and
provides:

- an executable, dependency-light reference kernel;
- canonical ARGOCell, event, consent, action, and projection contracts;
- append-only, hash-chained event storage with idempotency and optimistic
  concurrency, hash-verified snapshots, and a transactional outbox;
- an AES-256-GCM Bronze vault and durable transitive deletion invalidation;
- bitemporal state, isolated simulation branches, default-deny projection
  compilation, bounded durable replay/live synchronization, and signed cursors;
- gRPC, REST, WebSocket, JSON Schema, SQL, and Rego integration contracts;
- a repository-pattern disposition map and a host merge contract.

The broader ecosystem host was not supplied, so CHIP, MARC-1, Wausauk33,
MorphIQ, and QuestN bindings are defined as explicit ports and merge contracts
rather than falsely represented as already wired host imports.

## System shape

~~~mermaid
flowchart TB
  subgraph Z0["Zone 0 — private evidence"]
    SRC["HPI / Screenpipe / messages / interviews"]
    BRZ["Encrypted bronze vault"]
  end
  subgraph Z1["Zone 1 — authoritative K2S0"]
    ING["Normalize + resolve identity"]
    EVT["Hash-chained event ledger"]
    CLM["Evidence / claim / contradiction ledger"]
    TMP["Bitemporal ARGOCell model"]
  end
  subgraph Z2["Zone 2 — derived computation"]
    CMP["Kernel compiler + evaluation"]
    SIM["Isolated simulation branches"]
  end
  subgraph Z3["Zone 3 — consent boundary"]
    POL["Policy + consent + authority"]
    PRJ["Minimum-necessary projections"]
  end
  subgraph Z4["Zone 4 — consumers"]
    AGT["MARC-1 agents"]
    TOWN["Wausauk33 / MorphIQ"]
    UI["QuestN / avatar / external services"]
  end

  SRC --> BRZ --> ING --> EVT --> CLM --> TMP --> CMP
  TMP --> SIM
  CMP --> POL --> PRJ
  SIM -. "proposal only" .-> CLM
  PRJ --> AGT
  PRJ --> TOWN
  PRJ --> UI
  AGT -. "outcomes re-enter as evidence" .-> BRZ
~~~

## Architectural decisions

| Concern | Decision |
| --- | --- |
| Canonical truth | Append-only evidence and claim events; embeddings are indexes only |
| Universal contract | ARGOCell v1 with desired, observed, and predicted state |
| Time | Valid time + recorded/system time on every authoritative cell/event |
| Identity | Human principal, cognitive twin, agent identity, and avatar identity stay distinct |
| Synchronization | Ordered per-twin stream, idempotency key, expected-sequence CAS, resumable cursor |
| Persistence | SQLite/WAL authoritative store; recursive dependency tables; optional Neo4j read model only |
| Real-time path | gRPC bidi ingestion; NATS JetStream in production; bounded WebSocket egress |
| Durable work | Restate primary; Temporal is a supported alternative, not a second mandatory runtime |
| Simulation | Forked namespace and event plane; no automatic promotion into evidence |
| Disclosure | Purpose- and recipient-bound compilation before retrieval; default deny |
| Execution | K2S0 informs actions but does not execute them; ActionEnvelope is checked downstream |
| Hot path | Embedded Python/SQLite profile first; add native acceleration only after measurement |

## Quick verification

~~~bash
make test
make demo
~~~

The kernel uses Python's standard library. The encrypted Bronze adapter uses
the pinned `bronze` extra; optional lint and type checking tools are in `dev`.

## Start here

- [Architecture](docs/ARCHITECTURE.md)
- [Invariant registry](docs/INVARIANTS.md)
- [Integration blueprint](docs/INTEGRATION_BLUEPRINT.md)
- [Host merge contract](docs/MERGE_CONTRACT.md)
- [Reference repository map](docs/REPOSITORY_PATTERN_MAP.md)
- [Performance and security](docs/PERFORMANCE_SECURITY.md)
- [Implementation roadmap](docs/IMPLEMENTATION_ROADMAP.md)
- [gRPC contract](proto/argo/dt/v1/twin.proto)
- [REST contract](openapi/dt-v1.yaml)
- [ARGOCell schema](schemas/argocell-v1.schema.json)
- [State stream schema](schemas/state-stream-v1.schema.json)

## Reference implementation boundaries

The Python core demonstrates the non-negotiable semantics. Production
adapters should implement the protocols in argo_dt.ports without changing
domain types. SQLite is the authoritative embedded profile. Neo4j may be added
later only as a rebuildable graph read model; object storage, NATS JetStream,
Restate, OPA, Infisical, Wasmtime, OpenTelemetry, and ContextForge remain
optional adapters around the kernel.
