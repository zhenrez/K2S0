# R12.2 Day-0 implementation authority

Status: **ARCHITECTURE / DESIGN FREEZE COMPLETE — IMPLEMENTATION AUTHORIZED**

This document is the controlling implementation-facing index for the R2D2/K2S0
integration. Earlier architecture, adversarial, and debate documents remain
rationale and provenance; implementation decisions are governed by the
controlling contracts and conformance tests referenced here.

## Implementation baseline

```text
IMPLEMENTATION_BASELINE_COMMIT =
22b8506fde63c8080a99b39942a31eade59db042
```

Implementation starts from that commit, or from a descendant whose intervening
changes have been reconciled against the repository delta.

Repository drift does not reopen architecture:

```text
baseline commit
    -> repository diff/audit
    -> classify changes:
         already conforming
         conflict
         superseded delta
         unrelated
    -> refresh repository-delta metadata only
```

Only a change that exposes a genuine contradiction in a frozen public semantic
contract invokes semantic dependency-cone review.

## Final semantic corrections

### R12-F5 — admission does not require SemanticBasis sealing

Canonical admission is a hot write path. It reports the canonical representation
position produced by the accepted operation; it does not require creation of a
compound semantic basis.

```text
evidence arrives
    -> K2 validates
    -> canonical effects commit
    -> AdmissionResult returns resulting RepresentationSnapshotRef
```

A `SemanticBasisRef` is sealed later when a reproducible interpretation boundary
is required for seed compilation, readiness, fidelity, projection, or another
assessment.

Public admission results therefore carry a resulting canonical representation
state reference, not a mandatory SemanticBasisRef.

### R12-F6 — identity terms remain separate

```text
SubjectRef
    identifies the represented person

RepresentationSnapshotRef
    identifies one canonical representation state

SemanticBasisRef
    identifies one immutable, reproducible interpretation basis over a
    representation state under specified schemas, constructs, and policies
```

Derived readiness, fidelity, adequacy, and purpose assessments reference a
SemanticBasisRef. They do not redefine SubjectRef, RepresentationSnapshotRef,
or SemanticBasisRef identity.

The same RepresentationSnapshotRef may legitimately participate in different
SemanticBasisRefs when an interpretation-critical policy changes.

The same SemanticBasisRef may be evaluated by multiple readiness profiles and
produce different ReadinessResults.

### R12-F7 — repository reconciliation is baseline-relative metadata

The semantic design is frozen. The repository delta is an operational
reconciliation against IMPLEMENTATION_BASELINE_COMMIT.

If main advances, update only the delta layer unless a code change changes a
frozen public semantic contract.

## AT-58 — admission does not require SemanticBasis sealing

A conforming implementation must prove:

```text
1. canonical admission can commit valid canonical effects;
2. AdmissionResult returns the resulting canonical representation position;
3. no SemanticBasisRef must be created for that admission to succeed;
4. later sealing a SemanticBasisRef over that representation state preserves
   the admitted effects and their canonical position.
```

## Day-0 contract hardening

### AT-59 — non-admitted result parity

`REJECTED` and `DEFERRED` results cannot carry
`resulting_representation_state_ref`. If a future API needs to expose current
state on rejection/deferment, it must use a semantically distinct field rather
than overloading `resulting_*`.

### AT-60 — one admission is internally coherent

Every public `CanonicalEffect` in one `AdmissionResult` must:

- carry the same `source_request_id` as the enclosing result;
- belong to the same subject as the result's canonical position; and
- reference the exact same final canonical position as the result.

The exact-position rule matches the current event-store model, where one public
atomic admission resolves to one canonical event-chain position. It also avoids
exposing private intermediate event positions if an adapter later uses several
internal operations.

### AT-61 — interpretation-critical refs are a canonical set

`other_interpretation_critical_refs` is an unordered unique semantic dependency
set. Sealing rejects duplicates, lexicographically sorts the refs, hashes the
canonical order, and stores that same canonical order.

## Day-0 architecture proof

The first historical-AI vertical slice does not pass merely because an import
feature works. It must demonstrate:

```text
real imported source
-> immutable L0 preservation
-> source-native actor retained
-> mixed/span-level semantic authorship representable
-> provenance distinct from support
-> SupportRelation
-> K2 public admission
-> explicit CanonicalEffects
-> canonical representation state
-> separately sealed SemanticBasisRef
-> OWNER_CANONICAL_EXPORT
-> independent parser/consumer
-> authorized evidence resolution
-> no dependency on private SQLite/K2/R2 implementation
```

## Change control

```text
PUBLIC / SEMANTIC change
    -> architecture-contract change
    -> review affected contract + M-map + L-map + AT dependency cone

REFERENCE IMPLEMENTATION change
    -> engineering decision
    -> all applicable conformance tests must continue to pass

PRIVATE implementation change
    -> normal development
    -> no architecture review unless observable semantics change
```

A coding inconvenience is not grounds to weaken a semantic invariant.

## Implementation command

Begin with the conformance scaffold and shared semantic contracts. Wrap the
existing K2 substrate rather than rewriting it. Establish evidence/support/
admission semantics, then drive one real historical-AI source through the full
R2D2 -> K2S0 -> portable SeedEnvelope -> independent-consumer path.

Preserve KEEP / WRAP / EXTEND wherever possible. Replace working K2 internals
only when a frozen conformance requirement demonstrates that the current
mechanism cannot satisfy the contract.
