"""Hexagonal ports implemented by local or production adapters."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Protocol

from .types import (
    BronzeObject,
    ConsentGrant,
    EventEnvelope,
    EventPlane,
    InvalidationRecord,
    JsonObject,
    OutboxRecord,
    ProjectionRequest,
    SnapshotRecord,
)


class EventStore(Protocol):
    def append(self, event: EventEnvelope, *, expected_sequence: int) -> EventEnvelope: ...

    def load(
        self,
        twin_id: str,
        *,
        after_sequence: int = 0,
        up_to_sequence: int | None = None,
        planes: frozenset[EventPlane] | None = None,
        limit: int | None = None,
    ) -> list[EventEnvelope]: ...

    def head(self, twin_id: str) -> tuple[int, str]: ...

    def verify_chain(self, twin_id: str) -> bool: ...


class SnapshotStore(Protocol):
    def save_snapshot(self, snapshot: SnapshotRecord) -> SnapshotRecord: ...

    def load_snapshot(
        self,
        twin_id: str,
        *,
        up_to_sequence: int | None = None,
    ) -> SnapshotRecord | None: ...

    def prune_snapshots(self, twin_id: str, *, keep: int = 3) -> int: ...


class OutboxStore(Protocol):
    def claim_outbox(
        self,
        *,
        lease_owner: str,
        limit: int = 100,
        lease_seconds: int = 30,
    ) -> list[OutboxRecord]: ...

    def mark_outbox_published(self, record: OutboxRecord) -> bool: ...

    def release_outbox(self, record: OutboxRecord, *, error: str) -> bool: ...

    def outbox_backlog(self) -> int: ...

    def prune_outbox(self, *, published_before: datetime, limit: int = 1000) -> int: ...


class DependencyIndex(Protocol):
    def dependents(
        self,
        twin_id: str,
        source_kind: str,
        source_id: str,
        *,
        transitive: bool = True,
        up_to_sequence: int | None = None,
    ) -> tuple[tuple[str, str], ...]: ...

    def ancestors(
        self,
        twin_id: str,
        dependent_kind: str,
        dependent_id: str,
        *,
        transitive: bool = True,
        up_to_sequence: int | None = None,
    ) -> tuple[tuple[str, str], ...]: ...

    def pending_invalidations(self, *, limit: int = 100) -> list[InvalidationRecord]: ...

    def mark_invalidation_processed(self, record: InvalidationRecord) -> bool: ...


class DurableEventStore(EventStore, SnapshotStore, OutboxStore, DependencyIndex, Protocol):
    """Persistence contract required by the DT-1 service boundary."""


class BronzeVault(Protocol):
    def put(
        self,
        *,
        subject_id: str,
        media_type: str,
        content: bytes,
        metadata: Mapping[str, object],
    ) -> tuple[str, str]: ...

    def get(self, *, subject_id: str, object_uri: str) -> tuple[BronzeObject, bytes]: ...

    def delete(self, *, subject_id: str, object_uri: str) -> bool: ...


class ConsentStore(Protocol):
    def get_active(
        self,
        *,
        subject_id: str,
        recipient_id: str,
        purpose: str,
        at: datetime,
    ) -> ConsentGrant | None: ...


class PolicyEvaluator(Protocol):
    def evaluate_projection(
        self,
        request: ProjectionRequest,
        consent: ConsentGrant | None,
        *,
        available_fields: frozenset[str],
    ) -> tuple[bool, tuple[str, ...], frozenset[str]]: ...


class ProjectionSink(Protocol):
    def issue(self, artifact: JsonObject, receipt: JsonObject) -> str: ...


class TelemetryPublisher(Protocol):
    async def publish(self, event: EventEnvelope) -> None: ...


class ModelWorker(Protocol):
    def transform(
        self,
        *,
        prompt_id: str,
        prompt_version: str,
        input_artifact: JsonObject,
    ) -> JsonObject: ...


class Connector(Protocol):
    connector_id: str
    connector_version: str

    def read(self, cursor: str | None) -> Iterable[JsonObject]: ...
