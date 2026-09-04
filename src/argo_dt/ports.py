"""Hexagonal ports implemented by local or production adapters."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Mapping, Protocol

from .types import (
    ConsentGrant,
    EventEnvelope,
    EventPlane,
    JsonObject,
    ProjectionRequest,
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


class BronzeVault(Protocol):
    def put(
        self,
        *,
        subject_id: str,
        media_type: str,
        content: bytes,
        metadata: Mapping[str, object],
    ) -> tuple[str, str]: ...


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

