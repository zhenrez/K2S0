"""Bounded telemetry ingestion shared by gRPC and future binary transports."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..errors import (
    AuthorizationDenied,
    ConcurrencyConflict,
    InvariantViolation,
    ProtocolViolation,
)
from ..service import DigitalTwinService
from ..sync import (
    StreamPosition,
    TelemetryRecordAck,
    TelemetryRecordStatus,
    TelemetryStreamAck,
)
from ..types import ActorContext, Sensitivity

_HASH_PATTERN = re.compile(r"sha256:[a-f0-9]{64}\Z")
_MAX_JSON_DEPTH = 64
_MAX_JSON_NODES = 100_000


@dataclass(frozen=True, slots=True)
class TelemetryInputRecord:
    source_record_id: str
    valid_from: datetime
    valid_until: datetime | None
    media_type: str
    payload: bytes
    content_hash: str
    independence_group: str
    sensitivity: str
    rights: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class TelemetryInputBatch:
    twin_id: str
    subject_id: str
    source: str
    connector_version: str
    idempotency_key: str
    expected_sequence: int
    records: Sequence[TelemetryInputRecord]


class _DuplicateJsonKey(ValueError):
    pass


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _object_without_duplicate_keys(
    pairs: list[tuple[str, Any]],
) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateJsonKey(key)
        value[key] = item
    return value


def _validate_json_shape(root: object) -> None:
    pending: list[tuple[object, int]] = [(root, 1)]
    visited = 0
    while pending:
        value, depth = pending.pop()
        visited += 1
        if visited > _MAX_JSON_NODES:
            raise ValueError("JSON node limit exceeded")
        if depth > _MAX_JSON_DEPTH:
            raise ValueError("JSON depth limit exceeded")
        if isinstance(value, dict):
            if any(not isinstance(key, str) for key in value):
                raise ValueError("JSON object keys must be strings")
            pending.extend((item, depth + 1) for item in value.values())
        elif isinstance(value, list):
            pending.extend((item, depth + 1) for item in value)


def _decode_json_payload(record: TelemetryInputRecord) -> dict[str, Any]:
    media_type = record.media_type.split(";", 1)[0].strip().lower()
    if media_type != "application/json" and not media_type.endswith("+json"):
        raise InvariantViolation("UNSUPPORTED_MEDIA_TYPE")
    expected_hash = "sha256:" + hashlib.sha256(record.payload).hexdigest()
    if not _HASH_PATTERN.fullmatch(record.content_hash):
        raise InvariantViolation("INVALID_CONTENT_HASH")
    if expected_hash != record.content_hash:
        raise InvariantViolation("CONTENT_HASH_MISMATCH")
    try:
        decoded = json.loads(
            record.payload.decode("utf-8"),
            object_pairs_hook=_object_without_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise InvariantViolation("INVALID_JSON_PAYLOAD") from exc
    if not isinstance(decoded, dict):
        raise InvariantViolation("JSON_PAYLOAD_MUST_BE_OBJECT")
    try:
        _validate_json_shape(decoded)
    except ValueError as exc:
        raise InvariantViolation("INVALID_JSON_SHAPE") from exc
    return decoded


class TelemetryBatchProcessor:
    """Validate one bounded batch and expose partial-commit status explicitly."""

    def __init__(self, service: DigitalTwinService) -> None:
        self._service = service

    async def process(
        self,
        batch: TelemetryInputBatch,
        *,
        actor: ActorContext,
        record_sizes: Sequence[int],
        serialized_batch_bytes: int,
    ) -> TelemetryStreamAck:
        if len(record_sizes) != len(batch.records):
            raise ProtocolViolation(
                "serialized record sizes must correspond to every telemetry record"
            )
        self._service.sync_limits.validate_batch(
            record_sizes=record_sizes,
            serialized_batch_bytes=serialized_batch_bytes,
        )
        self._validate_batch_header(batch)
        signer = self._service.cursor_signer
        if signer is None:
            raise InvariantViolation("telemetry ingestion requires a cursor signer")
        expected = batch.expected_sequence
        acknowledgements: list[TelemetryRecordAck] = []
        blocked = False
        for index, record in enumerate(batch.records):
            if blocked:
                acknowledgements.append(
                    self._rejected(
                        index,
                        record.source_record_id,
                        "CONCURRENCY_CONFLICT",
                        retryable=True,
                    )
                )
                continue
            try:
                payload = self._validate_record(record)
                head_before, _ = self._service.store.head(batch.twin_id)
                persisted = await self._service.ingest_evidence(
                    twin_id=batch.twin_id,
                    subject_id=batch.subject_id,
                    source=batch.source,
                    source_record_id=record.source_record_id,
                    payload=payload,
                    rights=record.rights,
                    sensitivity=Sensitivity(record.sensitivity),
                    valid_from=record.valid_from,
                    valid_until=record.valid_until,
                    independence_group=record.independence_group,
                    expected_sequence=expected,
                    idempotency_key=self._record_idempotency_key(batch, record),
                    actor=actor,
                    connector_version=batch.connector_version,
                    media_type=record.media_type,
                    source_content_hash=record.content_hash,
                )
            except ConcurrencyConflict:
                blocked = True
                acknowledgements.append(
                    self._rejected(
                        index,
                        record.source_record_id,
                        "CONCURRENCY_CONFLICT",
                        retryable=True,
                    )
                )
                continue
            except AuthorizationDenied:
                raise
            except (InvariantViolation, ValueError) as exc:
                code = str(exc)
                if not re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", code):
                    code = "INVALID_RECORD"
                acknowledgements.append(
                    self._rejected(index, record.source_record_id, code)
                )
                continue
            status = (
                TelemetryRecordStatus.DUPLICATE
                if persisted.sequence <= head_before
                else TelemetryRecordStatus.COMMITTED
            )
            expected = max(expected, persisted.sequence)
            acknowledgements.append(
                TelemetryRecordAck(
                    record_index=index,
                    source_record_id=record.source_record_id,
                    status=status,
                    event_id=persisted.event_id,
                    sequence=persisted.sequence,
                )
            )
        head_sequence, head_hash = self._service.store.head(batch.twin_id)
        position = StreamPosition(batch.twin_id, head_sequence, head_hash)
        return TelemetryStreamAck(
            twin_id=batch.twin_id,
            idempotency_key=batch.idempotency_key,
            committed_sequence=head_sequence,
            records=tuple(acknowledgements),
            resume_token=signer.issue(position),
        )

    @staticmethod
    def _validate_batch_header(batch: TelemetryInputBatch) -> None:
        values = {
            "twin_id": batch.twin_id,
            "subject_id": batch.subject_id,
            "source": batch.source,
            "connector_version": batch.connector_version,
            "idempotency_key": batch.idempotency_key,
        }
        if any(not value or len(value) > 512 for value in values.values()):
            raise ProtocolViolation("telemetry batch identity is invalid")
        if batch.expected_sequence < 0:
            raise ProtocolViolation("expected_sequence cannot be negative")
        source_ids = [record.source_record_id for record in batch.records]
        if len(source_ids) != len(set(source_ids)):
            raise ProtocolViolation("source_record_id must be unique within a batch")

    @staticmethod
    def _validate_record(record: TelemetryInputRecord) -> dict[str, Any]:
        if not record.source_record_id or len(record.source_record_id) > 512:
            raise InvariantViolation("INVALID_SOURCE_RECORD_ID")
        if not record.independence_group or len(record.independence_group) > 512:
            raise InvariantViolation("INVALID_INDEPENDENCE_GROUP")
        if record.valid_from.tzinfo is None:
            raise InvariantViolation("INVALID_VALID_FROM")
        if record.valid_until is not None and (
            record.valid_until.tzinfo is None or record.valid_until < record.valid_from
        ):
            raise InvariantViolation("INVALID_VALID_UNTIL")
        try:
            Sensitivity(record.sensitivity)
        except ValueError as exc:
            raise InvariantViolation("INVALID_SENSITIVITY") from exc
        if not isinstance(record.rights, Mapping):
            raise InvariantViolation("INVALID_RIGHTS")
        return _decode_json_payload(record)

    @staticmethod
    def _record_idempotency_key(
        batch: TelemetryInputBatch,
        record: TelemetryInputRecord,
    ) -> str:
        material = "argo-dt-telemetry:" + json.dumps(
            [batch.twin_id, batch.idempotency_key, record.source_record_id],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return "telemetry:" + str(uuid.uuid5(uuid.NAMESPACE_URL, material))

    @staticmethod
    def _rejected(
        index: int,
        source_record_id: str,
        code: str,
        *,
        retryable: bool = False,
    ) -> TelemetryRecordAck:
        return TelemetryRecordAck(
            record_index=index,
            source_record_id=source_record_id or f"invalid-{index}",
            status=TelemetryRecordStatus.REJECTED,
            error_code=code,
            retryable=retryable,
        )
