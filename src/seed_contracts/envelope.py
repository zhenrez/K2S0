from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping

from .semantic import RepresentationSnapshotRef, SubjectRef

SCHEMA_VERSION = "k2s0.seed-envelope/v1"


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _require_json_object(value: Mapping[str, Any], name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    material = dict(value)
    try:
        _canonical_json(material)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be JSON-serializable") from exc
    return material


@dataclass(frozen=True, slots=True)
class SeedSection:
    name: str
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("section name is required")
        object.__setattr__(self, "payload", _require_json_object(self.payload, "section payload"))


@dataclass(frozen=True, slots=True)
class ExtensionBlock:
    type_uri: str
    version: str
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.type_uri.strip():
            raise ValueError("extension type_uri is required")
        if not self.version.strip():
            raise ValueError("extension version is required")
        object.__setattr__(self, "payload", _require_json_object(self.payload, "extension payload"))


@dataclass(frozen=True, slots=True)
class SeedEnvelope:
    """Portable DT-4 container over already-admitted canonical representation state.

    This first DT-4 slice is intentionally transport/storage neutral. It accepts an
    explicit authoritative source sequence, preserves bounded acquisition metadata,
    preserves unknown extension blocks without interpretation, and contains no Bronze
    payload reader or policy bypass.
    """

    subject_ref: SubjectRef
    representation_snapshot_ref: RepresentationSnapshotRef
    source_sequence: int
    compiler_ref: str
    sections: tuple[SeedSection, ...]
    acquisition_info: tuple[Mapping[str, Any], ...] = ()
    extension_blocks: tuple[ExtensionBlock, ...] = ()
    loss_manifest: tuple[Mapping[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if self.representation_snapshot_ref.subject_ref != self.subject_ref:
            raise ValueError("representation snapshot subject must match envelope subject")
        if self.source_sequence < 0:
            raise ValueError("source_sequence cannot be negative")
        if not self.compiler_ref.strip():
            raise ValueError("compiler_ref is required")
        names = [section.name for section in self.sections]
        if len(names) != len(set(names)):
            raise ValueError("section names must be unique")
        object.__setattr__(
            self,
            "acquisition_info",
            tuple(_require_json_object(item, "acquisition_info item") for item in self.acquisition_info),
        )
        object.__setattr__(
            self,
            "loss_manifest",
            tuple(_require_json_object(item, "loss_manifest item") for item in self.loss_manifest),
        )


def _body(envelope: SeedEnvelope) -> dict[str, Any]:
    snapshot = envelope.representation_snapshot_ref
    return {
        "schema_version": SCHEMA_VERSION,
        "subject_ref": {"subject_id": envelope.subject_ref.subject_id},
        "representation_snapshot_ref": {
            "subject_id": snapshot.subject_ref.subject_id,
            "snapshot_id": snapshot.snapshot_id,
            "canonical_sequence": snapshot.canonical_sequence,
            "integrity_ref": snapshot.integrity_ref,
        },
        "source_sequence": envelope.source_sequence,
        "compiler_ref": envelope.compiler_ref,
        "sections": [
            {"name": section.name, "payload": dict(section.payload)}
            for section in envelope.sections
        ],
        "acquisition_info": [dict(item) for item in envelope.acquisition_info],
        "extension_blocks": [
            {
                "type_uri": block.type_uri,
                "version": block.version,
                "payload": dict(block.payload),
            }
            for block in envelope.extension_blocks
        ],
        "loss_manifest": [dict(item) for item in envelope.loss_manifest],
    }


def compile_seed_envelope(envelope: SeedEnvelope) -> bytes:
    body = _body(envelope)
    digest = hashlib.sha256(_canonical_json(body)).hexdigest()
    wire = {
        **body,
        "integrity_ref": f"seed-envelope:sha256:{digest}",
    }
    return _canonical_json(wire) + b"\n"


def read_seed_envelope(payload: bytes | bytearray | memoryview) -> SeedEnvelope:
    try:
        wire = json.loads(bytes(payload))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("seed envelope must be valid JSON") from exc
    if not isinstance(wire, dict):
        raise ValueError("seed envelope must be a JSON object")
    if wire.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"unsupported schema_version: {wire.get('schema_version')!r}")

    integrity_ref = wire.get("integrity_ref")
    if not isinstance(integrity_ref, str) or not integrity_ref.startswith("seed-envelope:sha256:"):
        raise ValueError("integrity_ref is required")
    body = dict(wire)
    del body["integrity_ref"]
    expected = "seed-envelope:sha256:" + hashlib.sha256(_canonical_json(body)).hexdigest()
    if integrity_ref != expected:
        raise ValueError("seed envelope integrity verification failed")

    subject_wire = wire.get("subject_ref")
    snapshot_wire = wire.get("representation_snapshot_ref")
    if not isinstance(subject_wire, dict) or not isinstance(snapshot_wire, dict):
        raise ValueError("subject_ref and representation_snapshot_ref are required")
    subject = SubjectRef(str(subject_wire.get("subject_id", "")))
    snapshot_subject = SubjectRef(str(snapshot_wire.get("subject_id", "")))
    snapshot = RepresentationSnapshotRef(
        subject_ref=snapshot_subject,
        snapshot_id=str(snapshot_wire.get("snapshot_id", "")),
        canonical_sequence=int(snapshot_wire.get("canonical_sequence")),
        integrity_ref=str(snapshot_wire.get("integrity_ref", "")),
    )

    sections_wire = wire.get("sections")
    acquisitions_wire = wire.get("acquisition_info")
    extensions_wire = wire.get("extension_blocks")
    losses_wire = wire.get("loss_manifest")
    if not isinstance(sections_wire, list):
        raise ValueError("sections must be an array")
    if not isinstance(acquisitions_wire, list):
        raise ValueError("acquisition_info must be an array")
    if not isinstance(extensions_wire, list):
        raise ValueError("extension_blocks must be an array")
    if not isinstance(losses_wire, list):
        raise ValueError("loss_manifest must be an array")

    envelope = SeedEnvelope(
        subject_ref=subject,
        representation_snapshot_ref=snapshot,
        source_sequence=int(wire.get("source_sequence")),
        compiler_ref=str(wire.get("compiler_ref", "")),
        sections=tuple(
            SeedSection(name=str(item.get("name", "")), payload=item.get("payload", {}))
            for item in sections_wire
            if isinstance(item, dict)
        ),
        acquisition_info=tuple(item for item in acquisitions_wire if isinstance(item, dict)),
        extension_blocks=tuple(
            ExtensionBlock(
                type_uri=str(item.get("type_uri", "")),
                version=str(item.get("version", "")),
                payload=item.get("payload", {}),
            )
            for item in extensions_wire
            if isinstance(item, dict)
        ),
        loss_manifest=tuple(item for item in losses_wire if isinstance(item, dict)),
    )
    if len(envelope.sections) != len(sections_wire):
        raise ValueError("every section must be an object")
    if len(envelope.acquisition_info) != len(acquisitions_wire):
        raise ValueError("every acquisition_info entry must be an object")
    if len(envelope.extension_blocks) != len(extensions_wire):
        raise ValueError("every extension block must be an object")
    if len(envelope.loss_manifest) != len(losses_wire):
        raise ValueError("every loss_manifest entry must be an object")
    return envelope
