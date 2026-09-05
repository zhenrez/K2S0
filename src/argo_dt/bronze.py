"""Encrypted local Bronze object adapter for reference and edge deployments."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from .errors import IntegrityError, InvariantViolation, NotFound
from .types import BronzeObject, Sensitivity, canonical_json, parse_time, utc_now

_MAGIC = b"ARGODTBRONZE1\x00"
_NONCE_BYTES = 12


class KeyProvider(Protocol):
    """Resolves data-encryption keys without exposing storage to key policy."""

    def active_key(self, subject_id: str) -> tuple[str, bytes]: ...

    def key_for_id(self, subject_id: str, key_id: str) -> bytes: ...


class StaticKeyProvider:
    """Single-key provider for tests and offline development only."""

    def __init__(self, *, key_id: str, key: bytes) -> None:
        if not key_id:
            raise ValueError("key_id is required")
        if len(key) != 32:
            raise ValueError("AES-256-GCM requires a 32-byte key")
        self._key_id = key_id
        self._key = bytes(key)

    def active_key(self, subject_id: str) -> tuple[str, bytes]:
        if not subject_id:
            raise ValueError("subject_id is required")
        return self._key_id, self._key

    def key_for_id(self, subject_id: str, key_id: str) -> bytes:
        if not subject_id or key_id != self._key_id:
            raise KeyError(key_id)
        return self._key


@dataclass(frozen=True, slots=True)
class AcquisitionManifest:
    connector_id: str
    connector_version: str
    source_record_id: str
    acquired_at: datetime
    rights: Mapping[str, Any]
    sensitivity: Sensitivity

    def __post_init__(self) -> None:
        if not self.connector_id or not self.connector_version or not self.source_record_id:
            raise InvariantViolation("acquisition source identity is required")
        if self.acquired_at.tzinfo is None:
            raise InvariantViolation("acquired_at must be timezone-aware")

    def to_dict(self) -> dict[str, Any]:
        return {
            "connector_id": self.connector_id,
            "connector_version": self.connector_version,
            "source_record_id": self.source_record_id,
            "acquired_at": self.acquired_at.isoformat(),
            "rights": dict(self.rights),
            "sensitivity": self.sensitivity.value,
        }


class EncryptedFileBronzeVault:
    """AES-256-GCM object vault with deterministic acquisition identities.

    Metadata and content are encrypted together. The cleartext header contains
    only routing/key identifiers needed before decryption. A production object
    store adapter should retain this envelope contract and replace the local
    key provider with KMS/HSM-backed key resolution.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        key_provider: KeyProvider,
        max_object_bytes: int = 64 * 1024 * 1024,
    ) -> None:
        if max_object_bytes < 1:
            raise ValueError("max_object_bytes must be positive")
        self._root = Path(root).resolve()
        self._root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self._root, 0o700)
        self._key_provider = key_provider
        self._max_object_bytes = max_object_bytes

    @staticmethod
    def _subject_digest(subject_id: str) -> str:
        return hashlib.sha256(subject_id.encode("utf-8")).hexdigest()

    @staticmethod
    def _content_hash(content: bytes) -> str:
        return "sha256:" + hashlib.sha256(content).hexdigest()

    @staticmethod
    def _deterministic_object_id(
        *,
        subject_id: str,
        media_type: str,
        content_hash: str,
        metadata: Mapping[str, object],
    ) -> str:
        connector_id = str(metadata.get("connector_id", ""))
        source_record_id = str(metadata.get("source_record_id", ""))
        identity = (
            f"source:{connector_id}:{source_record_id}"
            if connector_id and source_record_id
            else f"content:{media_type}:{content_hash}"
        )
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"argo-dt:{subject_id}:{identity}"))

    def _path(self, subject_digest: str, object_id: str) -> Path:
        directory = self._root / subject_digest
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(directory, 0o700)
        path = directory / f"{object_id}.dtb"
        if path.parent.resolve() != directory.resolve():
            raise InvariantViolation("Bronze object path escaped its subject boundary")
        if path.is_symlink():
            raise IntegrityError("Bronze object cannot be a symbolic link")
        return path

    @staticmethod
    def _uri(subject_digest: str, object_id: str) -> str:
        return f"bronze://{subject_digest}/{object_id}"

    @staticmethod
    def _parse_uri(object_uri: str) -> tuple[str, str]:
        prefix = "bronze://"
        if not object_uri.startswith(prefix):
            raise InvariantViolation("invalid Bronze object URI")
        parts = object_uri[len(prefix) :].split("/")
        if len(parts) != 2 or len(parts[0]) != 64:
            raise InvariantViolation("invalid Bronze object URI")
        if any(character not in "0123456789abcdef" for character in parts[0]):
            raise InvariantViolation("invalid Bronze subject digest")
        try:
            uuid.UUID(parts[1])
        except ValueError as exc:
            raise InvariantViolation("invalid Bronze object identifier") from exc
        return parts[0], parts[1]

    @staticmethod
    def _aesgcm(key: bytes) -> Any:
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError as exc:  # pragma: no cover - environment dependency gate
            raise RuntimeError(
                "install argo-dt[bronze] to use encrypted Bronze storage"
            ) from exc
        return AESGCM(key)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def put(
        self,
        *,
        subject_id: str,
        media_type: str,
        content: bytes,
        metadata: Mapping[str, object],
    ) -> tuple[str, str]:
        if not subject_id or not media_type:
            raise InvariantViolation("subject_id and media_type are required")
        if len(content) > self._max_object_bytes:
            raise InvariantViolation("Bronze object exceeds the configured byte limit")
        digest = self._subject_digest(subject_id)
        value_hash = self._content_hash(content)
        object_id = self._deterministic_object_id(
            subject_id=subject_id,
            media_type=media_type,
            content_hash=value_hash,
            metadata=metadata,
        )
        object_uri = self._uri(digest, object_id)
        path = self._path(digest, object_id)
        if path.exists():
            stored, stored_content = self.get(
                subject_id=subject_id,
                object_uri=object_uri,
            )
            if (
                stored.content_hash != value_hash
                or stored_content != content
                or stored.media_type != media_type
                or canonical_json(stored.metadata) != canonical_json(dict(metadata))
            ):
                raise InvariantViolation(
                    "deterministic Bronze source identity changed acquisition material"
                )
            return object_uri, value_hash

        key_id, key = self._key_provider.active_key(subject_id)
        if len(key) != 32:
            raise InvariantViolation("key provider did not return an AES-256 key")
        created_at = utc_now()
        nonce = os.urandom(_NONCE_BYTES)
        header = canonical_json(
            {
                "version": "argo.dt.bronze/v1",
                "key_id": key_id,
                "nonce": base64.b64encode(nonce).decode("ascii"),
                "object_id": object_id,
                "subject_digest": digest,
            }
        ).encode("utf-8")
        encrypted_metadata = canonical_json(
            {
                "media_type": media_type,
                "content_hash": value_hash,
                "metadata": dict(metadata),
                "created_at": created_at.isoformat(),
            }
        ).encode("utf-8")
        plaintext = len(encrypted_metadata).to_bytes(8, "big") + encrypted_metadata + content
        ciphertext = self._aesgcm(key).encrypt(nonce, plaintext, header)
        envelope = _MAGIC + len(header).to_bytes(4, "big") + header + ciphertext
        temporary = path.with_name(f".{object_id}.{uuid.uuid4()}.tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(envelope)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                stored, stored_content = self.get(
                    subject_id=subject_id,
                    object_uri=object_uri,
                )
                if (
                    stored.content_hash != value_hash
                    or stored_content != content
                    or stored.media_type != media_type
                    or canonical_json(stored.metadata) != canonical_json(dict(metadata))
                ):
                    raise InvariantViolation(
                        "concurrent Bronze source identity changed acquisition material"
                    ) from None
            self._fsync_directory(path.parent)
        finally:
            temporary.unlink(missing_ok=True)
        return object_uri, value_hash

    def get(self, *, subject_id: str, object_uri: str) -> tuple[BronzeObject, bytes]:
        digest, object_id = self._parse_uri(object_uri)
        if digest != self._subject_digest(subject_id):
            raise NotFound("Bronze object does not belong to this subject")
        path = self._path(digest, object_id)
        try:
            envelope = path.read_bytes()
        except FileNotFoundError as exc:
            raise NotFound("Bronze object was not found") from exc
        try:
            if not envelope.startswith(_MAGIC):
                raise IntegrityError("Bronze envelope magic mismatch")
            offset = len(_MAGIC)
            header_length = int.from_bytes(envelope[offset : offset + 4], "big")
            if header_length < 1 or header_length > 64 * 1024:
                raise IntegrityError("Bronze envelope header length is invalid")
            header_start = offset + 4
            header_end = header_start + header_length
            header_bytes = envelope[header_start:header_end]
            header = json.loads(header_bytes)
            if (
                header.get("version") != "argo.dt.bronze/v1"
                or header.get("object_id") != object_id
                or header.get("subject_digest") != digest
            ):
                raise IntegrityError("Bronze envelope routing identity mismatch")
            nonce = base64.b64decode(str(header["nonce"]), validate=True)
            if len(nonce) != _NONCE_BYTES:
                raise IntegrityError("Bronze envelope nonce length is invalid")
            key_id = str(header["key_id"])
            key = self._key_provider.key_for_id(subject_id, key_id)
            plaintext = self._aesgcm(key).decrypt(
                nonce,
                envelope[header_end:],
                header_bytes,
            )
            metadata_length = int.from_bytes(plaintext[:8], "big")
            metadata_end = 8 + metadata_length
            if metadata_length < 2 or metadata_end > len(plaintext):
                raise IntegrityError("Bronze encrypted metadata length is invalid")
            metadata = json.loads(plaintext[8:metadata_end])
            content = plaintext[metadata_end:]
            value_hash = self._content_hash(content)
            if metadata.get("content_hash") != value_hash:
                raise IntegrityError("Bronze plaintext content hash mismatch")
            record = BronzeObject(
                object_uri=object_uri,
                subject_id=subject_id,
                media_type=str(metadata["media_type"]),
                content_hash=value_hash,
                metadata=dict(metadata["metadata"]),
                key_id=key_id,
                created_at=parse_time(str(metadata["created_at"])),
            )
            return record, content
        except (IntegrityError, NotFound):
            raise
        except Exception as exc:
            raise IntegrityError("Bronze object authentication failed") from exc

    def delete(self, *, subject_id: str, object_uri: str) -> bool:
        digest, object_id = self._parse_uri(object_uri)
        if digest != self._subject_digest(subject_id):
            raise NotFound("Bronze object does not belong to this subject")
        path = self._path(digest, object_id)
        try:
            path.unlink()
        except FileNotFoundError:
            return False
        self._fsync_directory(path.parent)
        return True
