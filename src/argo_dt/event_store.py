"""SQLite reference adapter for the authoritative event ledger."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timedelta
from pathlib import Path

from .errors import ConcurrencyConflict, IntegrityError, InvariantViolation
from .ownership import sole_event_owner
from .types import (
    EventEnvelope,
    EventPlane,
    InvalidationRecord,
    OutboxRecord,
    ProducerRole,
    SnapshotRecord,
    canonical_json,
    parse_time,
    utc_now,
)

_DDL = """
CREATE TABLE IF NOT EXISTS dt_events (
    twin_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    plane TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    producer TEXT NOT NULL,
    producer_role TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    causation_id TEXT,
    correlation_id TEXT,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL,
    PRIMARY KEY (twin_id, sequence),
    UNIQUE (twin_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS dt_events_type_idx
    ON dt_events (twin_id, event_type, sequence);
CREATE INDEX IF NOT EXISTS dt_events_recorded_idx
    ON dt_events (recorded_at);

CREATE TABLE IF NOT EXISTS dt_snapshots (
    twin_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    schema_version TEXT NOT NULL,
    state_json TEXT NOT NULL,
    state_hash TEXT NOT NULL,
    last_event_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (twin_id, sequence),
    FOREIGN KEY (twin_id, sequence) REFERENCES dt_events(twin_id, sequence)
);

CREATE TABLE IF NOT EXISTS dt_outbox (
    twin_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    topic TEXT NOT NULL,
    message_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    published_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    lease_owner TEXT,
    lease_until TEXT,
    PRIMARY KEY (twin_id, sequence, topic),
    FOREIGN KEY (twin_id, sequence) REFERENCES dt_events(twin_id, sequence)
);
CREATE INDEX IF NOT EXISTS dt_outbox_pending_idx
    ON dt_outbox (published_at, lease_until, created_at);

CREATE TABLE IF NOT EXISTS dt_dependencies (
    twin_id TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    source_id TEXT NOT NULL,
    dependent_kind TEXT NOT NULL,
    dependent_id TEXT NOT NULL,
    created_sequence INTEGER NOT NULL,
    PRIMARY KEY (
        twin_id, source_kind, source_id, dependent_kind, dependent_id
    ),
    FOREIGN KEY (twin_id, created_sequence)
        REFERENCES dt_events(twin_id, sequence),
    CHECK (source_kind IN ('evidence', 'claim')),
    CHECK (dependent_kind IN ('claim', 'projection'))
);
CREATE INDEX IF NOT EXISTS dt_dependencies_source_idx
    ON dt_dependencies (twin_id, source_kind, source_id);

CREATE TABLE IF NOT EXISTS dt_invalidation_queue (
    twin_id TEXT NOT NULL,
    deletion_sequence INTEGER NOT NULL,
    source_evidence_id TEXT NOT NULL,
    dependent_kind TEXT NOT NULL,
    dependent_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    processed_at TEXT,
    PRIMARY KEY (
        twin_id, deletion_sequence, dependent_kind, dependent_id
    ),
    FOREIGN KEY (twin_id, deletion_sequence)
        REFERENCES dt_events(twin_id, sequence)
);
CREATE INDEX IF NOT EXISTS dt_invalidation_pending_idx
    ON dt_invalidation_queue (processed_at, created_at);

CREATE TABLE IF NOT EXISTS dt_adapter_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

_EVENT_TOPIC = "dt.events"


class SQLiteEventStore:
    """Transactional event store with per-twin linearizable appends.

    BEGIN IMMEDIATE serializes writers in one database file. Sharded SQLite or
    durable-runtime adapters must preserve the same expected-sequence,
    idempotency, snapshot, dependency, and outbox semantics.
    """

    def __init__(self, path: str | Path = ":memory:") -> None:
        self._path = str(path)
        database_existed = self._path == ":memory:" or Path(self._path).exists()
        empty_database = (
            self._path == ":memory:"
            or not Path(self._path).exists()
            or Path(self._path).stat().st_size == 0
        )
        self._connection = sqlite3.connect(
            self._path,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._connection.execute("PRAGMA foreign_keys = ON")
            if empty_database:
                self._connection.execute("PRAGMA auto_vacuum = INCREMENTAL")
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = FULL")
            self._connection.executescript(_DDL)
            self._migrate_legacy_producer_roles()
            self._migrate_dependency_index()
        if not database_existed:
            os.chmod(self._path, 0o600)

    def _migrate_legacy_producer_roles(self) -> None:
        columns = {
            str(row["name"])
            for row in self._connection.execute("PRAGMA table_info(dt_events)")
        }
        if "producer_role" in columns:
            return
        cursor = self._connection.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute("ALTER TABLE dt_events ADD COLUMN producer_role TEXT")
            event_types = cursor.execute(
                "SELECT DISTINCT event_type FROM dt_events"
            ).fetchall()
            for row in event_types:
                event_type = str(row["event_type"])
                role = sole_event_owner(event_type)
                cursor.execute(
                    "UPDATE dt_events SET producer_role = ? WHERE event_type = ?",
                    (role.value, event_type),
                )
            unresolved = cursor.execute(
                "SELECT COUNT(*) AS count FROM dt_events WHERE producer_role IS NULL"
            ).fetchone()
            if unresolved is not None and int(unresolved["count"]) != 0:
                raise IntegrityError("legacy producer-role migration was incomplete")
            cursor.execute("COMMIT")
        except Exception:
            if self._connection.in_transaction:
                cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.close()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def _migrate_dependency_index(self) -> None:
        marker = self._connection.execute(
            "SELECT value FROM dt_adapter_metadata WHERE key = 'dependency_index'"
        ).fetchone()
        if marker is not None and str(marker["value"]) == "v1":
            return
        cursor = self._connection.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")
            events = cursor.execute(
                """
                SELECT * FROM dt_events
                WHERE event_type IN ('ClaimProposed', 'ProjectionIssued', 'EvidenceDeleted')
                ORDER BY twin_id, sequence
                """
            ).fetchall()
            for row in events:
                self._index_dependencies(cursor, self._row_to_event(row))
            cursor.execute(
                """
                INSERT INTO dt_adapter_metadata (key, value)
                VALUES ('dependency_index', 'v1')
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """
            )
            cursor.execute("COMMIT")
        except Exception:
            if self._connection.in_transaction:
                cursor.execute("ROLLBACK")
            raise
        finally:
            cursor.close()

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> EventEnvelope:
        return EventEnvelope(
            event_id=row["event_id"],
            twin_id=row["twin_id"],
            event_type=row["event_type"],
            plane=EventPlane(row["plane"]),
            payload=json.loads(row["payload_json"]),
            occurred_at=parse_time(row["occurred_at"]),
            recorded_at=parse_time(row["recorded_at"]),
            producer=row["producer"],
            producer_role=ProducerRole(row["producer_role"]),
            idempotency_key=row["idempotency_key"],
            schema_version=row["schema_version"],
            sequence=int(row["sequence"]),
            causation_id=row["causation_id"],
            correlation_id=row["correlation_id"],
            previous_hash=row["previous_hash"],
            event_hash=row["event_hash"],
        )

    @staticmethod
    def _row_to_snapshot(row: sqlite3.Row) -> SnapshotRecord:
        snapshot = SnapshotRecord(
            twin_id=str(row["twin_id"]),
            sequence=int(row["sequence"]),
            schema_version=str(row["schema_version"]),
            state=json.loads(str(row["state_json"])),
            state_hash=str(row["state_hash"]),
            last_event_hash=str(row["last_event_hash"]),
            created_at=parse_time(str(row["created_at"])),
        )
        snapshot.verify()
        return snapshot

    @staticmethod
    def _insert_outbox(cursor: sqlite3.Cursor, event: EventEnvelope) -> None:
        cursor.execute(
            """
            INSERT INTO dt_outbox (
                twin_id, sequence, topic, message_key, created_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                event.twin_id,
                event.sequence,
                _EVENT_TOPIC,
                event.twin_id,
                event.recorded_at.isoformat(),
            ),
        )

    @staticmethod
    def _index_dependencies(cursor: sqlite3.Cursor, event: EventEnvelope) -> None:
        if event.event_type == "ClaimProposed":
            claim_id = str(event.payload.get("claim_id", ""))
            for reference in event.payload.get("provenance", []):
                if not isinstance(reference, dict):
                    continue
                evidence_id = str(reference.get("evidence_id", ""))
                if claim_id and evidence_id:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO dt_dependencies (
                            twin_id, source_kind, source_id, dependent_kind,
                            dependent_id, created_sequence
                        ) VALUES (?, 'evidence', ?, 'claim', ?, ?)
                        """,
                        (event.twin_id, evidence_id, claim_id, event.sequence),
                    )
        elif event.event_type == "ProjectionIssued":
            projection_id = str(event.payload.get("projection_id", ""))
            for claim_id_value in event.payload.get("source_claim_ids", []):
                claim_id = str(claim_id_value)
                if projection_id and claim_id:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO dt_dependencies (
                            twin_id, source_kind, source_id, dependent_kind,
                            dependent_id, created_sequence
                        ) VALUES (?, 'claim', ?, 'projection', ?, ?)
                        """,
                        (event.twin_id, claim_id, projection_id, event.sequence),
                    )
        elif event.event_type == "EvidenceDeleted":
            evidence_id = str(event.payload.get("evidence_id", ""))
            if not evidence_id:
                return
            impacted = cursor.execute(
                """
                WITH RECURSIVE impacted(kind, identifier) AS (
                    SELECT dependent_kind, dependent_id
                    FROM dt_dependencies
                    WHERE twin_id = ? AND source_kind = 'evidence' AND source_id = ?
                    UNION
                    SELECT dependency.dependent_kind, dependency.dependent_id
                    FROM dt_dependencies AS dependency
                    JOIN impacted
                      ON dependency.source_kind = impacted.kind
                     AND dependency.source_id = impacted.identifier
                    WHERE dependency.twin_id = ?
                )
                SELECT kind, identifier FROM impacted ORDER BY kind, identifier
                """,
                (event.twin_id, evidence_id, event.twin_id),
            ).fetchall()
            for row in impacted:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO dt_invalidation_queue (
                        twin_id, deletion_sequence, source_evidence_id,
                        dependent_kind, dependent_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.twin_id,
                        event.sequence,
                        evidence_id,
                        str(row["kind"]),
                        str(row["identifier"]),
                        event.recorded_at.isoformat(),
                    ),
                )

    def append(self, event: EventEnvelope, *, expected_sequence: int) -> EventEnvelope:
        if event.schema_version != "argo.dt.event/v2":
            raise InvariantViolation("new event-store writes require EventEnvelope v2")
        if event.plane is EventPlane.SIMULATION:
            raise InvariantViolation("simulation events require an isolated branch store")
        with self._lock:
            cursor = self._connection.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                duplicate = cursor.execute(
                    """
                    SELECT * FROM dt_events
                    WHERE twin_id = ? AND idempotency_key = ?
                    """,
                    (event.twin_id, event.idempotency_key),
                ).fetchone()
                if duplicate is not None:
                    existing = self._row_to_event(duplicate)
                    if (
                        existing.event_type != event.event_type
                        or existing.plane is not event.plane
                        or canonical_json(existing.payload) != canonical_json(event.payload)
                        or existing.producer != event.producer
                        or existing.producer_role is not event.producer_role
                        or existing.schema_version != event.schema_version
                        or existing.occurred_at != event.occurred_at
                        or existing.causation_id != event.causation_id
                        or existing.correlation_id != event.correlation_id
                    ):
                        raise InvariantViolation(
                            "idempotency key was reused for a different request"
                        )
                    cursor.execute("COMMIT")
                    return existing

                head = cursor.execute(
                    """
                    SELECT sequence, event_hash FROM dt_events
                    WHERE twin_id = ? ORDER BY sequence DESC LIMIT 1
                    """,
                    (event.twin_id,),
                ).fetchone()
                actual_sequence = int(head["sequence"]) if head is not None else 0
                previous_hash = str(head["event_hash"]) if head is not None else ""
                if actual_sequence != expected_sequence:
                    raise ConcurrencyConflict(
                        f"expected stream {event.twin_id!r} sequence "
                        f"{expected_sequence}, found {actual_sequence}"
                    )

                sealed = event.seal(
                    sequence=actual_sequence + 1,
                    previous_hash=previous_hash,
                    recorded_at=utc_now(),
                )
                cursor.execute(
                    """
                    INSERT INTO dt_events (
                        twin_id, sequence, event_id, event_type, plane,
                        schema_version, payload_json, occurred_at, recorded_at,
                        producer, producer_role, idempotency_key, causation_id, correlation_id,
                        previous_hash, event_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        sealed.twin_id,
                        sealed.sequence,
                        sealed.event_id,
                        sealed.event_type,
                        sealed.plane.value,
                        sealed.schema_version,
                        canonical_json(sealed.payload),
                        sealed.occurred_at.isoformat(),
                        sealed.recorded_at.isoformat(),
                        sealed.producer,
                        sealed.producer_role.value,
                        sealed.idempotency_key,
                        sealed.causation_id,
                        sealed.correlation_id,
                        sealed.previous_hash,
                        sealed.event_hash,
                    ),
                )
                self._index_dependencies(cursor, sealed)
                self._insert_outbox(cursor, sealed)
                cursor.execute("COMMIT")
                return sealed
            except Exception:
                if self._connection.in_transaction:
                    cursor.execute("ROLLBACK")
                raise
            finally:
                cursor.close()

    def load(
        self,
        twin_id: str,
        *,
        after_sequence: int = 0,
        up_to_sequence: int | None = None,
        planes: frozenset[EventPlane] | None = None,
        limit: int | None = None,
    ) -> list[EventEnvelope]:
        clauses = ["twin_id = ?", "sequence > ?"]
        params: list[object] = [twin_id, after_sequence]
        if up_to_sequence is not None:
            clauses.append("sequence <= ?")
            params.append(up_to_sequence)
        if planes:
            placeholders = ",".join("?" for _ in planes)
            clauses.append(f"plane IN ({placeholders})")
            params.extend(sorted(plane.value for plane in planes))
        sql = f"SELECT * FROM dt_events WHERE {' AND '.join(clauses)} ORDER BY sequence"
        if limit is not None:
            if limit <= 0:
                return []
            sql += " LIMIT ?"
            params.append(limit)
        with self._lock:
            rows = self._connection.execute(sql, params).fetchall()
        return [self._row_to_event(row) for row in rows]

    def head(self, twin_id: str) -> tuple[int, str]:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT sequence, event_hash FROM dt_events
                WHERE twin_id = ? ORDER BY sequence DESC LIMIT 1
                """,
                (twin_id,),
            ).fetchone()
        if row is None:
            return 0, ""
        return int(row["sequence"]), str(row["event_hash"])

    def verify_chain(self, twin_id: str) -> bool:
        previous_hash = ""
        expected_sequence = 1
        for event in self.load(twin_id):
            if event.sequence != expected_sequence:
                raise IntegrityError(
                    f"sequence gap: expected {expected_sequence}, found {event.sequence}"
                )
            event.verify(previous_hash)
            previous_hash = event.event_hash
            expected_sequence += 1
        return True

    def save_snapshot(self, snapshot: SnapshotRecord) -> SnapshotRecord:
        snapshot.verify()
        with self._lock:
            cursor = self._connection.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                event = cursor.execute(
                    """
                    SELECT event_hash FROM dt_events
                    WHERE twin_id = ? AND sequence = ?
                    """,
                    (snapshot.twin_id, snapshot.sequence),
                ).fetchone()
                if event is None:
                    raise IntegrityError("snapshot does not reference a persisted event")
                if str(event["event_hash"]) != snapshot.last_event_hash:
                    raise IntegrityError("snapshot does not match the event-chain position")
                existing = cursor.execute(
                    """
                    SELECT * FROM dt_snapshots
                    WHERE twin_id = ? AND sequence = ?
                    """,
                    (snapshot.twin_id, snapshot.sequence),
                ).fetchone()
                if existing is not None:
                    stored = self._row_to_snapshot(existing)
                    if stored.state_hash != snapshot.state_hash:
                        raise IntegrityError("conflicting snapshot already exists")
                    cursor.execute("COMMIT")
                    return stored
                cursor.execute(
                    """
                    INSERT INTO dt_snapshots (
                        twin_id, sequence, schema_version, state_json, state_hash,
                        last_event_hash, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot.twin_id,
                        snapshot.sequence,
                        snapshot.schema_version,
                        canonical_json(snapshot.state),
                        snapshot.state_hash,
                        snapshot.last_event_hash,
                        snapshot.created_at.isoformat(),
                    ),
                )
                cursor.execute("COMMIT")
                return snapshot
            except Exception:
                if self._connection.in_transaction:
                    cursor.execute("ROLLBACK")
                raise
            finally:
                cursor.close()

    def load_snapshot(
        self,
        twin_id: str,
        *,
        up_to_sequence: int | None = None,
    ) -> SnapshotRecord | None:
        clauses = ["snapshot.twin_id = ?"]
        params: list[object] = [twin_id]
        if up_to_sequence is not None:
            clauses.append("snapshot.sequence <= ?")
            params.append(up_to_sequence)
        with self._lock:
            row = self._connection.execute(
                f"""
                SELECT snapshot.*, event.event_hash AS linked_event_hash
                FROM dt_snapshots AS snapshot
                JOIN dt_events AS event
                  ON event.twin_id = snapshot.twin_id
                 AND event.sequence = snapshot.sequence
                WHERE {' AND '.join(clauses)}
                ORDER BY snapshot.sequence DESC LIMIT 1
                """,
                params,
            ).fetchone()
        if row is None:
            return None
        snapshot = self._row_to_snapshot(row)
        if str(row["linked_event_hash"]) != snapshot.last_event_hash:
            raise IntegrityError("snapshot event-chain link was modified")
        return snapshot

    def prune_snapshots(self, twin_id: str, *, keep: int = 3) -> int:
        if keep < 1:
            raise ValueError("keep must be positive")
        with self._lock:
            cursor = self._connection.execute(
                """
                DELETE FROM dt_snapshots
                WHERE twin_id = ? AND sequence IN (
                    SELECT sequence FROM dt_snapshots
                    WHERE twin_id = ? ORDER BY sequence DESC
                    LIMIT -1 OFFSET ?
                )
                """,
                (twin_id, twin_id, keep),
            )
            return cursor.rowcount

    def claim_outbox(
        self,
        *,
        lease_owner: str,
        limit: int = 100,
        lease_seconds: int = 30,
    ) -> list[OutboxRecord]:
        if not lease_owner:
            raise ValueError("lease_owner is required")
        if limit <= 0:
            return []
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        now = utc_now()
        lease_until = now + timedelta(seconds=lease_seconds)
        records: list[OutboxRecord] = []
        with self._lock:
            cursor = self._connection.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                selected = cursor.execute(
                    """
                    SELECT twin_id, sequence, topic
                    FROM dt_outbox
                    WHERE published_at IS NULL
                      AND (lease_until IS NULL OR lease_until <= ?)
                    ORDER BY created_at, twin_id, sequence, topic
                    LIMIT ?
                    """,
                    (now.isoformat(), limit),
                ).fetchall()
                for selected_row in selected:
                    cursor.execute(
                        """
                        UPDATE dt_outbox
                        SET lease_owner = ?, lease_until = ?, attempts = attempts + 1
                        WHERE twin_id = ? AND sequence = ? AND topic = ?
                          AND published_at IS NULL
                          AND (lease_until IS NULL OR lease_until <= ?)
                        """,
                        (
                            lease_owner,
                            lease_until.isoformat(),
                            selected_row["twin_id"],
                            selected_row["sequence"],
                            selected_row["topic"],
                            now.isoformat(),
                        ),
                    )
                    if cursor.rowcount != 1:
                        continue
                    row = cursor.execute(
                        """
                        SELECT event.*, outbox.topic AS outbox_topic,
                               outbox.message_key AS outbox_key,
                               outbox.created_at AS outbox_created_at,
                               outbox.attempts AS outbox_attempts
                        FROM dt_outbox AS outbox
                        JOIN dt_events AS event
                          ON event.twin_id = outbox.twin_id
                         AND event.sequence = outbox.sequence
                        WHERE outbox.twin_id = ? AND outbox.sequence = ?
                          AND outbox.topic = ?
                        """,
                        (
                            selected_row["twin_id"],
                            selected_row["sequence"],
                            selected_row["topic"],
                        ),
                    ).fetchone()
                    if row is None:
                        raise IntegrityError("outbox row lost its event")
                    event = self._row_to_event(row)
                    event.verify(event.previous_hash)
                    records.append(
                        OutboxRecord(
                            topic=str(row["outbox_topic"]),
                            key=str(row["outbox_key"]),
                            event=event,
                            created_at=parse_time(str(row["outbox_created_at"])),
                            attempts=int(row["outbox_attempts"]),
                            lease_owner=lease_owner,
                        )
                    )
                cursor.execute("COMMIT")
            except Exception:
                if self._connection.in_transaction:
                    cursor.execute("ROLLBACK")
                raise
            finally:
                cursor.close()
        return records

    def mark_outbox_published(self, record: OutboxRecord) -> bool:
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE dt_outbox
                SET published_at = ?, lease_owner = NULL, lease_until = NULL,
                    last_error = NULL
                WHERE twin_id = ? AND sequence = ? AND topic = ?
                  AND published_at IS NULL AND lease_owner = ? AND attempts = ?
                """,
                (
                    utc_now().isoformat(),
                    record.event.twin_id,
                    record.event.sequence,
                    record.topic,
                    record.lease_owner,
                    record.attempts,
                ),
            )
            return cursor.rowcount == 1

    def release_outbox(self, record: OutboxRecord, *, error: str) -> bool:
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE dt_outbox
                SET lease_owner = NULL, lease_until = NULL, last_error = ?
                WHERE twin_id = ? AND sequence = ? AND topic = ?
                  AND published_at IS NULL AND lease_owner = ? AND attempts = ?
                """,
                (
                    error[:2048],
                    record.event.twin_id,
                    record.event.sequence,
                    record.topic,
                    record.lease_owner,
                    record.attempts,
                ),
            )
            return cursor.rowcount == 1

    def outbox_backlog(self) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) AS count FROM dt_outbox WHERE published_at IS NULL"
            ).fetchone()
        return int(row["count"]) if row is not None else 0

    def prune_outbox(self, *, published_before: datetime, limit: int = 1000) -> int:
        if published_before.tzinfo is None:
            raise ValueError("published_before must be timezone-aware")
        if limit <= 0:
            return 0
        with self._lock:
            cursor = self._connection.execute(
                """
                DELETE FROM dt_outbox
                WHERE rowid IN (
                    SELECT rowid FROM dt_outbox
                    WHERE published_at IS NOT NULL AND published_at < ?
                    ORDER BY published_at LIMIT ?
                )
                """,
                (published_before.isoformat(), limit),
            )
            return cursor.rowcount

    def reclaim_space(self, *, pages: int = 1000) -> None:
        if pages < 1:
            raise ValueError("pages must be positive")
        with self._lock:
            self._connection.execute(f"PRAGMA incremental_vacuum({int(pages)})")

    def checkpoint_wal(self) -> tuple[int, int, int]:
        with self._lock:
            row = self._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if row is None:
            raise IntegrityError("SQLite did not return WAL checkpoint status")
        return int(row[0]), int(row[1]), int(row[2])

    def dependents(
        self,
        twin_id: str,
        source_kind: str,
        source_id: str,
        *,
        transitive: bool = True,
    ) -> tuple[tuple[str, str], ...]:
        if source_kind not in {"evidence", "claim"}:
            raise ValueError("source_kind must be evidence or claim")
        with self._lock:
            if transitive:
                rows = self._connection.execute(
                    """
                    WITH RECURSIVE impacted(kind, identifier) AS (
                        SELECT dependent_kind, dependent_id
                        FROM dt_dependencies
                        WHERE twin_id = ? AND source_kind = ? AND source_id = ?
                        UNION
                        SELECT dependency.dependent_kind, dependency.dependent_id
                        FROM dt_dependencies AS dependency
                        JOIN impacted
                          ON dependency.source_kind = impacted.kind
                         AND dependency.source_id = impacted.identifier
                        WHERE dependency.twin_id = ?
                    )
                    SELECT kind, identifier FROM impacted ORDER BY kind, identifier
                    """,
                    (twin_id, source_kind, source_id, twin_id),
                ).fetchall()
            else:
                rows = self._connection.execute(
                    """
                    SELECT dependent_kind AS kind, dependent_id AS identifier
                    FROM dt_dependencies
                    WHERE twin_id = ? AND source_kind = ? AND source_id = ?
                    ORDER BY dependent_kind, dependent_id
                    """,
                    (twin_id, source_kind, source_id),
                ).fetchall()
        return tuple((str(row["kind"]), str(row["identifier"])) for row in rows)

    def pending_invalidations(self, *, limit: int = 100) -> list[InvalidationRecord]:
        if limit <= 0:
            return []
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM dt_invalidation_queue
                WHERE processed_at IS NULL
                ORDER BY created_at, twin_id, deletion_sequence,
                         dependent_kind, dependent_id
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            InvalidationRecord(
                twin_id=str(row["twin_id"]),
                deletion_sequence=int(row["deletion_sequence"]),
                source_evidence_id=str(row["source_evidence_id"]),
                dependent_kind=str(row["dependent_kind"]),
                dependent_id=str(row["dependent_id"]),
                created_at=parse_time(str(row["created_at"])),
            )
            for row in rows
        ]

    def mark_invalidation_processed(self, record: InvalidationRecord) -> bool:
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE dt_invalidation_queue SET processed_at = ?
                WHERE twin_id = ? AND deletion_sequence = ?
                  AND dependent_kind = ? AND dependent_id = ?
                  AND processed_at IS NULL
                """,
                (
                    utc_now().isoformat(),
                    record.twin_id,
                    record.deletion_sequence,
                    record.dependent_kind,
                    record.dependent_id,
                ),
            )
            return cursor.rowcount == 1

    def __enter__(self) -> SQLiteEventStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        self.close()
