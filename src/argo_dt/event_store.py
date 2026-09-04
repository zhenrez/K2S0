"""SQLite reference adapter for the authoritative event ledger."""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

from .errors import ConcurrencyConflict, IntegrityError, InvariantViolation
from .types import EventEnvelope, EventPlane, canonical_json, parse_time, utc_now

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
"""


class SQLiteEventStore:
    """Transactional event store with per-twin linearizable appends.

    BEGIN IMMEDIATE serializes writers. Production PostgreSQL and Restate
    adapters should preserve the same expected-sequence and idempotency
    semantics.
    """

    def __init__(self, path: str | Path = ":memory:") -> None:
        self._path = str(path)
        self._connection = sqlite3.connect(
            self._path,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute("PRAGMA journal_mode = WAL")
            self._connection.execute("PRAGMA synchronous = FULL")
            self._connection.executescript(_DDL)

    def close(self) -> None:
        with self._lock:
            self._connection.close()

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
            idempotency_key=row["idempotency_key"],
            schema_version=row["schema_version"],
            sequence=int(row["sequence"]),
            causation_id=row["causation_id"],
            correlation_id=row["correlation_id"],
            previous_hash=row["previous_hash"],
            event_hash=row["event_hash"],
        )

    def append(self, event: EventEnvelope, *, expected_sequence: int) -> EventEnvelope:
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
                        or existing.schema_version != event.schema_version
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
                        producer, idempotency_key, causation_id, correlation_id,
                        previous_hash, event_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        sealed.idempotency_key,
                        sealed.causation_id,
                        sealed.correlation_id,
                        sealed.previous_hash,
                        sealed.event_hash,
                    ),
                )
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

    def __enter__(self) -> SQLiteEventStore:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        self.close()
