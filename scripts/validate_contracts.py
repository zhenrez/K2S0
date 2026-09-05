"""Fail CI when language-neutral contracts drift from executable semantics."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

try:
    import yaml
except ImportError as exc:  # pragma: no cover - exercised by environment setup
    raise SystemExit("PyYAML 6.0.2 is required for contract validation") from exc


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_ROLES = {
    "ingest_service",
    "identity_worker",
    "adjudication_worker",
    "human_review",
    "compiler",
    "projection_service",
    "simulation_service",
    "downstream_agent",
    "operations_service",
}


def fail(message: str) -> None:
    raise SystemExit(f"contract validation failed: {message}")


def main() -> None:
    schema_files = sorted((ROOT / "schemas").glob("*.schema.json"))
    if not schema_files:
        fail("no JSON Schemas found")
    schemas = {
        path.name: json.loads(path.read_text(encoding="utf-8"))
        for path in schema_files
    }
    legacy_schema = schemas["event-envelope-v1.schema.json"]
    if "producer_role" in legacy_schema["required"]:
        fail("EventEnvelope v1 must remain byte/hash compatible")
    event_schema = schemas["event-envelope-v2.schema.json"]
    if "producer_role" not in event_schema["required"]:
        fail("EventEnvelope v2 does not require producer_role")
    schema_roles = set(event_schema["properties"]["producer_role"]["enum"])
    if schema_roles != EXPECTED_ROLES:
        fail("EventEnvelope producer roles drifted from the frozen role set")
    disclosed_claim = schemas["projection-v1.schema.json"]["$defs"][
        "disclosedClaim"
    ]
    if disclosed_claim.get("additionalProperties") is not False:
        fail("projection claims must deny undisclosed internal fields")
    forbidden_projection_fields = {
        "claim_id",
        "subject_id",
        "provenance",
        "model_trace",
        "recorded_at",
        "review_state",
        "sensitivity",
    }
    if forbidden_projection_fields.intersection(disclosed_claim["properties"]):
        fail("projection claim schema exposes internal lineage fields")

    openapi = yaml.safe_load((ROOT / "openapi" / "dt-v1.yaml").read_text())
    if openapi.get("openapi") != "3.1.0":
        fail("OpenAPI version must be 3.1.0")
    projection = openapi["components"]["schemas"]["ProjectionResponse"]
    if projection.get("required") != ["projection", "receipt"]:
        fail("projection response shape drifted")
    revoke = openapi["paths"][
        "/v1/twins/{twin_id}/projections/{projection_id}/revoke"
    ]["post"]
    if "requestBody" not in revoke:
        fail("projection revocation requires an explicit reason body")

    proto = (ROOT / "proto" / "argo" / "dt" / "v1" / "twin.proto").read_text()
    if "string producer_role = 15;" not in proto:
        fail("protobuf TwinEvent is missing producer_role field 15")
    if "google.protobuf.Struct projection = 1;" not in proto:
        fail("protobuf projection response drifted from the REST/service contract")
    sql = (ROOT / "db" / "migrations" / "0001_dt.sql").read_text()
    if "producer_role text not null" not in sql.lower():
        fail("SQLite event ledger is missing producer_role")
    forbidden_postgres = {
        "PARTITION BY",
        "tstzrange",
        "::jsonb",
        "ENABLE ROW LEVEL SECURITY",
    }
    if any(token in sql for token in forbidden_postgres):
        fail("SQLite migration contains PostgreSQL-only syntax")
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript(sql)
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    except sqlite3.Error as exc:
        fail(f"SQLite migration does not execute: {exc}")
    finally:
        connection.close()
    required_tables = {
        "dt_events",
        "dt_snapshots",
        "dt_outbox",
        "dt_dependencies",
        "dt_invalidation_queue",
    }
    if not required_tables.issubset(tables):
        fail("SQLite migration is missing authoritative persistence tables")

    print(
        f"validated {len(schema_files)} JSON Schemas, OpenAPI 3.1, protobuf, and SQLite"
    )


if __name__ == "__main__":
    main()
