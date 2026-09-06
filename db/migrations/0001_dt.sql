-- SQLite 3.45+ authoritative embedded schema.
-- Use one database file per tenant/security boundary and mode 0600 files.
PRAGMA foreign_keys = ON;
PRAGMA auto_vacuum = INCREMENTAL;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;

CREATE TABLE IF NOT EXISTS dt_subjects (
    subject_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'suspended', 'deleted'))
);

CREATE TABLE IF NOT EXISTS dt_identities (
    subject_id TEXT NOT NULL REFERENCES dt_subjects(subject_id),
    identity_id TEXT NOT NULL,
    identity_kind TEXT NOT NULL CHECK (
        identity_kind IN (
            'human_principal', 'cognitive_twin',
            'agent_service', 'avatar_likeness'
        )
    ),
    valid_from TEXT NOT NULL,
    valid_until TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (subject_id, identity_id),
    CHECK (valid_until IS NULL OR valid_until > valid_from),
    CHECK (json_valid(metadata_json))
);

CREATE TABLE IF NOT EXISTS dt_events (
    twin_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    plane TEXT NOT NULL CHECK (plane IN ('authoritative', 'projection')),
    schema_version TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    occurred_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    producer TEXT NOT NULL,
    producer_role TEXT NOT NULL CHECK (
        producer_role IN (
            'ingest_service', 'identity_worker', 'adjudication_worker',
            'human_review', 'compiler', 'projection_service',
            'simulation_service', 'downstream_agent', 'operations_service'
        )
    ),
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

CREATE TABLE IF NOT EXISTS dt_bronze_objects (
    subject_id TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    object_uri TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    media_type TEXT NOT NULL,
    sensitivity TEXT NOT NULL CHECK (
        sensitivity IN ('public', 'internal', 'confidential', 'restricted')
    ),
    encryption_key_ref TEXT NOT NULL,
    source_manifest_json TEXT NOT NULL CHECK (json_valid(source_manifest_json)),
    rights_json TEXT NOT NULL CHECK (json_valid(rights_json)),
    valid_from TEXT NOT NULL,
    valid_until TEXT,
    recorded_at TEXT NOT NULL,
    deleted_at TEXT,
    PRIMARY KEY (subject_id, evidence_id),
    UNIQUE (subject_id, content_hash, object_uri)
);

CREATE TABLE IF NOT EXISTS dt_snapshots (
    twin_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    schema_version TEXT NOT NULL,
    state_json TEXT NOT NULL CHECK (json_valid(state_json)),
    state_hash TEXT NOT NULL,
    last_event_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (twin_id, sequence),
    FOREIGN KEY (twin_id, sequence) REFERENCES dt_events(twin_id, sequence)
);

CREATE TABLE IF NOT EXISTS dt_consent_grants (
    subject_id TEXT NOT NULL,
    consent_id TEXT NOT NULL,
    recipient_id TEXT NOT NULL,
    purposes_json TEXT NOT NULL CHECK (json_valid(purposes_json)),
    allowed_fields_json TEXT NOT NULL CHECK (json_valid(allowed_fields_json)),
    max_sensitivity TEXT NOT NULL CHECK (
        max_sensitivity IN ('public', 'internal', 'confidential', 'restricted')
    ),
    policy_version TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_until TEXT NOT NULL,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (subject_id, consent_id)
);

CREATE INDEX IF NOT EXISTS dt_consent_lookup_idx
    ON dt_consent_grants (subject_id, recipient_id)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS dt_projection_receipts (
    subject_id TEXT NOT NULL,
    projection_id TEXT NOT NULL,
    twin_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    recipient_id TEXT NOT NULL,
    purpose TEXT NOT NULL,
    consent_id TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    source_sequence INTEGER NOT NULL,
    disclosed_fields_json TEXT NOT NULL CHECK (json_valid(disclosed_fields_json)),
    source_claim_ids_json TEXT NOT NULL CHECK (json_valid(source_claim_ids_json)),
    artifact_hash TEXT NOT NULL,
    issued_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    revoked_at TEXT,
    PRIMARY KEY (subject_id, projection_id),
    CHECK (expires_at > issued_at)
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
    source_kind TEXT NOT NULL CHECK (
        source_kind IN (
            'evidence', 'entity', 'claim', 'contradiction',
            'correction', 'projection'
        )
    ),
    source_id TEXT NOT NULL,
    dependent_kind TEXT NOT NULL CHECK (
        dependent_kind IN (
            'evidence', 'entity', 'claim', 'contradiction',
            'correction', 'projection'
        )
    ),
    dependent_id TEXT NOT NULL,
    created_sequence INTEGER NOT NULL,
    relation TEXT NOT NULL,
    PRIMARY KEY (
        twin_id, source_kind, source_id, dependent_kind, dependent_id, relation
    ),
    FOREIGN KEY (twin_id, created_sequence) REFERENCES dt_events(twin_id, sequence)
);

CREATE INDEX IF NOT EXISTS dt_dependencies_source_idx
    ON dt_dependencies (
        twin_id, source_kind, source_id, created_sequence,
        dependent_kind, dependent_id
    );
CREATE INDEX IF NOT EXISTS dt_dependencies_dependent_idx
    ON dt_dependencies (
        twin_id, dependent_kind, dependent_id, created_sequence,
        source_kind, source_id
    );

CREATE TABLE IF NOT EXISTS dt_invalidation_queue (
    twin_id TEXT NOT NULL,
    deletion_sequence INTEGER NOT NULL,
    source_evidence_id TEXT NOT NULL,
    dependent_kind TEXT NOT NULL,
    dependent_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    processed_at TEXT,
    PRIMARY KEY (twin_id, deletion_sequence, dependent_kind, dependent_id),
    FOREIGN KEY (twin_id, deletion_sequence) REFERENCES dt_events(twin_id, sequence)
);

CREATE INDEX IF NOT EXISTS dt_invalidation_pending_idx
    ON dt_invalidation_queue (processed_at, created_at);

CREATE TABLE IF NOT EXISTS dt_adapter_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Event, dependency, invalidation, and outbox inserts are one BEGIN IMMEDIATE
-- transaction in SQLiteEventStore. Publication remains at-least-once.
