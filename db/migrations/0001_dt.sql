-- PostgreSQL 16 reference schema. Run under a migration role, never an app role.
CREATE SCHEMA IF NOT EXISTS dt;

CREATE TABLE dt.subjects (
    subject_id uuid PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    status text NOT NULL CHECK (status IN ('active', 'suspended', 'deleted'))
);

CREATE TABLE dt.identities (
    subject_id uuid NOT NULL REFERENCES dt.subjects(subject_id),
    identity_id uuid NOT NULL,
    identity_kind text NOT NULL CHECK (
        identity_kind IN (
            'human_principal',
            'cognitive_twin',
            'agent_service',
            'avatar_likeness'
        )
    ),
    valid_from timestamptz NOT NULL,
    valid_until timestamptz,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (subject_id, identity_id),
    CHECK (valid_until IS NULL OR valid_until > valid_from)
);

CREATE TABLE dt.streams (
    twin_id uuid PRIMARY KEY,
    subject_id uuid NOT NULL REFERENCES dt.subjects(subject_id),
    head_sequence bigint NOT NULL DEFAULT 0 CHECK (head_sequence >= 0),
    head_hash text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE dt.events (
    twin_id uuid NOT NULL REFERENCES dt.streams(twin_id),
    sequence bigint NOT NULL CHECK (sequence > 0),
    event_id uuid NOT NULL,
    event_type text NOT NULL,
    plane text NOT NULL CHECK (plane IN ('authoritative', 'projection', 'simulation')),
    schema_version text NOT NULL,
    payload jsonb NOT NULL,
    valid_time tstzrange NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    producer_identity_id uuid NOT NULL,
    idempotency_key text NOT NULL,
    causation_id uuid,
    correlation_id uuid,
    previous_hash text NOT NULL,
    event_hash text NOT NULL,
    PRIMARY KEY (twin_id, sequence),
    UNIQUE (twin_id, event_id),
    UNIQUE (twin_id, idempotency_key)
) PARTITION BY HASH (twin_id);

CREATE TABLE dt.events_p0 PARTITION OF dt.events
    FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE dt.events_p1 PARTITION OF dt.events
    FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE dt.events_p2 PARTITION OF dt.events
    FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE dt.events_p3 PARTITION OF dt.events
    FOR VALUES WITH (MODULUS 4, REMAINDER 3);

CREATE INDEX dt_events_recorded_brin ON dt.events USING brin (recorded_at);
CREATE INDEX dt_events_type_idx ON dt.events (twin_id, event_type, sequence);
CREATE INDEX dt_events_payload_gin ON dt.events USING gin (payload jsonb_path_ops);

CREATE TABLE dt.bronze_objects (
    subject_id uuid NOT NULL REFERENCES dt.subjects(subject_id),
    evidence_id uuid NOT NULL,
    object_uri text NOT NULL,
    content_hash text NOT NULL,
    media_type text NOT NULL,
    sensitivity text NOT NULL CHECK (
        sensitivity IN ('public', 'internal', 'confidential', 'restricted')
    ),
    encryption_key_ref text NOT NULL,
    source_manifest jsonb NOT NULL,
    rights jsonb NOT NULL,
    valid_time tstzrange NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deleted_at timestamptz,
    PRIMARY KEY (subject_id, evidence_id),
    UNIQUE (subject_id, content_hash, object_uri)
);

CREATE TABLE dt.snapshots (
    twin_id uuid NOT NULL REFERENCES dt.streams(twin_id),
    sequence bigint NOT NULL,
    schema_version text NOT NULL,
    state jsonb NOT NULL,
    state_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (twin_id, sequence)
);

CREATE TABLE dt.consent_grants (
    subject_id uuid NOT NULL REFERENCES dt.subjects(subject_id),
    consent_id uuid NOT NULL,
    recipient_id uuid NOT NULL,
    purposes text[] NOT NULL,
    allowed_fields text[] NOT NULL,
    max_sensitivity text NOT NULL CHECK (
        max_sensitivity IN ('public', 'internal', 'confidential', 'restricted')
    ),
    policy_version text NOT NULL,
    valid_time tstzrange NOT NULL,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (subject_id, consent_id)
);

CREATE INDEX dt_consent_lookup_idx
    ON dt.consent_grants (subject_id, recipient_id)
    WHERE revoked_at IS NULL;

CREATE TABLE dt.projection_receipts (
    subject_id uuid NOT NULL REFERENCES dt.subjects(subject_id),
    projection_id uuid NOT NULL,
    twin_id uuid NOT NULL REFERENCES dt.streams(twin_id),
    request_id uuid NOT NULL,
    recipient_id uuid NOT NULL,
    purpose text NOT NULL,
    consent_id uuid NOT NULL,
    policy_version text NOT NULL,
    source_sequence bigint NOT NULL,
    disclosed_fields text[] NOT NULL,
    source_claim_ids uuid[] NOT NULL,
    artifact_hash text NOT NULL,
    issued_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    PRIMARY KEY (subject_id, projection_id),
    CHECK (expires_at > issued_at)
);

CREATE TABLE dt.outbox (
    twin_id uuid NOT NULL,
    sequence bigint NOT NULL,
    topic text NOT NULL,
    key text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    PRIMARY KEY (twin_id, sequence, topic),
    FOREIGN KEY (twin_id, sequence) REFERENCES dt.events(twin_id, sequence)
);

CREATE INDEX dt_outbox_pending_idx ON dt.outbox (created_at)
    WHERE published_at IS NULL;

-- App writes must update streams, insert events, and insert the outbox row in
-- one SERIALIZABLE transaction. Hash computation remains in the application
-- so canonicalization is identical across storage adapters.

ALTER TABLE dt.identities ENABLE ROW LEVEL SECURITY;
ALTER TABLE dt.streams ENABLE ROW LEVEL SECURITY;
ALTER TABLE dt.events ENABLE ROW LEVEL SECURITY;
ALTER TABLE dt.bronze_objects ENABLE ROW LEVEL SECURITY;
ALTER TABLE dt.snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE dt.consent_grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE dt.projection_receipts ENABLE ROW LEVEL SECURITY;

CREATE POLICY subject_identity_isolation ON dt.identities
    USING (subject_id::text = current_setting('argo.subject_id', true));
CREATE POLICY subject_stream_isolation ON dt.streams
    USING (subject_id::text = current_setting('argo.subject_id', true));
CREATE POLICY subject_bronze_isolation ON dt.bronze_objects
    USING (subject_id::text = current_setting('argo.subject_id', true));
CREATE POLICY subject_consent_isolation ON dt.consent_grants
    USING (subject_id::text = current_setting('argo.subject_id', true));
CREATE POLICY subject_projection_isolation ON dt.projection_receipts
    USING (subject_id::text = current_setting('argo.subject_id', true));

CREATE POLICY subject_event_isolation ON dt.events
    USING (
        EXISTS (
            SELECT 1
            FROM dt.streams s
            WHERE s.twin_id = events.twin_id
              AND s.subject_id::text = current_setting('argo.subject_id', true)
        )
    );
CREATE POLICY subject_snapshot_isolation ON dt.snapshots
    USING (
        EXISTS (
            SELECT 1
            FROM dt.streams s
            WHERE s.twin_id = snapshots.twin_id
              AND s.subject_id::text = current_setting('argo.subject_id', true)
        )
    );

REVOKE ALL ON SCHEMA dt FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA dt FROM PUBLIC;

