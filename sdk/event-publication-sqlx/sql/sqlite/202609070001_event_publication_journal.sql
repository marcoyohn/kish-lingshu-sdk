CREATE TABLE sdk_event_publication_record (
    idempotency_key TEXT NOT NULL PRIMARY KEY,
    request_digest TEXT NOT NULL,
    topic TEXT NOT NULL,
    event_type TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    event_json TEXT NOT NULL CHECK (json_valid(event_json)),
    state TEXT NOT NULL CHECK (
        state IN ('PENDING', 'RETRYABLE_FAILURE', 'PERMANENT_FAILURE', 'ACCEPTED')
    ),
    recover_after TEXT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_attempt_at TEXT NULL,
    last_failure_code TEXT NULL CHECK (
        last_failure_code IS NULL OR length(last_failure_code) <= 64
    ),
    last_failure_message TEXT NULL CHECK (
        last_failure_message IS NULL OR length(last_failure_message) <= 512
    ),
    receipt_json TEXT NULL CHECK (
        receipt_json IS NULL OR json_valid(receipt_json)
    ),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (
        (state IN ('PENDING', 'RETRYABLE_FAILURE') AND recover_after IS NOT NULL)
        OR
        (state IN ('PERMANENT_FAILURE', 'ACCEPTED') AND recover_after IS NULL)
    )
);

CREATE INDEX idx_sdk_event_publication_recovery
    ON sdk_event_publication_record (recover_after, idempotency_key);
