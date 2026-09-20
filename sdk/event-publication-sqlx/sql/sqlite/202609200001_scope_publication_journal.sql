-- Stop old writers and drain recoverable rows before upgrading. Preserve terminal
-- history in the reserved empty owner, which runtime scope validation rejects.
CREATE TEMPORARY TABLE sdk_unscoped_publications_must_be_drained (id INTEGER PRIMARY KEY);
INSERT INTO sdk_unscoped_publications_must_be_drained VALUES (1);
INSERT INTO sdk_unscoped_publications_must_be_drained SELECT 1 FROM sdk_event_publication_record WHERE state IN ('PENDING','RETRYABLE_FAILURE') LIMIT 1;
DROP TABLE sdk_unscoped_publications_must_be_drained;
ALTER TABLE sdk_event_publication_record RENAME TO sdk_event_publication_record_unscoped;
DROP INDEX idx_sdk_event_publication_recovery;
CREATE TABLE sdk_event_publication_record (
    application_id TEXT NOT NULL,
    publisher_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
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
    PRIMARY KEY (application_id, publisher_id, idempotency_key),
    CHECK (
        (state IN ('PENDING', 'RETRYABLE_FAILURE') AND recover_after IS NOT NULL)
        OR
        (state IN ('PERMANENT_FAILURE', 'ACCEPTED') AND recover_after IS NULL)
    )
);

CREATE INDEX idx_sdk_event_publication_recovery
    ON sdk_event_publication_record (application_id, publisher_id, recover_after, idempotency_key);

INSERT INTO sdk_event_publication_record (application_id,publisher_id,idempotency_key,request_digest,topic,event_type,schema_version,event_json,state,recover_after,attempt_count,last_attempt_at,last_failure_code,last_failure_message,receipt_json,created_at,updated_at) SELECT '', '', idempotency_key,request_digest,topic,event_type,schema_version,event_json,state,recover_after,attempt_count,last_attempt_at,last_failure_code,last_failure_message,receipt_json,created_at,updated_at FROM sdk_event_publication_record_unscoped;
DROP TABLE sdk_event_publication_record_unscoped;
