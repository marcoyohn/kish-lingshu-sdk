-- Stop old writers and drain recoverable rows before upgrading. Preserve terminal
-- history in the reserved empty owner, which runtime scope validation rejects.
CREATE TEMPORARY TABLE sdk_unscoped_publications_must_be_drained (id INTEGER PRIMARY KEY);
INSERT INTO sdk_unscoped_publications_must_be_drained VALUES (1);
INSERT INTO sdk_unscoped_publications_must_be_drained SELECT 1 FROM sdk_event_publication_record WHERE state IN ('PENDING','RETRYABLE_FAILURE') LIMIT 1;
DROP TABLE sdk_unscoped_publications_must_be_drained;
ALTER TABLE sdk_event_publication_record
    ADD COLUMN application_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' FIRST,
    ADD COLUMN publisher_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '' AFTER application_id,
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (application_id, publisher_id, idempotency_key),
    DROP INDEX idx_sdk_event_publication_recovery,
    ADD INDEX idx_sdk_event_publication_recovery (application_id, publisher_id, recover_after, idempotency_key);
ALTER TABLE sdk_event_publication_record
    ALTER COLUMN application_id DROP DEFAULT,
    ALTER COLUMN publisher_id DROP DEFAULT;
