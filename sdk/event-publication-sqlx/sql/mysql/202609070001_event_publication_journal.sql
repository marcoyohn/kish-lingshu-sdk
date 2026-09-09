CREATE TABLE `sdk_event_publication_record` (
    `idempotency_key` VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `request_digest` CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `topic` VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `event_type` VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `schema_version` VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `event_json` JSON NOT NULL,
    `state` VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    `recover_after` DATETIME(6) NULL,
    `attempt_count` BIGINT UNSIGNED NOT NULL DEFAULT 0,
    `last_attempt_at` DATETIME(6) NULL,
    `last_failure_code` VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    `last_failure_message` VARCHAR(2048) NULL,
    `receipt_json` JSON NULL,
    `created_at` DATETIME(6) NOT NULL,
    `updated_at` DATETIME(6) NOT NULL,
    PRIMARY KEY (`idempotency_key`),
    KEY `idx_sdk_event_publication_recovery` (`recover_after`, `idempotency_key`),
    CONSTRAINT `chk_sdk_event_publication_state`
        CHECK (`state` IN ('PENDING', 'RETRYABLE_FAILURE', 'PERMANENT_FAILURE', 'ACCEPTED')),
    CONSTRAINT `chk_sdk_event_publication_recoverable`
        CHECK (
            (`state` IN ('PENDING', 'RETRYABLE_FAILURE') AND `recover_after` IS NOT NULL)
            OR
            (`state` IN ('PERMANENT_FAILURE', 'ACCEPTED') AND `recover_after` IS NULL)
        )
) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
