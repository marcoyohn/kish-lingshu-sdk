use async_trait::async_trait;
use chrono::{DateTime, Utc};
use kish_lingshu_sdk::event_dispatch::{
    DurableEventPublication, EventPublicationFailure, EventPublicationFailureKind,
    EventPublicationJournal, EventPublicationJournalError, EventPublicationJournalState,
    EventPublicationScope, PublishReceipt,
};
use sqlx::{migrate::Migrator, Sqlite, SqlitePool, Transaction};

use crate::record::{
    decode_publication, decode_state, encode_event, encode_receipt, ensure_digest, journal_error,
    StoredPublicationRow, StoredStateRow, STATE_ACCEPTED, STATE_PENDING, STATE_PERMANENT_FAILURE,
    STATE_RETRYABLE_FAILURE,
};

static SQLITE_MIGRATOR: Migrator = sqlx::migrate!("./sql/sqlite");

/// SQLite producer journal for direct reliable Event Dispatch publication.
#[derive(Clone)]
pub struct SqliteEventPublicationJournal {
    pool: SqlitePool,
    scope: EventPublicationScope,
}

impl SqliteEventPublicationJournal {
    pub fn new(pool: SqlitePool, scope: EventPublicationScope) -> Self {
        Self { pool, scope }
    }

    /// Applies only the versioned SQLite publication-journal migrations.
    pub async fn migrate(&self) -> Result<(), EventPublicationJournalError> {
        // Preflight avoids a dirty MySQL migration ledger for unresolved old rows.
        // Stop old writers before upgrade; the SQL migration repeats this guard.
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='sdk_event_publication_record'")
            .fetch_one(&self.pool).await.map_err(|e| journal_error("migration_preflight", e))?;
        if exists > 0 {
            let scoped: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pragma_table_info('sdk_event_publication_record') WHERE name='application_id'")
                .fetch_one(&self.pool).await.map_err(|e| journal_error("migration_preflight", e))?;
            if scoped == 0 {
                let pending: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sdk_event_publication_record WHERE state IN ('PENDING','RETRYABLE_FAILURE')")
                    .fetch_one(&self.pool).await.map_err(|e| journal_error("migration_preflight", e))?;
                if pending > 0 {
                    return Err(journal_error("unscoped_publications_pending", "drain pending publications with the previous SDK before upgrading journal ownership"));
                }
            }
        }
        SQLITE_MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|error| journal_error("migration_failure", error))
    }

    async fn stored_state(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        publication: &DurableEventPublication,
    ) -> Result<EventPublicationJournalState, EventPublicationJournalError> {
        self.scope.ensure_matches(publication.scope())?;
        let row = sqlx::query_as::<_, StoredStateRow>(
            "SELECT request_digest, state, receipt_json, last_failure_code, \
             last_failure_message FROM sdk_event_publication_record \
             WHERE application_id = ? AND publisher_id = ? AND idempotency_key = ?",
        )
        .bind(self.scope.application_id())
        .bind(self.scope.publisher_id())
        .bind(publication.idempotency_key())
        .fetch_one(&mut **transaction)
        .await
        .map_err(|error| journal_error("state_read_failure", error))?;
        decode_state(row, publication)
    }

    async fn verify_exact_record(
        &self,
        publication: &DurableEventPublication,
    ) -> Result<(), EventPublicationJournalError> {
        self.scope.ensure_matches(publication.scope())?;
        let stored_digest = sqlx::query_scalar::<_, String>(
            "SELECT request_digest FROM sdk_event_publication_record \
             WHERE application_id = ? AND publisher_id = ? AND idempotency_key = ?",
        )
        .bind(self.scope.application_id())
        .bind(self.scope.publisher_id())
        .bind(publication.idempotency_key())
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| journal_error("state_read_failure", error))?
        .ok_or_else(|| journal_error("missing_record", "publication journal entry not found"))?;
        ensure_digest(&stored_digest, publication)
    }

    async fn mark_failed_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        publication: &DurableEventPublication,
        failure: &EventPublicationFailure,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<bool, EventPublicationJournalError> {
        self.scope.ensure_matches(publication.scope())?;
        let (state, retry_at) = match failure.kind() {
            EventPublicationFailureKind::Retryable => (
                STATE_RETRYABLE_FAILURE,
                Some(
                    retry_at
                        .ok_or_else(|| {
                            journal_error(
                                "missing_retry_time",
                                "retryable failure requires a retry time",
                            )
                        })?
                        .naive_utc(),
                ),
            ),
            EventPublicationFailureKind::Permanent => (STATE_PERMANENT_FAILURE, None),
        };
        let now = Utc::now().naive_utc();
        let result = sqlx::query(
            "UPDATE sdk_event_publication_record \
             SET state = ?, recover_after = ?, attempt_count = attempt_count + 1, \
                 last_attempt_at = ?, last_failure_code = ?, last_failure_message = ?, \
                 receipt_json = NULL, updated_at = ? \
             WHERE application_id = ? AND publisher_id = ? AND idempotency_key = ? AND request_digest = ? AND state IN (?, ?)",
        )
        .bind(state)
        .bind(retry_at)
        .bind(now)
        .bind(failure.code())
        .bind(failure.message())
        .bind(now)
        .bind(self.scope.application_id())
        .bind(self.scope.publisher_id())
        .bind(publication.idempotency_key())
        .bind(publication.request_digest())
        .bind(STATE_PENDING)
        .bind(STATE_RETRYABLE_FAILURE)
        .execute(&mut **transaction)
        .await
        .map_err(|error| journal_error("failure_update_failure", error))?;
        if result.rows_affected() == 1 {
            return Ok(true);
        }
        self.stored_state(transaction, publication).await?;
        Ok(false)
    }
}

#[async_trait]
impl EventPublicationJournal for SqliteEventPublicationJournal {
    fn scope(&self) -> &EventPublicationScope {
        &self.scope
    }

    type Transaction<'transaction> = Transaction<'transaction, Sqlite>;

    async fn append_standalone(
        &self,
        publication: &DurableEventPublication,
        recover_after: DateTime<Utc>,
    ) -> Result<EventPublicationJournalState, EventPublicationJournalError> {
        self.scope.ensure_matches(publication.scope())?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| journal_error("transaction_begin_failure", error))?;
        let state = self
            .append(&mut transaction, publication, recover_after)
            .await?;
        transaction
            .commit()
            .await
            .map_err(|error| journal_error("transaction_commit_failure", error))?;
        Ok(state)
    }

    async fn record_failure_standalone(
        &self,
        publication: &DurableEventPublication,
        failure: &EventPublicationFailure,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<EventPublicationJournalState, EventPublicationJournalError> {
        // The intermediate intent and failure transition share one transaction.
        // In particular, a permanent failure can never escape as a recoverable row.
        self.scope.ensure_matches(publication.scope())?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| journal_error("transaction_begin_failure", error))?;
        let state = self
            .append(
                &mut transaction,
                publication,
                retry_at.unwrap_or_else(Utc::now),
            )
            .await?;
        let state = match state {
            EventPublicationJournalState::Pending => {
                self.mark_failed_in_transaction(&mut transaction, publication, failure, retry_at)
                    .await?;
                self.stored_state(&mut transaction, publication).await?
            }
            terminal => terminal,
        };
        transaction
            .commit()
            .await
            .map_err(|error| journal_error("transaction_commit_failure", error))?;
        Ok(state)
    }

    async fn append(
        &self,
        transaction: &mut Self::Transaction<'_>,
        publication: &DurableEventPublication,
        recover_after: DateTime<Utc>,
    ) -> Result<EventPublicationJournalState, EventPublicationJournalError> {
        self.scope.ensure_matches(publication.scope())?;
        let event_json = encode_event(publication)?;
        let now = Utc::now().naive_utc();
        sqlx::query(
            "INSERT OR IGNORE INTO sdk_event_publication_record \
             (application_id, publisher_id, idempotency_key, request_digest, topic, event_type, schema_version, event_json, \
              state, recover_after, attempt_count, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
        )
        .bind(self.scope.application_id())
        .bind(self.scope.publisher_id())
        .bind(publication.idempotency_key())
        .bind(publication.request_digest())
        .bind(&publication.event().topic)
        .bind(&publication.event().event_type)
        .bind(&publication.event().schema_version)
        .bind(event_json)
        .bind(STATE_PENDING)
        .bind(recover_after.naive_utc())
        .bind(now)
        .bind(now)
        .execute(&mut **transaction)
        .await
        .map_err(|error| journal_error("append_failure", error))?;
        self.stored_state(transaction, publication).await
    }

    async fn load_due(
        &self,
        due_at: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<DurableEventPublication>, EventPublicationJournalError> {
        let rows = sqlx::query_as::<_, StoredPublicationRow>(
            "SELECT application_id, publisher_id, idempotency_key, request_digest, event_json \
             FROM sdk_event_publication_record \
             WHERE application_id = ? AND publisher_id = ? AND recover_after <= ? AND state IN (?, ?) \
             ORDER BY recover_after, idempotency_key LIMIT ?",
        )
        .bind(self.scope.application_id())
        .bind(self.scope.publisher_id())
        .bind(due_at.naive_utc())
        .bind(STATE_PENDING)
        .bind(STATE_RETRYABLE_FAILURE)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| journal_error("due_read_failure", error))?;
        rows.into_iter().map(decode_publication).collect()
    }

    async fn mark_accepted(
        &self,
        publication: &DurableEventPublication,
        receipt: &PublishReceipt,
    ) -> Result<bool, EventPublicationJournalError> {
        self.scope.ensure_matches(publication.scope())?;
        let receipt_json = encode_receipt(receipt)?;
        let now = Utc::now().naive_utc();
        let result = sqlx::query(
            "UPDATE sdk_event_publication_record \
             SET state = ?, recover_after = NULL, attempt_count = attempt_count + 1, \
                 last_attempt_at = ?, last_failure_code = NULL, last_failure_message = NULL, \
                 receipt_json = ?, updated_at = ? \
             WHERE application_id = ? AND publisher_id = ? AND idempotency_key = ? AND request_digest = ? AND state IN (?, ?)",
        )
        .bind(STATE_ACCEPTED)
        .bind(now)
        .bind(receipt_json)
        .bind(now)
        .bind(self.scope.application_id())
        .bind(self.scope.publisher_id())
        .bind(publication.idempotency_key())
        .bind(publication.request_digest())
        .bind(STATE_PENDING)
        .bind(STATE_RETRYABLE_FAILURE)
        .execute(&self.pool)
        .await
        .map_err(|error| journal_error("accept_update_failure", error))?;
        if result.rows_affected() == 1 {
            return Ok(true);
        }
        self.verify_exact_record(publication).await?;
        Ok(false)
    }

    async fn mark_failed(
        &self,
        publication: &DurableEventPublication,
        failure: &EventPublicationFailure,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<bool, EventPublicationJournalError> {
        self.scope.ensure_matches(publication.scope())?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| journal_error("transaction_begin_failure", error))?;
        let updated = self
            .mark_failed_in_transaction(&mut transaction, publication, failure, retry_at)
            .await?;
        transaction
            .commit()
            .await
            .map_err(|error| journal_error("transaction_commit_failure", error))?;
        Ok(updated)
    }
}
