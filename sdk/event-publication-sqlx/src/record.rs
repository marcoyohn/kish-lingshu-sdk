use kish_lingshu_sdk::event_dispatch::{
    DurableEventPublication, EventPublicationFailure, EventPublicationJournalError,
    EventPublicationJournalState, PublishEvent, PublishReceipt,
};

pub(crate) const STATE_PENDING: &str = "PENDING";
pub(crate) const STATE_RETRYABLE_FAILURE: &str = "RETRYABLE_FAILURE";
pub(crate) const STATE_PERMANENT_FAILURE: &str = "PERMANENT_FAILURE";
pub(crate) const STATE_ACCEPTED: &str = "ACCEPTED";

#[derive(sqlx::FromRow)]
pub(crate) struct StoredStateRow {
    pub(crate) request_digest: String,
    pub(crate) state: String,
    pub(crate) receipt_json: Option<String>,
    pub(crate) last_failure_code: Option<String>,
    pub(crate) last_failure_message: Option<String>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct StoredPublicationRow {
    pub(crate) idempotency_key: String,
    pub(crate) request_digest: String,
    pub(crate) event_json: String,
}

pub(crate) fn encode_event(
    publication: &DurableEventPublication,
) -> Result<String, EventPublicationJournalError> {
    serde_json::to_string(publication.event()).map_err(|error| {
        journal_error(
            "event_encode_failure",
            format!("failed to encode durable Event: {error}"),
        )
    })
}

pub(crate) fn encode_receipt(
    receipt: &PublishReceipt,
) -> Result<String, EventPublicationJournalError> {
    serde_json::to_string(receipt).map_err(|error| {
        journal_error(
            "receipt_encode_failure",
            format!("failed to encode Event publication receipt: {error}"),
        )
    })
}

pub(crate) fn decode_publication(
    row: StoredPublicationRow,
) -> Result<DurableEventPublication, EventPublicationJournalError> {
    let event = serde_json::from_str::<PublishEvent>(&row.event_json).map_err(|error| {
        journal_error(
            "event_decode_failure",
            format!("stored durable Event is invalid: {error}"),
        )
    })?;
    let publication =
        DurableEventPublication::new(event, row.idempotency_key).map_err(|error| {
            journal_error(
                "event_validation_failure",
                format!("stored durable Event failed validation: {error}"),
            )
        })?;
    ensure_digest(&row.request_digest, &publication)?;
    Ok(publication)
}

pub(crate) fn decode_state(
    row: StoredStateRow,
    publication: &DurableEventPublication,
) -> Result<EventPublicationJournalState, EventPublicationJournalError> {
    ensure_digest(&row.request_digest, publication)?;
    match row.state.as_str() {
        STATE_PENDING | STATE_RETRYABLE_FAILURE => Ok(EventPublicationJournalState::Pending),
        STATE_ACCEPTED => {
            let receipt_json = row.receipt_json.ok_or_else(|| {
                journal_error(
                    "missing_receipt",
                    "accepted publication journal entry has no receipt",
                )
            })?;
            let receipt =
                serde_json::from_str::<PublishReceipt>(&receipt_json).map_err(|error| {
                    journal_error(
                        "receipt_decode_failure",
                        format!("stored Event publication receipt is invalid: {error}"),
                    )
                })?;
            Ok(EventPublicationJournalState::Accepted(receipt))
        }
        STATE_PERMANENT_FAILURE => {
            let code = row.last_failure_code.ok_or_else(|| {
                journal_error(
                    "missing_failure_code",
                    "permanently failed publication journal entry has no failure code",
                )
            })?;
            Ok(EventPublicationJournalState::PermanentFailure(
                EventPublicationFailure::permanent(code, row.last_failure_message),
            ))
        }
        state => Err(journal_error(
            "invalid_journal_state",
            format!("unsupported publication journal state {state}"),
        )),
    }
}

pub(crate) fn ensure_digest(
    stored_digest: &str,
    publication: &DurableEventPublication,
) -> Result<(), EventPublicationJournalError> {
    if stored_digest == publication.request_digest() {
        return Ok(());
    }
    Err(journal_error(
        "idempotency_conflict",
        "idempotency key is already bound to another Event publication",
    ))
}

pub(crate) fn journal_error(
    code: impl Into<String>,
    error: impl std::fmt::Display,
) -> EventPublicationJournalError {
    EventPublicationJournalError::new(code, error.to_string())
}
