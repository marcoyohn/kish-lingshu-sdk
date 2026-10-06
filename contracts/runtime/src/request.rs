use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub use kish_lingshu_foundation_contract::{
    IdempotencyKey, InvalidIdempotencyKey, MutationDisposition, MutationReceipt,
    MAX_IDEMPOTENCY_KEY_BYTES,
};

/// Absolute deadline for one logical SDK operation, preserved across attempts.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RequestDeadline(pub DateTime<Utc>);

impl RequestDeadline {
    pub fn new(value: DateTime<Utc>) -> Self {
        Self(value)
    }

    pub fn as_datetime(&self) -> &DateTime<Utc> {
        &self.0
    }

    pub fn is_elapsed_at(&self, now: DateTime<Utc>) -> bool {
        self.0 <= now
    }
}

impl From<DateTime<Utc>> for RequestDeadline {
    fn from(value: DateTime<Utc>) -> Self {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idempotency_keys_are_non_empty_bounded_visible_ascii() {
        assert!(IdempotencyKey::new("order/42/created").is_ok());
        assert!(IdempotencyKey::new("").is_err());
        assert!(IdempotencyKey::new("contains space").is_err());
        assert!(IdempotencyKey::new("x".repeat(MAX_IDEMPOTENCY_KEY_BYTES + 1)).is_err());
    }

    #[test]
    fn deadline_and_receipt_use_utc_wire_timestamps() {
        let accepted_at = "2026-09-06T08:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let deadline = RequestDeadline::new(accepted_at);
        assert_eq!(
            serde_json::to_string(&deadline).unwrap(),
            "\"2026-09-06T08:00:00Z\""
        );

        let receipt = MutationReceipt::accepted("request-1", accepted_at);
        assert_eq!(
            serde_json::to_value(receipt).unwrap()["disposition"],
            "accepted"
        );
    }
}
