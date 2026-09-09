//! Dependency-leaf value primitives shared by Kish Lingshu contracts.
//!
//! Domain ports remain in their owning contracts. This crate contains only
//! identities and receipts needed on both sides of those contract boundaries.

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use chrono::{DateTime, Utc};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

macro_rules! string_identity {
    ($name:ident) => {
        #[derive(
            Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                Display::fmt(&self.0, formatter)
            }
        }
    };
}

string_identity!(RequestId);
string_identity!(CorrelationId);

pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 255;

/// Stable identity used to bind a mutation to its normalized request digest.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidIdempotencyKey> {
        let value = value.into();
        validate_idempotency_key(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for IdempotencyKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for IdempotencyKey {
    type Err = InvalidIdempotencyKey;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for IdempotencyKey {
    type Error = InvalidIdempotencyKey;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for IdempotencyKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for IdempotencyKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid idempotency key: {reason}")]
pub struct InvalidIdempotencyKey {
    reason: &'static str,
}

fn validate_idempotency_key(value: &str) -> Result<(), InvalidIdempotencyKey> {
    if value.is_empty() {
        return Err(InvalidIdempotencyKey {
            reason: "value must not be empty",
        });
    }
    if value.len() > MAX_IDEMPOTENCY_KEY_BYTES {
        return Err(InvalidIdempotencyKey {
            reason: "value is longer than 255 bytes",
        });
    }
    if !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
        return Err(InvalidIdempotencyKey {
            reason: "value must contain visible ASCII characters only",
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationDisposition {
    Accepted,
    Duplicate,
}

/// Common fields carried by receipts for durably accepted SDK mutations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MutationReceipt {
    pub request_id: RequestId,
    pub disposition: MutationDisposition,
    pub accepted_at: DateTime<Utc>,
}

impl MutationReceipt {
    pub fn accepted(request_id: impl Into<RequestId>, accepted_at: DateTime<Utc>) -> Self {
        Self {
            request_id: request_id.into(),
            disposition: MutationDisposition::Accepted,
            accepted_at,
        }
    }

    pub fn duplicate(request_id: impl Into<RequestId>, accepted_at: DateTime<Utc>) -> Self {
        Self {
            request_id: request_id.into(),
            disposition: MutationDisposition::Duplicate,
            accepted_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_and_receipts_have_stable_wire_forms() {
        let accepted_at = "2026-09-06T08:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let receipt = MutationReceipt::accepted("request-1", accepted_at);
        assert_eq!(receipt.request_id.as_ref(), "request-1");
        assert_eq!(
            serde_json::to_value(receipt).unwrap()["disposition"],
            "accepted"
        );
    }

    #[test]
    fn idempotency_keys_are_non_empty_bounded_visible_ascii() {
        assert!(IdempotencyKey::new("order/42/created").is_ok());
        assert!(IdempotencyKey::new("").is_err());
        assert!(IdempotencyKey::new("contains space").is_err());
        assert!(IdempotencyKey::new("x".repeat(MAX_IDEMPOTENCY_KEY_BYTES + 1)).is_err());
    }
}
