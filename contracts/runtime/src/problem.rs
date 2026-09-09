use std::{fmt, str::FromStr};

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use thiserror::Error;

use crate::RequestId;

pub const INVALID_REQUEST_PROBLEM: &str = "invalid_request";
pub const UNAUTHORIZED_PROBLEM: &str = "unauthorized";
pub const FORBIDDEN_PROBLEM: &str = "forbidden";
pub const NOT_FOUND_PROBLEM: &str = "not_found";
pub const CONFLICT_PROBLEM: &str = "conflict";
pub const STALE_TASK_REVISION_PROBLEM: &str = "stale_task_revision";
pub const STALE_SUSPENSION_PROBLEM: &str = "stale_suspension";
pub const REPLAY_UNAVAILABLE_PROBLEM: &str = "replay_unavailable";
pub const UNSUPPORTED_PROBLEM: &str = "unsupported";
pub const UNAVAILABLE_PROBLEM: &str = "unavailable";
pub const INTERNAL_PROBLEM: &str = "internal";

const MAX_PROBLEM_CODE_BYTES: usize = 128;

pub type ApplicationResult<T> = Result<T, ApplicationProblem>;

/// A forward-compatible application problem code.
///
/// Known values have constants above, while unknown values remain intact as
/// long as they follow the stable lower-case identifier grammar.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProblemCode(String);

impl ProblemCode {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidProblemCode> {
        let value = value.into();
        validate_problem_code(&value)?;
        Ok(Self(value))
    }

    pub fn known(value: &'static str) -> Self {
        Self::new(value).expect("built-in application problem codes must be valid")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ProblemCode {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for ProblemCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ProblemCode {
    type Err = InvalidProblemCode;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for ProblemCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ProblemCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid application problem code: {reason}")]
pub struct InvalidProblemCode {
    reason: &'static str,
}

fn validate_problem_code(value: &str) -> Result<(), InvalidProblemCode> {
    if value.is_empty() {
        return Err(InvalidProblemCode {
            reason: "value must not be empty",
        });
    }
    if value.len() > MAX_PROBLEM_CODE_BYTES {
        return Err(InvalidProblemCode {
            reason: "value is longer than 128 bytes",
        });
    }
    if !value
        .bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_lowercase())
    {
        return Err(InvalidProblemCode {
            reason: "value must start with a lower-case ASCII letter",
        });
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
    }) {
        return Err(InvalidProblemCode {
            reason: "value contains a character outside [a-z0-9_.-]",
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Error, PartialEq, Serialize)]
#[error("{code}: {message}")]
pub struct ApplicationProblem {
    pub code: ProblemCode,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    pub request_id: RequestId,
}

impl ApplicationProblem {
    pub fn new(
        code: ProblemCode,
        message: impl Into<String>,
        request_id: impl Into<RequestId>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
            details: None,
            request_id: request_id.into(),
        }
    }

    pub fn unsupported(message: impl Into<String>, request_id: impl Into<RequestId>) -> Self {
        Self::new(ProblemCode::known(UNSUPPORTED_PROBLEM), message, request_id)
    }

    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_problem_codes_round_trip_without_classification_loss() {
        let problem: ApplicationProblem = serde_json::from_value(serde_json::json!({
            "code": "future_domain.limit_changed",
            "message": "a newer server returned a new problem",
            "retryable": false,
            "request_id": "request-1"
        }))
        .unwrap();

        assert_eq!(problem.code.as_str(), "future_domain.limit_changed");
        assert_eq!(
            serde_json::to_value(problem).unwrap()["code"],
            "future_domain.limit_changed"
        );
    }

    #[test]
    fn invalid_problem_code_is_rejected_during_decode() {
        let result = serde_json::from_value::<ApplicationProblem>(serde_json::json!({
            "code": "Invalid Code",
            "message": "invalid",
            "request_id": "request-1"
        }));
        assert!(result.is_err());
    }
}
