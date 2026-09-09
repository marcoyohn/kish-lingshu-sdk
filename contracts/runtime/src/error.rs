use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeErrorCode {
    InvalidRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    StaleSuspension,
    ReplayUnavailable,
    Unsupported,
    Connectivity,
    Unavailable,
    Internal,
}

#[derive(Clone, Debug, Deserialize, Error, PartialEq, Serialize)]
#[error("{code:?}: {message}")]
pub struct RuntimeError {
    pub code: RuntimeErrorCode,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl RuntimeError {
    pub fn new(code: RuntimeErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retryable: false,
            details: None,
        }
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(RuntimeErrorCode::InvalidRequest, message)
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
