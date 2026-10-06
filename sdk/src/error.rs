use std::{fmt, time::Duration};

use kish_lingshu_runtime_contract::{ApplicationProblem, RequestId};
use thiserror::Error;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid Client configuration for {field}: {message}")]
pub struct ConfigurationError {
    pub field: &'static str,
    pub message: String,
}

impl ConfigurationError {
    pub(crate) fn new(field: &'static str, message: impl Into<String>) -> Self {
        Self {
            field,
            message: message.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportKind {
    Connect,
    Request,
    Stream,
}

impl fmt::Display for TransportKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Connect => "connect",
            Self::Request => "request",
            Self::Stream => "stream",
        })
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{kind} transport failure: {message}")]
pub struct TransportFailure {
    pub kind: TransportKind,
    pub message: String,
    pub request_id: Option<RequestId>,
    pub retryable: bool,
}

#[derive(Clone, Debug, Error, PartialEq)]
#[error("{problem}")]
pub struct ApplicationFailure {
    pub problem: ApplicationProblem,
    pub http_status: Option<u16>,
    pub retry_after: Option<Duration>,
}

impl ApplicationFailure {
    pub fn new(problem: ApplicationProblem) -> Self {
        Self {
            problem,
            http_status: None,
            retry_after: None,
        }
    }

    pub fn with_http_context(mut self, status: u16, retry_after: Option<Duration>) -> Self {
        self.http_status = Some(status);
        self.retry_after = retry_after;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolDirection {
    EncodeRequest,
    DecodeResponse,
    DecodeEvent,
}

impl fmt::Display for ProtocolDirection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EncodeRequest => "encode request",
            Self::DecodeResponse => "decode response",
            Self::DecodeEvent => "decode event",
        })
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("protocol failure while attempting to {direction}: {message}")]
pub struct ProtocolError {
    pub direction: ProtocolDirection,
    pub message: String,
    pub request_id: Option<RequestId>,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("contract violation: {message}")]
pub struct ContractViolation {
    pub message: String,
    pub request_id: Option<RequestId>,
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    #[error(transparent)]
    Transport(TransportFailure),
    #[error(transparent)]
    Application(ApplicationFailure),
    #[error(transparent)]
    Protocol(ProtocolError),
    #[error(transparent)]
    ContractViolation(ContractViolation),
}

impl Error {
    pub fn application_code(&self) -> Option<&str> {
        match self {
            Self::Application(failure) => Some(failure.problem.code.as_str()),
            _ => None,
        }
    }

    pub(crate) fn configuration(field: &'static str, message: impl Into<String>) -> Self {
        ConfigurationError::new(field, message).into()
    }

    pub(crate) fn contract(message: impl Into<String>, request_id: Option<RequestId>) -> Self {
        Self::ContractViolation(ContractViolation {
            message: message.into(),
            request_id,
        })
    }
}
