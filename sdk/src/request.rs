use chrono::{DateTime, Utc};
use kish_lingshu_runtime_contract::{
    CorrelationId, IdempotencyKey, RequestDeadline, RequestId, TraceContext,
};

use crate::{config::MAX_RETRY_LIMIT, Error};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestOptions {
    request_id: RequestId,
    correlation_id: CorrelationId,
    trace: Option<TraceContext>,
    deadline: Option<RequestDeadline>,
    retry_limit: Option<u32>,
}

impl RequestOptions {
    pub fn new() -> Self {
        Self {
            request_id: RequestId::from(format!("req_{}", uuid::Uuid::now_v7().simple())),
            correlation_id: CorrelationId::from(format!("corr_{}", uuid::Uuid::now_v7().simple())),
            trace: None,
            deadline: None,
            retry_limit: None,
        }
    }

    pub fn with_correlation_id(
        mut self,
        correlation_id: impl Into<CorrelationId>,
    ) -> Result<Self, Error> {
        let correlation_id = correlation_id.into();
        if correlation_id.as_ref().trim().is_empty() {
            return Err(Error::configuration(
                "correlation_id",
                "value must not be empty",
            ));
        }
        self.correlation_id = correlation_id;
        Ok(self)
    }

    pub fn with_trace(mut self, trace: TraceContext) -> Result<Self, Error> {
        if trace
            .traceparent
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
            || trace
                .tracestate
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(Error::configuration(
                "trace",
                "trace values must not be empty when present",
            ));
        }
        self.trace = Some(trace);
        Ok(self)
    }

    pub fn with_deadline(mut self, deadline: DateTime<Utc>) -> Self {
        self.deadline = Some(RequestDeadline::new(deadline));
        self
    }

    pub fn with_retry_limit(mut self, retry_limit: u32) -> Result<Self, Error> {
        if retry_limit > MAX_RETRY_LIMIT {
            return Err(Error::configuration(
                "retry_limit",
                format!("value must not exceed {MAX_RETRY_LIMIT}"),
            ));
        }
        self.retry_limit = Some(retry_limit);
        Ok(self)
    }

    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    pub fn correlation_id(&self) -> &CorrelationId {
        &self.correlation_id
    }

    pub fn trace(&self) -> Option<&TraceContext> {
        self.trace.as_ref()
    }

    pub fn deadline(&self) -> Option<RequestDeadline> {
        self.deadline
    }

    pub fn retry_limit(&self) -> Option<u32> {
        self.retry_limit
    }

    #[cfg(any(feature = "http-client", test))]
    pub(crate) fn attempt(&self, number: u32) -> TransportAttempt {
        TransportAttempt {
            request_id: self.request_id.clone(),
            correlation_id: self.correlation_id.clone(),
            number,
        }
    }
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationOptions {
    request: RequestOptions,
    idempotency_key: IdempotencyKey,
}

impl MutationOptions {
    pub fn generate(scope: impl AsRef<str>) -> Result<Self, Error> {
        let scope = scope.as_ref().trim();
        if scope.is_empty() {
            return Err(Error::configuration(
                "idempotency_scope",
                "value must not be empty",
            ));
        }
        Self::new(format!("{scope}/{}", uuid::Uuid::now_v7().simple()))
    }

    pub fn new(idempotency_key: impl Into<String>) -> Result<Self, Error> {
        let idempotency_key = IdempotencyKey::new(idempotency_key.into())
            .map_err(|error| Error::configuration("idempotency_key", error.to_string()))?;
        Ok(Self {
            request: RequestOptions::new(),
            idempotency_key,
        })
    }

    pub fn with_request_options(mut self, request: RequestOptions) -> Self {
        self.request = request;
        self
    }

    pub fn request(&self) -> &RequestOptions {
        &self.request
    }

    pub fn idempotency_key(&self) -> &IdempotencyKey {
        &self.idempotency_key
    }

    pub(crate) fn without_retry(mut self) -> Self {
        self.request.retry_limit = Some(0);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(any(feature = "http-client", test))]
pub(crate) struct TransportAttempt {
    pub request_id: RequestId,
    pub correlation_id: CorrelationId,
    pub number: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_request_identities_are_generated_once_and_shared_by_attempts() {
        let deadline = "2026-09-06T09:00:00Z".parse().unwrap();
        let options = RequestOptions::new()
            .with_correlation_id("contract-correlation")
            .unwrap()
            .with_trace(TraceContext {
                traceparent: Some(
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
                ),
                tracestate: Some("vendor=value".to_string()),
            })
            .unwrap()
            .with_deadline(deadline)
            .with_retry_limit(2)
            .unwrap();
        assert!(!options.request_id().as_ref().is_empty());
        assert!(!options.correlation_id().as_ref().is_empty());
        let first = options.attempt(1);
        let second = options.attempt(2);
        assert_eq!(first.request_id, second.request_id);
        assert_eq!(first.correlation_id, second.correlation_id);
        assert_ne!(first.number, second.number);
        let retried = options.clone();
        assert_eq!(retried.trace(), options.trace());
        assert_eq!(retried.deadline(), options.deadline());
        assert_eq!(retried.retry_limit(), Some(2));
    }

    #[test]
    fn mutation_options_require_a_valid_idempotency_key() {
        assert!(MutationOptions::new("event/order-42").is_ok());
        assert!(MutationOptions::new("").is_err());
    }

    #[test]
    fn best_effort_override_disables_transport_retry() {
        let options = MutationOptions::new("event/order-42")
            .unwrap()
            .without_retry();
        assert_eq!(options.request().retry_limit(), Some(0));
    }
}
