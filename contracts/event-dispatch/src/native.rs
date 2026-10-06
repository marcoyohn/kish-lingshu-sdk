//! Native delivery uses the existing invocation identity and normalized outcomes.
//! These values contain no Zenoh types or callback credentials.
use crate::{InvocationV1, PublishEvent, PublishReceipt};
use kish_lingshu_foundation_contract::service_transport::LaneIdentity;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const NATIVE_CONSUMER_TARGET_VERSION: &str = "1";
pub const NATIVE_CONSUMER_TIMEOUT_MS: i64 = 30_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeConsumerRequest {
    pub route_revision: u64,
    pub lane: LaneIdentity,
    pub member_id: u64,
    pub membership_generation: u64,
    pub invocation: InvocationV1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeConsumerResponse {
    Completed {
        result: Value,
    },
    Throttled {
        code: String,
        retry_after_ms: Option<u64>,
    },
    RetryableFailure {
        code: String,
        message: String,
        retry_after_ms: Option<u64>,
    },
    PermanentFailure {
        code: String,
        message: String,
    },
    TimedOut,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativePublicationRequest {
    pub event: PublishEvent,
    pub idempotency_key: String,
    pub request_id: String,
    pub correlation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativePublicationResponse {
    Accepted {
        receipt: PublishReceipt,
    },
    Rejected {
        code: String,
        message: String,
        retryable: bool,
        retry_after_ms: Option<u64>,
    },
}
