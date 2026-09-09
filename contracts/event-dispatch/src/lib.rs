//! Stable wire contracts shared by Event Dispatch and its application SDKs.

#![deny(unreachable_pub)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, fmt, str::FromStr};

mod declaration;
mod event;
mod import;
mod manifest;

pub use declaration::*;
pub use event::*;
pub use import::*;
pub use manifest::*;

pub const INVOCATION_CONTRACT_VERSION: &str = "1.0";
pub const IDEMPOTENCY_HEADER: &str = "Idempotency-Key";
pub const INVOCATION_ID_HEADER: &str = "X-Event-Invocation-Id";
pub const CALLBACK_TOKEN_HEADER: &str = "X-Event-Callback-Token";
pub const CONSUMER_REGISTRATION_AUTH_SCHEME: &str = "Bearer";

macro_rules! contract_enum {
    (
        pub enum $name:ident {
            $($variant:ident => $value:literal),+ $(,)?
        }
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub enum $name {
            $(#[serde(rename = $value)] $variant),+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(format!("invalid {} value: {value}", stringify!($name))),
                }
            }
        }
    };
}

contract_enum! { pub enum DeliveryMode { Sync => "sync", Async => "async" } }
contract_enum! { pub enum OverlapPolicy { Allow => "allow", Serialize => "serialize" } }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelopeV1 {
    pub event_id: u64,
    pub app_id: String,
    pub topic: String,
    pub event_type: String,
    pub schema_version: String,
    pub source: String,
    pub subject: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub published_at: DateTime<Utc>,
    pub not_before: DateTime<Utc>,
    pub partition_key: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, Value>,
    pub payload: Value,
    pub schedule: Option<ScheduleEventMetadataV1>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScheduleEventMetadataV1 {
    pub schedule_id: u64,
    pub scheduled_at: DateTime<Utc>,
    pub schedule_key: String,
    pub overlap_policy: OverlapPolicy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvocationV1 {
    pub contract_version: String,
    pub event: EventEnvelopeV1,
    pub consumption: InvocationConsumptionV1,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion: Option<InvocationCompletionV1>,
    pub trace: InvocationTraceV1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvocationConsumptionV1 {
    pub consumption_id: u64,
    pub subscription_id: u64,
    pub group_id: u64,
    pub subscription_epoch: u64,
    pub queue_epoch: u64,
    pub queue_id: u32,
    pub queue_offset: u64,
    pub invocation_id: u64,
    pub attempt_generation: u64,
    pub mode: DeliveryMode,
    pub idempotency_key: String,
    pub invocation_deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvocationCompletionV1 {
    pub url: String,
    pub heartbeat_url: String,
    pub auth: InvocationCallbackAuthV1,
    pub expires_at: DateTime<Utc>,
    pub maximum_expires_at: DateTime<Utc>,
    pub heartbeat_interval_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationCallbackAuthV1 {
    #[serde(rename = "type")]
    pub kind: String,
    pub header_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationTraceV1 {
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionStatus {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompletionFailure {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionRequest {
    pub completion_id: String,
    pub consumer_task_id: String,
    pub status: CompletionStatus,
    pub completed_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<CompletionFailure>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatRequest {
    pub heartbeat_id: String,
    pub consumer_task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallbackDisposition {
    Recorded,
    Duplicate,
    PendingAcceptance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionResponse {
    pub disposition: CallbackDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub disposition: CallbackDisposition,
    pub completion_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerInstanceRegistrationRequestV1 {
    pub app_id: String,
    pub group_id: u64,
    pub node_id: String,
    pub invocation_url: String,
    pub maximum_in_flight: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsumerInstanceLeaseV1 {
    pub member_id: u64,
    pub node_id: String,
    pub membership_generation: u64,
    pub lease_seconds: u64,
    pub heartbeat_interval_seconds: u64,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerInstanceHeartbeatRequestV1 {
    pub group_id: u64,
    pub node_id: String,
    pub membership_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerInstanceDeregisterRequestV1 {
    pub group_id: u64,
    pub node_id: String,
    pub membership_generation: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_modes_keep_stable_wire_values() {
        assert_eq!(
            serde_json::to_string(&DeliveryMode::Sync).unwrap(),
            "\"sync\""
        );
        assert_eq!(
            "async".parse::<DeliveryMode>().unwrap(),
            DeliveryMode::Async
        );
    }

    #[test]
    fn callback_requests_reject_unknown_fields() {
        let result = serde_json::from_value::<CompletionRequest>(serde_json::json!({
            "completion_id": "completion-1",
            "consumer_task_id": "task-1",
            "status": "succeeded",
            "completed_at": "2026-09-06T00:00:00Z",
            "unexpected": true
        }));
        assert!(result.is_err());
    }

    #[test]
    fn instance_registration_contract_is_strict_and_contains_no_api_key() {
        let request =
            serde_json::from_value::<ConsumerInstanceRegistrationRequestV1>(serde_json::json!({
                "app_id": "orders",
                "group_id": 41,
                "node_id": "orders-pod-1",
                "invocation_url": "https://orders.example.test/events",
                "maximum_in_flight": 8
            }))
            .unwrap();
        assert_eq!(request.node_id, "orders-pod-1");
        let encoded = serde_json::to_string(&request).unwrap();
        assert!(!encoded.contains("api_key"));
        assert!(
            serde_json::from_value::<ConsumerInstanceRegistrationRequestV1>(serde_json::json!({
                "app_id": "orders",
                "group_id": 41,
                "node_id": "orders-pod-1",
                "invocation_url": "https://orders.example.test/events",
                "maximum_in_flight": 8,
                "schema_version": "1"
            }))
            .is_err()
        );
    }
}
