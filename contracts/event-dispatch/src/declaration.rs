use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{DeliveryMode, OverlapPolicy};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumptionOrder {
    PartitionOrdered,
    Unordered,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TopicDefaults {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub partition_count: u32,
    pub consumption_order: ConsumptionOrder,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventDefinition {
    pub key: String,
    pub topic: String,
    pub event_type: String,
    pub schema_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub payload_schema: Value,
    pub topic_defaults: TopicDefaults,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventSelector {
    pub event_key: String,
    pub topic: String,
    pub event_type: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerDefinition {
    pub key: String,
    pub group_key: String,
    pub selectors: Vec<EventSelector>,
    pub delivery_mode: DeliveryMode,
    pub maximum_concurrency: u32,
    pub policy: ConsumerPolicy,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<RateLimitPolicy>,
    pub retry: RetryPolicy,
    pub throttle: ThrottlePolicy,
    pub timeout: TimeoutPolicy,
    pub dispatch: DispatchPolicy,
    pub pause: PausePolicy,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitPolicy {
    pub requests: u32,
    pub interval_milliseconds: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetryPolicy {
    pub maximum_failure_attempts: u32,
    pub initial_delay_milliseconds: u64,
    pub maximum_delay_milliseconds: u64,
    pub multiplier: f64,
    pub jitter_ratio: f64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ThrottlePolicy {
    pub minimum_cooldown_milliseconds: u64,
    pub maximum_cooldown_milliseconds: u64,
    pub maximum_throttle_duration_milliseconds: u64,
    pub half_open_probe_limit: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TimeoutPolicy {
    pub invocation_milliseconds: u64,
    pub completion_milliseconds: u64,
    pub maximum_completion_milliseconds: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchPolicy {
    pub ordering_scope: OrderingScope,
    pub mode: ExecutionMode,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingScope {
    Partition,
    None,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Serial,
    Parallel,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PausePolicy {
    Retain,
    Skip,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JobDefinition {
    pub key: String,
    pub trigger: JobTrigger,
    pub event: JobEventTemplate,
    pub misfire_policy: MisfirePolicy,
    pub misfire_batch_cap: u32,
    pub overlap_policy: OverlapPolicy,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JobTrigger {
    Cron {
        expression: String,
        timezone: String,
    },
    Interval {
        every_milliseconds: u64,
        anchor_at: DateTime<Utc>,
        #[serde(default)]
        basis: IntervalBasis,
    },
    Once {
        execute_at: DateTime<Utc>,
    },
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntervalBasis {
    #[default]
    TriggeredAt,
    CompletedAt,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JobEventTemplate {
    pub event_key: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, Value>,
    pub payload: Value,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MisfirePolicy {
    Skip,
    FireOnce,
    CatchUp,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentBindingRequirement {
    pub key: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}
