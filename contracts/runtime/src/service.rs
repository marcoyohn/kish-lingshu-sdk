//! Versioned provider capabilities and service invocation wire contract.
//!
//! Call identity is correlation data, not a request to persist a call ledger.
use std::collections::BTreeSet;

pub use kish_lingshu_foundation_contract::{ServiceInstanceIdentity, ServiceInstanceRegistration};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const SERVICE_CONTRACT_VERSION: u32 = 1;
pub const SERVICE_API_PATH: &str = "api/user/services/v1/";
pub const MAX_SERVICE_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const MAX_SERVICE_DEADLINE_MS: u64 = 24 * 60 * 60 * 1000;
pub const DEFAULT_SERVICE_DEADLINE_MS: u64 = 30_000;
pub const SERVICE_RESUME_EVENT: &str = "__lingshu_service_completion_v1";
mod admission;
pub use admission::*;
mod heartbeat;
pub use heartbeat::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallMode {
    Sync,
    Async,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallBinding {
    pub modes: BTreeSet<CallMode>,
    pub maximum_concurrency: u32,
    pub timeout_ms: u64,
}

/// Mutable Call governance, separate from the immutable Operation digest.
/// The concurrency ceiling is per provider instance, not a fleet-wide quota.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceCallPolicy {
    pub operation: OperationRef,
    pub enabled: bool,
    pub maximum_concurrency: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateServiceCallPolicy {
    pub expected_revision: i64,
    pub policy: ServiceCallPolicy,
}

/// Signed transport admission ceiling; never exposed to the business handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceCallAdmission {
    pub policy_revision: i64,
    pub maximum_concurrency: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventBinding {
    pub topic: String,
    pub event_type: String,
    pub consumer_group: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationDefinition {
    pub operation_key: String,
    pub version: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_task_completion: Option<UserTaskCompletionDefinition>,
    /// Effectful operations must implement persistent business idempotency.
    pub idempotent: bool,
    pub input_schema: Value,
    pub output_schema: Value,
    pub error_schema: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call: Option<CallBinding>,
    #[serde(default)]
    pub events: BTreeSet<EventBinding>,
}

/// A Call specialization; it does not introduce an instance registration role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskCompletionDefinition {
    pub task_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceDefinition {
    pub service_key: String,
    pub description: String,
    pub operations: Vec<OperationDefinition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceManifest {
    pub contract_version: u32,
    pub application_id: String,
    pub services: Vec<ServiceDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationRef {
    pub service_key: String,
    pub operation_key: String,
    pub version: String,
    pub contract_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowCallTarget {
    pub workflow_id: String,
    pub workflow_version: String,
    pub root_workflow_instance_id: String,
    pub workflow_instance_id: String,
    pub execution_path: String,
    pub execution_seq: u64,
    pub node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_task_completion: Option<Box<crate::UserTaskCompletionInvocationV1>>,
    pub call_id: String,
    pub attempt: u32,
    pub caller: String,
    pub mode: CallMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<WorkflowCallTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventContext {
    pub event_id: String,
    pub source: String,
    pub occurred_at_ms: i64,
    pub binding: EventBinding,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "role", content = "context", rename_all = "snake_case")]
pub enum InvocationRole {
    Event(EventContext),
    Call(CallContext),
}

/// Verified SDK context only. Completion secrets never enter a business handler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceContext {
    pub application_id: String,
    pub idempotency_key: String,
    pub deadline_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    pub invocation: InvocationRole,
}

impl ServiceContext {
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionTarget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat: Option<CallHeartbeatPolicy>,
    /// An opaque, expiring capability for the fixed platform completion endpoint.
    pub token: String,
}

impl std::fmt::Debug for CompletionTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CompletionTarget([REDACTED])")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceInvocation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission: Option<ServiceCallAdmission>,
    /// Transport-only fencing, excluded from the business context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_instance: Option<ServiceInstanceTarget>,
    pub contract_version: u32,
    pub operation: OperationRef,
    pub context: ServiceContext,
    pub input: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion: Option<CompletionTarget>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[error("{code}: {message}")]
#[serde(deny_unknown_fields)]
pub struct ServiceError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl ServiceError {
    pub fn rejected(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
            details: None,
        }
    }
    pub fn retryable(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            retryable: true,
            ..Self::rejected(code, message)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ServiceOutcome {
    Succeeded { result: Value },
    Failed { error: ServiceError },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum InvocationResponse {
    Accepted { call_id: String, attempt: u32 },
    Completed { outcome: ServiceOutcome },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceCompletion {
    pub contract_version: u32,
    pub call_id: String,
    pub attempt: u32,
    pub outcome: ServiceOutcome,
}

/// Liveness only: neither this request nor its acknowledgement extends a deadline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceProgress {
    pub contract_version: u32,
    pub call_id: String,
    pub attempt: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressDisposition {
    Running,
    Invalidated,
}

/// Signed platform control, never part of a business handler's context.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceCancellation {
    pub contract_version: u32,
    pub target_instance: ServiceInstanceTarget,
    pub operation: OperationRef,
    pub call_id: String,
    pub attempt: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionDisposition {
    Recorded,
    Duplicate,
    Invalidated,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceCapability {
    pub operation: OperationRef,
    pub call: bool,
    #[serde(default)]
    pub events: BTreeSet<EventBinding>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceEnrollment {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<ServiceInstanceRegistration>,
    pub node_id: String,
    pub invocation_url: String,
    pub maximum_in_flight: u32,
    pub capabilities: Vec<InstanceCapability>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceSession {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<ServiceInstanceIdentity>,
    pub node_id: String,
    pub generation: String,
    pub credential: String,
    pub lease_expires_at_ms: i64,
    pub heartbeat_interval_ms: u64,
}

impl std::fmt::Debug for ServiceSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceSession")
            .field("node_id", &self.node_id)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

pub fn valid_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 128
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

pub fn canonical_digest<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    fn canonical(value: Value) -> Value {
        match value {
            Value::Object(fields) => {
                let sorted: std::collections::BTreeMap<_, _> = fields.into_iter().collect();
                Value::Object(sorted.into_iter().map(|(k, v)| (k, canonical(v))).collect())
            }
            Value::Array(values) => Value::Array(values.into_iter().map(canonical).collect()),
            value => value,
        }
    }
    let bytes = serde_json::to_vec(&canonical(serde_json::to_value(value)?))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

impl OperationDefinition {
    pub fn reference(&self, service_key: &str) -> Result<OperationRef, serde_json::Error> {
        Ok(OperationRef {
            service_key: service_key.into(),
            operation_key: self.operation_key.clone(),
            version: self.version.clone(),
            contract_digest: canonical_digest(self)?,
        })
    }
    pub fn validate(&self) -> Result<(), ServiceError> {
        if !valid_key(&self.operation_key)
            || !valid_key(&self.version)
            || (!self.action.is_empty() && !valid_key(&self.action))
            || (self.action.is_empty() && self.user_task_completion.is_none())
            || self.call.is_none() && self.events.is_empty()
        {
            return Err(ServiceError::rejected(
                "invalid_operation",
                "Operation identity, action and at least one role are required",
            ));
        }
        if let Some(completion) = &self.user_task_completion {
            if !valid_key(&completion.task_type) || self.call.is_none() || !self.events.is_empty() {
                return Err(ServiceError::rejected(
                    "invalid_completion_binding",
                    "User Task completion requires a task type and Call-only operation",
                ));
            }
        }
        if let Some(call) = &self.call {
            if call.modes.is_empty()
                || call.maximum_concurrency == 0
                || call.timeout_ms == 0
                || call.timeout_ms > MAX_SERVICE_DEADLINE_MS
            {
                return Err(ServiceError::rejected(
                    "invalid_call_binding",
                    "Invalid mode, concurrency or timeout",
                ));
            }
        }
        for event in &self.events {
            if !valid_key(&event.topic)
                || !valid_key(&event.event_type)
                || !valid_key(&event.consumer_group)
            {
                return Err(ServiceError::rejected(
                    "invalid_event_binding",
                    "Invalid event route",
                ));
            }
        }
        for schema in [&self.input_schema, &self.output_schema, &self.error_schema] {
            if (!schema.is_object() && !schema.is_boolean()) || !local_schema_refs(schema) {
                return Err(ServiceError::rejected(
                    "invalid_schema",
                    "Schema must be an object or boolean",
                ));
            }
        }
        Ok(())
    }
}

impl ServiceManifest {
    pub fn validate(&self) -> Result<(), ServiceError> {
        if self.contract_version != SERVICE_CONTRACT_VERSION
            || !valid_key(&self.application_id)
            || self.services.is_empty()
        {
            return Err(ServiceError::rejected(
                "invalid_manifest",
                "Unsupported version or missing application/services",
            ));
        }
        let mut keys = BTreeSet::new();
        let mut routes = std::collections::BTreeMap::new();
        for service in &self.services {
            if !valid_key(&service.service_key)
                || !keys.insert(service.service_key.clone())
                || service.operations.is_empty()
            {
                return Err(ServiceError::rejected(
                    "duplicate_service",
                    "Invalid or duplicate Service definition",
                ));
            }
            let mut operations = BTreeSet::new();
            for operation in &service.operations {
                operation.validate()?;
                if !operations.insert((&operation.operation_key, &operation.version)) {
                    return Err(ServiceError::rejected(
                        "duplicate_operation",
                        "Duplicate Operation version",
                    ));
                }
                for event in &operation.events {
                    let logical = (&service.service_key, &operation.operation_key);
                    if routes
                        .insert(event, logical)
                        .is_some_and(|previous| previous != logical)
                    {
                        return Err(ServiceError::rejected(
                            "duplicate_event_route",
                            "Event route has multiple logical handlers",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
    pub fn normalize(&mut self) {
        self.services
            .sort_by(|a, b| a.service_key.cmp(&b.service_key));
        for service in &mut self.services {
            service.operations.sort_by(|a, b| {
                (&a.operation_key, &a.version).cmp(&(&b.operation_key, &b.version))
            });
        }
    }
}

impl ServiceInvocation {
    pub fn validate(&self, app: &str, now_ms: i64) -> Result<(), ServiceError> {
        if self.contract_version != SERVICE_CONTRACT_VERSION
            || self.context.application_id != app
            || self.context.idempotency_key.trim().is_empty()
            || self.context.idempotency_key.len() > 512
            || self.context.deadline_ms <= now_ms
            || self.context.deadline_ms.saturating_sub(now_ms) as u64 > MAX_SERVICE_DEADLINE_MS
        {
            return Err(ServiceError::rejected(
                "invalid_invocation",
                "Invalid identity, application or deadline",
            ));
        }
        if let Some(policy) = self.completion.as_ref().and_then(|c| c.heartbeat.as_ref()) {
            if policy.version != CALL_HEARTBEAT_VERSION
                || policy.epoch.is_empty()
                || policy.interval_ms != CALL_HEARTBEAT_INTERVAL_MS
                || policy.execution_deadline_ms != self.context.deadline_ms
                || policy.delivery_deadline_ms < policy.execution_deadline_ms
                || policy.delivery_deadline_ms
                    > policy
                        .execution_deadline_ms
                        .saturating_add(CALL_DELIVERY_MS)
                || self.target_instance.is_none()
            {
                return Err(ServiceError::rejected(
                    "unsupported_heartbeat",
                    "Invalid call heartbeat policy",
                ));
            }
        }
        match &self.context.invocation {
            InvocationRole::Call(call) => {
                if !valid_key(&call.call_id)
                    || call.attempt == 0
                    || call.caller.trim().is_empty()
                    || (call.mode == CallMode::Async) != self.completion.is_some()
                {
                    return Err(ServiceError::rejected(
                        "invalid_call",
                        "Invalid call identity or completion mode",
                    ));
                }
            }
            InvocationRole::Event(_) if self.completion.is_some() => {
                return Err(ServiceError::rejected(
                    "invalid_event",
                    "Event delivery uses Dispatch completion",
                ));
            }
            InvocationRole::Event(_) => {}
        }
        if serde_json::to_vec(self).map_or(true, |b| b.len() > MAX_SERVICE_PAYLOAD_BYTES) {
            return Err(ServiceError::rejected(
                "payload_too_large",
                "Invocation exceeds the payload limit",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

/// Ephemeral discovery state shared across platform replicas, not call history.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceInstance {
    pub application_id: String,
    pub enrollment: ServiceEnrollment,
    pub generation: String,
    pub lease_expires_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceInstanceTarget {
    pub node_id: String,
    pub generation: String,
}

/// Contract export/validation must never fetch a URL or read a local file.
fn local_schema_refs(value: &Value) -> bool {
    match value {
        Value::Object(fields) => fields.iter().all(|(key, value)| {
            (key != "$ref" || value.as_str().is_some_and(|r| r.starts_with('#')))
                && local_schema_refs(value)
        }),
        Value::Array(values) => values.iter().all(local_schema_refs),
        _ => true,
    }
}

/// Direct, immutable User Task completion binding saved with the task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskServiceBinding {
    #[serde(flatten)]
    pub operation: OperationRef,
    #[serde(default = "completion_default_mode")]
    pub mode: CallMode,
    #[serde(default = "completion_default_deadline")]
    pub deadline_ms: u64,
    #[serde(default = "completion_default_attempts")]
    pub maximum_attempts: u32,
}
fn completion_default_mode() -> CallMode {
    CallMode::Async
}
fn completion_default_deadline() -> u64 {
    300_000
}
fn completion_default_attempts() -> u32 {
    3
}
impl UserTaskServiceBinding {
    pub fn validate(&self) -> Result<(), ServiceError> {
        if !valid_key(&self.operation.service_key)
            || !valid_key(&self.operation.operation_key)
            || !valid_key(&self.operation.version)
            || self.operation.contract_digest.len() != 64
            || !self
                .operation
                .contract_digest
                .bytes()
                .all(|c| c.is_ascii_hexdigit())
            || self.deadline_ms == 0
            || self.deadline_ms > MAX_SERVICE_DEADLINE_MS
            || self.maximum_attempts == 0
            || self.maximum_attempts > 8
        {
            return Err(ServiceError::rejected(
                "invalid_completion_binding",
                "Completion requires a pinned Operation and bounded execution budget",
            ));
        }
        Ok(())
    }
}
