//! Backend-neutral cluster admission. No business payload or call ledger.
use super::*;
use async_trait::async_trait;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdmissionScope {
    Application,
    Service {
        service_key: String,
    },
    Operation {
        service_key: String,
        operation_key: String,
    },
}
impl AdmissionScope {
    pub fn matches(&self, op: &OperationRef) -> bool {
        match self {
            Self::Application => true,
            Self::Service { service_key } => service_key == &op.service_key,
            Self::Operation {
                service_key,
                operation_key,
            } => service_key == &op.service_key && operation_key == &op.operation_key,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmissionRate {
    pub tokens: u32,
    pub interval_ms: u64,
    pub burst: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalAdmissionPolicy {
    pub scope: AdmissionScope,
    pub global_max_in_flight: Option<u32>,
    pub rate: Option<AdmissionRate>,
    pub maximum_wait_ms: u64,
}
impl GlobalAdmissionPolicy {
    pub fn validate(&self) -> Result<(), String> {
        let valid = match &self.scope {
            AdmissionScope::Application => true,
            AdmissionScope::Service { service_key } => valid_key(service_key),
            AdmissionScope::Operation {
                service_key,
                operation_key,
            } => valid_key(service_key) && valid_key(operation_key),
        };
        if !valid
            || self.maximum_wait_ms == 0
            || self.maximum_wait_ms > MAX_SERVICE_DEADLINE_MS
            || self
                .global_max_in_flight
                .is_some_and(|n| n == 0 || n > 65536)
            || self.rate.as_ref().is_some_and(|r| {
                r.tokens == 0
                    || r.tokens > 65536
                    || r.burst == 0
                    || r.burst > 65536
                    || r.interval_ms == 0
                    || r.interval_ms > MAX_SERVICE_DEADLINE_MS
            })
            || self.global_max_in_flight.is_none() && self.rate.is_none()
        {
            return Err("Invalid global Service admission policy".into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateGlobalAdmissionPolicies {
    pub expected_revision: i64,
    pub policies: Vec<GlobalAdmissionPolicy>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionPermit {
    pub application_id: String,
    pub call_id: String,
    pub attempt: u32,
    pub operation: OperationRef,
    pub request_id: String,
    pub revision: i64,
    pub epoch: String,
    pub hard_deadline_ms: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionRequest {
    pub call_id: String,
    pub attempt: u32,
    /// Stable opaque reservation nonce; the backend also binds call/attempt and Operation.
    pub request_id: String,
    pub operation: OperationRef,
    pub hard_deadline_ms: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdmissionDecision {
    Granted {
        permit: AdmissionPermit,
    },
    Waiting {
        next_check_at_ms: i64,
        reason: String,
    },
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermitTransition {
    Activate,
    Send,
    Release,
    CancelUnsent,
}

/// Implementations must use storage time, atomic all-scope admission, monotonic
/// policy revisions, idempotent reservations and exactly-once send claims.
/// Missing authority must fail closed; releasing an old identity cannot free a successor.
#[async_trait]
pub trait ServiceAdmission: Send + Sync {
    async fn install(
        &self,
        app: &str,
        revision: i64,
        policies: &[GlobalAdmissionPolicy],
    ) -> Result<(), CoordinationError>;
    async fn try_admit(
        &self,
        app: &str,
        revision: i64,
        request: &AdmissionRequest,
    ) -> Result<AdmissionDecision, CoordinationError>;
    async fn transition(
        &self,
        app: &str,
        permit: &AdmissionPermit,
        action: PermitTransition,
    ) -> Result<bool, CoordinationError>;
}
