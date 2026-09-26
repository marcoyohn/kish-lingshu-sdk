//! Resource admission for Workflow execution. No provider payloads or scheduler ownership.
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionAdmissionConfig {
    /// Shared by every Runtime replica in this capacity domain.
    pub namespace: String,
    pub revision: u64,
    #[serde(default)]
    pub active_roots: Option<u32>,
    /// Keys are logical model names. `*` is a shared pool for all models.
    #[serde(default)]
    pub models: BTreeMap<String, u32>,
    /// Named resources for additional node adapters; runtime keys use `resources/`.
    #[serde(default)]
    pub resources: BTreeMap<String, u32>,
    #[serde(default = "default_wait")]
    pub maximum_wait_ms: u64,
    #[serde(default = "default_hold")]
    pub maximum_hold_ms: u64,
}
fn default_wait() -> u64 {
    60_000
}
fn default_hold() -> u64 {
    3_600_000
}
impl ExecutionAdmissionConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.namespace.is_empty()
            || self.namespace.len() > 128
            || self.revision == 0
            || self.maximum_wait_ms == 0
            || self.maximum_wait_ms > 86_400_000
            || self.maximum_hold_ms < 1000
            || self.maximum_hold_ms > 86_400_000
            || self.models.len() + self.resources.len() > 256
            || self
                .active_roots
                .into_iter()
                .chain(self.models.values().copied())
                .chain(self.resources.values().copied())
                .any(|n| n == 0 || n > 65536)
            || self
                .models
                .keys()
                .chain(self.resources.keys())
                .any(|k| k.is_empty() || k.len() > 256)
        {
            return Err("Invalid execution admission configuration".into());
        }
        Ok(())
    }
    pub fn enabled(&self) -> bool {
        self.active_roots.is_some() || !self.models.is_empty() || !self.resources.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceClaim {
    pub key: String,
    pub capacity: u32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRequest {
    pub request_id: String,
    /// Root, execution path, execution sequence and internal operation.
    pub owner: String,
    /// The saved wait cannot authorize a new execution after this absolute deadline.
    pub valid_until_ms: i64,
    pub claims: Vec<ResourceClaim>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourcePermit {
    pub request: ResourceRequest,
    pub expires_at_ms: i64,
    /// Server-computed remaining lease; callers subtract request round-trip time.
    pub remaining_ms: u64,
}
#[derive(Debug, Clone)]
pub enum ResourceDecision {
    Granted(ResourcePermit),
    Waiting { next_check_at_ms: i64 },
}

#[async_trait]
pub trait ResourceAdmission: Send + Sync {
    /// Atomically claim all resources and a single execution authorization.
    /// Repeating a claimed nonce MUST NOT authorize a second execution.
    async fn acquire(
        &self,
        request: &ResourceRequest,
    ) -> Result<ResourceDecision, crate::service::CoordinationError>;
    async fn release(
        &self,
        permit: &ResourcePermit,
    ) -> Result<(), crate::service::CoordinationError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn admission_configuration_defaults_and_bounds() {
        let config: ExecutionAdmissionConfig =
            serde_json::from_value(json!({"namespace":"test","revision":1})).unwrap();
        config.validate().unwrap();
        assert!(!config.enabled());
        assert_eq!(config.maximum_wait_ms, 60000);
        assert_eq!(config.maximum_hold_ms, 3600000);
        for patch in [
            json!({"active_roots":0}),
            json!({"models":{"x":65537}}),
            json!({"resources":{"":1}}),
            json!({"maximum_hold_ms":999}),
            json!({"maximum_wait_ms":86400001}),
            json!({"revision":0}),
        ] {
            let mut value = serde_json::to_value(&config).unwrap();
            value
                .as_object_mut()
                .unwrap()
                .extend(patch.as_object().unwrap().clone());
            assert!(serde_json::from_value::<ExecutionAdmissionConfig>(value)
                .unwrap()
                .validate()
                .is_err());
        }
        assert!(serde_json::from_value::<ExecutionAdmissionConfig>(
            json!({"namespace":"test","revision":1,"models_typo":{}})
        )
        .is_err());
    }
}
