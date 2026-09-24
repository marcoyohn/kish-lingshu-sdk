//! Source catalogs describe desired configuration; they never authorize execution.
use crate::service::{
    canonical_digest, valid_key, ServiceInstanceIdentity, ServiceInstanceRegistration,
    ServiceManifest,
};
pub use kish_lingshu_event_dispatch_contract::{
    EventDispatchImportApplyRequest, EventDispatchImportPlan, EventDispatchImportPreviewRequest,
    EventDispatchImportReceipt, EventDispatchManifestV1, ManifestResourceChange, RetirementMode,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_PROVIDER_CATALOG_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProviderWorkflow {
    /// Never change this key when renaming or releasing a Workflow.
    pub key: String,
    pub name: String,
    pub description: Option<String>,
    pub define_schema: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProviderCatalog {
    pub format_version: u32,
    pub application_id: String,
    pub provider_key: String,
    pub release: String,
    pub services: Option<ServiceManifest>,
    pub events: Option<EventDispatchManifestV1>,
    #[serde(default)]
    pub workflows: Vec<ProviderWorkflow>,
}
impl ProviderCatalog {
    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != 1
            || !valid_key(&self.application_id)
            || !valid_key(&self.provider_key)
            || self.release.is_empty()
            || self.release.len() > 128
            || self.workflows.len() > 256
        {
            return Err("invalid provider identity, format or catalog size".into());
        }
        if let Some(services) = &self.services {
            if services.application_id != self.application_id {
                return Err("catalog application mismatch".into());
            }
            services.validate().map_err(|e| e.to_string())?;
        }
        if let Some(events) = &self.events {
            events.verify_digest().map_err(|e| e.to_string())?;
        }
        let mut keys = BTreeSet::new();
        for workflow in &self.workflows {
            if !valid_key(&workflow.key)
                || workflow.name.trim().is_empty()
                || !keys.insert(&workflow.key)
                || workflow.define_schema.is_empty()
            {
                return Err("invalid or duplicate Workflow source key".into());
            }
        }
        if serde_json::to_vec(self).map_err(|e| e.to_string())?.len() > MAX_PROVIDER_CATALOG_BYTES {
            return Err("catalog too large".into());
        }
        Ok(())
    }
    pub fn digest(&self) -> Result<String, String> {
        self.validate()?;
        let mut normalized = self.clone();
        normalized.workflows.sort_by(|a, b| a.key.cmp(&b.key));
        if let Some(services) = &mut normalized.services {
            services.normalize();
        }
        canonical_digest(&normalized).map_err(|e| e.to_string())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderEnrollment {
    pub instance: ServiceInstanceRegistration,
    pub provider_key: String,
    pub release: String,
    pub catalog_url: String,
    pub catalog_digest: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderSession {
    pub instance: ServiceInstanceIdentity,
    pub generation: String,
    pub credential: String,
    pub lease_expires_at_ms: i64,
    pub heartbeat_interval_ms: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderInstance {
    pub enrollment: ProviderEnrollment,
    pub generation: String,
    pub lease_expires_at_ms: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderPreviewRequest {
    pub instance_id: String,
    pub generation: String,
    pub catalog_digest: String,
    #[serde(default)]
    pub environment_bindings: BTreeMap<String, Value>,
    /// Explicit adoption of legacy Workflow IDs; names are never identity.
    #[serde(default)]
    pub workflow_bindings: BTreeMap<String, String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderChangeKind {
    Create,
    Update,
    Unchanged,
    Adopt,
    Conflict,
    Missing,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderChange {
    pub kind: String,
    pub key: String,
    pub target_id: Option<String>,
    pub change: ProviderChangeKind,
    pub message: Option<String>,
    pub expected_revision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderPlan {
    pub id: String,
    pub provider_key: String,
    pub release: String,
    pub catalog_digest: String,
    pub expires_at_ms: i64,
    pub changes: Vec<ProviderChange>,
    pub events: Option<EventDispatchImportPlan>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderApplyRequest {
    pub plan_id: String,
    /// Explicit: import drafts by default, never implicitly publish workflows.
    #[serde(default)]
    pub publish: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderReceipt {
    pub plan_id: String,
    pub complete: bool,
    pub items: BTreeMap<String, Value>,
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identity_does_not_depend_on_release_and_duplicate_keys_fail() {
        let workflow = ProviderWorkflow {
            key: "refund".into(),
            name: "退款".into(),
            description: None,
            define_schema: BTreeMap::from([("nodes".into(), serde_json::json!([]))]),
        };
        let mut catalog = ProviderCatalog {
            format_version: 1,
            application_id: "app".into(),
            provider_key: "backend".into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![workflow.clone()],
        };
        let first = catalog.digest().unwrap();
        catalog.release = "2".into();
        assert_eq!(catalog.workflows[0].key, "refund");
        assert_ne!(first, catalog.digest().unwrap());
        catalog.workflows.push(workflow);
        assert!(catalog.validate().is_err());
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProviderImportStatus {
    pub plan: ProviderPlan,
    pub receipt: ProviderReceipt,
    pub publish: Option<bool>,
}
