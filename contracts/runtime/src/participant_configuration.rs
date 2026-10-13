//! Read-only design-time choices. Providers retain rules, scope and assignment ownership.
use crate::{
    human_task::{HumanTaskClaimPolicy, HumanTaskKind},
    service::valid_key,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const PARTICIPANT_CONFIGURATION_PROTOCOL: &str = "participant_configuration_v1";
pub const MAX_CONFIGURATION_QUERY_BYTES: usize = 16 * 1024;
pub const MAX_CONFIGURATION_RESPONSE_BYTES: usize = 128 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParticipantRuleRef {
    pub key: String,
    pub revision: String,
}
impl ParticipantRuleRef {
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        if !valid_key(&self.key) || !valid_key(&self.revision) {
            return Err(ConfigurationError::InvalidRequest);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParticipantSelector {
    pub provider_key: String,
    pub capability_key: String,
    pub rule: ParticipantRuleRef,
}
impl ParticipantSelector {
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        if !valid_key(&self.provider_key) || !valid_key(&self.capability_key) {
            return Err(ConfigurationError::InvalidRequest);
        }
        self.rule.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParticipantCompatibility {
    pub task_type: String,
    pub node_kind: HumanTaskKind,
    pub claim_policy: HumanTaskClaimPolicy,
}
impl ParticipantCompatibility {
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        if !valid_key(&self.task_type)
            || self.node_kind == HumanTaskKind::Draft
            || (self.node_kind == HumanTaskKind::Read
                && self.claim_policy != HumanTaskClaimPolicy::None)
        {
            return Err(ConfigurationError::Incompatible);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParticipantConfigurationCapability {
    pub key: String,
    pub label: String,
    pub compatibility: Vec<ParticipantCompatibility>,
    #[serde(default)]
    pub supports_preview: bool,
    /// Keys only. Deployment-owned allowed origins are never taken from a query result.
    #[serde(default)]
    pub management_site_keys: Vec<String>,
}
impl ParticipantConfigurationCapability {
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        if !valid_key(&self.key)
            || !text(&self.label, 120)
            || self.compatibility.is_empty()
            || self.compatibility.len() > 64
            || self.management_site_keys.len() > 8
        {
            return Err(ConfigurationError::InvalidRequest);
        }
        for compatibility in &self.compatibility {
            compatibility.validate()?;
        }
        let mut seen = BTreeSet::new();
        for site in &self.management_site_keys {
            if !valid_key(site) || !seen.insert(site) {
                return Err(ConfigurationError::InvalidRequest);
            }
        }
        Ok(())
    }
}

/// A provider-issued delegation reference, not a caller-selected organization ID.
/// It must not be logged, exported, or saved in Workflow definitions.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigurationQuery {
    pub protocol: String,
    pub capability_key: String,
    pub compatibility: ParticipantCompatibility,
    pub context_ref: String,
    pub operation: ConfigurationOperation,
}
impl std::fmt::Debug for ConfigurationQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigurationQuery")
            .field("capability_key", &self.capability_key)
            .field("compatibility", &self.compatibility)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConfigurationOperation {
    List {
        search: String,
        cursor: Option<String>,
        limit: u16,
    },
    Detail {
        rule: ParticipantRuleRef,
    },
    Validate {
        rule: ParticipantRuleRef,
    },
    Preview {
        rule: ParticipantRuleRef,
        business_ref: String,
    },
}
impl ConfigurationQuery {
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        if self.context_ref.trim().is_empty() {
            return Err(ConfigurationError::ContextRequired);
        }
        if self.protocol != PARTICIPANT_CONFIGURATION_PROTOCOL
            || !valid_key(&self.capability_key)
            || !text(&self.context_ref, 2048)
        {
            return Err(ConfigurationError::InvalidRequest);
        }
        self.compatibility.validate()?;
        match &self.operation {
            ConfigurationOperation::List {
                search,
                cursor,
                limit,
            } => {
                if search.len() > 240
                    || search.chars().any(char::is_control)
                    || cursor.as_ref().is_some_and(|value| !text(value, 1024))
                    || !(1..=50).contains(limit)
                {
                    return Err(ConfigurationError::InvalidRequest);
                }
            }
            ConfigurationOperation::Detail { rule } | ConfigurationOperation::Validate { rule } => {
                rule.validate()?
            }
            ConfigurationOperation::Preview { rule, business_ref } => {
                rule.validate()?;
                if !text(business_ref, 1024) {
                    return Err(ConfigurationError::InvalidRequest);
                }
            }
        }
        if serde_json::to_vec(self)
            .map_err(|_| ConfigurationError::InvalidRequest)?
            .len()
            > MAX_CONFIGURATION_QUERY_BYTES
        {
            return Err(ConfigurationError::InvalidRequest);
        }
        Ok(())
    }
}

/// Supplied by the authenticated platform, never deserialized from the browser body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigurationQueryContext {
    pub application_id: String,
    pub provider_key: String,
    /// Lingshu subject; provider MUST validate its delegation mapping, not assume a shared user ID.
    pub editor_subject: String,
}
impl ConfigurationQueryContext {
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        if !valid_key(&self.application_id)
            || !valid_key(&self.provider_key)
            || !text(&self.editor_subject, 256)
        {
            return Err(ConfigurationError::Forbidden);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigurationSummary {
    pub label: String,
    pub text: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigurationManagementLink {
    pub site_key: String,
    /// Canonical, encoded absolute path only; no query, fragment or authority.
    pub path: String,
}
impl ConfigurationManagementLink {
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        // Restrict to unambiguous path segments. Site origin is configured separately.
        if !valid_key(&self.site_key)
            || !self.path.starts_with('/')
            || self.path.len() > 1024
            || self.path.contains("//")
            || self
                .path
                .bytes()
                .any(|b| !(b.is_ascii_alphanumeric() || b"/-_.~".contains(&b)))
            || self.path.split('/').any(|part| part == "." || part == "..")
        {
            return Err(ConfigurationError::InvalidResponse);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParticipantOption {
    #[serde(rename = "ref")]
    pub rule: ParticipantRuleRef,
    pub label: String,
    pub description: String,
    pub summary: Vec<ConfigurationSummary>,
    pub compatibility: ParticipantCompatibility,
    pub unavailable_reason: Option<String>,
    pub management_link: Option<ConfigurationManagementLink>,
}
impl ParticipantOption {
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        self.rule.validate()?;
        self.compatibility.validate()?;
        if !text(&self.label, 120)
            || self.description.len() > 1000
            || self.description.chars().any(char::is_control)
            || self.summary.len() > 12
            || self
                .summary
                .iter()
                .any(|entry| !text(&entry.label, 80) || !text(&entry.text, 1000))
            || self
                .unavailable_reason
                .as_ref()
                .is_some_and(|reason| !text(reason, 500))
        {
            return Err(ConfigurationError::InvalidResponse);
        }
        if let Some(link) = &self.management_link {
            link.validate()?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConfigurationResponse {
    Options {
        items: Vec<ParticipantOption>,
        next_cursor: Option<String>,
    },
    Detail {
        item: ParticipantOption,
    },
    Validated {
        rule: ParticipantRuleRef,
    },
    /// Provider-redacted human-readable outcome, bounded and separately authorized.
    Preview {
        rule: ParticipantRuleRef,
        summary: Vec<ConfigurationSummary>,
    },
    Error {
        code: ConfigurationError,
    },
}
impl ConfigurationResponse {
    pub fn validate_for(
        &self,
        query: &ConfigurationQuery,
        capability: &ParticipantConfigurationCapability,
    ) -> Result<(), ConfigurationError> {
        if serde_json::to_vec(self)
            .map_err(|_| ConfigurationError::InvalidResponse)?
            .len()
            > MAX_CONFIGURATION_RESPONSE_BYTES
        {
            return Err(ConfigurationError::InvalidResponse);
        }
        let check_option = |item: &ParticipantOption| -> Result<(), ConfigurationError> {
            item.validate()?;
            if item.compatibility != query.compatibility
                || item
                    .management_link
                    .as_ref()
                    .is_some_and(|link| !capability.management_site_keys.contains(&link.site_key))
            {
                return Err(ConfigurationError::InvalidResponse);
            }
            Ok(())
        };
        match (&query.operation, self) {
            (_, Self::Error { .. }) => Ok(()),
            (ConfigurationOperation::List { limit, .. }, Self::Options { items, next_cursor }) => {
                if items.len() > usize::from(*limit)
                    || next_cursor.as_ref().is_some_and(|value| !text(value, 1024))
                {
                    return Err(ConfigurationError::InvalidResponse);
                }
                let mut seen = BTreeSet::new();
                for item in items {
                    check_option(item)?;
                    if !seen.insert((&item.rule.key, &item.rule.revision)) {
                        return Err(ConfigurationError::InvalidResponse);
                    }
                }
                Ok(())
            }
            (ConfigurationOperation::Detail { rule }, Self::Detail { item })
                if rule == &item.rule =>
            {
                check_option(item)
            }
            (ConfigurationOperation::Validate { rule }, Self::Validated { rule: actual })
                if rule == actual =>
            {
                Ok(())
            }
            (
                ConfigurationOperation::Preview { rule, .. },
                Self::Preview {
                    rule: actual,
                    summary,
                },
            ) if capability.supports_preview
                && rule == actual
                && summary.len() <= 50
                && summary
                    .iter()
                    .all(|entry| text(&entry.label, 120) && text(&entry.text, 1000)) =>
            {
                Ok(())
            }
            _ => Err(ConfigurationError::InvalidResponse),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(rename_all = "snake_case")]
pub enum ConfigurationError {
    #[error("configuration context required")]
    ContextRequired,
    #[error("configuration access forbidden")]
    Forbidden,
    #[error("configuration reference not found")]
    NotFound,
    #[error("configuration incompatible with node")]
    Incompatible,
    #[error("invalid configuration request")]
    InvalidRequest,
    #[error("invalid provider response")]
    InvalidResponse,
    #[error("configuration provider unavailable")]
    Unavailable,
}
fn text(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn mode() -> ParticipantCompatibility {
        ParticipantCompatibility {
            task_type: "refund.review".into(),
            node_kind: HumanTaskKind::Todo,
            claim_policy: HumanTaskClaimPolicy::Required,
        }
    }
    fn query() -> ConfigurationQuery {
        ConfigurationQuery {
            protocol: PARTICIPANT_CONFIGURATION_PROTOCOL.into(),
            capability_key: "approvers".into(),
            compatibility: mode(),
            context_ref: "secret-delegation".into(),
            operation: ConfigurationOperation::List {
                search: "".into(),
                cursor: None,
                limit: 20,
            },
        }
    }
    #[test]
    fn rejects_missing_context_unbounded_queries_and_read_claims() {
        let mut q = query();
        assert!(q.validate().is_ok());
        q.context_ref.clear();
        assert_eq!(q.validate(), Err(ConfigurationError::ContextRequired));
        q = query();
        q.operation = ConfigurationOperation::List {
            search: "".into(),
            cursor: None,
            limit: 51,
        };
        assert_eq!(q.validate(), Err(ConfigurationError::InvalidRequest));
        q = query();
        q.compatibility.node_kind = HumanTaskKind::Read;
        assert_eq!(q.validate(), Err(ConfigurationError::Incompatible));
        assert!(!format!("{:?}", query()).contains("secret-delegation"));
    }
    #[test]
    fn management_paths_cannot_escape_registered_origin() {
        for path in [
            "//evil.test",
            "/\\evil",
            "/%2f%2fevil",
            "/a/../b",
            "/a?token=secret",
            "javascript:alert(1)",
            "/a#b",
        ] {
            assert!(
                ConfigurationManagementLink {
                    site_key: "admin".into(),
                    path: path.into()
                }
                .validate()
                .is_err(),
                "{path}"
            );
        }
        assert!(ConfigurationManagementLink {
            site_key: "admin".into(),
            path: "/approval/rules/123".into()
        }
        .validate()
        .is_ok());
    }
    #[test]
    fn exact_revision_and_operation_must_match_response() {
        let mut q = query();
        let rule = ParticipantRuleRef {
            key: "review".into(),
            revision: "3".into(),
        };
        q.operation = ConfigurationOperation::Validate { rule: rule.clone() };
        let cap = ParticipantConfigurationCapability {
            key: "approvers".into(),
            label: "Approvers".into(),
            compatibility: vec![mode()],
            supports_preview: false,
            management_site_keys: vec![],
        };
        assert!(ConfigurationResponse::Validated { rule }
            .validate_for(&q, &cap)
            .is_ok());
        assert!(ConfigurationResponse::Validated {
            rule: ParticipantRuleRef {
                key: "review".into(),
                revision: "4".into()
            }
        }
        .validate_for(&q, &cap)
        .is_err());
        assert!(ConfigurationResponse::Options {
            items: vec![],
            next_cursor: None
        }
        .validate_for(&q, &cap)
        .is_err());
    }
}
