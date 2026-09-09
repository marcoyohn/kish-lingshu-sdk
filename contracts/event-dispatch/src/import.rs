use std::{collections::BTreeMap, fmt, str::FromStr};

use kish_lingshu_foundation_contract::{IdempotencyKey, MutationReceipt};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use thiserror::Error;

use crate::{EventDispatchManifestV1, ManifestDigest};

macro_rules! string_token {
    ($name:ident, $label:literal) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, InvalidImportToken> {
                let value = value.into();
                if value.is_empty()
                    || value.len() > 512
                    || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
                {
                    return Err(InvalidImportToken { kind: $label });
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = InvalidImportToken;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
            }
        }
    };
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventDispatchImportPreviewRequest {
    pub manifest: EventDispatchManifestV1,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub environment_bindings: BTreeMap<String, Value>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestResourceKind {
    Topic,
    EventDefinition,
    ConsumerGroup,
    Subscription,
    Schedule,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestResource {
    pub kind: ManifestResourceKind,
    pub source_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_id: Option<String>,
}

string_token!(ImportPlanToken, "import plan token");
string_token!(ResourceRevisionToken, "resource revision token");

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ObservedResourceRevision {
    Absent,
    Present { token: ResourceRevisionToken },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRevision {
    pub resource: ManifestResource,
    pub observed: ObservedResourceRevision,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum ManifestResourceChange {
    Create {
        resource: ManifestResource,
    },
    Update {
        resource: ManifestResource,
    },
    Unchanged {
        resource: ManifestResource,
    },
    Conflict {
        resource: ManifestResource,
        code: String,
        message: String,
    },
    Missing {
        resource: ManifestResource,
    },
}

impl ManifestResourceChange {
    pub fn resource(&self) -> &ManifestResource {
        match self {
            Self::Create { resource }
            | Self::Update { resource }
            | Self::Unchanged { resource }
            | Self::Conflict { resource, .. }
            | Self::Missing { resource } => resource,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ImportDiagnostic {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_key: Option<String>,
}

/// Side-effect-free reconciliation result for an authenticated Application.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventDispatchImportPlan {
    pub manifest_digest: ManifestDigest,
    pub plan_token: ImportPlanToken,
    #[serde(default)]
    pub changes: Vec<ManifestResourceChange>,
    #[serde(default)]
    pub resource_revisions: Vec<ResourceRevision>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_environment_bindings: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<ImportDiagnostic>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetirementMode {
    #[default]
    PreserveMissing,
    RetireMissing,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventDispatchImportApplyRequest {
    pub manifest_digest: ManifestDigest,
    pub plan_token: ImportPlanToken,
    pub resource_revisions: Vec<ResourceRevision>,
    pub idempotency_key: IdempotencyKey,
    #[serde(default)]
    pub retirement: RetirementMode,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportedResourceDisposition {
    Created,
    Updated,
    Unchanged,
    Preserved,
    Retired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ImportedResource {
    pub resource: ManifestResource,
    pub revision: ResourceRevisionToken,
    pub disposition: ImportedResourceDisposition,
}

/// Durable receipt for applying one manifest reconciliation plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventDispatchImportReceipt {
    #[serde(flatten)]
    pub mutation: MutationReceipt,
    pub manifest_digest: ManifestDigest,
    pub plan_token: ImportPlanToken,
    pub lineage_id: String,
    pub retirement: RetirementMode,
    #[serde(default)]
    pub resources: Vec<ImportedResource>,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("{kind} must be 1..=512 visible ASCII bytes")]
pub struct InvalidImportToken {
    kind: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_changes_preserve_all_reconciliation_states() {
        let resource = ManifestResource {
            kind: ManifestResourceKind::Schedule,
            source_key: "orders.expire".into(),
            target_id: Some("schedule-42".into()),
        };
        for change in [
            ManifestResourceChange::Create {
                resource: resource.clone(),
            },
            ManifestResourceChange::Update {
                resource: resource.clone(),
            },
            ManifestResourceChange::Unchanged {
                resource: resource.clone(),
            },
            ManifestResourceChange::Conflict {
                resource: resource.clone(),
                code: "manual_owner".into(),
                message: "resource is manually managed".into(),
            },
            ManifestResourceChange::Missing {
                resource: resource.clone(),
            },
        ] {
            let encoded = serde_json::to_value(&change).unwrap();
            let decoded: ManifestResourceChange = serde_json::from_value(encoded).unwrap();
            assert_eq!(decoded, change);
        }
    }

    #[test]
    fn revision_tokens_reject_empty_values() {
        assert!(ResourceRevisionToken::new("revision-7").is_ok());
        assert!(ResourceRevisionToken::new("").is_err());
    }
}
