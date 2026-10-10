//! Source declarations can be registered before governance resource IDs exist.
//! These contracts carry no authority to import resources or invoke a handler.
use crate::{ConsumerDefinition, EventDefinition, ManifestDigest, ProducerMetadata};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

pub const MAX_CONSUMER_DECLARATION_BYTES: usize = 16 * 1024;
pub const MAX_CONSUMER_DECLARATION_EVENTS: usize = 64;
pub const MAX_REGISTERED_CONSUMERS: usize = 8192;
pub const MAX_REGISTERED_CONSUMERS_PER_APPLICATION: usize = 2048;
pub const MAX_REGISTERED_CONSUMERS_PER_CONNECTION: usize = 1023;

/// Negotiated independently of the legacy finite-lease enrollment protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsumerRegistrationProtocol {
    #[serde(rename = "consumer-declarations/1")]
    V1,
}

/// Immutable, source-owned subset for exactly one bound logical consumer.
/// Package version and digest are evidence, not resource identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerDeclaration {
    pub provider_key: String,
    pub producer: ProducerMetadata,
    pub consumer: ConsumerDefinition,
    pub events: Vec<EventDefinition>,
}
impl ConsumerDeclaration {
    pub fn validate(&self) -> Result<(), ConsumerRegistrationError> {
        for key in [
            &self.provider_key,
            &self.producer.package_name,
            &self.producer.package_version,
            &self.consumer.key,
            &self.consumer.group_key,
        ] {
            validate_registration_key(key)?;
        }
        if self.events.is_empty()
            || self.events.len() > MAX_CONSUMER_DECLARATION_EVENTS
            || self.consumer.selectors.is_empty()
            || self.consumer.selectors.len() > MAX_CONSUMER_DECLARATION_EVENTS
            || !(1..=65_536).contains(&self.consumer.maximum_concurrency)
            || self.consumer.delivery_mode != crate::DeliveryMode::Sync
        {
            return Err(ConsumerRegistrationError::InvalidDeclaration);
        }
        let mut events = BTreeSet::new();
        let mut identities = BTreeSet::new();
        for event in &self.events {
            for key in [
                &event.key,
                &event.topic,
                &event.event_type,
                &event.schema_version,
            ] {
                validate_registration_key(key)?;
            }
            if !events.insert(&event.key)
                || !identities.insert((&event.topic, &event.event_type, &event.schema_version))
                || !(event.payload_schema.is_object() || event.payload_schema.is_boolean())
            {
                return Err(ConsumerRegistrationError::InvalidDeclaration);
            }
        }
        let mut selectors = BTreeSet::new();
        let mut selected_events = BTreeSet::new();
        for selector in &self.consumer.selectors {
            if !selectors.insert((&selector.topic, &selector.event_type))
                || !selected_events.insert(&selector.event_key)
                || !self.events.iter().any(|event| {
                    event.key == selector.event_key
                        && event.topic == selector.topic
                        && event.event_type == selector.event_type
                })
            {
                return Err(ConsumerRegistrationError::InvalidDeclaration);
            }
        }
        if self
            .events
            .iter()
            .any(|event| !selected_events.contains(&event.key))
        {
            return Err(ConsumerRegistrationError::InvalidDeclaration);
        }
        if serde_json::to_vec(self)
            .map_err(|_| ConsumerRegistrationError::InvalidDeclaration)?
            .len()
            > MAX_CONSUMER_DECLARATION_BYTES
        {
            return Err(ConsumerRegistrationError::CapacityExceeded);
        }
        Ok(())
    }

    /// Package release, descriptions and import-policy suggestions are not a
    /// change to what an already bound Handler can execute. Runtime concurrency,
    /// source identity, selectors and event schemas are part of that contract.
    pub fn same_execution_contract(&self, other: &Self) -> bool {
        if self.provider_key != other.provider_key
            || self.producer.package_name != other.producer.package_name
            || self.consumer.key != other.consumer.key
            || self.consumer.group_key != other.consumer.group_key
            || self.consumer.delivery_mode != other.consumer.delivery_mode
            || self.consumer.maximum_concurrency != other.consumer.maximum_concurrency
            || self.consumer.selectors.len() != other.consumer.selectors.len()
            || self.events.len() != other.events.len()
        {
            return false;
        }
        self.consumer
            .selectors
            .iter()
            .all(|selector| other.consumer.selectors.contains(selector))
            && self.events.iter().all(|event| {
                other.events.iter().any(|candidate| {
                    event.key == candidate.key
                        && event.topic == candidate.topic
                        && event.event_type == candidate.event_type
                        && event.schema_version == candidate.schema_version
                        && event.payload_schema == candidate.payload_schema
                })
            })
    }

    /// Canonicalized using the same rules as the existing import manifest.
    pub fn digest(&self) -> Result<ManifestDigest, ConsumerRegistrationError> {
        self.validate()?;
        let manifest = crate::EventDispatchManifestV1::new(
            self.producer.clone(),
            vec![],
            self.events.clone(),
            vec![self.consumer.clone()],
            vec![],
        )
        .map_err(|_| ConsumerRegistrationError::InvalidDeclaration)?;
        // Bind Provider ownership too; it is deliberately absent from Event lineage.
        let bytes = serde_json::to_vec(&(&self.provider_key, manifest.digest))
            .map_err(|_| ConsumerRegistrationError::InvalidDeclaration)?;
        Ok(ManifestDigest::sha256(&bytes))
    }
}

/// The authenticated application and server-issued connection epoch are supplied
/// by the transport boundary, never selected from this request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerDeclarationUpdate {
    pub protocol: ConsumerRegistrationProtocol,
    pub registration_key: String,
    pub operation_id: String,
    /// None creates a new, monotonically fenced revision; updates require the
    /// exact current revision. A removed declaration never reuses its version.
    pub expected_revision: Option<u64>,
    pub declaration: ConsumerDeclaration,
}
impl ConsumerDeclarationUpdate {
    pub fn validate(&self) -> Result<(), ConsumerRegistrationError> {
        validate_registration_key(&self.registration_key)?;
        validate_registration_key(&self.operation_id)?;
        if self.expected_revision == Some(0) || self.expected_revision == Some(u64::MAX) {
            return Err(ConsumerRegistrationError::RevisionConflict);
        }
        self.declaration.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerActivationState {
    WaitingForCatalog,
    Activating,
    Active,
    Disabled,
    Incompatible,
    /// The platform cannot currently verify the authoritative catalog.
    Unavailable,
    Offline,
    Revoked,
}

/// Binding for activation acknowledgement. Every component must still match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerActivationVersion {
    pub owner_boot: String,
    pub connection_epoch: String,
    pub registration_key: String,
    pub declaration_revision: u64,
    pub catalog_revision: u64,
    pub activation_revision: u64,
}
impl ConsumerActivationVersion {
    /// Only release metadata may advance without changing this proved binding.
    /// A governance/contract reactivation always has a different activation revision.
    pub fn preserves_route_from(&self, previous: &Self) -> bool {
        self.owner_boot == previous.owner_boot
            && self.connection_epoch == previous.connection_epoch
            && self.registration_key == previous.registration_key
            && self.catalog_revision == previous.catalog_revision
            && self.activation_revision == previous.activation_revision
            && self.declaration_revision >= previous.declaration_revision
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerRegistrationReceipt {
    pub protocol: ConsumerRegistrationProtocol,
    pub version: ConsumerActivationVersion,
    pub declaration_digest: ManifestDigest,
    pub state: ConsumerActivationState,
    pub directory_revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ConsumerRegistrationError {
    #[error("invalid consumer declaration")]
    InvalidDeclaration,
    #[error("consumer registration capacity exceeded")]
    CapacityExceeded,
    #[error("consumer declaration revision conflict")]
    RevisionConflict,
    #[error("consumer registration operation conflicts with a previous operation")]
    OperationConflict,
    #[error("consumer registration does not belong to the current connection")]
    StaleConnection,
    #[error("consumer registration is unavailable")]
    NotFound,
    #[error("consumer activation version is stale")]
    StaleActivation,
    #[error("consumer source identity cannot change within a registration")]
    SourceConflict,
}

pub fn validate_registration_key(value: &str) -> Result<(), ConsumerRegistrationError> {
    if value.is_empty()
        || value.len() > 256
        || value.trim() != value
        || value
            .chars()
            .any(|c| c.is_control() || matches!(c, '*' | '?' | '#'))
    {
        return Err(ConsumerRegistrationError::InvalidDeclaration);
    }
    Ok(())
}

#[cfg(test)]
#[path = "registration_tests.rs"]
mod tests;
