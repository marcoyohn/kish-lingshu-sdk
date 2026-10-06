//! V2 catalog transport is independent of the immutable Provider catalog format.
use super::ProviderEnrollment;
use crate::service::{valid_key, ServiceInstanceRegistration};
use kish_lingshu_foundation_contract::service_transport::{
    enrollment::{
        validate_registration, EnrollmentVersion, ProviderEndpoint, RequestedProviderEndpoint,
    },
    TransportContractError,
};
use serde::{Deserialize, Serialize};

/// Signed, exact read of the registered immutable snapshot. These values must
/// come from current registration/route authority, never from a preview URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCatalogRead {
    pub provider_key: String,
    pub release: String,
    pub catalog_digest: String,
    pub route_revision: u64,
    pub lane: kish_lingshu_foundation_contract::service_transport::LaneIdentity,
}

#[async_trait::async_trait]
pub trait ProviderCatalogReader: Send + Sync {
    /// The caller has selected this exact registered instance. Implementations
    /// must check current authority and return a single authenticated snapshot.
    async fn read(
        &self,
        application: &str,
        instance: &ProviderInstanceV2,
    ) -> Result<Vec<u8>, kish_lingshu_foundation_contract::service_transport::TransportFailureKind>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderEnrollmentV2 {
    pub enrollment_version: EnrollmentVersion,
    pub instance: ServiceInstanceRegistration,
    pub provider_key: String,
    pub release: String,
    pub endpoint: RequestedProviderEndpoint,
    pub catalog_digest: String,
}
impl ProviderEnrollmentV2 {
    pub fn validate(&self) -> Result<(), TransportContractError> {
        validate_registration(&self.instance)?;
        self.endpoint.validate()?;
        if !valid_key(&self.provider_key)
            || self.release.is_empty()
            || self.release.len() > 128
            || self.catalog_digest.len() != 64
            || !self
                .catalog_digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || matches!(self.endpoint, RequestedProviderEndpoint::Zenoh { .. })
                && self.instance.generation.is_none()
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        Ok(())
    }
}
impl TryFrom<ProviderEnrollmentV2> for ProviderEnrollment {
    type Error = TransportContractError;
    fn try_from(value: ProviderEnrollmentV2) -> Result<Self, Self::Error> {
        value.validate()?;
        let RequestedProviderEndpoint::Http { catalog_url } = value.endpoint else {
            return Err(TransportContractError::UnsupportedProtocol);
        };
        Ok(Self {
            instance: value.instance,
            provider_key: value.provider_key,
            release: value.release,
            catalog_url,
            catalog_digest: value.catalog_digest,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderEnrollmentResponseV2 {
    pub enrollment_version: EnrollmentVersion,
    pub session: super::ProviderSession,
    pub endpoint: ProviderEndpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderInstanceV2 {
    pub enrollment: ProviderEnrollmentV2,
    pub generation: String,
    pub lease_expires_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn catalog_v2_rejects_unknown_transport_without_changing_v1() {
        let value = json!({"enrollment_version":2,"instance":{"instance_id":"sdk","incarnation_id":"boot","generation":"base"},"provider_key":"provider","release":"1","catalog_digest":"a".repeat(64),"endpoint":{"transport":"zenoh","protocol_version":"lingshu-zenoh/1"}});
        let request: ProviderEnrollmentV2 = serde_json::from_value(value.clone()).unwrap();
        assert!(request.validate().is_ok());
        assert!(ProviderEnrollment::try_from(request).is_err());
        let mut unknown = value.clone();
        unknown["endpoint"]["transport"] = json!("quic");
        assert!(serde_json::from_value::<ProviderEnrollmentV2>(unknown).is_err());
        let mut http = value;
        http["endpoint"] = json!({"transport":"http","catalog_url":"https://sdk.example/catalog"});
        let request: ProviderEnrollmentV2 = serde_json::from_value(http).unwrap();
        let digest = request.catalog_digest.clone();
        let legacy = ProviderEnrollment::try_from(request).unwrap();
        assert_eq!(legacy.catalog_digest, digest);
        assert!(serde_json::from_value::<ProviderEnrollment>(
            serde_json::to_value(legacy).unwrap()
        )
        .is_ok());
    }
}
