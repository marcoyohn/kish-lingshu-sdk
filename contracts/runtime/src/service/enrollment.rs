//! Separate v2 wire; published Operations and legacy HTTP v1 are unchanged.
use super::{
    valid_key, InstanceCapability, ServiceEnrollment, ServiceInstanceRegistration, ServiceSession,
};
use kish_lingshu_foundation_contract::service_transport::{
    enrollment::{validate_registration, EnrollmentVersion, RequestedServiceEndpoint},
    ServiceEndpoint, TransportContractError,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceEnrollmentV2 {
    pub enrollment_version: EnrollmentVersion,
    pub instance: ServiceInstanceRegistration,
    pub node_id: String,
    pub endpoint: RequestedServiceEndpoint,
    pub maximum_in_flight: u32,
    pub capabilities: Vec<InstanceCapability>,
}
impl ServiceEnrollmentV2 {
    pub fn validate(&self) -> Result<(), TransportContractError> {
        validate_registration(&self.instance)?;
        self.endpoint.validate()?;
        if !valid_key(&self.node_id)
            || !(1..=65_536).contains(&self.maximum_in_flight)
            || self.capabilities.is_empty()
            || self.capabilities.len() > 1024
            || matches!(self.endpoint, RequestedServiceEndpoint::Zenoh { .. })
                && self.instance.generation.is_none()
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        let mut operations = BTreeSet::new();
        let mut events = BTreeSet::new();
        for capability in &self.capabilities {
            if !operations.insert(&capability.operation)
                || !capability.call && capability.events.is_empty()
            {
                return Err(TransportContractError::InvalidIdentity);
            }
            for event in &capability.events {
                if !events.insert(event) {
                    return Err(TransportContractError::InvalidIdentity);
                }
            }
        }
        Ok(())
    }
}

/// Explicit compatibility conversion only; a Zenoh request cannot fall back to
/// v1 or acquire a fabricated callback URL.
impl TryFrom<ServiceEnrollmentV2> for ServiceEnrollment {
    type Error = TransportContractError;
    fn try_from(value: ServiceEnrollmentV2) -> Result<Self, Self::Error> {
        value.validate()?;
        let RequestedServiceEndpoint::Http { invocation_url } = value.endpoint else {
            return Err(TransportContractError::UnsupportedProtocol);
        };
        Ok(Self {
            instance: Some(value.instance),
            node_id: value.node_id,
            invocation_url,
            maximum_in_flight: value.maximum_in_flight,
            capabilities: value.capabilities,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceEnrollmentResponseV2 {
    pub enrollment_version: EnrollmentVersion,
    pub session: ServiceSession,
    pub endpoint: ServiceEndpoint,
}

/// Ephemeral v2 role metadata. A registered role is not a confirmed route.
/// Keep the legacy ServiceInstance wire unchanged for HTTP-only readers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceInstanceV2 {
    pub application_id: String,
    pub enrollment: ServiceEnrollmentV2,
    pub generation: String,
    pub lease_expires_at_ms: i64,
}

/// Role registration on the authenticated platform control channel. Application
/// and routing identities are derived from its verified native receipt.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "role",
    content = "registration",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ChannelRoleEnrollment {
    Call(ServiceEnrollmentV2),
    Provider(crate::provider::ProviderEnrollmentV2),
    Consumer(kish_lingshu_event_dispatch_contract::ConsumerEnrollmentRequestV2),
}
impl ChannelRoleEnrollment {
    pub fn instance(&self) -> &ServiceInstanceRegistration {
        match self {
            Self::Call(request) => &request.instance,
            Self::Provider(request) => &request.instance,
            Self::Consumer(request) => &request.instance,
        }
    }
    pub fn validate(&self) -> Result<(), TransportContractError> {
        match self {
            Self::Call(request) => request.validate(),
            Self::Provider(request) => request.validate(),
            Self::Consumer(request) => request.validate(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(
    tag = "role",
    content = "registration",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ChannelRoleEnrollmentResponse {
    Call(ServiceEnrollmentResponseV2),
    Provider(crate::provider::ProviderEnrollmentResponseV2),
    Consumer(kish_lingshu_event_dispatch_contract::ConsumerEnrollmentResponseV2),
}

/// This receipt requests confirmation of the previously server-issued lanes.
/// It does not allow clients to supply replacement destinations or epochs.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelRoleRouteConfirmation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_registration_version: Option<super::CallActivationVersion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration_version:
        Option<kish_lingshu_event_dispatch_contract::ConsumerActivationVersion>,
    pub role_generation: String,
    pub route_revision: u64,
}

/// Withdraw one role on its original authenticated channel. Scope and the
/// native grant revision are resolved by the platform, never by the caller.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelRoleDeregistration {
    pub role_generation: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelRoleDeregistrationResponse {
    pub role_generation: String,
}

/// Scope and predecessor come from the authenticated candidate's server record.
/// Clients cannot choose a predecessor, grant, lane epoch or replacement address.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelRoleAdoption {
    pub role_generation: String,
}
impl ChannelRoleAdoption {
    pub fn validate(&self) -> Result<(), TransportContractError> {
        kish_lingshu_foundation_contract::service_transport::RouteIdentity::new(
            &self.role_generation,
        )?;
        Ok(())
    }
}

/// Transfer receipt only. A new signed lane proof is still required for readiness.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelRoleAdoptionResponse {
    pub previous_certificate_identity:
        kish_lingshu_foundation_contract::service_transport::RouteIdentity,
    pub endpoint: ServiceEndpoint,
    pub authorization_issued_at_ms: i64,
    pub authorization_expires_at_ms: i64,
}

/// Keep each renewal inside the existing 32KiB control envelope and queue.
/// A pool may own 1024 roles; callers explicitly submit bounded batches.
pub const MAX_CHANNEL_RENEWAL_ROLES: usize = 64;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelRoleRenewal {
    pub role_generations: Vec<String>,
}
impl ChannelRoleRenewal {
    pub fn validate(&self) -> Result<(), TransportContractError> {
        let mut seen = BTreeSet::new();
        if self.role_generations.is_empty()
            || self.role_generations.len() > MAX_CHANNEL_RENEWAL_ROLES
            || self.role_generations.iter().any(|id| {
                kish_lingshu_foundation_contract::service_transport::RouteIdentity::new(id).is_err()
                    || !seen.insert(id)
            })
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelRoleRenewalResult {
    pub role_generation: String,
    /// 200 confirms the domain lease and native grant, not route readiness.
    /// 403/409 end this role; 503 retains its previous finite authority.
    pub status: u16,
    pub lease_expires_at_ms: Option<i64>,
    pub authorization_expires_at_ms: Option<i64>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelRoleRenewalResponse {
    pub renewal_issued_at_ms: i64,
    pub roles: Vec<ChannelRoleRenewalResult>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::OperationRef;
    use serde_json::json;

    #[test]
    fn adoption_request_cannot_select_predecessor_or_destination() {
        let request = ChannelRoleAdoption {
            role_generation: "role".into(),
        };
        request.validate().unwrap();
        assert!(ChannelRoleAdoption {
            role_generation: "".into()
        }
        .validate()
        .is_err());
        for (field, value) in [
            ("previous_certificate_identity", json!("foreign")),
            ("endpoint", json!({})),
            ("grant_revision", json!(2)),
        ] {
            let mut wire = serde_json::to_value(&request).unwrap();
            wire[field] = value;
            assert!(serde_json::from_value::<ChannelRoleAdoption>(wire).is_err());
        }
    }

    fn request() -> ServiceEnrollmentV2 {
        ServiceEnrollmentV2 {
            enrollment_version: EnrollmentVersion::V2,
            instance: ServiceInstanceRegistration {
                instance_id: "sdk".into(),
                incarnation_id: "boot".into(),
                generation: Some("base".into()),
            },
            node_id: "call".into(),
            endpoint: RequestedServiceEndpoint::Zenoh {
                protocol_version:
                    kish_lingshu_foundation_contract::service_transport::ProtocolVersion::V1,
                lane_count: Default::default(),
            },
            maximum_in_flight: 1,
            capabilities: vec![InstanceCapability {
                operation: OperationRef {
                    service_key: "svc".into(),
                    operation_key: "op".into(),
                    version: "v1".into(),
                    contract_digest: "d".repeat(64),
                },
                call: true,
                events: BTreeSet::new(),
            }],
        }
    }
    #[test]
    fn v2_requires_explicit_wire_and_preserves_v1() {
        let mut request = request();
        assert!(request.validate().is_ok());
        assert!(ServiceEnrollment::try_from(request.clone()).is_err());
        let mut value = serde_json::to_value(&request).unwrap();
        value.as_object_mut().unwrap().remove("enrollment_version");
        assert!(serde_json::from_value::<ServiceEnrollmentV2>(value).is_err());
        request.endpoint = RequestedServiceEndpoint::Http {
            invocation_url: "https://sdk.example/invoke?route=one".into(),
        };
        let legacy = ServiceEnrollment::try_from(request.clone()).unwrap();
        assert_eq!(legacy.capabilities, request.capabilities);
        let legacy = serde_json::to_value(legacy).unwrap();
        assert!(legacy.get("enrollment_version").is_none());
        assert!(legacy.get("endpoint").is_none());
        assert!(serde_json::from_value::<ServiceEnrollmentV2>(legacy.clone()).is_err());
        assert!(serde_json::from_value::<ServiceEnrollment>(legacy).is_ok());
        let mut value = serde_json::to_value(request).unwrap();
        value["invocation_url"] = json!("https://foreign.example");
        assert!(serde_json::from_value::<ServiceEnrollmentV2>(value).is_err());
    }
    #[test]
    fn v2_cannot_join_without_base_or_duplicate_capabilities() {
        let mut request = request();
        request.instance.generation = None;
        assert!(request.validate().is_err());
        request.instance.generation = Some("base".into());
        request.capabilities.push(request.capabilities[0].clone());
        assert!(request.validate().is_err());
        request.capabilities.pop();
        request.maximum_in_flight = 65_537;
        assert!(request.validate().is_err());
    }

    #[test]
    fn deregistration_cannot_select_another_scope_or_grant_revision() {
        let request = json!({"role_generation": "issued-role"});
        assert!(serde_json::from_value::<ChannelRoleDeregistration>(request.clone()).is_ok());
        for field in [
            "application_id",
            "certificate_identity",
            "route",
            "revision",
        ] {
            let mut forged = request.clone();
            forged[field] = json!("foreign");
            assert!(serde_json::from_value::<ChannelRoleDeregistration>(forged).is_err());
        }
        assert!(serde_json::from_value::<ChannelRoleDeregistration>(json!({})).is_err());
    }

    #[test]
    fn renewal_rejects_duplicate_oversized_or_caller_selected_scope() {
        for ids in [
            vec![],
            vec!["role".into(); 2],
            vec!["role".into(); 65],
            vec!["a/b".into()],
        ] {
            assert!(ChannelRoleRenewal {
                role_generations: ids
            }
            .validate()
            .is_err());
        }
        let mut request = json!({"role_generations": ["role"]});
        assert!(
            serde_json::from_value::<ChannelRoleRenewal>(request.clone())
                .unwrap()
                .validate()
                .is_ok()
        );
        request["certificate_identity"] = json!("foreign");
        assert!(serde_json::from_value::<ChannelRoleRenewal>(request).is_err());
    }
}
