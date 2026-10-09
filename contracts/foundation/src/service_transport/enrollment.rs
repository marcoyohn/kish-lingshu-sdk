//! V2 registration selects a transport, never a client-chosen routing address.
//! Returned routes are descriptions, not credentials or readiness evidence.
use super::{
    InstanceRoute, LaneIdentity, ProtocolVersion, ServiceEndpoint, TransportContractError,
};
use crate::{ServiceInstanceIdentity, ServiceInstanceRegistration, ServiceInstanceTransport};
use serde::{Deserialize, Serialize};

// Reserved wire paths; defining a DTO does not install an HTTP handler.
pub const SERVICE_ENROLLMENT_V2_PATH: &str = "api/user/services/v2/enrollments";
pub const PROVIDER_ENROLLMENT_V2_PATH: &str = "api/user/services/v2/provider-enrollments";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub enum EnrollmentVersion {
    V2,
}
impl TryFrom<u32> for EnrollmentVersion {
    type Error = TransportContractError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            2 => Ok(Self::V2),
            _ => Err(TransportContractError::UnsupportedProtocol),
        }
    }
}
impl From<EnrollmentVersion> for u32 {
    fn from(_: EnrollmentVersion) -> Self {
        2
    }
}

pub fn validate_registration(
    instance: &ServiceInstanceRegistration,
) -> Result<(), TransportContractError> {
    for value in [&instance.instance_id, &instance.incarnation_id]
        .into_iter()
        .chain(instance.generation.iter())
    {
        // Registration uses the existing common-owner identity alphabet.
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        {
            return Err(TransportContractError::InvalidIdentity);
        }
    }
    Ok(())
}

/// HTTPS or development loopback HTTP. Catalog endpoints disallow query strings;
/// invocation URLs retain the v1 query-string contract.
pub fn validate_http_url(value: &str, catalog: bool) -> Result<(), TransportContractError> {
    if value.len() > if catalog { 4096 } else { super::MAX_KEY_BYTES } {
        return Err(TransportContractError::InvalidIdentity);
    }
    let url = url::Url::parse(value).map_err(|_| TransportContractError::InvalidIdentity)?;
    let loopback = match url.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain(host)) => host == "localhost",
        None => false,
    };
    if url.host().is_none()
        || !(url.scheme() == "https" || url.scheme() == "http" && loopback)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || catalog && url.query().is_some()
    {
        return Err(TransportContractError::InvalidIdentity);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case", deny_unknown_fields)]
pub enum RequestedServiceEndpoint {
    Http {
        invocation_url: String,
    },
    Zenoh {
        protocol_version: ProtocolVersion,
        /// Capacity request only. The platform issues every lane and epoch.
        #[serde(default, skip_serializing_if = "DataLaneCount::is_one")]
        lane_count: DataLaneCount,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct DataLaneCount(u8);
impl Default for DataLaneCount {
    fn default() -> Self {
        Self(1)
    }
}
impl TryFrom<u8> for DataLaneCount {
    type Error = TransportContractError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value == 0 || usize::from(value) > super::MAX_DATA_LANES {
            return Err(TransportContractError::InvalidIdentity);
        }
        Ok(Self(value))
    }
}
impl From<DataLaneCount> for u8 {
    fn from(value: DataLaneCount) -> Self {
        value.0
    }
}
impl DataLaneCount {
    pub fn get(self) -> usize {
        usize::from(self.0)
    }
    fn is_one(&self) -> bool {
        self.0 == 1
    }
}
impl RequestedServiceEndpoint {
    pub fn lane_count(&self) -> usize {
        match self {
            Self::Http { .. } => 1,
            Self::Zenoh { lane_count, .. } => lane_count.get(),
        }
    }
    pub fn transport(&self) -> ServiceInstanceTransport {
        match self {
            Self::Http { .. } => ServiceInstanceTransport::Http,
            Self::Zenoh { .. } => ServiceInstanceTransport::Zenoh,
        }
    }
    pub fn validate(&self) -> Result<(), TransportContractError> {
        match self {
            Self::Http { invocation_url } => validate_http_url(invocation_url, false),
            Self::Zenoh { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case", deny_unknown_fields)]
pub enum RequestedProviderEndpoint {
    Http { catalog_url: String },
    Zenoh { protocol_version: ProtocolVersion },
}
impl RequestedProviderEndpoint {
    pub fn transport(&self) -> ServiceInstanceTransport {
        match self {
            Self::Http { .. } => ServiceInstanceTransport::Http,
            Self::Zenoh { .. } => ServiceInstanceTransport::Zenoh,
        }
    }
    pub fn validate(&self) -> Result<(), TransportContractError> {
        match self {
            Self::Http { catalog_url } => validate_http_url(catalog_url, true),
            Self::Zenoh { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderEndpoint {
    Http {
        catalog_url: String,
    },
    Zenoh {
        protocol_version: ProtocolVersion,
        route: InstanceRoute,
        route_revision: u64,
        /// Exactly one independently bound lane owns a catalog declaration.
        lane: LaneIdentity,
        catalog_digest: String,
    },
}
impl ProviderEndpoint {
    pub fn validate(&self) -> Result<(), TransportContractError> {
        match self {
            Self::Http { catalog_url } => validate_http_url(catalog_url, true),
            Self::Zenoh {
                route,
                route_revision,
                catalog_digest,
                ..
            } => {
                if *route_revision == 0 {
                    return Err(TransportContractError::InvalidIdentity);
                }
                route.catalog_key(catalog_digest)?;
                Ok(())
            }
        }
    }
}

/// Check a server-issued route against independently authenticated/issued
/// identities, not against values taken from that same endpoint.
pub fn validate_route_binding(
    route: &InstanceRoute,
    deployment: &str,
    application: &str,
    instance: &ServiceInstanceIdentity,
    role_generation: &str,
) -> Result<(), TransportContractError> {
    if route.deployment.as_str() != deployment
        || route.application_id.as_str() != application
        || route.instance_id.as_str() != instance.instance_id
        || route.base_generation.as_str() != instance.generation
        || route.role_generation.as_str() != role_generation
    {
        return Err(TransportContractError::WrongTarget);
    }
    Ok(())
}

impl ServiceEndpoint {
    pub fn transport(&self) -> ServiceInstanceTransport {
        match self {
            Self::Http { .. } => ServiceInstanceTransport::Http,
            Self::Zenoh { .. } => ServiceInstanceTransport::Zenoh,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn lane_capacity_is_bounded_and_legacy_requests_default_to_one() {
        let legacy = json!({"transport":"zenoh","protocol_version":"lingshu-zenoh/1"});
        assert_eq!(
            serde_json::from_value::<RequestedServiceEndpoint>(legacy.clone())
                .unwrap()
                .lane_count(),
            1
        );
        assert_eq!(
            serde_json::to_value(
                serde_json::from_value::<RequestedServiceEndpoint>(legacy.clone()).unwrap()
            )
            .unwrap(),
            legacy,
            "one-lane serialization retains the old request wire"
        );
        for count in [1, 2, 4] {
            let mut value = legacy.clone();
            value["lane_count"] = json!(count);
            assert_eq!(
                serde_json::from_value::<RequestedServiceEndpoint>(value)
                    .unwrap()
                    .lane_count(),
                count as usize
            );
        }
        for count in [0, 5, 255, 256, -1] {
            let mut value = legacy.clone();
            value["lane_count"] = json!(count);
            assert!(serde_json::from_value::<RequestedServiceEndpoint>(value).is_err());
        }
    }

    #[test]
    fn requests_cannot_supply_routes_lanes_or_unknown_protocols() {
        for value in [
            json!({"transport":"zenoh","protocol_version":"lingshu-zenoh/1","route":{}}),
            json!({"transport":"zenoh","protocol_version":"lingshu-zenoh/1","lanes":[]}),
            json!({"transport":"zenoh","protocol_version":"lingshu-zenoh/2"}),
            json!({"transport":"zenoh"}),
            json!({"transport":"http"}),
        ] {
            assert!(serde_json::from_value::<RequestedServiceEndpoint>(value).is_err());
        }
        assert!(serde_json::from_value::<RequestedProviderEndpoint>(json!({"transport":"zenoh","protocol_version":"lingshu-zenoh/1","catalog_url":"https://foreign.example"})).is_err());
        for version in [0, 1, 3] {
            assert!(serde_json::from_value::<EnrollmentVersion>(json!(version)).is_err());
        }
    }

    #[test]
    fn url_validation_keeps_http_callback_and_catalog_rules_distinct() {
        for url in [
            "https://sdk.example/invoke?route=one",
            "http://127.0.0.1/invoke",
            "http://[::1]/invoke",
            "http://localhost/invoke",
        ] {
            assert!(validate_http_url(url, false).is_ok(), "{url}");
        }
        for url in [
            "http://sdk.example/invoke",
            "http://10.0.0.1/invoke",
            "https://user:pw@sdk.example",
            "https://sdk.example/#secret",
            "zenoh://sdk/invoke",
        ] {
            assert!(validate_http_url(url, false).is_err(), "{url}");
        }
        assert!(validate_http_url("https://sdk.example/catalog?digest=a", true).is_err());
    }

    #[test]
    fn issued_route_cannot_change_scope_or_business_generation() {
        let id = |v| super::super::RouteIdentity::new(v).unwrap();
        let route = InstanceRoute {
            deployment: id("dev"),
            application_id: id("app"),
            instance_id: id("sdk"),
            base_generation: id("base"),
            role_generation: id("role"),
        };
        let instance = ServiceInstanceIdentity {
            instance_id: "sdk".into(),
            generation: "base".into(),
        };
        assert!(validate_route_binding(&route, "dev", "app", &instance, "role").is_ok());
        for (deployment, application, role) in [
            ("prod", "app", "role"),
            ("dev", "other", "role"),
            ("dev", "app", "old"),
        ] {
            assert!(
                validate_route_binding(&route, deployment, application, &instance, role).is_err()
            );
        }
        let replacement = ServiceInstanceIdentity {
            generation: "replacement".into(),
            ..instance
        };
        assert!(validate_route_binding(&route, "dev", "app", &replacement, "role").is_err());
    }
}
