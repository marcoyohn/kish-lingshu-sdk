//! HTTPS bootstrap values. Private keys and native host types never cross this boundary.
use super::{
    ExactRouteKey, PlatformControlRoute, ProtocolVersion, RouteIdentity, TransportContractError,
};
use crate::{ServiceInstanceIdentity, ServiceInstanceRegistration};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt};

pub const MAX_BOOTSTRAP_BYTES: usize = 32 * 1024;
pub const MAX_CSR_BYTES: usize = 16 * 1024;
pub const MAX_BOOTSTRAP_RESPONSE_BYTES: usize = 64 * 1024;
pub const MAX_CHANNEL_ENDPOINTS: usize = 8;
pub const CHANNEL_AUTHORIZATION_MS: i64 = 30_000;
pub const CHANNEL_CERTIFICATE_MS: i64 = 300_000;
pub const MAX_BOOTSTRAP_CLOCK_SKEW_MS: i64 = 5_000;

/// Native old-channel proof supplies all scope. Never accept a caller-selected
/// predecessor, instance, destination or certificate identity in this request.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelRotationRequest {
    pub csr_pem: String,
}
impl ChannelRotationRequest {
    pub fn validate(&self) -> Result<(), TransportContractError> {
        if self.csr_pem.is_empty() || self.csr_pem.len() > MAX_CSR_BYTES {
            return Err(TransportContractError::InvalidEnvelope);
        }
        Ok(())
    }
}

/// A control-only candidate, not evidence of role adoption or old retirement.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelRotationPreparation {
    pub previous_certificate_identity: RouteIdentity,
    pub candidate: ChannelBootstrapResponse,
}
impl ChannelRotationPreparation {
    pub fn validate(
        &self,
        previous: &ChannelBootstrapResponse,
    ) -> Result<(), TransportContractError> {
        let candidate = &self.candidate;
        if self.previous_certificate_identity != previous.certificate.certificate_identity
            || candidate.certificate.certificate_identity == self.previous_certificate_identity
            || candidate.certificate.message_public_key == previous.certificate.message_public_key
            || candidate.application_id != previous.application_id
            || candidate.instance != previous.instance
            || candidate.parent_credential_fingerprint != previous.parent_credential_fingerprint
            || candidate.deployment != previous.deployment
            || candidate.endpoints != previous.endpoints
            || candidate.control_route != previous.control_route
            || candidate.certificate.root_ca_pem != previous.certificate.root_ca_pem
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        Ok(())
    }
}

/// A TLS locator with no Zenoh configuration suffix, credentials or discovery.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TlsEndpoint(String);
impl TlsEndpoint {
    pub fn new(value: String) -> Result<Self, TransportContractError> {
        let address = value
            .strip_prefix("tls/")
            .ok_or(TransportContractError::InvalidIdentity)?;
        let url = url::Url::parse(&format!("tls://{address}"))
            .map_err(|_| TransportContractError::InvalidIdentity)?;
        if value.len() > 512
            || value.chars().any(char::is_whitespace)
            || url.host_str().is_none()
            || url.port().is_none_or(|p| p == 0)
            || !url.username().is_empty()
            || url.password().is_some()
            || !url.path().is_empty()
            || url.query().is_some()
            || url.fragment().is_some()
            || address.contains(['\\', '%'])
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for TlsEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TlsEndpoint(..)")
    }
}
impl TryFrom<String> for TlsEndpoint {
    type Error = TransportContractError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<TlsEndpoint> for String {
    fn from(value: TlsEndpoint) -> Self {
        value.0
    }
}

/// Explicit channel security profile. No connection failure changes this choice.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelTransport {
    #[default]
    Mtls,
    IntranetPlaintext,
}

/// Validated bootstrap locator; every endpoint in a response uses one profile.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ChannelEndpoint(String);
impl ChannelEndpoint {
    pub fn new(value: String) -> Result<Self, TransportContractError> {
        if value.starts_with("tls/") {
            TlsEndpoint::new(value).map(Into::into)
        } else if let Some(address) = value.strip_prefix("tcp/") {
            // Share the strict locator grammar, including IPv6 and nonzero ports.
            TlsEndpoint::new(format!("tls/{address}"))?;
            Ok(Self(value))
        } else {
            Err(TransportContractError::InvalidIdentity)
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn transport(&self) -> ChannelTransport {
        if self.0.starts_with("tls/") {
            ChannelTransport::Mtls
        } else {
            ChannelTransport::IntranetPlaintext
        }
    }
}
impl fmt::Debug for ChannelEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ChannelEndpoint")
            .field(&self.transport())
            .finish()
    }
}
impl TryFrom<String> for ChannelEndpoint {
    type Error = TransportContractError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<ChannelEndpoint> for String {
    fn from(value: ChannelEndpoint) -> Self {
        value.0
    }
}
impl From<TlsEndpoint> for ChannelEndpoint {
    fn from(value: TlsEndpoint) -> Self {
        Self(value.0)
    }
}

/// Verified CSR URI SAN binds the transport's proven RSA key to a signing identity.
pub const TCP_PUBLIC_KEY_SAN_PREFIX: &str = "urn:zenss:tcp-pubkey-sha256:";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelBootstrapRequest {
    pub protocol_version: ProtocolVersion,
    pub instance: ServiceInstanceRegistration,
    pub csr_pem: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_deployment: Option<RouteIdentity>,
}
impl fmt::Debug for ChannelBootstrapRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelBootstrapRequest")
            .field("instance", &self.instance)
            .finish_non_exhaustive()
    }
}
impl ChannelBootstrapRequest {
    pub fn validate(&self) -> Result<(), TransportContractError> {
        RouteIdentity::new(self.instance.instance_id.clone())?;
        RouteIdentity::new(self.instance.incarnation_id.clone())?;
        if let Some(generation) = &self.instance.generation {
            RouteIdentity::new(generation.clone())?;
        }
        if self.csr_pem.is_empty() || self.csr_pem.len() > MAX_CSR_BYTES {
            return Err(TransportContractError::InvalidEnvelope);
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelCertificate {
    pub certificate_identity: RouteIdentity,
    pub certificate_pem: String,
    pub root_ca_pem: String,
    pub message_public_key: String,
    pub expires_unix_ms: i64,
}
impl fmt::Debug for ChannelCertificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelCertificate")
            .field("expires_unix_ms", &self.expires_unix_ms)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelBootstrapResponse {
    pub protocol_version: ProtocolVersion,
    pub deployment: RouteIdentity,
    pub application_id: RouteIdentity,
    /// One-way identity of the authoritative parent key, never the API Key.
    pub parent_credential_fingerprint: String,
    pub instance: ServiceInstanceIdentity,
    pub endpoints: Vec<ChannelEndpoint>,
    pub control_route: PlatformControlRoute,
    pub certificate: ChannelCertificate,
    /// HTTPS-authenticated keys for the distinct zenoh-message signing domain.
    pub transport_trust: crate::service_auth::ServiceTrust,
    /// Server observation made before reading current authority. Durations are
    /// bounded independently of the client's wall clock.
    pub authorization_issued_unix_ms: i64,
    pub authorization_expires_unix_ms: i64,
}
impl fmt::Debug for ChannelBootstrapResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelBootstrapResponse")
            .field("application_id", &self.application_id)
            .field("instance", &self.instance)
            .field(
                "authorization_expires_unix_ms",
                &self.authorization_expires_unix_ms,
            )
            .finish_non_exhaustive()
    }
}
impl ChannelBootstrapResponse {
    /// Called before installing any received credential or creating a session.
    pub fn validate(
        &self,
        application: &str,
        request: &ChannelBootstrapRequest,
        now: i64,
    ) -> Result<ExactRouteKey, TransportContractError> {
        request.validate()?;
        if self.application_id.as_str() != application {
            return Err(TransportContractError::WrongApplication);
        }
        if request
            .expected_deployment
            .as_ref()
            .is_some_and(|d| d != &self.deployment)
            || self.control_route.deployment != self.deployment
            || self.instance.instance_id != request.instance.instance_id
            || request
                .instance
                .generation
                .as_ref()
                .is_some_and(|g| g != &self.instance.generation)
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        RouteIdentity::new(self.instance.generation.clone())?;
        if self.endpoints.is_empty()
            || self.endpoints.len() > MAX_CHANNEL_ENDPOINTS
            || self
                .endpoints
                .iter()
                .any(|e| e.transport() != self.endpoints[0].transport())
            || self.endpoints.iter().collect::<HashSet<_>>().len() != self.endpoints.len()
            || self.certificate.certificate_pem.is_empty()
            || self.certificate.certificate_pem.len() > 16 * 1024
            || self.certificate.root_ca_pem.is_empty()
            || self.certificate.root_ca_pem.len() > 16 * 1024
            || self.certificate.message_public_key.len() != 64
            || self.parent_credential_fingerprint.len() != 64
            || !self
                .parent_credential_fingerprint
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || !self
                .certificate
                .message_public_key
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(TransportContractError::InvalidEnvelope);
        }
        if self.transport_trust.app_id != application
            || self.transport_trust.expires_at <= now / 1000
            || self.transport_trust.keys.is_empty()
            || self.transport_trust.keys.len() > 8
            || !self
                .transport_trust
                .keys
                .iter()
                .any(|key| key.not_before <= now / 1000 && key.not_after > now / 1000)
            || self.transport_trust.keys.iter().any(|key| {
                key.key_id.is_empty()
                    || key.public_key.is_empty()
                    || key.not_after <= key.not_before
            })
        {
            return Err(TransportContractError::InvalidEnvelope);
        }
        let auth_lifetime = self
            .authorization_expires_unix_ms
            .checked_sub(self.authorization_issued_unix_ms)
            .ok_or(TransportContractError::Expired)?;
        let cert_lifetime = self
            .certificate
            .expires_unix_ms
            .checked_sub(self.authorization_issued_unix_ms)
            .ok_or(TransportContractError::Expired)?;
        if !(1..=CHANNEL_AUTHORIZATION_MS).contains(&auth_lifetime)
            || !(1..=CHANNEL_CERTIFICATE_MS).contains(&cert_lifetime)
            || self.authorization_issued_unix_ms < 0
            || self.authorization_issued_unix_ms > now.saturating_add(MAX_BOOTSTRAP_CLOCK_SKEW_MS)
            || self.authorization_expires_unix_ms <= now
            || self.authorization_expires_unix_ms > self.certificate.expires_unix_ms
        {
            return Err(TransportContractError::Expired);
        }
        self.control_route.key()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn channel_endpoints_keep_tls_strict_and_reject_mixed_profiles() {
        assert!(TlsEndpoint::new("tcp/router:7447".into()).is_err());
        for invalid in [
            "tcp/router",
            "tcp/router:0",
            "tcp/user@router:7447",
            "tcp/router:7447?x=y",
            "udp/router:7447",
        ] {
            assert!(ChannelEndpoint::new(invalid.into()).is_err());
        }
        let tcp = ChannelEndpoint::new("tcp/[::1]:7447".into()).unwrap();
        assert_eq!(tcp.transport(), ChannelTransport::IntranetPlaintext);
        let mut reply = response();
        reply.endpoints.push(tcp.clone());
        assert!(reply.validate("app", &request(), 1).is_err());
        reply.endpoints = vec![tcp];
        assert!(reply.validate("app", &request(), 1).is_ok());
    }
    #[test]
    fn tls_endpoints_reject_discovery_credentials_and_transport_options() {
        for invalid in [
            "tcp/a:7447",
            "tls/a",
            "tls/a:0",
            "tls/a:7447/",
            "tls/user@a:7447",
            "tls/a:7447?x=y",
            "tls/a:7447#x=y",
            "tls/a%2fb:7447",
            "tls/ a:7447",
            "tls/a\\b:7447",
        ] {
            assert!(TlsEndpoint::new(invalid.into()).is_err(), "{invalid}");
        }
        for valid in [
            "tls/router.example:7447",
            "tls/127.0.0.1:7447",
            "tls/[::1]:7447",
        ] {
            assert!(TlsEndpoint::new(valid.into()).is_ok(), "{valid}");
        }
    }
    fn request() -> ChannelBootstrapRequest {
        ChannelBootstrapRequest {
            protocol_version: ProtocolVersion::V1,
            instance: ServiceInstanceRegistration {
                instance_id: "sdk".into(),
                incarnation_id: "boot".into(),
                generation: None,
            },
            csr_pem: "PRIVATE-CSR-CONTENT".into(),
            expected_deployment: None,
        }
    }
    fn response() -> ChannelBootstrapResponse {
        ChannelBootstrapResponse {
            protocol_version: ProtocolVersion::V1,
            deployment: RouteIdentity::new("dev").unwrap(),
            application_id: RouteIdentity::new("app").unwrap(),
            parent_credential_fingerprint: "b".repeat(64),
            instance: ServiceInstanceIdentity {
                instance_id: "sdk".into(),
                generation: "base".into(),
            },
            endpoints: vec![ChannelEndpoint::new("tls/router.example:7447".into()).unwrap()],
            control_route: PlatformControlRoute {
                deployment: RouteIdentity::new("dev").unwrap(),
                platform_node: RouteIdentity::new("node").unwrap(),
                boot_epoch: RouteIdentity::new("boot").unwrap(),
            },
            certificate: ChannelCertificate {
                certificate_identity: RouteIdentity::new("cn").unwrap(),
                certificate_pem: "CERT-CONTENT".into(),
                root_ca_pem: "ROOT-CONTENT".into(),
                message_public_key: "a".repeat(64),
                expires_unix_ms: 300_000,
            },
            transport_trust: crate::service_auth::ServiceTrust {
                app_id: "app".into(),
                keys: vec![crate::service_auth::ServicePublicKey {
                    key_id: "key".into(),
                    public_key: "public-key".into(),
                    not_before: -100,
                    not_after: 300,
                }],
                expires_at: 300,
            },
            authorization_expires_unix_ms: 30_000,
            authorization_issued_unix_ms: 0,
        }
    }
    #[test]
    fn rotation_preparation_rejects_scope_changes_reused_keys_and_unrequested_fields() {
        let old = response();
        let mut candidate = old.clone();
        candidate.certificate.certificate_identity = RouteIdentity::new("next").unwrap();
        candidate.certificate.message_public_key = "b".repeat(64);
        let valid = ChannelRotationPreparation {
            previous_certificate_identity: old.certificate.certificate_identity.clone(),
            candidate,
        };
        valid.validate(&old).unwrap();
        for field in 0..8 {
            let mut bad = valid.clone();
            match field {
                0 => bad.previous_certificate_identity = RouteIdentity::new("foreign").unwrap(),
                1 => {
                    bad.candidate.certificate.message_public_key =
                        old.certificate.message_public_key.clone()
                }
                2 => bad.candidate.instance.generation = "foreign".into(),
                3 => bad.candidate.application_id = RouteIdentity::new("foreign").unwrap(),
                4 => bad.candidate.parent_credential_fingerprint = "c".repeat(64),
                5 => {
                    bad.candidate.control_route.boot_epoch = RouteIdentity::new("foreign").unwrap()
                }
                6 => {
                    bad.candidate.endpoints =
                        vec![ChannelEndpoint::new("tls/foreign:7447".into()).unwrap()]
                }
                _ => bad.candidate.certificate.root_ca_pem = "foreign".into(),
            }
            assert!(bad.validate(&old).is_err());
        }
        assert!(serde_json::from_str::<ChannelRotationRequest>(
            r#"{"csr_pem":"csr","certificate_identity":"chosen"}"#
        )
        .is_err());
        assert!(ChannelRotationRequest {
            csr_pem: String::new()
        }
        .validate()
        .is_err());
        assert!(ChannelRotationRequest {
            csr_pem: "x".repeat(MAX_CSR_BYTES + 1)
        }
        .validate()
        .is_err());
    }
    #[test]
    fn response_is_bound_to_application_instance_deployment_and_hard_deadlines() {
        let request = request();
        let response = response();
        assert!(response.validate("app", &request, 0).is_ok());
        assert!(response.validate("app", &request, -1000).is_ok());
        assert!(response.validate("app", &request, -5001).is_err());
        assert!(response.validate("other", &request, 0).is_err());
        assert!(response.validate("app", &request, 30_000).is_err());
        let mut changed = response.clone();
        changed.transport_trust.app_id = "other-app".into();
        assert!(changed.validate("app", &request, 0).is_err());
        let mut changed = response.clone();
        changed.authorization_expires_unix_ms += 1;
        assert!(changed.validate("app", &request, 0).is_err());
        let mut changed = response.clone();
        changed.instance.instance_id = "other".into();
        assert!(changed.validate("app", &request, 0).is_err());
        let mut changed = response.clone();
        changed.control_route.deployment = RouteIdentity::new("other").unwrap();
        assert!(changed.validate("app", &request, 0).is_err());
        let mut changed = response.clone();
        changed.endpoints.push(changed.endpoints[0].clone());
        assert!(changed.validate("app", &request, 0).is_err());
        let mut request = request;
        request.instance.generation = Some("old".into());
        assert!(response.validate("app", &request, 0).is_err());
    }
    #[test]
    fn authorization_observation_binds_certificate_base_and_finite_window() {
        use super::super::channel::ChannelAuthorization;
        let initial = response();
        let authority = ChannelAuthorization {
            connection: None,
            application_id: initial.application_id.clone(),
            instance: initial.instance.clone(),
            certificate_identity: initial.certificate.certificate_identity.clone(),
            authorization_issued_unix_ms: 10_000,
            authorization_expires_unix_ms: 40_000,
        };
        assert!(authority.validate(&initial, 10_000).is_ok());
        assert!(authority.validate(&initial, 40_000).is_err());
        let mut changed = authority.clone();
        changed.certificate_identity = RouteIdentity::new("foreign-cert").unwrap();
        assert!(changed.validate(&initial, 10_000).is_err());
        let mut changed = authority.clone();
        changed.instance.generation = "other-base".into();
        assert!(changed.validate(&initial, 10_000).is_err());
        let mut changed = authority;
        changed.authorization_expires_unix_ms += 1;
        assert!(changed.validate(&initial, 10_000).is_err());
    }
    #[test]
    fn rotation_finalization_binds_lineage_current_identity_and_finite_deadline() {
        use super::super::channel::{ChannelAuthorization, ChannelRotationFinalization};
        let initial = response();
        let predecessor = RouteIdentity::new("old-certificate").unwrap();
        let receipt = ChannelRotationFinalization {
            previous_certificate_identity: predecessor.clone(),
            authorization: ChannelAuthorization {
                connection: None,
                application_id: initial.application_id.clone(),
                instance: initial.instance.clone(),
                certificate_identity: initial.certificate.certificate_identity.clone(),
                authorization_issued_unix_ms: 10_000,
                authorization_expires_unix_ms: 40_000,
            },
        };
        assert!(receipt.validate(&initial, &predecessor, 10_000).is_ok());
        assert!(receipt
            .validate(&initial, &RouteIdentity::new("foreign").unwrap(), 10_000)
            .is_err());
        assert!(receipt.validate(&initial, &predecessor, 40_000).is_err());
        let mut changed = receipt.clone();
        changed.authorization.certificate_identity = predecessor.clone();
        assert!(changed.validate(&initial, &predecessor, 10_000).is_err());
        let mut changed = receipt.clone();
        changed.authorization.instance.generation = "foreign-base".into();
        assert!(changed.validate(&initial, &predecessor, 10_000).is_err());
        let mut changed = receipt.clone();
        changed.authorization.authorization_expires_unix_ms += 1;
        assert!(changed.validate(&initial, &predecessor, 10_000).is_err());
        let mut wire = serde_json::to_value(&receipt).unwrap();
        wire["roles"] = serde_json::json!([]);
        assert!(serde_json::from_value::<ChannelRotationFinalization>(wire).is_err());
    }
    #[test]
    fn bootstrap_wire_is_versioned_closed_and_does_not_debug_credentials() {
        let request = request();
        let mut wire = serde_json::to_value(&request).unwrap();
        wire["private_key_pem"] = "never accepted".into();
        assert!(serde_json::from_value::<ChannelBootstrapRequest>(wire).is_err());
        let mut wire = serde_json::to_value(&request).unwrap();
        wire["protocol_version"] = "lingshu-zenoh/2".into();
        assert!(serde_json::from_value::<ChannelBootstrapRequest>(wire).is_err());
        assert!(!format!("{request:?}").contains("PRIVATE-CSR-CONTENT"));
        let response = response();
        assert!(!format!("{response:?}").contains("CERT-CONTENT"));
        assert!(!format!("{response:?}").contains("ROOT-CONTENT"));
    }
}
