//! Bounded wire values. No Zenoh types, sockets, role authority or persistence.
use std::{collections::HashSet, fmt};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

pub mod bootstrap;
pub mod budget;
pub mod channel;
pub mod enrollment;
pub mod probe;

pub const PROTOCOL_VERSION: &str = "lingshu-zenoh/1";
pub const MAX_DATA_LANES: usize = 4;
pub const MAX_KEY_BYTES: usize = 2048;
pub const MAX_ENVELOPE_OVERHEAD_BYTES: usize = 32 * 1024;
pub const MAX_CONTROL_PAYLOAD_BYTES: usize = 20 * 1024 * 1024;
pub const MAX_CATALOG_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_BUSINESS_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const MAX_PROOF_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TransportContractError {
    #[error("invalid transport identity or route")]
    InvalidIdentity,
    #[error("unsupported transport protocol")]
    UnsupportedProtocol,
    #[error("invalid or oversized transport envelope")]
    InvalidEnvelope,
    #[error("transport request deadline expired")]
    Expired,
    #[error("transport target does not match the expected route")]
    WrongTarget,
    #[error("transport application does not match the authenticated scope")]
    WrongApplication,
}

/// Raw identity and its canonical, injective lowercase-hex key segment.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RouteIdentity(String);

impl RouteIdentity {
    pub fn new(value: impl Into<String>) -> Result<Self, TransportContractError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || value.chars().any(|c| {
                c.is_control()
                    || c.is_whitespace()
                    || matches!(c, '/' | '\\' | '*' | '$' | '?' | '#')
            })
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn key_segment(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(2 * self.0.len());
        for b in self.0.bytes() {
            encoded.push(HEX[(b >> 4) as usize] as char);
            encoded.push(HEX[(b & 15) as usize] as char);
        }
        encoded
    }
}

impl TryFrom<String> for RouteIdentity {
    type Error = TransportContractError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<RouteIdentity> for String {
    fn from(value: RouteIdentity) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct DataLaneId(u8);

impl DataLaneId {
    pub fn new(value: u8) -> Result<Self, TransportContractError> {
        if usize::from(value) >= MAX_DATA_LANES {
            return Err(TransportContractError::InvalidIdentity);
        }
        Ok(Self(value))
    }
    pub fn index(self) -> u8 {
        self.0
    }
}
impl TryFrom<u8> for DataLaneId {
    type Error = TransportContractError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<DataLaneId> for u8 {
    fn from(value: DataLaneId) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolVersion {
    #[serde(rename = "lingshu-zenoh/1")]
    V1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaneIdentity {
    pub lane: DataLaneId,
    pub epoch: RouteIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceRoute {
    pub deployment: RouteIdentity,
    pub application_id: RouteIdentity,
    pub instance_id: RouteIdentity,
    pub base_generation: RouteIdentity,
    pub role_generation: RouteIdentity,
}

impl InstanceRoute {
    fn base_key(&self) -> String {
        format!(
            "ls/v1/{}/apps/{}/instances/{}/{}",
            self.deployment.key_segment(),
            self.application_id.key_segment(),
            self.instance_id.key_segment(),
            self.base_generation.key_segment(),
        )
    }

    pub fn invoke_key(&self, lane: &LaneIdentity) -> Result<ExactRouteKey, TransportContractError> {
        ExactRouteKey::new(format!(
            "{}/roles/{}/lanes/{}/{}/invoke",
            self.base_key(),
            self.role_generation.key_segment(),
            lane.lane.index(),
            lane.epoch.key_segment(),
        ))
    }

    pub fn consumer_presence_key(
        &self,
        lane: &LaneIdentity,
        owner: &PlatformControlRoute,
    ) -> Result<ExactRouteKey, TransportContractError> {
        if owner.deployment != self.deployment || lane.lane.index() != 0 {
            return Err(TransportContractError::WrongTarget);
        }
        ExactRouteKey::new(format!(
            "{}/presence/{}/{}",
            self.invoke_key(lane)?.as_str(),
            owner.platform_node.key_segment(),
            owner.boot_epoch.key_segment()
        ))
    }

    pub fn catalog_key(&self, digest: &str) -> Result<ExactRouteKey, TransportContractError> {
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        ExactRouteKey::new(format!("{}/catalog/{digest}", self.base_key()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlatformControlRoute {
    pub deployment: RouteIdentity,
    pub platform_node: RouteIdentity,
    pub boot_epoch: RouteIdentity,
}

/// Logical call reporting address. No Router node, lane, certificate or secret
/// belongs to this identity. Address construction does not grant authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallReportRoute {
    pub deployment: RouteIdentity,
    pub application_id: RouteIdentity,
    pub call_id: RouteIdentity,
    pub attempt: u32,
    pub epoch: RouteIdentity,
}
impl CallReportRoute {
    pub fn key(&self) -> Result<ExactRouteKey, TransportContractError> {
        if self.attempt == 0 {
            return Err(TransportContractError::InvalidIdentity);
        }
        ExactRouteKey::new(format!(
            "ls/v1/{}/apps/{}/calls/{}/{}/{}/report",
            self.deployment.key_segment(),
            self.application_id.key_segment(),
            self.call_id.key_segment(),
            self.attempt,
            self.epoch.key_segment(),
        ))
    }
}

impl PlatformControlRoute {
    pub fn consumer_metadata_key(&self) -> Result<ExactRouteKey, TransportContractError> {
        ExactRouteKey::new(format!(
            "ls/v1/{}/platform/{}/{}/consumers/metadata",
            self.deployment.key_segment(),
            self.platform_node.key_segment(),
            self.boot_epoch.key_segment()
        ))
    }
    pub fn publication_key(&self) -> Result<ExactRouteKey, TransportContractError> {
        ExactRouteKey::new(format!(
            "ls/v1/{}/platform/{}/{}/publish",
            self.deployment.key_segment(),
            self.platform_node.key_segment(),
            self.boot_epoch.key_segment(),
        ))
    }
    pub fn key(&self) -> Result<ExactRouteKey, TransportContractError> {
        ExactRouteKey::new(format!(
            "ls/v1/{}/platform/{}/{}/control",
            self.deployment.key_segment(),
            self.platform_node.key_segment(),
            self.boot_epoch.key_segment(),
        ))
    }
}

/// Exact addressing is a value constraint, never an authorization decision.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ExactRouteKey(String);

impl ExactRouteKey {
    pub fn new(value: impl Into<String>) -> Result<Self, TransportContractError> {
        let value = value.into();
        if !value.starts_with("ls/v1/")
            || value.len() > MAX_KEY_BYTES
            || value.split('/').any(str::is_empty)
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'_' | b'-'))
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for ExactRouteKey {
    type Error = TransportContractError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<ExactRouteKey> for String {
    fn from(value: ExactRouteKey) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServiceEndpoint {
    Http {
        invocation_url: String,
    },
    Zenoh {
        protocol_version: ProtocolVersion,
        route: InstanceRoute,
        route_revision: u64,
        lanes: Vec<LaneIdentity>,
    },
}

impl ServiceEndpoint {
    /// Required before accepting a deserialized endpoint or writing a projection.
    /// TLS credentials, authority and readiness are deliberately outside this DTO.
    pub fn validate(&self) -> Result<(), TransportContractError> {
        match self {
            Self::Http { invocation_url } => {
                if invocation_url.len() > MAX_KEY_BYTES {
                    return Err(TransportContractError::InvalidIdentity);
                }
                let url = url::Url::parse(invocation_url)
                    .map_err(|_| TransportContractError::InvalidIdentity)?;
                if !matches!(url.scheme(), "http" | "https")
                    || url.host_str().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.fragment().is_some()
                {
                    return Err(TransportContractError::InvalidIdentity);
                }
            }
            Self::Zenoh {
                route,
                route_revision,
                lanes,
                ..
            } => {
                if *route_revision == 0 || lanes.is_empty() || lanes.len() > MAX_DATA_LANES {
                    return Err(TransportContractError::InvalidIdentity);
                }
                let mut ids = HashSet::new();
                for lane in lanes {
                    if !ids.insert(lane.lane) {
                        return Err(TransportContractError::InvalidIdentity);
                    }
                    route.invoke_key(lane)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    ChannelAuthorization,
    PrepareChannelRotation,
    AdoptChannelRole,
    FinalizeChannelRotation,
    Register,
    RenewRoles,
    Deregister,
    BindLane,
    /// Product-internal direct Router report installation; never a Client grant.
    InstallCallReport,
    CatalogRead,
    InvokeCall,
    CancelCall,
    CallHeartbeat,
    CompleteCall,
    InvokeEvent,
    CompleteEvent,
    EventHeartbeat,
    PublishEvent,
}

impl MessageKind {
    pub fn payload_limit(self) -> usize {
        match self {
            Self::Register | Self::RenewRoles => MAX_CONTROL_PAYLOAD_BYTES,
            Self::CatalogRead => MAX_CATALOG_PAYLOAD_BYTES,
            _ => MAX_BUSINESS_PAYLOAD_BYTES,
        }
    }
}

/// Submission evidence is separate from handler outcome and durable acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionDisposition {
    NotSubmitted,
    RejectedBeforeAcceptance,
    Accepted,
    OutcomeUnknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportFailureKind {
    Unauthorized,
    ExpiredAuthority,
    StaleGeneration,
    RouteUnavailable,
    Overloaded,
    ProtocolRejected,
    InvalidMessage,
    OutcomeUnknown,
}

#[derive(Clone, Serialize)]
pub struct TransportEnvelope {
    pub protocol_version: ProtocolVersion,
    pub kind: MessageKind,
    pub request_id: RouteIdentity,
    pub application_id: RouteIdentity,
    pub target: ExactRouteKey,
    pub deadline_unix_ms: i64,
    pub proof: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_parent: Option<String>,
    pub payload: Box<RawValue>,
}

impl fmt::Debug for TransportEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportEnvelope")
            .field("kind", &self.kind)
            .field("payload_bytes", &self.payload.get().len())
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BorrowedEnvelope<'a> {
    protocol_version: &'a str,
    kind: MessageKind,
    request_id: RouteIdentity,
    application_id: RouteIdentity,
    target: ExactRouteKey,
    deadline_unix_ms: i64,
    proof: String,
    #[serde(default)]
    trace_parent: Option<String>,
    #[serde(borrow)]
    payload: &'a RawValue,
}

impl TransportEnvelope {
    pub fn decode(
        bytes: &[u8],
        expected_target: &ExactRouteKey,
        expected_application: &RouteIdentity,
        now_unix_ms: i64,
    ) -> Result<Self, TransportContractError> {
        if bytes.len() > MAX_CONTROL_PAYLOAD_BYTES + MAX_ENVELOPE_OVERHEAD_BYTES {
            return Err(TransportContractError::InvalidEnvelope);
        }
        // Inspect raw body size before allocating or decoding the business payload.
        let value: BorrowedEnvelope<'_> =
            serde_json::from_slice(bytes).map_err(|_| TransportContractError::InvalidEnvelope)?;
        if value.protocol_version != PROTOCOL_VERSION {
            return Err(TransportContractError::UnsupportedProtocol);
        }
        if &value.application_id != expected_application {
            return Err(TransportContractError::WrongApplication);
        }
        validate_trace(value.trace_parent.as_deref())?;
        validate_envelope(
            value.kind,
            value.payload.get().len(),
            bytes.len(),
            value.proof.len(),
            value.deadline_unix_ms,
            &value.target,
            expected_target,
            now_unix_ms,
        )?;
        Ok(Self {
            protocol_version: ProtocolVersion::V1,
            kind: value.kind,
            request_id: value.request_id,
            application_id: value.application_id,
            target: value.target,
            deadline_unix_ms: value.deadline_unix_ms,
            proof: value.proof,
            trace_parent: value.trace_parent,
            payload: value.payload.to_owned(),
        })
    }

    pub fn encode(&self, now_unix_ms: i64) -> Result<Vec<u8>, TransportContractError> {
        validate_trace(self.trace_parent.as_deref())?;
        validate_envelope(
            self.kind,
            self.payload.get().len(),
            self.payload.get().len(),
            self.proof.len(),
            self.deadline_unix_ms,
            &self.target,
            &self.target,
            now_unix_ms,
        )?;
        let bytes =
            serde_json::to_vec(self).map_err(|_| TransportContractError::InvalidEnvelope)?;
        validate_envelope(
            self.kind,
            self.payload.get().len(),
            bytes.len(),
            self.proof.len(),
            self.deadline_unix_ms,
            &self.target,
            &self.target,
            now_unix_ms,
        )?;
        Ok(bytes)
    }
}

fn validate_trace(trace: Option<&str>) -> Result<(), TransportContractError> {
    if trace.is_some_and(|t| t.is_empty() || t.len() > 256 || t.chars().any(|c| c.is_control())) {
        return Err(TransportContractError::InvalidEnvelope);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_envelope(
    kind: MessageKind,
    payload_bytes: usize,
    total_bytes: usize,
    proof_bytes: usize,
    deadline: i64,
    target: &ExactRouteKey,
    expected: &ExactRouteKey,
    now: i64,
) -> Result<(), TransportContractError> {
    if payload_bytes > kind.payload_limit()
        || total_bytes.saturating_sub(payload_bytes) > MAX_ENVELOPE_OVERHEAD_BYTES
        || proof_bytes == 0
        || proof_bytes > MAX_PROOF_BYTES
    {
        return Err(TransportContractError::InvalidEnvelope);
    }
    if now < 0 || deadline <= now {
        return Err(TransportContractError::Expired);
    }
    if target != expected {
        return Err(TransportContractError::WrongTarget);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(value: &str) -> RouteIdentity {
        RouteIdentity::new(value).unwrap()
    }
    fn route() -> InstanceRoute {
        InstanceRoute {
            deployment: id("dev"),
            application_id: id("app-a"),
            instance_id: id("sdk-a"),
            base_generation: id("base-1"),
            role_generation: id("role-1"),
        }
    }
    fn lane(index: u8, epoch: &str) -> LaneIdentity {
        LaneIdentity {
            lane: DataLaneId::new(index).unwrap(),
            epoch: id(epoch),
        }
    }
    fn envelope() -> TransportEnvelope {
        TransportEnvelope {
            protocol_version: ProtocolVersion::V1,
            kind: MessageKind::InvokeCall,
            request_id: id("request-1"),
            application_id: id("app-a"),
            target: route().invoke_key(&lane(0, "epoch-1")).unwrap(),
            deadline_unix_ms: 2000,
            proof: "fixture-proof".into(),
            trace_parent: None,
            payload: RawValue::from_string(r#"{"input":1}"#.into()).unwrap(),
        }
    }

    #[test]
    fn canonical_keys_are_injective_and_fence_lanes_and_generations() {
        assert_eq!(id("app-a").key_segment(), "6170702d61");
        for bad in ["", "a/b", "a*", "a$", "a?", "a#", "a\\b", " a", "a\n"] {
            assert!(RouteIdentity::new(bad).is_err(), "{bad:?}");
            assert!(serde_json::from_value::<RouteIdentity>(serde_json::json!(bad)).is_err());
        }
        let first = route().invoke_key(&lane(0, "epoch-1")).unwrap();
        assert_ne!(first, route().invoke_key(&lane(1, "epoch-1")).unwrap());
        assert_ne!(first, route().invoke_key(&lane(0, "epoch-2")).unwrap());
        let mut other = route();
        other.base_generation = id("base-2");
        assert_ne!(first, other.invoke_key(&lane(0, "epoch-1")).unwrap());
        assert!(DataLaneId::new(4).is_err());
        assert_ne!(id("A").key_segment(), id("a").key_segment());
        assert_ne!(id("é").key_segment(), id("c3a9").key_segment());
    }

    #[test]
    fn consumer_presence_binds_metadata_owner_and_first_lane() {
        let r = route();
        let owner = PlatformControlRoute {
            deployment: r.deployment.clone(),
            platform_node: id("node"),
            boot_epoch: id("boot"),
        };
        let token = r.consumer_presence_key(&lane(0, "epoch"), &owner).unwrap();
        let mut next = owner.clone();
        next.boot_epoch = id("next");
        assert_ne!(
            token,
            r.consumer_presence_key(&lane(0, "epoch"), &next).unwrap()
        );
        assert_ne!(
            owner.consumer_metadata_key().unwrap(),
            next.consumer_metadata_key().unwrap()
        );
        assert!(r.consumer_presence_key(&lane(1, "epoch"), &owner).is_err());
        next.deployment = id("foreign");
        assert!(r.consumer_presence_key(&lane(0, "epoch"), &next).is_err());
    }

    #[test]
    fn endpoint_rejects_duplicates_and_unknown_protocol_without_http_fallback() {
        let mut endpoint = ServiceEndpoint::Zenoh {
            protocol_version: ProtocolVersion::V1,
            route: route(),
            route_revision: 1,
            lanes: vec![lane(0, "one")],
        };
        assert!(endpoint.validate().is_ok());
        if let ServiceEndpoint::Zenoh { lanes, .. } = &mut endpoint {
            lanes.push(lane(0, "two"));
        }
        assert!(endpoint.validate().is_err());
        let mut value = serde_json::to_value(&endpoint).unwrap();
        value["protocol_version"] = serde_json::json!("lingshu-zenoh/2");
        assert!(serde_json::from_value::<ServiceEndpoint>(value).is_err());
        let legacy = serde_json::json!({"invocation_url":"https://sdk.example/invoke"});
        assert!(serde_json::from_value::<ServiceEndpoint>(legacy).is_err());
        assert!(ServiceEndpoint::Http {
            invocation_url: "https://sdk.example/invoke".into()
        }
        .validate()
        .is_ok());
        assert!(ServiceEndpoint::Http {
            invocation_url: "zenoh://sdk/invoke".into()
        }
        .validate()
        .is_err());
    }

    #[test]
    fn decoding_preserves_bytes_and_checks_target_deadline_and_family_limits() {
        let mut value = envelope();
        let bytes = value.encode(1000).unwrap();
        let decoded =
            TransportEnvelope::decode(&bytes, &value.target, &value.application_id, 1000).unwrap();
        assert_eq!(decoded.payload.get(), value.payload.get());
        assert!(matches!(
            TransportEnvelope::decode(&bytes, &value.target, &value.application_id, 2000),
            Err(TransportContractError::Expired)
        ));
        assert!(matches!(
            TransportEnvelope::decode(&bytes, &value.target, &id("app-b"), 1000),
            Err(TransportContractError::WrongApplication)
        ));
        let mut unknown: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        unknown["protocol_version"] = serde_json::json!("lingshu-zenoh/2");
        assert!(matches!(
            TransportEnvelope::decode(
                &serde_json::to_vec(&unknown).unwrap(),
                &value.target,
                &value.application_id,
                1000
            ),
            Err(TransportContractError::UnsupportedProtocol)
        ));
        let foreign = route().invoke_key(&lane(0, "other")).unwrap();
        assert!(matches!(
            TransportEnvelope::decode(&bytes, &foreign, &value.application_id, 1000),
            Err(TransportContractError::WrongTarget)
        ));
        value.payload =
            RawValue::from_string(format!("\"{}\"", "x".repeat(MAX_BUSINESS_PAYLOAD_BYTES)))
                .unwrap();
        // Raw wire bytes, including JSON escaping/quotes, determine the budget.
        assert!(value.encode(1000).is_err());
        let malicious = serde_json::to_vec(&value).unwrap();
        assert!(
            TransportEnvelope::decode(&malicious, &value.target, &value.application_id, 1000)
                .is_err()
        );
        assert!(TransportEnvelope::decode(
            &vec![b' '; MAX_CONTROL_PAYLOAD_BYTES + MAX_ENVELOPE_OVERHEAD_BYTES + 1],
            &value.target,
            &value.application_id,
            1000
        )
        .is_err());
    }

    #[test]
    fn proof_and_payload_are_redacted_from_debug_and_unknown_fields_fail() {
        let envelope = envelope();
        let debug = format!("{envelope:?}");
        assert!(!debug.contains("fixture-proof") && !debug.contains("input"));
        let mut value = serde_json::to_value(&envelope).unwrap();
        value["root_api_key"] = serde_json::json!("must-not-be-on-wire");
        assert!(TransportEnvelope::decode(
            &serde_json::to_vec(&value).unwrap(),
            &envelope.target,
            &envelope.application_id,
            1000
        )
        .is_err());
        assert!(ExactRouteKey::new("ls/v1/dev/**").is_err());
    }
}
