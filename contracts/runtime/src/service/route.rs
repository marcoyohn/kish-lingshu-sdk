//! A trusted transport adapter verifies the actual remote route proof. Merely
//! matching a declaration or receiving liveliness is never a successful probe.
use async_trait::async_trait;
use kish_lingshu_foundation_contract::service_transport::{
    ExactRouteKey, LaneIdentity, RouteIdentity, TransportFailureKind,
};

#[derive(Clone, Debug)]
pub struct RouteProbeRequest {
    pub target: ExactRouteKey,
    pub challenge: RouteIdentity,
    pub lane: LaneIdentity,
    pub route_revision: u64,
    pub deadline_unix_ms: i64,
}

#[async_trait]
pub trait AuthorizedRouteProbe: Send + Sync {
    /// Success requires exactly one authenticated response from the authorized
    /// instance/base/role/lane epoch, bound to this challenge, exact target and
    /// deadline. Unsigned, duplicate and foreign responses must fail. The
    /// adapter must not invoke a business handler while checking its route.
    async fn verify(&self, request: RouteProbeRequest) -> Result<(), TransportFailureKind>;
}
