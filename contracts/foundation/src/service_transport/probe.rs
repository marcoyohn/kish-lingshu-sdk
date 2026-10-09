//! A signed route handshake proves reachability only. Its caller still checks
//! current root/base/role authority before confirming readiness.
use super::{LaneIdentity, RouteIdentity};
use serde::{Deserialize, Serialize};

pub const MAX_ROUTE_PROBE_BYTES: usize = 32 * 1024;
pub const ROUTE_PROBE_TIMEOUT_MS: i64 = 5_000;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteProbeChallenge {
    pub challenge: RouteIdentity,
    pub lane: LaneIdentity,
    pub route_revision: u64,
}
