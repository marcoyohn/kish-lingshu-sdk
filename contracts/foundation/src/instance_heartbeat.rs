//! Bounded transport envelope for renewing the roles of one provider instance.
//! Role sessions remain owned and validated by their respective domain contracts.
use crate::ServiceInstanceIdentity;
use serde::{Deserialize, Serialize};

pub const INSTANCE_HEARTBEAT_PATH: &str = "api/user/services/v1/instance-sessions/heartbeat";
pub const MAX_INSTANCE_HEARTBEAT_ROLES: usize = 1024;
pub const MAX_INSTANCE_HEARTBEAT_BYTES: usize = 20 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceRoleKind {
    Event,
    Call,
    Provider,
}

// Deliberately no Debug: these envelopes contain renewable credentials.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceHeartbeatRole {
    pub id: u64,
    pub kind: InstanceRoleKind,
    pub credential: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceHeartbeatRequest {
    pub instance: ServiceInstanceIdentity,
    pub roles: Vec<InstanceHeartbeatRole>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceHeartbeatResult {
    pub id: u64,
    /// 200 carries the original role-specific session; 403/404/409 end only this
    /// role. 503 preserves its previous finite lease without claiming renewal.
    pub status: u16,
    pub session: Option<serde_json::Value>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceHeartbeatResponse {
    pub instance: ServiceInstanceIdentity,
    pub roles: Vec<InstanceHeartbeatResult>,
}
