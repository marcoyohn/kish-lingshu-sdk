//! Call liveness protocol. This is independent of registration and legacy progress.
use super::ServiceInstanceTarget;
use serde::{Deserialize, Serialize};

pub const CALL_HEARTBEAT_VERSION: u32 = 1;
pub const CALL_HEARTBEAT_INTERVAL_MS: u64 = 10_000;
pub const CALL_LIVENESS_MS: i64 = 33_000;
pub const CALL_ACCEPTANCE_MS: i64 = 10_000;
pub const CALL_DELIVERY_MS: i64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallHeartbeatPolicy {
    pub version: u32,
    pub epoch: String,
    pub interval_ms: u64,
    pub execution_deadline_ms: i64,
    pub delivery_deadline_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallPhase {
    Running,
    Completing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceHeartbeat {
    pub version: u32,
    pub call_id: String,
    pub attempt: u32,
    pub epoch: String,
    pub instance: ServiceInstanceTarget,
    pub phase: CallPhase,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum HeartbeatDisposition {
    Renewed {
        store_now_ms: i64,
        liveness_until_ms: i64,
    },
    /// Coordination is unknown; only a Workflow owner may reconcile it.
    Recovering,
    Invalidated,
}

/// Compact active state, never a payload/result ledger. All time decisions use
/// the store clock. The Workflow checkpoint remains the recovery authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallLease {
    pub application_id: String,
    pub root_id: String,
    pub call_id: String,
    pub attempt: u32,
    pub epoch: String,
    pub execution_deadline_ms: i64,
    pub delivery_deadline_ms: i64,
    pub total_deadline_ms: i64,
    pub phase: String,
    pub due_ms: i64,
    pub instance: Option<ServiceInstanceTarget>,
    pub completion_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum CallLeaseCommand {
    Prepare {
        state: CallLease,
    },
    Recover {
        state: CallLease,
    },
    Activate {
        instance: ServiceInstanceTarget,
    },
    Heartbeat {
        instance: ServiceInstanceTarget,
        phase: CallPhase,
    },
    ClaimCompletion {
        digest: String,
    },
    Close,
    Expire,
    /// A checkpoint already committed the retry fence.
    Fence,
    Read,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallLeaseReply {
    pub now_ms: i64,
    pub applied: bool,
    pub state: Option<CallLease>,
}

pub type CoordinationError = Box<dyn std::error::Error + Send + Sync>;

#[async_trait::async_trait]
pub trait CallCoordination: Send + Sync {
    /// Commands cannot recreate unknown work from a provider heartbeat.
    async fn transition(
        &self,
        application: &str,
        root: &str,
        call: &str,
        attempt: u32,
        epoch: &str,
        command: CallLeaseCommand,
    ) -> Result<CallLeaseReply, CoordinationError>;
    /// Read-only hints. Callers still require partition and Workflow ownership.
    async fn due(
        &self,
        partition: u32,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, String)>, CoordinationError>;
}
