//! Independent renewable-call reporting. The original completion capability
//! and Workflow remain authoritative; these DTOs create neither grants nor work.
use super::*;
use kish_lingshu_foundation_contract::service_transport::CallReportRoute;

pub const MAX_NATIVE_CALL_REPORT_REPLY_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCallReportTarget {
    pub route: CallReportRoute,
    pub instance: ServiceInstanceTarget,
    pub heartbeat: CallHeartbeatPolicy,
}
impl NativeCallReportTarget {
    pub fn validate(&self, now: i64) -> Result<(), ServiceError> {
        let p = &self.heartbeat;
        if self.route.key().is_err()
            || self.route.epoch.as_str() != p.epoch
            || self.instance.node_id.is_empty()
            || self.instance.generation.is_empty()
            || self.instance.node_id.len() > 256
            || self.instance.generation.len() > 256
            || p.version != CALL_HEARTBEAT_VERSION
            || p.interval_ms != CALL_HEARTBEAT_INTERVAL_MS
            || p.execution_deadline_ms <= now
            || p.execution_deadline_ms.saturating_sub(now) as u64 > MAX_SERVICE_DEADLINE_MS
            || p.delivery_deadline_ms < p.execution_deadline_ms
            || p.delivery_deadline_ms > p.execution_deadline_ms.saturating_add(CALL_DELIVERY_MS)
        {
            return Err(ServiceError::rejected(
                "invalid_native_report",
                "Invalid finite call report target",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCallReportRequest {
    pub instance: ServiceInstanceTarget,
    /// Original opaque call capability, never a root API key.
    pub token: String,
    pub action: NativeCallReportAction,
}
impl std::fmt::Debug for NativeCallReportRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeCallReportRequest([REDACTED])")
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeCallReportAction {
    Heartbeat { heartbeat: ServiceHeartbeat },
    Complete { completion: ServiceCompletion },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeCallReportResponse {
    Heartbeat {
        disposition: HeartbeatDisposition,
    },
    Completed {
        disposition: CompletionDisposition,
    },
    /// Only a scoped, verified platform reply can classify authority rejection.
    Rejected {
        reason: NativeCallReportRejection,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeCallReportRejection {
    AuthorityUnavailable,
    Rejected,
    Unavailable,
}
