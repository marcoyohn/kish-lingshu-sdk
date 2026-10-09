//! A finite synchronous wire stage; asynchronous acceptance needs its own reporter.
use super::{ServiceCancellation, ServiceError, ServiceInvocation, ServiceOutcome};
use kish_lingshu_foundation_contract::service_transport::LaneIdentity;
use serde::{Deserialize, Serialize};
pub const NATIVE_SYNC_CALL_TIMEOUT_MS: i64 = 10_000;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCallRequest {
    pub route_revision: u64,
    pub lane: LaneIdentity,
    pub action: NativeCallAction,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeCallAction {
    /// Signed inspection of an installed execution binding; no Handler runs.
    Readiness {
        target_instance: super::ServiceInstanceTarget,
    },
    /// Inspect the explicit finite Async binding; synchronous Ready is insufficient.
    AsyncReadiness {
        target_instance: super::ServiceInstanceTarget,
    },
    Invoke {
        invocation: Box<ServiceInvocation>,
    },
    Cancel {
        cancellation: ServiceCancellation,
    },
}
/// Rejection is affirmative pre-acceptance evidence, never a bare link error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeCallResponse {
    Ready,
    AsyncReady,
    Accepted { call_id: String, attempt: u32 },
    Completed { outcome: ServiceOutcome },
    Cancelled,
    Rejected { error: ServiceError },
    OutcomeUnknown,
}

/// Product-injected native transport. Implementations use current product
/// authority and exact CSR pins; readiness observations never renew a lease.
/// No Zenoh/host types cross the Workflow dependency boundary.
#[async_trait::async_trait]
pub trait NativeCallTransport: Send + Sync {
    async fn ready(
        &self,
        instance: &super::ServiceInstanceV2,
    ) -> Result<bool, kish_lingshu_foundation_contract::service_transport::TransportFailureKind>;

    /// Existing transports remain synchronous unless they explicitly prove Async.
    async fn ready_for(
        &self,
        instance: &super::ServiceInstanceV2,
        mode: super::CallMode,
    ) -> Result<bool, kish_lingshu_foundation_contract::service_transport::TransportFailureKind>
    {
        if mode == super::CallMode::Sync {
            self.ready(instance).await
        } else {
            Ok(false)
        }
    }

    /// Exactly one submission to the selected generation, with no fallback.
    /// After submitting, unproved results MUST become OutcomeUnknown.
    async fn invoke(
        &self,
        instance: &super::ServiceInstanceV2,
        invocation: ServiceInvocation,
    ) -> Result<
        NativeCallResponse,
        kish_lingshu_foundation_contract::service_transport::TransportFailureKind,
    >;

    async fn cancel(
        &self,
        instance: &super::ServiceInstanceV2,
        cancellation: ServiceCancellation,
    ) -> Result<
        NativeCallResponse,
        kish_lingshu_foundation_contract::service_transport::TransportFailureKind,
    >;
}
