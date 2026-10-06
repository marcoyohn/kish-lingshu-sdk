//! One-use Service submission authority, independent of concurrency/rate quotas.
use super::*;
use async_trait::async_trait;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionPermit {
    pub application_id: String,
    pub call_id: String,
    pub attempt: u32,
    pub operation: OperationRef,
    pub request_id: String,
    pub revision: i64,
    pub epoch: String,
    pub hard_deadline_ms: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdmissionRequest {
    pub call_id: String,
    pub attempt: u32,
    /// Stable opaque reservation nonce; the backend also binds call/attempt and Operation.
    pub request_id: String,
    pub operation: OperationRef,
    pub hard_deadline_ms: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdmissionDecision {
    Granted {
        permit: AdmissionPermit,
    },
    Waiting {
        next_check_at_ms: i64,
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resource: Option<String>,
    },
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PermitTransition {
    Activate,
    Send,
    Release,
    CancelUnsent,
}

/// Implementations must use storage time, monotonic
/// policy revisions, idempotent reservations and exactly-once send claims.
/// Missing authority must fail closed; releasing an old identity cannot free a successor.
#[async_trait]
pub trait ServiceAdmission: Send + Sync {
    async fn synchronize_and_admit(
        &self,
        app: &str,
        revision: i64,
        request: &AdmissionRequest,
    ) -> Result<AdmissionDecision, CoordinationError> {
        self.install(app, revision).await?;
        self.try_admit(app, revision, request).await
    }
    async fn synchronize_and_send(
        &self,
        app: &str,
        revision: i64,
        permit: &AdmissionPermit,
    ) -> Result<bool, CoordinationError> {
        self.install(app, revision).await?;
        self.transition(app, permit, PermitTransition::Send).await
    }
    async fn install(&self, app: &str, revision: i64) -> Result<(), CoordinationError>;
    async fn try_admit(
        &self,
        app: &str,
        revision: i64,
        request: &AdmissionRequest,
    ) -> Result<AdmissionDecision, CoordinationError>;
    async fn transition(
        &self,
        app: &str,
        permit: &AdmissionPermit,
        action: PermitTransition,
    ) -> Result<bool, CoordinationError>;
}
