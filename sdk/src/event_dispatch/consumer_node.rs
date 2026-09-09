use std::{fmt, time::Duration};

use kish_lingshu_event_dispatch_contract::{
    ConsumerInstanceDeregisterRequestV1, ConsumerInstanceHeartbeatRequestV1,
    ConsumerInstanceLeaseV1, ConsumerInstanceRegistrationRequestV1,
};
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{sleep, Instant},
};

use crate::ConsumerGroupRegistrationCredential;

const MAX_CONTROL_RESPONSE_BYTES: usize = 64 * 1024;
const DEFAULT_CONTROL_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Deployment-owned configuration for one HTTP Consumer process instance.
pub struct ConsumerNodeConfig {
    /// Base URL ending at the versioned Event Dispatch API.
    pub control_plane_url: String,
    pub app_id: String,
    pub group_id: u64,
    pub registration_credential: ConsumerGroupRegistrationCredential,
    /// Stable across restarts of one instance and unique among live replicas.
    pub node_id: String,
    /// Externally reachable URL served by [`super::ConsumerHttpAdapter`].
    pub invocation_url: String,
    pub maximum_in_flight: u32,
}

impl fmt::Debug for ConsumerNodeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerNodeConfig")
            .field("control_plane_url", &self.control_plane_url)
            .field("app_id", &self.app_id)
            .field("group_id", &self.group_id)
            .field("registration_credential", &"[REDACTED]")
            .field("node_id", &self.node_id)
            .field("invocation_url", &self.invocation_url)
            .field("maximum_in_flight", &self.maximum_in_flight)
            .finish()
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ConsumerNodeConfigError {
    #[error("control_plane_url must be an absolute HTTP or HTTPS URL without credentials, query, or fragment")]
    InvalidControlPlaneUrl,
    #[error("app_id must be non-empty and contain no control characters")]
    InvalidApplicationId,
    #[error("group_id must be positive")]
    InvalidGroupId,
    #[error("node_id must be 1..=255 printable ASCII bytes")]
    InvalidNodeId,
    #[error(
        "invocation_url must be an absolute HTTP or HTTPS URL without credentials or fragment"
    )]
    InvalidInvocationUrl,
    #[error("maximum_in_flight must be positive")]
    InvalidMaximumInFlight,
}

/// Secret-safe failure summary published through node status.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[error("{code}: {message}")]
pub struct ConsumerNodeFailure {
    code: String,
    message: String,
}

impl ConsumerNodeFailure {
    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConsumerNodeError {
    #[error(transparent)]
    InvalidConfig(#[from] ConsumerNodeConfigError),
    #[error("failed to construct Event Dispatch control-plane client: {0}")]
    Client(String),
    #[error("Event Dispatch node registration failed: {0}")]
    Registration(ConsumerNodeFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerNodeStatus {
    Registered {
        lease: ConsumerInstanceLeaseV1,
    },
    HeartbeatRetrying {
        lease: ConsumerInstanceLeaseV1,
        consecutive_failures: u32,
        retry_after: Duration,
        last_failure: ConsumerNodeFailure,
    },
    CredentialRejected {
        lease: ConsumerInstanceLeaseV1,
        failure: ConsumerNodeFailure,
    },
    Fenced {
        lease: ConsumerInstanceLeaseV1,
        failure: ConsumerNodeFailure,
    },
    LeaseExpired {
        lease: ConsumerInstanceLeaseV1,
        last_failure: ConsumerNodeFailure,
    },
    Failed {
        lease: ConsumerInstanceLeaseV1,
        failure: ConsumerNodeFailure,
    },
    Stopped {
        lease: ConsumerInstanceLeaseV1,
    },
}

impl ConsumerNodeStatus {
    pub fn lease(&self) -> &ConsumerInstanceLeaseV1 {
        match self {
            Self::Registered { lease }
            | Self::HeartbeatRetrying { lease, .. }
            | Self::CredentialRejected { lease, .. }
            | Self::Fenced { lease, .. }
            | Self::LeaseExpired { lease, .. }
            | Self::Failed { lease, .. }
            | Self::Stopped { lease } => lease,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::CredentialRejected { .. }
                | Self::Fenced { .. }
                | Self::LeaseExpired { .. }
                | Self::Failed { .. }
                | Self::Stopped { .. }
        )
    }
}

/// Registered Consumer node plus its cancellable membership heartbeat task.
pub struct ConsumerNode {
    cancel: watch::Sender<bool>,
    status: watch::Receiver<ConsumerNodeStatus>,
    task: Option<JoinHandle<()>>,
}

impl fmt::Debug for ConsumerNode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConsumerNode")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl ConsumerNode {
    /// Registers before returning. The returned handle is ready to receive Events.
    pub async fn start(config: ConsumerNodeConfig) -> Result<Self, ConsumerNodeError> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = Client::builder()
            .timeout(DEFAULT_CONTROL_REQUEST_TIMEOUT)
            .build()
            .map_err(|error| ConsumerNodeError::Client(error.to_string()))?;
        Self::start_with_client(config, client).await
    }

    /// Uses a caller-supplied client for custom TLS, proxy, or testing policy.
    pub async fn start_with_client(
        config: ConsumerNodeConfig,
        client: Client,
    ) -> Result<Self, ConsumerNodeError> {
        let config = ValidatedNodeConfig::new(config)?;
        let control = NodeControlClient { client, config };
        let lease = control
            .register()
            .await
            .map_err(|error| ConsumerNodeError::Registration(error.failure))?;
        let initial_status = ConsumerNodeStatus::Registered {
            lease: lease.clone(),
        };
        let (status_tx, status) = watch::channel(initial_status);
        let (cancel, cancel_rx) = watch::channel(false);
        let task = tokio::spawn(run_heartbeat_loop(control, lease, status_tx, cancel_rx));
        Ok(Self {
            cancel,
            status,
            task: Some(task),
        })
    }

    pub fn status(&self) -> ConsumerNodeStatus {
        self.status.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<ConsumerNodeStatus> {
        self.status.clone()
    }

    /// Stops heartbeats, attempts generation-fenced deregistration, and joins the task.
    pub async fn shutdown(mut self) {
        let _ = self.cancel.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ConsumerNode {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
    }
}

#[derive(Debug)]
struct ValidatedNodeConfig {
    control_plane_url: Url,
    app_id: String,
    group_id: u64,
    registration_credential: ConsumerGroupRegistrationCredential,
    node_id: String,
    invocation_url: String,
    maximum_in_flight: u32,
}

impl ValidatedNodeConfig {
    fn new(config: ConsumerNodeConfig) -> Result<Self, ConsumerNodeConfigError> {
        let mut control_plane_url = valid_http_url(&config.control_plane_url)
            .ok_or(ConsumerNodeConfigError::InvalidControlPlaneUrl)?;
        if !control_plane_url.username().is_empty()
            || control_plane_url.password().is_some()
            || control_plane_url.query().is_some()
            || control_plane_url.fragment().is_some()
        {
            return Err(ConsumerNodeConfigError::InvalidControlPlaneUrl);
        }
        if !control_plane_url.path().ends_with('/') {
            let path = format!("{}/", control_plane_url.path());
            control_plane_url.set_path(&path);
        }
        if config.app_id.trim().is_empty()
            || config.app_id.len() > 255
            || config.app_id.chars().any(char::is_control)
        {
            return Err(ConsumerNodeConfigError::InvalidApplicationId);
        }
        if config.group_id == 0 {
            return Err(ConsumerNodeConfigError::InvalidGroupId);
        }
        if config.node_id.trim().is_empty()
            || config.node_id.len() > 255
            || !config.node_id.is_ascii()
            || config.node_id.bytes().any(|value| value.is_ascii_control())
        {
            return Err(ConsumerNodeConfigError::InvalidNodeId);
        }
        let invocation_url = valid_http_url(&config.invocation_url)
            .filter(|url| {
                url.username().is_empty() && url.password().is_none() && url.fragment().is_none()
            })
            .ok_or(ConsumerNodeConfigError::InvalidInvocationUrl)?;
        if config.maximum_in_flight == 0 {
            return Err(ConsumerNodeConfigError::InvalidMaximumInFlight);
        }
        Ok(Self {
            control_plane_url,
            app_id: config.app_id,
            group_id: config.group_id,
            registration_credential: config.registration_credential,
            node_id: config.node_id,
            invocation_url: invocation_url.to_string(),
            maximum_in_flight: config.maximum_in_flight,
        })
    }

    fn endpoint(&self, path: &str) -> Url {
        self.control_plane_url
            .join(path)
            .expect("validated relative control-plane path")
    }
}

fn valid_http_url(value: &str) -> Option<Url> {
    Url::parse(value)
        .ok()
        .filter(|url| matches!(url.scheme(), "http" | "https") && url.host_str().is_some())
}

struct NodeControlClient {
    client: Client,
    config: ValidatedNodeConfig,
}

impl NodeControlClient {
    async fn register(&self) -> Result<ConsumerInstanceLeaseV1, ControlRequestError> {
        let response = self
            .client
            .post(self.config.endpoint("consumer-instances"))
            .bearer_auth(self.config.registration_credential.expose())
            .json(&ConsumerInstanceRegistrationRequestV1 {
                app_id: self.config.app_id.clone(),
                group_id: self.config.group_id,
                node_id: self.config.node_id.clone(),
                invocation_url: self.config.invocation_url.clone(),
                maximum_in_flight: self.config.maximum_in_flight,
            })
            .send()
            .await
            .map_err(ControlRequestError::transport)?;
        self.read_lease(response).await
    }

    async fn heartbeat(
        &self,
        lease: &ConsumerInstanceLeaseV1,
    ) -> Result<ConsumerInstanceLeaseV1, ControlRequestError> {
        let response = self
            .client
            .post(
                self.config
                    .endpoint(&format!("consumer-instances/{}/heartbeat", lease.member_id)),
            )
            .bearer_auth(self.config.registration_credential.expose())
            .json(&ConsumerInstanceHeartbeatRequestV1 {
                group_id: self.config.group_id,
                node_id: self.config.node_id.clone(),
                membership_generation: lease.membership_generation,
            })
            .send()
            .await
            .map_err(ControlRequestError::transport)?;
        self.read_lease(response).await
    }

    async fn deregister(&self, lease: &ConsumerInstanceLeaseV1) {
        let _ = self
            .client
            .post(self.config.endpoint(&format!(
                "consumer-instances/{}/deregister",
                lease.member_id
            )))
            .bearer_auth(self.config.registration_credential.expose())
            .json(&ConsumerInstanceDeregisterRequestV1 {
                group_id: self.config.group_id,
                node_id: self.config.node_id.clone(),
                membership_generation: lease.membership_generation,
            })
            .send()
            .await;
    }

    async fn read_lease(
        &self,
        response: reqwest::Response,
    ) -> Result<ConsumerInstanceLeaseV1, ControlRequestError> {
        if !response.status().is_success() {
            return Err(ControlRequestError::from_response(response).await);
        }
        let bytes = bounded_response(response).await?;
        let lease = serde_json::from_slice::<ConsumerInstanceLeaseV1>(&bytes).map_err(|error| {
            ControlRequestError::protocol(format!("invalid membership lease response: {error}"))
        })?;
        if lease.member_id == 0
            || lease.node_id != self.config.node_id
            || lease.membership_generation == 0
            || lease.lease_seconds == 0
            || lease.heartbeat_interval_seconds == 0
            || lease.heartbeat_interval_seconds > lease.lease_seconds
        {
            return Err(ControlRequestError::protocol(
                "membership lease response contains invalid identity or timing",
            ));
        }
        Ok(lease)
    }
}

async fn run_heartbeat_loop(
    control: NodeControlClient,
    mut lease: ConsumerInstanceLeaseV1,
    status: watch::Sender<ConsumerNodeStatus>,
    mut cancel: watch::Receiver<bool>,
) {
    let mut next_delay = Duration::from_secs(lease.heartbeat_interval_seconds);
    let mut local_lease_deadline = Instant::now() + Duration::from_secs(lease.lease_seconds);
    let mut consecutive_failures = 0_u32;
    loop {
        let cancelled = tokio::select! {
            _ = sleep(next_delay) => false,
            changed = cancel.changed() => changed.is_err() || *cancel.borrow(),
        };
        if cancelled {
            control.deregister(&lease).await;
            let _ = status.send(ConsumerNodeStatus::Stopped { lease });
            return;
        }

        match control.heartbeat(&lease).await {
            Ok(updated) => {
                lease = updated;
                consecutive_failures = 0;
                local_lease_deadline = Instant::now() + Duration::from_secs(lease.lease_seconds);
                next_delay = Duration::from_secs(lease.heartbeat_interval_seconds);
                let _ = status.send(ConsumerNodeStatus::Registered {
                    lease: lease.clone(),
                });
            }
            Err(error) if error.kind == ControlFailureKind::CredentialRejected => {
                let _ = status.send(ConsumerNodeStatus::CredentialRejected {
                    lease,
                    failure: error.failure,
                });
                return;
            }
            Err(error) if error.kind == ControlFailureKind::Fenced => {
                let _ = status.send(ConsumerNodeStatus::Fenced {
                    lease,
                    failure: error.failure,
                });
                return;
            }
            Err(error) if error.kind != ControlFailureKind::Transient => {
                let _ = status.send(ConsumerNodeStatus::Failed {
                    lease,
                    failure: error.failure,
                });
                return;
            }
            Err(error) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                let remaining = local_lease_deadline.saturating_duration_since(Instant::now());
                if remaining <= Duration::from_millis(100) {
                    let _ = status.send(ConsumerNodeStatus::LeaseExpired {
                        lease,
                        last_failure: error.failure,
                    });
                    return;
                }
                next_delay = retry_delay(consecutive_failures, lease.heartbeat_interval_seconds)
                    .min(remaining / 2)
                    .max(Duration::from_millis(50));
                let _ = status.send(ConsumerNodeStatus::HeartbeatRetrying {
                    lease: lease.clone(),
                    consecutive_failures,
                    retry_after: next_delay,
                    last_failure: error.failure,
                });
            }
        }
    }
}

fn retry_delay(consecutive_failures: u32, heartbeat_interval_seconds: u64) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(5);
    let seconds = 1_u64
        .checked_shl(exponent)
        .unwrap_or(32)
        .min(heartbeat_interval_seconds.max(1));
    Duration::from_secs(seconds)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlFailureKind {
    Transient,
    CredentialRejected,
    Fenced,
    Terminal,
}

struct ControlRequestError {
    kind: ControlFailureKind,
    failure: ConsumerNodeFailure,
}

impl ControlRequestError {
    fn transport(error: reqwest::Error) -> Self {
        Self {
            kind: ControlFailureKind::Transient,
            failure: ConsumerNodeFailure {
                code: "control_plane_transport_error".into(),
                message: bounded(error.to_string(), 512),
            },
        }
    }

    fn protocol(message: impl Into<String>) -> Self {
        Self {
            kind: ControlFailureKind::Terminal,
            failure: ConsumerNodeFailure {
                code: "invalid_control_plane_response".into(),
                message: bounded(message.into(), 512),
            },
        }
    }

    async fn from_response(response: reqwest::Response) -> Self {
        let status = response.status();
        let body = bounded_response(response).await.ok();
        let envelope = body
            .as_deref()
            .and_then(|value| serde_json::from_slice::<ControlErrorEnvelope>(value).ok());
        let code = envelope
            .as_ref()
            .map(|value| value.code.as_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("control_plane_request_failed");
        let message = envelope
            .as_ref()
            .map(|value| value.message.as_str())
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("Event Dispatch control plane returned HTTP {status}"));
        let kind = if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
            ControlFailureKind::CredentialRejected
        } else if status == StatusCode::CONFLICT && code == "membership_generation_conflict" {
            ControlFailureKind::Fenced
        } else if status.is_server_error()
            || matches!(
                status,
                StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
            )
        {
            ControlFailureKind::Transient
        } else {
            ControlFailureKind::Terminal
        };
        Self {
            kind,
            failure: ConsumerNodeFailure {
                code: bounded(code.to_owned(), 64),
                message: bounded(message, 512),
            },
        }
    }
}

#[derive(Deserialize)]
struct ControlErrorEnvelope {
    code: String,
    message: String,
}

async fn bounded_response(response: reqwest::Response) -> Result<Vec<u8>, ControlRequestError> {
    let bytes = response
        .bytes()
        .await
        .map_err(ControlRequestError::transport)?;
    if bytes.len() > MAX_CONTROL_RESPONSE_BYTES {
        return Err(ControlRequestError::protocol(
            "control-plane response exceeded the configured SDK limit",
        ));
    }
    Ok(bytes.to_vec())
}

fn bounded(value: String, maximum_chars: usize) -> String {
    value.chars().take(maximum_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ConsumerNodeConfig {
        ConsumerNodeConfig {
            control_plane_url: "https://lingshu.example/api/user/event-dispatch/v1".into(),
            app_id: "orders".into(),
            group_id: 41,
            registration_credential: ConsumerGroupRegistrationCredential::new(
                "edrk_v1_secret-value",
            )
            .unwrap(),
            node_id: "orders-pod-1".into(),
            invocation_url: "https://orders.example/internal/events".into(),
            maximum_in_flight: 8,
        }
    }

    #[test]
    fn configuration_requires_node_identity_capacity_and_redacts_secret() {
        let valid = config();
        let debug = format!("{valid:?}");
        assert!(!debug.contains("secret-value"));
        assert!(ValidatedNodeConfig::new(valid).is_ok());

        let mut missing_node = config();
        missing_node.node_id.clear();
        assert_eq!(
            ValidatedNodeConfig::new(missing_node).unwrap_err(),
            ConsumerNodeConfigError::InvalidNodeId
        );

        let mut zero_capacity = config();
        zero_capacity.maximum_in_flight = 0;
        assert_eq!(
            ValidatedNodeConfig::new(zero_capacity).unwrap_err(),
            ConsumerNodeConfigError::InvalidMaximumInFlight
        );
    }

    #[test]
    fn endpoint_join_keeps_versioned_control_plane_path() {
        let validated = ValidatedNodeConfig::new(config()).unwrap();
        assert_eq!(
            validated.endpoint("consumer-instances").as_str(),
            "https://lingshu.example/api/user/event-dispatch/v1/consumer-instances"
        );
    }

    #[test]
    fn retry_is_bounded_by_heartbeat_interval() {
        assert_eq!(retry_delay(1, 30), Duration::from_secs(1));
        assert_eq!(retry_delay(4, 30), Duration::from_secs(8));
        assert_eq!(retry_delay(10, 3), Duration::from_secs(3));
    }
}
