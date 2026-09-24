//! Application-key enrollment with independent, renewable instance sessions.
use std::{fmt, time::Duration};

use kish_lingshu_event_dispatch_contract::{
    ConsumerEnrollmentRequest, ConsumerInstanceLeaseV1, ConsumerSession,
};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{sleep, sleep_until, Instant},
};

use crate::{
    service_auth::{callback_url, response_bytes, response_json},
    ServiceAuthError, ServiceConnection,
};

/// The only deployment inputs needed after establishing a ServiceConnection.
#[derive(Clone, Debug)]
pub struct EnrolledConsumerNodeConfig {
    pub group_key: String,
    /// Stable across restarts, distinct among live replicas; never a process ID.
    pub node_id: String,
    pub invocation_url: String,
    pub maximum_in_flight: u32,
}

/// Public lifecycle state contains no session or application credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnrolledConsumerNodeStatus {
    Registered {
        lease: ConsumerInstanceLeaseV1,
    },
    HeartbeatRetrying {
        lease: ConsumerInstanceLeaseV1,
        failure: ServiceAuthError,
    },
    CredentialRejected {
        lease: ConsumerInstanceLeaseV1,
    },
    Fenced {
        lease: ConsumerInstanceLeaseV1,
    },
    LeaseExpired {
        lease: ConsumerInstanceLeaseV1,
    },
    Failed {
        lease: ConsumerInstanceLeaseV1,
        failure: ServiceAuthError,
    },
    Stopped {
        lease: ConsumerInstanceLeaseV1,
    },
}

impl EnrolledConsumerNodeStatus {
    pub fn lease(&self) -> &ConsumerInstanceLeaseV1 {
        match self {
            Self::Registered { lease }
            | Self::HeartbeatRetrying { lease, .. }
            | Self::CredentialRejected { lease }
            | Self::Fenced { lease }
            | Self::LeaseExpired { lease }
            | Self::Failed { lease, .. }
            | Self::Stopped { lease } => lease,
        }
    }

    pub fn is_terminal(&self) -> bool {
        !matches!(
            self,
            Self::Registered { .. } | Self::HeartbeatRetrying { .. }
        )
    }
}

/// Owns the automatic heartbeat loop. Drop cancels renewal and attempts a
/// bounded, generation-fenced deregistration; shutdown additionally joins it.
pub struct EnrolledConsumerNode {
    group_key: String,
    cancel: watch::Sender<bool>,
    status: watch::Receiver<EnrolledConsumerNodeStatus>,
    task: Option<JoinHandle<()>>,
}

impl fmt::Debug for EnrolledConsumerNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnrolledConsumerNode")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl ServiceConnection {
    /// Enrolls in an existing application-scoped group without changing its key.
    pub async fn enroll_consumer(
        &self,
        config: EnrolledConsumerNodeConfig,
    ) -> Result<EnrolledConsumerNode, ServiceAuthError> {
        EnrolledConsumerNode::start(self.clone(), config).await
    }
}

impl EnrolledConsumerNode {
    pub async fn start(
        connection: ServiceConnection,
        config: EnrolledConsumerNodeConfig,
    ) -> Result<Self, ServiceAuthError> {
        validate_config(&config)?;
        let mut registration = connection.registration().await;
        let instance = registration.request(&config.node_id);
        let started = Instant::now();
        let session: ConsumerSession = connection
            .root_response_json(
                connection
                    .root_request(reqwest::Method::POST, "consumer-enrollments")?
                    .json(&ConsumerEnrollmentRequest {
                        instance: Some(instance),
                        group_key: config.group_key.clone(),
                        node_id: config.node_id.clone(),
                        invocation_url: callback_url(&config.invocation_url)?.to_string(),
                        maximum_in_flight: config.maximum_in_flight,
                    }),
            )
            .await?;
        validate_session(&session, &config, None)?;
        registration.accept(session.instance.as_ref())?;
        drop(registration);
        let deadline = lease_deadline(&session, started)?;
        let (status_tx, status) = watch::channel(EnrolledConsumerNodeStatus::Registered {
            lease: session.lease.clone(),
        });
        let (cancel, cancel_rx) = watch::channel(false);
        let group_key = config.group_key.clone();
        let task = tokio::spawn(renew(
            connection, config, session, deadline, status_tx, cancel_rx,
        ));
        Ok(Self {
            group_key,
            cancel,
            status,
            task: Some(task),
        })
    }

    pub fn status(&self) -> EnrolledConsumerNodeStatus {
        self.status.borrow().clone()
    }

    pub fn group_key(&self) -> &str {
        &self.group_key
    }

    pub fn subscribe(&self) -> watch::Receiver<EnrolledConsumerNodeStatus> {
        self.status.clone()
    }

    pub async fn shutdown(mut self) {
        let _ = self.cancel.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for EnrolledConsumerNode {
    fn drop(&mut self) {
        let _ = self.cancel.send(true);
    }
}

fn validate_config(config: &EnrolledConsumerNodeConfig) -> Result<(), ServiceAuthError> {
    let identity = |s: &str| {
        !s.trim().is_empty() && s.trim() == s && s.len() <= 255 && !s.chars().any(char::is_control)
    };
    if !identity(&config.group_key)
        || !identity(&config.node_id)
        || !config.node_id.is_ascii()
        || config.maximum_in_flight == 0
        || callback_url(&config.invocation_url).is_err()
    {
        return Err(ServiceAuthError::InvalidNodeConfig);
    }
    Ok(())
}

fn validate_session(
    session: &ConsumerSession,
    config: &EnrolledConsumerNodeConfig,
    previous: Option<&ConsumerSession>,
) -> Result<(), ServiceAuthError> {
    let lease = &session.lease;
    if session.group_id == 0
        || session.group_key != config.group_key
        || lease.node_id != config.node_id
        || lease.member_id == 0
        || lease.membership_generation == 0
        || lease.lease_seconds == 0
        || lease.lease_seconds > 86_400
        || lease.heartbeat_interval_seconds == 0
        || lease.heartbeat_interval_seconds >= lease.lease_seconds
        || session.credential.trim().is_empty()
        || session.credential.len() > 16 * 1024
        || !session.credential.is_ascii()
        || session
            .credential
            .bytes()
            .any(|byte| byte.is_ascii_control())
        || session.expires_at <= chrono::Utc::now().timestamp()
    {
        return Err(ServiceAuthError::InvalidResponse);
    }
    if let Some(previous) = previous {
        if session.instance != previous.instance
            || session.group_id != previous.group_id
            || lease.member_id != previous.lease.member_id
            || lease.membership_generation != previous.lease.membership_generation
        {
            return Err(ServiceAuthError::Http(409));
        }
    }
    Ok(())
}

fn lease_deadline(
    session: &ConsumerSession,
    started: Instant,
) -> Result<Instant, ServiceAuthError> {
    let now = chrono::Utc::now();
    let lease_remaining = (session.lease.lease_expires_at - now)
        .to_std()
        .map_err(|_| ServiceAuthError::InvalidResponse)?;
    let credential_remaining = (chrono::DateTime::from_timestamp(session.expires_at, 0)
        .ok_or(ServiceAuthError::InvalidResponse)?
        - now)
        .to_std()
        .map_err(|_| ServiceAuthError::InvalidResponse)?;
    let deadline = (started + Duration::from_secs(session.lease.lease_seconds)).min(
        Instant::now()
            + lease_remaining
                .min(credential_remaining)
                .min(Duration::from_secs(86_400)),
    );
    if deadline <= Instant::now() {
        return Err(ServiceAuthError::InvalidResponse);
    }
    Ok(deadline)
}

async fn deregister(connection: &ServiceConnection, session: &ConsumerSession) {
    if let Ok(request) =
        connection.session_request("consumer-sessions/deregister", &session.credential)
    {
        let _ = response_bytes(request).await;
    }
}

async fn renew(
    connection: ServiceConnection,
    config: EnrolledConsumerNodeConfig,
    mut session: ConsumerSession,
    mut deadline: Instant,
    status: watch::Sender<EnrolledConsumerNodeStatus>,
    mut cancel: watch::Receiver<bool>,
) {
    let mut delay = Duration::from_secs(session.lease.heartbeat_interval_seconds);
    let mut failures = 0_u32;
    let mut closed = connection.subscribe_closed();
    loop {
        let closure = closed.borrow_and_update().clone();
        if let Some(reason) = closure {
            let final_status = match reason {
                ServiceAuthError::Http(401 | 403) => {
                    EnrolledConsumerNodeStatus::CredentialRejected {
                        lease: session.lease,
                    }
                }
                _ => EnrolledConsumerNodeStatus::Stopped {
                    lease: session.lease,
                },
            };
            status.send_replace(final_status);
            return;
        }
        // Cancellation and absolute lease expiry also interrupt in-flight HTTP.
        let result = tokio::select! {
            biased;
            _ = closed.changed() => { continue; }
            _ = cancel.changed() => {
                deregister(&connection, &session).await;
                status.send_replace(EnrolledConsumerNodeStatus::Stopped { lease: session.lease });
                return;
            }
            _ = sleep_until(deadline) => {
                status.send_replace(EnrolledConsumerNodeStatus::LeaseExpired { lease: session.lease });
                return;
            }
            result = async {
                sleep(delay).await;
                let started = Instant::now();
                let updated: ConsumerSession = response_json(connection.session_request("consumer-sessions/heartbeat", &session.credential)?).await?;
                validate_session(&updated, &config, Some(&session))?;
                let deadline = lease_deadline(&updated, started)?;
                Ok::<_, ServiceAuthError>((updated, deadline))
            } => result,
        };
        match result {
            Ok((updated, updated_deadline)) => {
                session = updated;
                deadline = updated_deadline;
                failures = 0;
                delay = Duration::from_secs(session.lease.heartbeat_interval_seconds);
                status.send_replace(EnrolledConsumerNodeStatus::Registered {
                    lease: session.lease.clone(),
                });
            }
            Err(error @ ServiceAuthError::Http(401)) => {
                connection.reject_authentication(error);
                status.send_replace(EnrolledConsumerNodeStatus::CredentialRejected {
                    lease: session.lease,
                });
                return;
            }
            Err(ServiceAuthError::Http(403 | 404 | 409)) => {
                status.send_replace(EnrolledConsumerNodeStatus::Fenced {
                    lease: session.lease,
                });
                return;
            }
            Err(failure) => {
                let transient = matches!(
                    failure,
                    ServiceAuthError::Transport | ServiceAuthError::Http(408 | 429 | 500..=599)
                );
                if !transient {
                    status.send_replace(EnrolledConsumerNodeStatus::Failed {
                        lease: session.lease,
                        failure,
                    });
                    return;
                }
                failures = failures.saturating_add(1);
                delay = Duration::from_secs(
                    (1_u64 << failures.saturating_sub(1).min(5))
                        .min(session.lease.heartbeat_interval_seconds),
                );
                status.send_replace(EnrolledConsumerNodeStatus::HeartbeatRetrying {
                    lease: session.lease.clone(),
                    failure,
                });
            }
        }
    }
}
