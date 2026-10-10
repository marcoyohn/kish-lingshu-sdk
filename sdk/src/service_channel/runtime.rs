//! Desired native state survives replacement of a fully closed physical owner.
//! This owner never retries business work or infers successful execution.
use super::*;
use kish_lingshu_runtime_contract::provider::ProviderCatalog;
use std::sync::Arc;
use tokio::{sync::watch, task::JoinHandle};

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum NativeRuntimeError {
    #[error("native bootstrap: {0}")]
    Bootstrap(#[from] ServiceAuthError),
    #[error("native channel: {0}")]
    Channel(#[from] ChannelSessionError),
}
impl NativeRuntimeError {
    fn recoverable(&self) -> bool {
        matches!(
            self,
            Self::Bootstrap(
                ServiceAuthError::Transport | ServiceAuthError::Http(429 | 502 | 503 | 504)
            ) | Self::Channel(
                ChannelSessionError::Transport
                    | ChannelSessionError::Closed
                    | ChannelSessionError::AuthorityExpired
                    | ChannelSessionError::RotationFailed
            )
        )
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeRuntimeStatus {
    Connecting,
    /// Provider is installed; execution readiness is still per capability.
    Active {
        instance_generation: String,
    },
    Recovering {
        error: NativeRuntimeError,
    },
    Closed,
    Failed {
        error: NativeRuntimeError,
    },
}

/// One logical owner for Provider, pending declarations and publication. Use a
/// dedicated ServiceConnection; HTTP heartbeat roles cannot share this owner.
/// Plan update/status handles remain valid across physical channel replacement.
pub struct RegisteredServiceRuntime {
    connection: ServiceConnection,
    instance: ServiceInstanceRegistration,
    deployment: Option<RouteIdentity>,
    transport: ChannelTransport,
    sessions: ChannelSessionConfig,
    rotation: Option<ChannelCertificateRotationConfig>,
    connection_lifecycle: bool,
    catalog: Arc<ProviderCatalog>,
    #[cfg(feature = "service-call-zenoh")]
    calls: Option<RegisteredCallPlan>,
    #[cfg(feature = "event-consumer-zenoh")]
    consumers: Option<RegisteredConsumerPlan>,
}
impl RegisteredServiceRuntime {
    pub fn new(
        connection: ServiceConnection,
        instance: ServiceInstanceRegistration,
        deployment: Option<RouteIdentity>,
        catalog: ProviderCatalog,
        sessions: ChannelSessionConfig,
    ) -> Result<Self, ChannelSessionError> {
        if connection.application_id() != catalog.application_id
            || instance.generation.is_some()
            || RouteIdentity::new(&instance.instance_id).is_err()
            || RouteIdentity::new(&instance.incarnation_id).is_err()
            || catalog.digest().is_err()
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        Ok(Self {
            connection,
            instance,
            deployment,
            catalog: Arc::new(catalog),
            sessions,
            transport: ChannelTransport::Mtls,
            rotation: Some(ChannelCertificateRotationConfig::default()),
            connection_lifecycle: false,
            #[cfg(feature = "service-call-zenoh")]
            calls: None,
            #[cfg(feature = "event-consumer-zenoh")]
            consumers: None,
        })
    }
    pub fn with_transport(mut self, transport: ChannelTransport) -> Self {
        self.transport = transport;
        self
    }
    /// Requires matched Host/platform and event-driven ZenSS client artifacts.
    /// Negotiates and binds the physical pool before Provider enrollment; no
    /// fallback or ordinary per-role renewal occurs in this mode.
    pub fn with_connection_lifecycle(mut self, enabled: bool) -> Self {
        self.connection_lifecycle = enabled;
        self
    }
    pub fn with_rotation(mut self, rotation: Option<ChannelCertificateRotationConfig>) -> Self {
        self.rotation = rotation;
        self
    }
    #[cfg(feature = "service-call-zenoh")]
    pub fn with_calls(mut self, plan: RegisteredCallPlan) -> Result<Self, ChannelSessionError> {
        if plan.application_id() != self.connection.application_id() {
            return Err(ChannelSessionError::InvalidConfig);
        }
        self.calls = Some(plan);
        Ok(self)
    }
    #[cfg(feature = "event-consumer-zenoh")]
    pub fn with_consumers(
        mut self,
        plan: RegisteredConsumerPlan,
    ) -> Result<Self, ChannelSessionError> {
        if plan.application_id() != self.connection.application_id() {
            return Err(ChannelSessionError::InvalidConfig);
        }
        self.consumers = Some(plan);
        Ok(self)
    }
    pub fn start(self) -> Result<ManagedRegisteredService, ChannelSessionError> {
        if !tokio::runtime::Handle::try_current()
            .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
        {
            return Err(ChannelSessionError::UnsupportedRuntime);
        }
        let permit = self
            .connection
            .claim_native_runtime()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let (stop, stopped) = watch::channel(false);
        let (status_tx, status) = watch::channel(NativeRuntimeStatus::Connecting);
        let (role_tx, roles) = watch::channel(Vec::new());
        #[cfg(feature = "event-publication-zenoh")]
        let (publication_tx, current) = watch::channel(None);
        #[cfg(feature = "event-publication-zenoh")]
        let publication = publication::NativePublicationSource {
            app_id: self.connection.application_id().into(),
            current,
        };
        let task = tokio::spawn(async move {
            let _permit = permit;
            let result = self
                .run(
                    stopped,
                    &status_tx,
                    role_tx,
                    #[cfg(feature = "event-publication-zenoh")]
                    publication_tx,
                )
                .await;
            status_tx.send_replace(match &result {
                Ok(()) => NativeRuntimeStatus::Closed,
                Err(error) => NativeRuntimeStatus::Failed {
                    error: error.clone(),
                },
            });
            result
        });
        Ok(ManagedRegisteredService {
            stop,
            status,
            roles,
            task: Some(task),
            result: None,
            #[cfg(feature = "event-publication-zenoh")]
            publication,
        })
    }
    async fn open(
        &self,
        replacement: bool,
    ) -> Result<(ManagedRoleChannel, String), NativeRuntimeError> {
        let identity = if replacement {
            self.connection
                .rebootstrap_channel_after_close(self.deployment.clone(), self.transport)
                .await?
        } else {
            self.connection
                .bootstrap_channel_with_transport(
                    self.instance.clone(),
                    self.deployment.clone(),
                    self.transport,
                )
                .await?
        };
        let generation = identity.bootstrap_response().instance.generation.clone();
        let mut pool = identity.open_sessions(self.sessions).await?;
        if self.connection_lifecycle {
            let binding = RouteIdentity::new(format!("connection-{}", uuid::Uuid::new_v4()))
                .map_err(|_| ChannelSessionError::InvalidConfig)?;
            if let Err(error) = pool.activate_connection_authority(binding).await {
                pool.close().await?;
                return Err(error.into());
            }
        }
        let provider = match pool.register_provider_role(&self.catalog).await {
            Ok(provider) => provider,
            Err(error) => {
                pool.close().await?;
                return Err(error.into());
            }
        };
        let channel = match self.rotation {
            Some(rotation) => pool.manage_roles_with_rotation(vec![provider], rotation)?,
            None => pool.manage_roles(vec![provider])?,
        };
        Ok((channel, generation))
    }
    async fn run(
        mut self,
        mut stopped: watch::Receiver<bool>,
        status: &watch::Sender<NativeRuntimeStatus>,
        roles: watch::Sender<Vec<ChannelRoleStatus>>,
        #[cfg(feature = "event-publication-zenoh")] publication: watch::Sender<
            Option<Arc<publication::NativePublicationClient>>,
        >,
    ) -> Result<(), NativeRuntimeError> {
        let mut replacement = false;
        let mut failures = 0u32;
        loop {
            if *stopped.borrow() {
                return Ok(());
            }
            self.connection.ensure_open()?;
            // Opening/cleanup are joined, never cancelled mid native ownership transfer.
            let opened = self.open(replacement).await;
            // A bootstrap may have committed even when its response was lost.
            // The connection retains the original incarnation for outcome recovery.
            replacement = true;
            let error = match opened {
                Ok((mut channel, generation)) => {
                    failures = 0;
                    let mut channel_status = channel.subscribe_status();
                    let mut role_status = channel.subscribe_roles();
                    #[cfg(feature = "event-publication-zenoh")]
                    let mut channel_publication = channel.publication.current.clone();
                    status.send_replace(NativeRuntimeStatus::Active {
                        instance_generation: generation,
                    });
                    let result = {
                        let plans = async {
                            #[cfg(feature = "service-call-zenoh")]
                            let calls = async {
                                match &mut self.calls {
                                    Some(p) => p.run_on(&channel).await,
                                    None => std::future::pending().await,
                                }
                            };
                            #[cfg(not(feature = "service-call-zenoh"))]
                            let calls = std::future::pending::<Result<(), ChannelSessionError>>();
                            #[cfg(feature = "event-consumer-zenoh")]
                            let consumers = async {
                                match &mut self.consumers {
                                    Some(p) => p.run_on(&channel).await,
                                    None => std::future::pending().await,
                                }
                            };
                            #[cfg(not(feature = "event-consumer-zenoh"))]
                            let consumers =
                                std::future::pending::<Result<(), ChannelSessionError>>();
                            tokio::select! { result = calls => result, result = consumers => result }
                        };
                        tokio::pin!(plans);
                        loop {
                            roles.send_replace(role_status.borrow_and_update().clone());
                            #[cfg(feature = "event-publication-zenoh")]
                            publication
                                .send_replace(channel_publication.borrow_and_update().clone());
                            if *stopped.borrow() {
                                break Ok(());
                            }
                            match channel.status() {
                                ChannelSupervisorStatus::CleanupFailed => {
                                    break Err(ChannelSessionError::CleanupFailed)
                                }
                                ChannelSupervisorStatus::Stopping { reason }
                                | ChannelSupervisorStatus::Closed { reason } => {
                                    break Err(match reason {
                                        ChannelCloseReason::AuthorityExpired => {
                                            ChannelSessionError::AuthorityExpired
                                        }
                                        ChannelCloseReason::RotationFailed => {
                                            ChannelSessionError::RotationFailed
                                        }
                                        ChannelCloseReason::CleanupFailed => {
                                            ChannelSessionError::CleanupFailed
                                        }
                                        ChannelCloseReason::ConnectionClosed => {
                                            ChannelSessionError::Transport
                                        }
                                        _ => ChannelSessionError::Closed,
                                    });
                                }
                                _ => {}
                            }
                            // Provider is the first role. Terminal loss of common registration
                            // cannot be healed by replaying only Consumer/Call enrollment.
                            if channel
                                .role_statuses()
                                .first()
                                .is_some_and(|r| r.state == RoleLifecycleState::Stopped)
                            {
                                break Err(ChannelSessionError::AuthorityExpired);
                            }
                            tokio::select! {
                                _ = stop_requested(&mut stopped) => break Ok(()),
                                result = &mut plans => break result,
                                result = channel_status.changed() => if result.is_err() { break Err(ChannelSessionError::CleanupFailed); },
                                result = role_status.changed() => if result.is_err() { break Err(ChannelSessionError::CleanupFailed); },
                                _ = publication_changed(
                                    #[cfg(feature = "event-publication-zenoh")] &mut channel_publication
                                ) => {},
                            }
                        }
                    };
                    // Clear the logical publisher before joining old sessions. In-flight
                    // sends retain their exact original client and idempotency identity.
                    #[cfg(feature = "event-publication-zenoh")]
                    publication.send_replace(None);
                    roles.send_replace(Vec::new());
                    channel.close().await?;
                    #[cfg(feature = "service-call-zenoh")]
                    if let Some(plan) = &mut self.calls {
                        plan.reset_after_close();
                    }
                    #[cfg(feature = "event-consumer-zenoh")]
                    if let Some(plan) = &mut self.consumers {
                        plan.reset_after_close();
                    }
                    if *stopped.borrow() {
                        return Ok(());
                    }
                    match result {
                        Ok(()) => return Ok(()),
                        Err(error) => NativeRuntimeError::Channel(error),
                    }
                }
                Err(error) => error,
            };
            if !error.recoverable() {
                return Err(error);
            }
            self.connection.ensure_open()?;
            status.send_replace(NativeRuntimeStatus::Recovering { error });
            // Bounded rate for control-owner recovery, never catalog or business polling.
            let delay = Duration::from_secs(1u64 << failures.min(5));
            failures = failures.saturating_add(1);
            tokio::select! { _ = stop_requested(&mut stopped) => return Ok(()), _ = tokio::time::sleep(delay) => {} }
        }
    }
}
async fn stop_requested(stopped: &mut watch::Receiver<bool>) {
    loop {
        if *stopped.borrow_and_update() || stopped.changed().await.is_err() {
            return;
        }
    }
}
async fn publication_changed(
    #[cfg(feature = "event-publication-zenoh")] current: &mut watch::Receiver<
        Option<Arc<publication::NativePublicationClient>>,
    >,
) {
    #[cfg(feature = "event-publication-zenoh")]
    if current.changed().await.is_ok() {
        return;
    }
    std::future::pending::<()>().await;
}
/// Drop requests shutdown. `close` joins cleanup and is cancellation-safe.
pub struct ManagedRegisteredService {
    stop: watch::Sender<bool>,
    status: watch::Receiver<NativeRuntimeStatus>,
    roles: watch::Receiver<Vec<ChannelRoleStatus>>,
    task: Option<JoinHandle<Result<(), NativeRuntimeError>>>,
    result: Option<Result<(), NativeRuntimeError>>,
    #[cfg(feature = "event-publication-zenoh")]
    pub(crate) publication: publication::NativePublicationSource,
}
impl ManagedRegisteredService {
    pub fn status(&self) -> NativeRuntimeStatus {
        let status = self.status.borrow().clone();
        if self.task.as_ref().is_some_and(|t| t.is_finished())
            && !matches!(
                status,
                NativeRuntimeStatus::Closed | NativeRuntimeStatus::Failed { .. }
            )
        {
            NativeRuntimeStatus::Failed {
                error: ChannelSessionError::CleanupFailed.into(),
            }
        } else {
            status
        }
    }
    pub fn subscribe_status(&self) -> watch::Receiver<NativeRuntimeStatus> {
        self.status.clone()
    }
    pub fn subscribe_roles(&self) -> watch::Receiver<Vec<ChannelRoleStatus>> {
        self.roles.clone()
    }
    pub async fn close(&mut self) -> Result<(), NativeRuntimeError> {
        self.stop.send_replace(true);
        if let Some(task) = self.task.as_mut() {
            self.result = Some(
                task.await
                    .unwrap_or_else(|_| Err(ChannelSessionError::CleanupFailed.into())),
            );
            self.task.take();
        }
        self.result.clone().unwrap_or(Ok(()))
    }
}
impl Drop for ManagedRegisteredService {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recovery_never_treats_revocation_or_unknown_control_as_transient() {
        for error in [
            ServiceAuthError::Http(401),
            ServiceAuthError::Http(403),
            ServiceAuthError::Http(409),
            ServiceAuthError::InvalidResponse,
            ServiceAuthError::Closed,
        ] {
            assert!(!NativeRuntimeError::Bootstrap(error).recoverable());
        }
        for error in [
            ChannelSessionError::ControlRejected,
            ChannelSessionError::CleanupFailed,
            ChannelSessionError::InvalidResponse,
        ] {
            assert!(!NativeRuntimeError::Channel(error).recoverable());
        }
        assert!(NativeRuntimeError::Bootstrap(ServiceAuthError::Http(503)).recoverable());
        assert!(NativeRuntimeError::Channel(ChannelSessionError::AuthorityExpired).recoverable());
        // A plan can observe physical closure before the supervisor publishes
        // its terminal status. run() still rechecks the logical root and stop.
        assert!(NativeRuntimeError::Channel(ChannelSessionError::Closed).recoverable());
    }
    #[tokio::test]
    async fn already_requested_shutdown_does_not_wait_for_another_notification() {
        let (_, mut stopped) = watch::channel(true);
        tokio::time::timeout(Duration::from_secs(1), stop_requested(&mut stopped))
            .await
            .unwrap();
    }
}
