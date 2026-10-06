//! Explicit mutations are queued to the existing serial role lifecycle owner.
//! No caller chooses platform routes, certificate lineage or base generations.
use super::observation::{self, CommandReservation, Plane, Rejection};
use super::{ChannelRoleStatus, ChannelSessionError, ManagedRoleChannel};
use kish_lingshu_foundation_contract::service_transport::RouteIdentity;
use kish_lingshu_runtime_contract::provider::ProviderCatalog;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, Semaphore},
    time::Instant,
};

pub(super) const MAX_ROLE_COMMANDS: usize = 16;
const MAX_COMMAND_BYTES: usize = 8 * 1024 * 1024;
const COMMAND_BUDGET: Duration = Duration::from_secs(10);

pub(super) enum RoleMutation {
    Provider(ProviderCatalog),
    Remove(RouteIdentity),
    #[cfg(feature = "service-call-zenoh")]
    Call {
        node_id: String,
        registry: Arc<crate::services::ServiceRegistry>,
        budget: crate::ServiceExecutionBudget,
        asynchronous: bool,
    },
    #[cfg(feature = "event-consumer-zenoh")]
    Consumer {
        group_key: String,
        node_id: String,
        maximum_in_flight: u32,
        registry: Arc<crate::event_dispatch::ConsumerRegistry>,
        budget: crate::ServiceExecutionBudget,
    },
}
pub(super) struct RoleCommand {
    pub mutation: RoleMutation,
    pub deadline: Instant,
    pub response: oneshot::Sender<Result<ChannelRoleStatus, ChannelSessionError>>,
    reservation: CommandReservation,
}
impl RoleCommand {
    pub(super) fn start_processing(&mut self) {
        self.reservation.start_processing();
    }
}
pub(super) struct RoleCommands {
    pub sender: mpsc::Sender<RoleCommand>,
    bytes: Arc<Semaphore>,
}
impl RoleCommands {
    pub(super) fn new(sender: mpsc::Sender<RoleCommand>) -> Self {
        Self {
            sender,
            bytes: Arc::new(Semaphore::new(MAX_COMMAND_BYTES)),
        }
    }
    async fn send(
        &self,
        mutation: RoleMutation,
        bytes: usize,
    ) -> Result<ChannelRoleStatus, ChannelSessionError> {
        if bytes > MAX_COMMAND_BYTES {
            observation::rejected(Plane::RoleChange, Rejection::Invalid);
            return Err(ChannelSessionError::InvalidConfig);
        }
        let permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes.max(1) as u32)
            .map_err(|_| {
                observation::rejected(Plane::RoleChange, Rejection::BytesExhausted);
                ChannelSessionError::CapacityExceeded
            })?;
        let reservation = CommandReservation::new(permit, bytes.max(1) as u32);
        let deadline = Instant::now() + COMMAND_BUDGET;
        let (response, result) = oneshot::channel();
        self.sender
            .try_send(RoleCommand {
                mutation,
                deadline,
                response,
                reservation,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    observation::rejected(Plane::RoleChange, Rejection::QueueFull);
                    ChannelSessionError::CapacityExceeded
                }
                mpsc::error::TrySendError::Closed(_) => {
                    observation::rejected(Plane::RoleChange, Rejection::QueueClosed);
                    ChannelSessionError::Closed
                }
            })?;
        tokio::time::timeout_at(deadline, result)
            .await
            .map_err(|_| ChannelSessionError::Transport)?
            .map_err(|_| ChannelSessionError::Transport)?
    }
}
impl ManagedRoleChannel {
    /// Explicitly enroll and retain a new immutable catalog on the current pool.
    /// Cancelling after acceptance can leave an owned role; inspect role statuses
    /// before deciding on another operation. There is no automatic replay.
    pub async fn enroll_provider(
        &self,
        catalog: ProviderCatalog,
    ) -> Result<ChannelRoleStatus, ChannelSessionError> {
        if catalog.application_id != self.application_id {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let bytes = serde_json::to_vec(&catalog)
            .map_err(|_| ChannelSessionError::InvalidConfig)?
            .len();
        if bytes > 4 * 1024 * 1024 {
            return Err(ChannelSessionError::InvalidConfig);
        }
        self.commands
            .send(RoleMutation::Provider(catalog), bytes)
            .await
    }
    /// Remove only this exact current generation; siblings continue renewing.
    pub async fn deregister_role(
        &self,
        generation: RouteIdentity,
    ) -> Result<ChannelRoleStatus, ChannelSessionError> {
        let bytes = generation.as_str().len();
        self.commands
            .send(RoleMutation::Remove(generation), bytes)
            .await
    }
    /// Reuse the unchanged typed service registry and shared execution budget.
    #[cfg(feature = "service-call-zenoh")]
    pub async fn enroll_service(
        &self,
        node_id: impl Into<String>,
        registry: Arc<crate::services::ServiceRegistry>,
        budget: crate::ServiceExecutionBudget,
        asynchronous: bool,
    ) -> Result<ChannelRoleStatus, ChannelSessionError> {
        let node_id = node_id.into();
        if registry.manifest().application_id != self.application_id
            || RouteIdentity::new(node_id.clone()).is_err()
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let bytes = serde_json::to_vec(registry.manifest())
            .map_err(|_| ChannelSessionError::InvalidConfig)?
            .len()
            + node_id.len();
        self.commands
            .send(
                RoleMutation::Call {
                    node_id,
                    registry,
                    budget,
                    asynchronous,
                },
                bytes,
            )
            .await
    }
    /// Dispatch still owns group selection, retry and completion. This method
    /// creates one typed Sync consumer binding, sharing the provided Call budget.
    #[cfg(feature = "event-consumer-zenoh")]
    pub async fn enroll_consumer(
        &self,
        group_key: impl Into<String>,
        node_id: impl Into<String>,
        maximum_in_flight: u32,
        registry: Arc<crate::event_dispatch::ConsumerRegistry>,
        budget: crate::ServiceExecutionBudget,
    ) -> Result<ChannelRoleStatus, ChannelSessionError> {
        let group_key = group_key.into();
        let node_id = node_id.into();
        if registry.app_id() != self.application_id
            || !registry.supports_group(&group_key)
            || maximum_in_flight == 0
            || RouteIdentity::new(node_id.clone()).is_err()
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let bytes = group_key.len() + node_id.len() + 256;
        self.commands
            .send(
                RoleMutation::Consumer {
                    group_key,
                    node_id,
                    maximum_in_flight,
                    registry,
                    budget,
                },
                bytes,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn remove() -> RoleMutation {
        RoleMutation::Remove(RouteIdentity::new("role").unwrap())
    }
    #[tokio::test(start_paused = true)]
    async fn command_timeout_and_cancel_keep_one_request_and_release_queue_bytes_after_cleanup() {
        let capture = super::super::observation::tests::Capture::default();
        let (sender, mut receiver) = mpsc::channel(MAX_ROLE_COMMANDS);
        let commands = Arc::new(RoleCommands::new(sender));
        let mut waiting = Vec::new();
        for _ in 0..2 {
            let owner = commands.clone();
            let metrics = capture.clone();
            waiting.push(tokio::spawn(async move {
                metrics
                    .observe(owner.send(remove(), MAX_COMMAND_BYTES / 2))
                    .await
            }));
        }
        tokio::task::yield_now().await;
        assert_eq!(
            capture
                .observe(commands.send(remove(), 1))
                .await
                .unwrap_err(),
            ChannelSessionError::CapacityExceeded
        );
        assert_eq!(
            capture.sum("lingshu_sdk_channel_queued_commands", None, None),
            2.0
        );
        let mut first = receiver.recv().await.unwrap();
        let mut second = receiver.recv().await.unwrap();
        first.start_processing();
        first.start_processing();
        second.start_processing();
        assert_eq!(
            capture.sum("lingshu_sdk_channel_queued_commands", None, None),
            0.0
        );
        waiting[0].abort();
        let _ = (&mut waiting[0]).await;
        assert!(first.response.is_closed());
        assert_eq!(
            capture.sum("lingshu_sdk_channel_reserved_commands", None, None),
            2.0
        );
        assert_eq!(
            capture.sum("lingshu_sdk_channel_command_reserved_bytes", None, None),
            MAX_COMMAND_BYTES as f64
        );
        assert_eq!(
            commands.bytes.available_permits(),
            0,
            "waiting cancellation cannot release queued ownership early"
        );
        drop(first);
        tokio::time::advance(COMMAND_BUDGET).await;
        assert_eq!(
            (&mut waiting[1]).await.unwrap().unwrap_err(),
            ChannelSessionError::Transport
        );
        assert!(second.response.is_closed());
        assert_eq!(
            capture.sum("lingshu_sdk_channel_reserved_commands", None, None),
            1.0
        );
        assert!(
            receiver.try_recv().is_err(),
            "timeout must not enqueue a retry"
        );
        drop(second);
        assert_eq!(commands.bytes.available_permits(), MAX_COMMAND_BYTES);
        assert_eq!(
            capture.count("lingshu_sdk_channel_command_queue_wait_seconds"),
            2
        );
        assert_eq!(
            capture.sum(
                "lingshu_sdk_channel_admission_rejections_total",
                Some("role_change"),
                Some("bytes_exhausted")
            ),
            1.0
        );
        capture.assert_balanced_resource_gauges();
        capture.assert_bounded_labels();
    }
    #[tokio::test]
    async fn full_command_queue_rejects_without_native_work_or_leaking_capacity() {
        let capture = super::super::observation::tests::Capture::default();
        let (sender, mut receiver) = mpsc::channel(MAX_ROLE_COMMANDS);
        let commands = Arc::new(RoleCommands::new(sender));
        let mut tasks = Vec::new();
        for _ in 0..MAX_ROLE_COMMANDS {
            let owner = commands.clone();
            let metrics = capture.clone();
            tasks.push(tokio::spawn(async move {
                metrics.observe(owner.send(remove(), 64)).await
            }));
        }
        for _ in 0..MAX_ROLE_COMMANDS {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            capture
                .observe(commands.send(remove(), 64))
                .await
                .unwrap_err(),
            ChannelSessionError::CapacityExceeded
        );
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        receiver.close();
        while let Ok(command) = receiver.try_recv() {
            assert!(command.response.is_closed());
        }
        assert_eq!(commands.bytes.available_permits(), MAX_COMMAND_BYTES);
        assert_eq!(
            capture
                .observe(commands.send(remove(), 64))
                .await
                .unwrap_err(),
            ChannelSessionError::Closed
        );
        for reason in ["queue_full", "queue_closed"] {
            assert_eq!(
                capture.sum(
                    "lingshu_sdk_channel_admission_rejections_total",
                    Some("role_change"),
                    Some(reason)
                ),
                1.0
            );
        }
        assert_eq!(
            capture.count("lingshu_sdk_channel_command_queue_wait_seconds"),
            0
        );
        capture.assert_balanced_resource_gauges();
        capture.assert_bounded_labels();
    }
}
