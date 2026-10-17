//! One ephemeral application subscription, following the existing channel owner.
use super::{ChannelCloseReason, ChannelSessionError, ManagedRoleChannel, ServiceChannelSessions};
use crate::notifications::{
    Notification, NotificationDispatcher, NotificationError, NotificationResult,
    NotificationTransport as PublishTransport, Notifications,
};
use kish_lingshu_foundation_contract::notifications::MAX_NOTIFICATION_BYTES;
use kish_lingshu_foundation_contract::service_transport::notifications::application_notification_key;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::{watch, Semaphore};
use tokio::time::{Duration, Instant};

/// Cloneable logical handle. Signals coalesce; they are not a durable cursor.
#[derive(Clone)]
pub struct ApplicationNotifications {
    inner: Arc<Inner>,
    dispatcher: NotificationDispatcher,
}
struct Inner {
    current: Mutex<Option<Arc<NotificationTransport>>>,
    sending: Semaphore,
    bound: AtomicBool,
}
impl Default for ApplicationNotifications {
    fn default() -> Self {
        Self {
            dispatcher: NotificationDispatcher::default(),
            inner: Arc::new(Inner {
                current: Mutex::new(None),
                sending: Semaphore::new(1),
                bound: AtomicBool::new(false),
            }),
        }
    }
}
pub(super) struct NotificationOwner(ApplicationNotifications);
impl Drop for NotificationOwner {
    fn drop(&mut self) {
        self.0.inner.bound.store(false, Ordering::Release);
    }
}
impl ApplicationNotifications {
    pub(super) fn claim(&self) -> Result<NotificationOwner, ChannelSessionError> {
        self.inner
            .bound
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        Ok(NotificationOwner(self.clone()))
    }
    /// Shares the one application transport and the independent dispatcher.
    pub fn bus(&self) -> Notifications {
        Notifications::new(self.dispatcher.clone(), Arc::new(self.clone()))
    }
    pub fn is_connected(&self) -> bool {
        self.inner
            .current
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|c| c.active().is_ok())
    }
    /// Send once, without persistence, retries or delivery acknowledgement.
    pub async fn publish(&self, notification: &Notification) -> Result<(), ChannelSessionError> {
        let payload = notification
            .encode()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let _slot = self
            .inner
            .sending
            .try_acquire()
            .map_err(|_| ChannelSessionError::CapacityExceeded)?;
        let transport = self
            .inner
            .current
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or(ChannelSessionError::Closed)?;
        transport.active()?;
        tokio::time::timeout(
            Duration::from_secs(1),
            transport
                .session
                .put(transport.key.clone(), payload)
                .congestion_control(zenoh::qos::CongestionControl::Drop),
        )
        .await
        .map_err(|_| ChannelSessionError::Transport)?
        .map_err(|_| ChannelSessionError::Transport)
    }
    pub(super) async fn run_on(
        &self,
        channel: &ManagedRoleChannel,
    ) -> Result<(), ChannelSessionError> {
        struct Reset(ApplicationNotifications);
        impl Drop for Reset {
            fn drop(&mut self) {
                self.0
                    .inner
                    .current
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
            }
        }
        let _reset = Reset(self.clone());
        let mut transports = channel.notification_transport.clone();
        let mut connectivity = channel.subscribe_connectivity();
        loop {
            let current = transports.borrow_and_update().clone();
            let Some(current) = current else {
                // A rotation temporarily drains the old pool before publishing
                // its replacement; do not restart the whole runtime in this gap.
                transports
                    .changed()
                    .await
                    .map_err(|_| ChannelSessionError::Closed)?;
                continue;
            };
            let output = self.clone();
            let validation = current.clone();
            let subscriber = current
                .session
                .declare_subscriber(current.key.clone())
                .callback(move |sample| {
                    if validation.active().is_ok()
                        && sample.key_expr().as_str() == validation.key
                        && sample.kind() == zenoh::sample::SampleKind::Put
                        && sample.payload().len() <= MAX_NOTIFICATION_BYTES
                    {
                        let _ = output
                            .dispatcher
                            .receive(sample.payload().to_bytes().as_ref());
                    }
                })
                .await
                .map_err(|_| ChannelSessionError::Transport)?;
            *self.inner.current.lock().unwrap_or_else(|e| e.into_inner()) = Some(current);
            self.dispatcher.transport_ready();
            let changed = loop {
                tokio::select! {
                    changed = transports.changed() => break changed,
                    changed = connectivity.changed() => {
                        changed.map_err(|_| ChannelSessionError::Closed)?;
                        let status = connectivity.borrow_and_update();
                        if !status.closed && status.connected_data_lanes.first() == Some(&true) { self.dispatcher.transport_ready(); }
                    }
                }
            };
            self.inner
                .current
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            // The supervisor may already have closed the retired Session.
            // Cleanup of a lossy subscription must not fail the shared runtime.
            let _ = tokio::time::timeout(Duration::from_secs(1), subscriber.undeclare()).await;
            changed.map_err(|_| ChannelSessionError::Closed)?;
        }
    }
}
pub(super) struct NotificationTransport {
    session: zenoh::Session,
    key: String,
    connection: crate::ServiceConnection,
    closed: watch::Receiver<Option<ChannelCloseReason>>,
    authority: watch::Receiver<Instant>,
    connectivity: watch::Receiver<super::ChannelConnectivityStatus>,
}
impl NotificationTransport {
    fn active(&self) -> Result<(), ChannelSessionError> {
        if self.connection.ensure_open().is_err()
            || self.session.is_closed()
            || self.closed.borrow().is_some()
        {
            return Err(ChannelSessionError::Closed);
        }
        if self.connectivity.borrow().connected_data_lanes.first() != Some(&true) {
            return Err(ChannelSessionError::Transport);
        }
        if Instant::now() >= *self.authority.borrow() {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        Ok(())
    }
}
impl ServiceChannelSessions {
    pub(super) fn notification_transport(
        &self,
    ) -> Result<Arc<NotificationTransport>, ChannelSessionError> {
        self.active_role_channel()?;
        let initial = self.identity.bootstrap_response();
        Ok(Arc::new(NotificationTransport {
            session: self
                .sessions
                .first()
                .ok_or(ChannelSessionError::InvalidConfig)?
                .clone(),
            key: application_notification_key(&initial.deployment, &initial.application_id)
                .map_err(|_| ChannelSessionError::InvalidConfig)?
                .as_str()
                .into(),
            connectivity: self.subscribe_connectivity(),
            connection: self.identity.connection.clone(),
            closed: self.closed.clone(),
            authority: self.authority.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn disconnected_hints_are_not_retained() {
        let notifications = ApplicationNotifications::default();
        assert!(!notifications.is_connected());
        assert!(notifications.bus().notify("test.changed").await.is_err());
        assert_eq!(notifications.dispatcher.stats().invoked, 0);
    }
    #[test]
    fn handle_has_one_runtime_owner_until_it_closes() {
        let notifications = ApplicationNotifications::default();
        let owner = notifications.claim().unwrap();
        assert!(notifications.clone().claim().is_err());
        drop(owner);
        assert!(notifications.claim().is_ok());
    }
}

#[async_trait::async_trait]
impl PublishTransport for ApplicationNotifications {
    async fn publish(&self, notification: &Notification) -> NotificationResult {
        ApplicationNotifications::publish(self, notification)
            .await
            .map_err(|_| NotificationError::Unavailable)
    }
}
