//! Optional managed admission for legacy Consumer registries sharing a Service host.
use super::{ConsumerError, EnrolledConsumerNode, EnrolledConsumerNodeStatus};
use crate::{ServiceAuthError, ServiceConnection, ServiceExecutionBudget};
use chrono::Utc;
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};

struct Group {
    capacity: Arc<Semaphore>,
    membership: RwLock<Option<watch::Receiver<EnrolledConsumerNodeStatus>>>,
}

/// Group membership and capacity are independent; all groups share the host's
/// total execution budget with native Service calls. No calls are queued here.
#[derive(Clone)]
pub struct ConsumerHttpAdmission {
    connection: ServiceConnection,
    total: ServiceExecutionBudget,
    groups: Arc<BTreeMap<String, Group>>,
    stopped: watch::Sender<bool>,
}

impl ConsumerHttpAdmission {
    pub fn new(
        connection: ServiceConnection,
        total: ServiceExecutionBudget,
        groups: impl IntoIterator<Item = (String, u32)>,
    ) -> Result<Self, ServiceAuthError> {
        let mut limits = BTreeMap::new();
        for (key, maximum) in groups {
            if key.trim().is_empty() || maximum == 0 || maximum as usize > Semaphore::MAX_PERMITS {
                return Err(ServiceAuthError::InvalidNodeConfig);
            }
            if limits
                .insert(
                    key,
                    Group {
                        capacity: Arc::new(Semaphore::new(maximum as usize)),
                        membership: RwLock::new(None),
                    },
                )
                .is_some()
            {
                return Err(ServiceAuthError::InvalidNodeConfig);
            }
        }
        Ok(Self {
            connection,
            total,
            groups: Arc::new(limits),
            stopped: watch::channel(false).0,
        })
    }

    /// Stop admission and cancel in-flight handlers before deregistering nodes.
    /// Other adapters sharing the connection and total budget remain available.
    pub fn stop(&self) {
        self.stopped.send_replace(true);
    }

    /// Attach only after successful enrollment. Unattached and expired groups
    /// reject work even if the inbound request has a valid platform signature.
    pub fn attach(&self, group: &str, node: &EnrolledConsumerNode) -> Result<(), ServiceAuthError> {
        if node.group_key() != group {
            return Err(ServiceAuthError::InvalidNodeConfig);
        }
        let target = self
            .groups
            .get(group)
            .ok_or(ServiceAuthError::InvalidNodeConfig)?;
        *target
            .membership
            .write()
            .map_err(|_| ServiceAuthError::InvalidNodeConfig)? = Some(node.subscribe());
        Ok(())
    }

    pub fn has_live_membership(&self) -> bool {
        !*self.stopped.borrow()
            && self.connection.ensure_open().is_ok()
            && self.groups.values().any(|group| {
                group
                    .membership
                    .read()
                    .is_ok_and(|status| status.as_ref().is_some_and(|s| live(&s.borrow())))
            })
    }

    pub(super) fn acquire(
        &self,
        group: Option<&str>,
    ) -> Result<ConsumerAdmissionGuard, ConsumerError> {
        if *self.stopped.borrow() {
            return Err(unavailable());
        }
        self.connection.ensure_open().map_err(|_| unavailable())?;
        // Older envelopes may omit group_key only when exactly one group exists.
        let group = match group {
            Some(key) => self.groups.get(key),
            None if self.groups.len() == 1 => self.groups.values().next(),
            None => None,
        }
        .ok_or_else(unavailable)?;
        let membership = group
            .membership
            .read()
            .ok()
            .and_then(|s| s.clone())
            .ok_or_else(unavailable)?;
        if !live(&membership.borrow()) {
            return Err(unavailable());
        }
        let total = self.total.acquire().map_err(|_| capacity())?;
        let role = group
            .capacity
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())?;
        Ok(ConsumerAdmissionGuard {
            _total: total,
            _role: role,
            membership,
            closed: self.connection.subscribe_closed(),
            stopped: self.stopped.subscribe(),
        })
    }
}

fn live(status: &EnrolledConsumerNodeStatus) -> bool {
    !status.is_terminal() && status.lease().lease_expires_at > Utc::now()
}
fn unavailable() -> ConsumerError {
    ConsumerError::retryable("event_not_ready", "Event membership unavailable")
}
fn capacity() -> ConsumerError {
    ConsumerError::throttled(
        "capacity_exhausted",
        "Service execution capacity exhausted",
        None,
    )
}

pub(super) struct ConsumerAdmissionGuard {
    _total: OwnedSemaphorePermit,
    _role: OwnedSemaphorePermit,
    membership: watch::Receiver<EnrolledConsumerNodeStatus>,
    closed: watch::Receiver<Option<ServiceAuthError>>,
    stopped: watch::Receiver<bool>,
}
impl ConsumerAdmissionGuard {
    pub(super) async fn stopped(&mut self) {
        loop {
            let status = self.membership.borrow_and_update().clone();
            if !live(&status) || self.closed.borrow().is_some() || *self.stopped.borrow() {
                return;
            }
            let remaining = (status.lease().lease_expires_at - Utc::now())
                .to_std()
                .unwrap_or_default();
            tokio::select! {
                _ = self.closed.changed() => return,
                _ = self.stopped.changed() => return,
                _ = tokio::time::sleep(remaining) => return,
                result = self.membership.changed() => if result.is_err() { return; },
            }
        }
    }
}
