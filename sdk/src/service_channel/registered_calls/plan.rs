use super::*;
use crate::service_channel::{
    role_changes::RoleMutation, CapabilityActivationState as Activation,
    CapabilityActivationStatus as Status, ManagedRoleChannel,
};
use kish_lingshu_runtime_contract::service::{
    canonical_digest, CallDeclaration, CallRegistrationReceipt,
};
use std::sync::Arc;
use tokio::sync::watch;
trait RegistrationChannel: Sync {
    fn control(
        &self,
        command: Command,
    ) -> impl std::future::Future<Output = Result<Response, ChannelSessionError>> + Send;
    fn roles(&self) -> Vec<crate::service_channel::ChannelRoleStatus>;
    fn retire(
        &self,
        generation: String,
    ) -> impl std::future::Future<Output = Result<(), ChannelSessionError>> + Send;
    fn activate(
        &self,
        version: CallActivationVersion,
        registry: Arc<crate::services::ServiceRegistry>,
        budget: crate::ServiceExecutionBudget,
        asynchronous: bool,
    ) -> impl std::future::Future<
        Output = Result<crate::service_channel::ChannelRoleStatus, ChannelSessionError>,
    > + Send;
}
impl RegistrationChannel for ManagedRoleChannel {
    async fn control(&self, command: Command) -> Result<Response, ChannelSessionError> {
        self.call_registration_control(Control {
            call_registration: Protocol::V1,
            command,
        })
        .await
    }
    fn roles(&self) -> Vec<crate::service_channel::ChannelRoleStatus> {
        self.role_statuses()
    }
    async fn retire(&self, generation: String) -> Result<(), ChannelSessionError> {
        self.deregister_role(
            kish_lingshu_foundation_contract::service_transport::RouteIdentity::new(generation)
                .map_err(|_| ChannelSessionError::InvalidResponse)?,
        )
        .await
        .map(|_| ())
    }
    async fn activate(
        &self,
        version: CallActivationVersion,
        registry: Arc<crate::services::ServiceRegistry>,
        budget: crate::ServiceExecutionBudget,
        asynchronous: bool,
    ) -> Result<crate::service_channel::ChannelRoleStatus, ChannelSessionError> {
        let bytes = serde_json::to_vec(registry.manifest())
            .map_err(|_| ChannelSessionError::InvalidConfig)?
            .len();
        self.commands
            .send(
                RoleMutation::Call {
                    node_id: version.node_id.clone(),
                    registration: Some(version),
                    registry,
                    budget,
                    asynchronous,
                },
                bytes,
            )
            .await
    }
}
struct Desired {
    update_pending: bool,
    update_sent: bool,
    expected_revision: Option<u64>,
    remove_requested: bool,
    retire_before_update: bool,
    declaration: CallDeclaration,
    operation: String,
    connection: Option<(String, String)>,
    receipt: Option<CallRegistrationReceipt>,
    attempted: Option<CallActivationVersion>,
    registry: Arc<crate::services::ServiceRegistry>,
    status: Status,
    verified: bool,
}
/// One registration per bound immutable operation. The Provider must be enrolled
/// first. This plan waits on platform hints, physical changes and role state;
/// it does not poll unpublished operations or execute/replay application work.
struct PlanUpdate {
    plan: Box<RegisteredCallPlan>,
    result: tokio::sync::oneshot::Sender<Result<(), ChannelSessionError>>,
}
/// Bounded replacement of desired Call declarations. Acceptance records local
/// intent; subscribe to the plan for remote activation and failure status.
#[derive(Clone)]
pub struct RegisteredCallUpdates {
    application: String,
    sender: tokio::sync::mpsc::Sender<PlanUpdate>,
}
impl RegisteredCallUpdates {
    pub async fn replace(&self, plan: RegisteredCallPlan) -> Result<(), ChannelSessionError> {
        if plan.application != self.application {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let (result, response) = tokio::sync::oneshot::channel();
        self.sender
            .try_send(PlanUpdate {
                plan: Box::new(plan),
                result,
            })
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    ChannelSessionError::CapacityExceeded
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => ChannelSessionError::Closed,
            })?;
        response.await.map_err(|_| ChannelSessionError::Closed)?
    }
}
pub struct RegisteredCallPlan {
    registry: Arc<crate::services::ServiceRegistry>,
    updates: tokio::sync::mpsc::Receiver<PlanUpdate>,
    update_sender: tokio::sync::mpsc::Sender<PlanUpdate>,
    application: String,
    desired: Vec<Desired>,
    budget: crate::ServiceExecutionBudget,
    asynchronous: bool,
    status: watch::Sender<Vec<Status>>,
}
impl RegisteredCallPlan {
    pub fn new(
        node_id: &str,
        registry: Arc<crate::services::ServiceRegistry>,
        budget: crate::ServiceExecutionBudget,
        asynchronous: bool,
        lane_count: u8,
    ) -> Result<Self, ChannelSessionError> {
        kish_lingshu_foundation_contract::service_transport::RouteIdentity::new(node_id)
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let mut desired = Vec::new();
        let mut bytes = 0;
        for capability in registry.capabilities().into_iter().filter(|c| c.call) {
            let op = &capability.operation;
            let projected = Arc::new(
                registry
                    .select_operation(op)
                    .map_err(|_| ChannelSessionError::InvalidConfig)?,
            );
            bytes += serde_json::to_vec(projected.manifest())
                .map_err(|_| ChannelSessionError::InvalidConfig)?
                .len();
            if desired.len() >= 1023 || bytes > 8 * 1024 * 1024 {
                return Err(ChannelSessionError::CapacityExceeded);
            }
            let declaration = CallDeclaration {
                node_id: format!(
                    "operation-{}",
                    canonical_digest(&(node_id, op))
                        .map_err(|_| ChannelSessionError::InvalidConfig)?
                ),
                maximum_in_flight: budget.maximum_in_flight(),
                lane_count,
                capabilities: projected.capabilities(),
            };
            declaration
                .validate()
                .map_err(|_| ChannelSessionError::InvalidConfig)?;
            desired.push(Desired {
                update_pending: false,
                update_sent: false,
                expected_revision: None,
                remove_requested: false,
                retire_before_update: false,
                declaration,
                operation: uuid::Uuid::new_v4().to_string(),
                connection: None,
                receipt: None,
                attempted: None,
                registry: projected,
                verified: false,
                status: Status {
                    key: format!(
                        "call:{}/{}/{}:{}",
                        op.service_key, op.operation_key, op.version, op.contract_digest
                    ),
                    state: Activation::Pending,
                    role_generation: None,
                    ready_until: None,
                    last_error: None,
                },
            });
        }
        let (status, _) = watch::channel(desired.iter().map(|d| d.status.clone()).collect());
        let (update_sender, updates) = tokio::sync::mpsc::channel(1);
        Ok(Self {
            registry: registry.clone(),
            updates,
            update_sender,
            application: registry.manifest().application_id.clone(),
            desired,
            budget,
            asynchronous,
            status,
        })
    }
    pub fn updates(&self) -> RegisteredCallUpdates {
        RegisteredCallUpdates {
            application: self.application.clone(),
            sender: self.update_sender.clone(),
        }
    }
    fn replace_desired(&mut self, mut next: Self) -> Result<(), ChannelSessionError> {
        if self.application != next.application {
            return Err(ChannelSessionError::InvalidConfig);
        }
        if self.desired.iter().any(|e| {
            e.update_pending || e.remove_requested || e.connection.is_some() && e.receipt.is_none()
        }) {
            return Err(ChannelSessionError::ControlRejected);
        }
        let additional = next
            .desired
            .iter()
            .filter(|n| {
                !self
                    .desired
                    .iter()
                    .any(|o| o.declaration.node_id == n.declaration.node_id)
            })
            .count();
        let bytes = self
            .desired
            .iter()
            .chain(&next.desired)
            .try_fold(0usize, |sum, e| {
                serde_json::to_vec(e.registry.manifest())
                    .map(|v| sum.saturating_add(v.len()))
                    .map_err(|_| ChannelSessionError::InvalidConfig)
            })?;
        if self.desired.len() + additional > 1023 || bytes > 8 * 1024 * 1024 {
            return Err(ChannelSessionError::CapacityExceeded);
        }
        let handlers_changed = !Arc::ptr_eq(&self.registry, &next.registry)
            || self.asynchronous != next.asynchronous
            || self.budget.maximum_in_flight() != next.budget.maximum_in_flight();
        for old in &mut self.desired {
            let Some(index) = next
                .desired
                .iter()
                .position(|n| n.declaration.node_id == old.declaration.node_id)
            else {
                old.remove_requested = true;
                old.retire_before_update = true;
                continue;
            };
            let new = next.desired.remove(index);
            if old.declaration == new.declaration && !handlers_changed {
                continue;
            }
            old.expected_revision = old.receipt.as_ref().map(|r| r.version.revision);
            old.operation = new.operation;
            old.declaration = new.declaration;
            old.registry = new.registry;
            old.update_pending = old.connection.is_some();
            old.update_sent = false;
            old.retire_before_update = true;
        }
        self.desired.extend(next.desired);
        self.registry = next.registry;
        self.budget = next.budget;
        self.asynchronous = next.asynchronous;
        Ok(())
    }
    fn needs_recovery(&self) -> bool {
        self.desired.iter().any(|e| {
            e.receipt.is_none()
                || e.update_pending
                || e.remove_requested
                || !e.verified && (e.status.role_generation.is_some() || e.attempted.is_some())
        })
    }
    pub fn subscribe(&self) -> watch::Receiver<Vec<Status>> {
        self.status.subscribe()
    }
    fn publish(&self) {
        self.status.send_if_modified(|current| {
            let next: Vec<_> = self.desired.iter().map(|d| d.status.clone()).collect();
            if *current == next {
                false
            } else {
                *current = next;
                true
            }
        });
    }
    pub async fn run(mut self, channel: &ManagedRoleChannel) -> Result<(), ChannelSessionError> {
        self.run_on(channel).await
    }
    pub(crate) fn application_id(&self) -> &str {
        &self.application
    }
    /// Only the logical owner calls this after joining the previous channel.
    /// A new instance incarnation fences every old registration and operation.
    pub(crate) fn reset_after_close(&mut self) {
        self.desired.retain(|entry| !entry.remove_requested);
        for entry in &mut self.desired {
            entry.update_pending = false;
            entry.update_sent = false;
            entry.expected_revision = None;
            entry.retire_before_update = false;
            entry.operation = uuid::Uuid::new_v4().to_string();
            entry.connection = None;
            entry.receipt = None;
            entry.attempted = None;
            entry.verified = false;
            entry.status.state = Activation::Pending;
            entry.status.role_generation = None;
            entry.status.ready_until = None;
            entry.status.last_error = None;
        }
        self.publish();
    }
    pub(crate) async fn run_on(
        &mut self,
        channel: &ManagedRoleChannel,
    ) -> Result<(), ChannelSessionError> {
        if self.application != channel.application_id {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let mut hints = channel.subscribe_consumer_registration_changes();
        let mut roles = channel.subscribe_roles();
        let mut connection = channel.subscribe_connectivity();
        self.reconcile(channel).await;
        let mut observed_hint: Option<(String, u64)> = None;
        loop {
            self.publish();
            tokio::select! {
                Some(update) = self.updates.recv() => {
                    let result = self.replace_desired(*update.plan);
                    let applied = result.is_ok();
                    let _ = update.result.send(result);
                    if applied { self.reconcile(channel).await; }
                },
                hint = hints.recv() => {
                    if matches!(hint, Err(tokio::sync::broadcast::error::RecvError::Closed)) {return Err(ChannelSessionError::Closed);}
                    if let Ok(hint) = &hint {
                        if !hint.owner_boot.starts_with("call-") || observed_hint.as_ref().is_some_and(|(boot, rev)| boot == &hint.owner_boot && *rev >= hint.directory_revision) {continue;}
                    }
                    if self.reconcile(channel).await {
                        if let Ok(hint) = hint {observed_hint = Some((hint.owner_boot, hint.directory_revision));}
                    }
                },
                changed = connection.changed() => {changed.map_err(|_| ChannelSessionError::Closed)?; observed_hint = None; self.reconcile(channel).await;},
                changed = roles.changed() => {
                    changed.map_err(|_| ChannelSessionError::Closed)?;
                    {
                    let observed = roles.borrow_and_update();
                    for entry in &mut self.desired {
                        if let Some(generation) = &entry.status.role_generation {
                            let role = observed.iter().find(|r| &r.role_generation == generation);
                            entry.status.state = if entry.verified && role.is_some_and(|r| r.route_confirmed()) {Activation::Active} else {Activation::Unavailable};
                            entry.status.ready_until = role.filter(|_| entry.verified).map(|r| r.authorization_deadline.min(r.route_deadline));
                        }
                    }
                    }
                    if self.needs_recovery() { self.reconcile(channel).await; }
                },
            }
        }
    }
    async fn reconcile(&mut self, channel: &impl RegistrationChannel) -> bool {
        let connection = match channel.control(Command::Negotiate).await {
            Ok(Response::Supported {
                owner_boot,
                connection_epoch,
                ..
            }) => (owner_boot, connection_epoch),
            _ => {
                for entry in &mut self.desired {
                    entry.verified = false;
                    entry.status.state = Activation::Unavailable;
                    entry.status.ready_until = None;
                }
                return false;
            }
        };
        let mut verified = true;
        let mut removed = Vec::new();
        for entry in &mut self.desired {
            if entry.status.role_generation.is_none() {
                entry.status.role_generation = channel
                    .roles()
                    .into_iter()
                    .find(|r| {
                        r.call_registration
                            .as_ref()
                            .is_some_and(|v| v.node_id == entry.declaration.node_id)
                    })
                    .map(|r| r.role_generation);
            }
            let new_connection = entry.connection.as_ref() != Some(&connection);
            if new_connection || entry.retire_before_update || entry.remove_requested {
                entry.verified = false;
                entry.status.ready_until = None;
                if let Some(generation) = &entry.status.role_generation {
                    if channel
                        .roles()
                        .iter()
                        .any(|r| &r.role_generation == generation)
                        && channel.retire(generation.clone()).await.is_err()
                    {
                        verified = false;
                        continue;
                    }
                    entry.status.role_generation = None;
                }
                entry.retire_before_update = false;
            }
            if entry.remove_requested {
                if new_connection {
                    removed.push(entry.declaration.node_id.clone());
                    continue;
                }
                match channel
                    .control(Command::Status {
                        node_id: entry.declaration.node_id.clone(),
                    })
                    .await
                {
                    Ok(Response::Registered { receipt })
                        if receipt.version.owner_boot == connection.0
                            && receipt.version.connection_epoch == connection.1 =>
                    {
                        if matches!(
                            channel
                                .control(Command::Remove {
                                    version: receipt.version
                                })
                                .await,
                            Ok(Response::Removed { .. })
                        ) {
                            removed.push(entry.declaration.node_id.clone());
                        } else {
                            verified = false;
                        }
                    }
                    Ok(Response::Rejected {
                        code:
                            kish_lingshu_runtime_contract::service::CallRegistrationRejection::NotFound,
                    }) => removed.push(entry.declaration.node_id.clone()),
                    _ => verified = false,
                }
                continue;
            }
            let response = if !new_connection && entry.update_pending {
                if !entry.update_sent {
                    entry.update_sent = true;
                    channel
                        .control(Command::Declare {
                            operation_id: entry.operation.clone(),
                            previous_connection_epoch: None,
                            expected_revision: entry.expected_revision,
                            declaration: entry.declaration.clone(),
                        })
                        .await
                } else {
                    channel
                        .control(Command::Lookup {
                            node_id: entry.declaration.node_id.clone(),
                            operation_id: entry.operation.clone(),
                        })
                        .await
                }
            } else if new_connection {
                let previous_connection_epoch = entry.connection.as_ref().map(|(_, e)| e.clone());
                entry.connection = Some(connection.clone());
                entry.receipt = None;
                entry.update_pending = false;
                entry.update_sent = false;
                entry.expected_revision = None;
                entry.attempted = None;
                entry.operation = uuid::Uuid::new_v4().to_string();
                let result = channel
                    .control(Command::Declare {
                        operation_id: entry.operation.clone(),
                        previous_connection_epoch,
                        expected_revision: None,
                        declaration: entry.declaration.clone(),
                    })
                    .await;
                match result {
                    Err(ChannelSessionError::Transport) => {
                        channel
                            .control(Command::Lookup {
                                node_id: entry.declaration.node_id.clone(),
                                operation_id: entry.operation.clone(),
                            })
                            .await
                    }
                    other => other,
                }
            } else if entry.receipt.is_none() {
                channel
                    .control(Command::Lookup {
                        node_id: entry.declaration.node_id.clone(),
                        operation_id: entry.operation.clone(),
                    })
                    .await
            } else {
                channel
                    .control(Command::Status {
                        node_id: entry.declaration.node_id.clone(),
                    })
                    .await
            };
            let receipt = match response {
                Ok(Response::Registered { receipt })
                    if receipt.version.owner_boot == connection.0
                        && receipt.version.connection_epoch == connection.1
                        && receipt.declaration == entry.declaration =>
                {
                    receipt
                }
                _ => {
                    entry.verified = false;
                    entry.status.state = Activation::Unavailable;
                    entry.status.ready_until = None;
                    verified = false;
                    continue;
                }
            };
            entry.update_pending = false;
            entry.update_sent = false;
            entry.verified = receipt.state == State::Active;
            if let Some(generation) = &entry.status.role_generation {
                let role = channel
                    .roles()
                    .into_iter()
                    .find(|r| &r.role_generation == generation);
                if entry.verified
                    && role.as_ref().is_some_and(|r| r.authorized())
                    && entry
                        .receipt
                        .as_ref()
                        .is_some_and(|old| old.version == receipt.version)
                {
                    entry.status.state = if role.as_ref().is_some_and(|r| r.route_confirmed()) {
                        Activation::Active
                    } else {
                        Activation::Unavailable
                    };
                    entry.status.ready_until =
                        role.map(|r| r.authorization_deadline.min(r.route_deadline));
                    entry.receipt = Some(receipt);
                    continue;
                }
                if channel.retire(generation.clone()).await.is_err() {
                    entry.status.state = Activation::Unavailable;
                    verified = false;
                    continue;
                }
                entry.status.role_generation = None;
                entry.status.ready_until = None;
                entry.verified = false;
                entry.receipt = Some(receipt);
                verified = false;
                continue;
            }
            entry.receipt = Some(receipt.clone());
            match receipt.state {
                State::Activating if entry.attempted.as_ref() != Some(&receipt.version) => {
                    entry.attempted = Some(receipt.version.clone());
                    let result = channel
                        .activate(
                            receipt.version,
                            entry.registry.clone(),
                            self.budget.clone(),
                            self.asynchronous,
                        )
                        .await;
                    match result {
                        Ok(role) => {
                            entry.verified = true;
                            entry.status.state = if role.route_confirmed() {
                                Activation::Active
                            } else {
                                Activation::Unavailable
                            };
                            entry.status.ready_until =
                                Some(role.authorization_deadline.min(role.route_deadline));
                            entry.status.role_generation = Some(role.role_generation);
                            entry.status.last_error = role.last_error;
                        }
                        Err(error) => {
                            verified = false;
                            entry.status.state = Activation::Failed;
                            entry.status.last_error = Some(error);
                        }
                    }
                }
                State::WaitingForCatalog => entry.status.state = Activation::WaitingForCatalog,
                _ => {
                    entry.status.state = Activation::Unavailable;
                    entry.status.ready_until = None;
                }
            }
        }
        self.desired
            .retain(|e| !removed.contains(&e.declaration.node_id));
        verified
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(all(test, feature = "http-client"))]
#[path = "host_tests.rs"]
mod host_tests;
