//! Server-owned waiting: subscribe before registration, then reconcile only on
//! authenticated hints or role/connection changes. No missing-catalog retry loop.
use super::{
    CapabilityActivationState as State, CapabilityActivationStatus as Status, ChannelSessionError,
    ManagedRoleChannel,
};
use kish_lingshu_event_dispatch_contract::{
    ConsumerActivationState, ConsumerDeclarationUpdate, ConsumerRegistrationCommand as Command,
    ConsumerRegistrationControl, ConsumerRegistrationControlResponse as Response,
    ConsumerRegistrationProtocol, ConsumerRegistrationReceipt,
};
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone, Debug, PartialEq, Eq)]
struct RegistrationConnection {
    owner_boot: String,
    connection_epoch: String,
}

// Keep recovery policy testable independently of sockets. The production port
// always uses request-bound signed controls owned by ManagedRoleChannel.
trait RegistrationChannel: Sync {
    fn control(
        &self,
        command: Command,
    ) -> impl std::future::Future<Output = Result<Response, ChannelSessionError>> + Send;
    fn roles(&self) -> Vec<super::ChannelRoleStatus>;
    fn retire(
        &self,
        generation: String,
    ) -> impl std::future::Future<Output = Result<(), ChannelSessionError>> + Send;
    fn activate(
        &self,
        version: kish_lingshu_event_dispatch_contract::ConsumerActivationVersion,
        group: String,
        node: String,
        maximum: u32,
        registry: Arc<crate::event_dispatch::ConsumerRegistry>,
        budget: crate::ServiceExecutionBudget,
    ) -> impl std::future::Future<Output = Result<super::ChannelRoleStatus, ChannelSessionError>> + Send;
}
impl RegistrationChannel for ManagedRoleChannel {
    async fn control(&self, command: Command) -> Result<Response, ChannelSessionError> {
        self.consumer_registration_control(ConsumerRegistrationControl {
            consumer_registration: ConsumerRegistrationProtocol::V1,
            command,
        })
        .await
    }
    fn roles(&self) -> Vec<super::ChannelRoleStatus> {
        self.role_statuses()
    }
    async fn retire(&self, generation: String) -> Result<(), ChannelSessionError> {
        let generation =
            kish_lingshu_foundation_contract::service_transport::RouteIdentity::new(generation)
                .map_err(|_| ChannelSessionError::InvalidConfig)?;
        self.deregister_role(generation).await.map(|_| ())
    }
    async fn activate(
        &self,
        version: kish_lingshu_event_dispatch_contract::ConsumerActivationVersion,
        group: String,
        node: String,
        maximum: u32,
        registry: Arc<crate::event_dispatch::ConsumerRegistry>,
        budget: crate::ServiceExecutionBudget,
    ) -> Result<super::ChannelRoleStatus, ChannelSessionError> {
        self.activate_registered_consumer(version, group, node, maximum, registry, budget)
            .await
    }
}

struct Desired {
    update_pending: bool,
    update_sent: bool,
    remove_requested: bool,
    retire_before_update: bool,
    attempted_connection: Option<RegistrationConnection>,
    update: ConsumerDeclarationUpdate,
    node_id: String,
    receipt: Option<ConsumerRegistrationReceipt>,
    status: Status,
    authority_observed: bool,
    attempted_activation: Option<kish_lingshu_event_dispatch_contract::ConsumerActivationVersion>,
}
pub struct RegisteredConsumerPlan {
    application: String,
    desired: Vec<Desired>,
    registry: Arc<crate::event_dispatch::ConsumerRegistry>,
    budget: crate::ServiceExecutionBudget,
    status: watch::Sender<Vec<Status>>,
    updates: tokio::sync::mpsc::Receiver<PlanUpdate>,
    update_sender: tokio::sync::mpsc::Sender<PlanUpdate>,
}
/// Deployment selection and execution ceiling; governance still owns dispatch
/// policy. A binding never adds a Handler absent from the source registry.
pub struct RegisteredConsumerBinding {
    pub group_key: String,
    pub node_id: String,
    pub maximum_in_flight: u32,
}
struct PlanUpdate {
    plan: Box<RegisteredConsumerPlan>,
    result: tokio::sync::oneshot::Sender<Result<(), ChannelSessionError>>,
}
/// Bounded declaration updates, applied by the same serial registration owner.
#[derive(Clone)]
pub struct RegisteredConsumerUpdates {
    application: String,
    sender: tokio::sync::mpsc::Sender<PlanUpdate>,
}
impl RegisteredConsumerUpdates {
    /// Replace the desired catalog/handlers. Success means the local desired
    /// snapshot was accepted; observe capability statuses for remote activation.
    /// Cancellation after enqueue does not undo an accepted desired-state change.
    pub async fn replace(&self, plan: RegisteredConsumerPlan) -> Result<(), ChannelSessionError> {
        if plan.application != self.application {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let (result, received) = tokio::sync::oneshot::channel();
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
        received.await.map_err(|_| ChannelSessionError::Closed)?
    }
}
impl RegisteredConsumerPlan {
    pub fn new(
        catalog: &kish_lingshu_runtime_contract::provider::ProviderCatalog,
        node_id: &str,
        registry: Arc<crate::event_dispatch::ConsumerRegistry>,
        budget: crate::ServiceExecutionBudget,
    ) -> Result<Self, ChannelSessionError> {
        use kish_lingshu_runtime_contract::service::canonical_digest;
        kish_lingshu_foundation_contract::service_transport::RouteIdentity::new(node_id)
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let declarations = registry
            .registration_declarations(catalog)
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        if declarations.len()
            > kish_lingshu_event_dispatch_contract::MAX_REGISTERED_CONSUMERS_PER_CONNECTION
        {
            return Err(ChannelSessionError::CapacityExceeded);
        }
        let mut desired = Vec::new();
        let mut bytes = 0usize;
        for declaration in declarations {
            bytes += serde_json::to_vec(&declaration)
                .map_err(|_| ChannelSessionError::InvalidConfig)?
                .len();
            if bytes > 8 * 1024 * 1024 {
                return Err(ChannelSessionError::CapacityExceeded);
            }
            let registration_key = canonical_digest(&(
                &declaration.provider_key,
                &declaration.producer.package_name,
                &declaration.consumer.key,
            ))
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
            let derived_node = format!(
                "decl-{}",
                canonical_digest(&(node_id, &registration_key))
                    .map_err(|_| ChannelSessionError::InvalidConfig)?
            );
            desired.push(Desired {
                update_pending: false,
                update_sent: false,
                remove_requested: false,
                retire_before_update: false,
                attempted_connection: None,
                status: Status {
                    key: format!(
                        "consumer:{}:{}",
                        declaration.consumer.group_key, declaration.consumer.key
                    ),
                    state: State::Pending,
                    role_generation: None,
                    ready_until: None,
                    last_error: None,
                },
                update: ConsumerDeclarationUpdate {
                    protocol: ConsumerRegistrationProtocol::V1,
                    registration_key,
                    operation_id: uuid::Uuid::new_v4().to_string(),
                    expected_revision: None,
                    declaration,
                },
                node_id: derived_node,
                receipt: None,
                authority_observed: false,
                attempted_activation: None,
            });
        }
        let (status, _) =
            watch::channel(desired.iter().map(|entry| entry.status.clone()).collect());
        let (update_sender, updates) = tokio::sync::mpsc::channel(1);
        Ok(Self {
            updates,
            update_sender,
            application: catalog.application_id.clone(),
            desired,
            registry,
            budget,
            status,
        })
    }
    pub fn updates(&self) -> RegisteredConsumerUpdates {
        RegisteredConsumerUpdates {
            application: self.application.clone(),
            sender: self.update_sender.clone(),
        }
    }
    pub fn with_bindings(
        mut self,
        bindings: Vec<RegisteredConsumerBinding>,
    ) -> Result<Self, ChannelSessionError> {
        let mut groups = std::collections::BTreeMap::new();
        for binding in bindings {
            if binding.maximum_in_flight == 0
                || kish_lingshu_foundation_contract::service_transport::RouteIdentity::new(
                    &binding.node_id,
                )
                .is_err()
                || !self
                    .desired
                    .iter()
                    .any(|entry| entry.update.declaration.consumer.group_key == binding.group_key)
                || groups.insert(binding.group_key.clone(), binding).is_some()
            {
                return Err(ChannelSessionError::InvalidConfig);
            }
        }
        self.desired
            .retain(|entry| groups.contains_key(&entry.update.declaration.consumer.group_key));
        for entry in &mut self.desired {
            let binding = &groups[&entry.update.declaration.consumer.group_key];
            entry.node_id = format!(
                "decl-{}",
                kish_lingshu_runtime_contract::service::canonical_digest(&(
                    &binding.node_id,
                    &entry.update.registration_key
                ))
                .map_err(|_| ChannelSessionError::InvalidConfig)?
            );
            entry.update.declaration.consumer.maximum_concurrency = entry
                .update
                .declaration
                .consumer
                .maximum_concurrency
                .min(binding.maximum_in_flight);
        }
        self.publish();
        Ok(self)
    }
    fn replace_desired(&mut self, mut next: Self) -> Result<(), ChannelSessionError> {
        if next.application != self.application {
            return Err(ChannelSessionError::InvalidConfig);
        }
        // Never replace an uncertain operation with a new operation ID.
        if self.desired.iter().any(|entry| {
            entry.update_pending
                || entry.remove_requested
                || entry.attempted_connection.is_some() && entry.receipt.is_none()
        }) {
            return Err(ChannelSessionError::ControlRejected);
        }
        let handlers_changed = !Arc::ptr_eq(&self.registry, &next.registry)
            || self.budget.maximum_in_flight() != next.budget.maximum_in_flight();
        let additional = next
            .desired
            .iter()
            .filter(|new| {
                !self
                    .desired
                    .iter()
                    .any(|old| old.update.registration_key == new.update.registration_key)
            })
            .count();
        if self.desired.len() + additional
            > kish_lingshu_event_dispatch_contract::MAX_REGISTERED_CONSUMERS_PER_CONNECTION
        {
            return Err(ChannelSessionError::CapacityExceeded);
        }
        if next.desired.iter().any(|new| {
            self.desired.iter().any(|old| {
                old.update.registration_key == new.update.registration_key
                    && old.node_id != new.node_id
            })
        }) {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let retained_bytes = self
            .desired
            .iter()
            .map(|d| {
                serde_json::to_vec(&d.update.declaration).map_or(usize::MAX / 2048, |v| v.len())
            })
            .sum::<usize>();
        let new_bytes = next
            .desired
            .iter()
            .map(|d| {
                serde_json::to_vec(&d.update.declaration).map_or(usize::MAX / 2048, |v| v.len())
            })
            .sum::<usize>();
        if retained_bytes.saturating_add(new_bytes) > 8 * 1024 * 1024 {
            return Err(ChannelSessionError::CapacityExceeded);
        }
        for old in &mut self.desired {
            let Some(index) = next
                .desired
                .iter()
                .position(|new| new.update.registration_key == old.update.registration_key)
            else {
                old.remove_requested = true;
                old.retire_before_update = true;
                continue;
            };
            let new = next.desired.remove(index);
            if old.node_id != new.node_id {
                return Err(ChannelSessionError::InvalidConfig);
            }
            if old.update.declaration == new.update.declaration && !handlers_changed {
                continue;
            }
            old.retire_before_update = handlers_changed
                || !old
                    .update
                    .declaration
                    .same_execution_contract(&new.update.declaration);
            old.update.expected_revision =
                old.receipt.as_ref().map(|r| r.version.declaration_revision);
            old.update.operation_id = new.update.operation_id;
            old.update.declaration = new.update.declaration;
            old.update_pending = old.attempted_connection.is_some();
            old.update_sent = false;
        }
        self.desired.extend(next.desired);
        self.registry = next.registry;
        self.budget = next.budget;
        Ok(())
    }
    pub fn subscribe(&self) -> watch::Receiver<Vec<Status>> {
        self.status.subscribe()
    }
    fn publish(&self) {
        let next: Vec<_> = self
            .desired
            .iter()
            .map(|entry| entry.status.clone())
            .collect();
        self.status.send_if_modified(|current| {
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
    pub(crate) fn reset_after_close(&mut self) {
        self.desired.retain(|entry| !entry.remove_requested);
        for entry in &mut self.desired {
            entry.update_pending = false;
            entry.update_sent = false;
            entry.retire_before_update = false;
            entry.attempted_connection = None;
            entry.update.expected_revision = None;
            entry.update.operation_id = uuid::Uuid::new_v4().to_string();
            entry.receipt = None;
            entry.authority_observed = false;
            entry.attempted_activation = None;
            entry.status.state = State::Pending;
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
        let mut owner_boot = String::new();
        // Subscribe before the first mutation. A lost result is recovered by
        // read-only Lookup; no enrollment is blindly replayed.
        self.reconcile(channel, &mut owner_boot).await;
        let mut observed_hint: Option<(String, u64)> = None;
        loop {
            self.publish();
            tokio::select! {
                Some(update) = self.updates.recv() => {
                    let result = self.replace_desired(*update.plan);
                    let applied = result.is_ok();
                    let _ = update.result.send(result);
                    if applied { self.reconcile(channel, &mut owner_boot).await; }
                },
                hint = hints.recv() => {
                    let hint = match hint {
                        Ok(hint) => {
                            if hint.owner_boot.starts_with("call-") { continue; }
                            if observed_hint.as_ref().is_some_and(|(boot, revision)| boot == &hint.owner_boot && *revision >= hint.directory_revision) {
                                continue;
                            }
                            Some(hint)
                        },
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => None,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return Err(ChannelSessionError::Closed),
                    };
                    if self.reconcile(channel, &mut owner_boot).await {
                        if let Some(hint) = hint.filter(|hint| hint.owner_boot == owner_boot) {
                            observed_hint = Some((hint.owner_boot, hint.directory_revision));
                        }
                    }
                },
                changed = roles.changed() => {
                    changed.map_err(|_| ChannelSessionError::Closed)?;
                    {
                    let observed = roles.borrow_and_update();
                    for entry in &mut self.desired {
                        if let Some(generation) = &entry.status.role_generation {
                            let role = observed.iter().find(|role| &role.role_generation == generation);
                            entry.status.ready_until = role.map(|role| role.authorization_deadline.min(role.route_deadline));
                            entry.status.state = if entry.authority_observed && role.is_some_and(|role| role.route_confirmed()) { State::Active } else { State::Unavailable };
                            if !entry.authority_observed { entry.status.ready_until = None; }
                        }
                    }
                    }
                    if self.desired.iter().any(|entry| entry.receipt.is_none() || entry.update_pending || entry.remove_requested || !entry.authority_observed && entry.attempted_activation.is_some()) {
                        self.reconcile(channel, &mut owner_boot).await;
                    }
                },
                changed = connection.changed() => {
                    changed.map_err(|_| ChannelSessionError::Closed)?;
                    observed_hint = None;
                    self.reconcile(channel, &mut owner_boot).await;
                },
            }
        }
    }
    async fn reconcile(
        &mut self,
        channel: &impl RegistrationChannel,
        owner_boot: &mut String,
    ) -> bool {
        let connection = match channel.control(Command::Negotiate).await {
            Ok(Response::Supported {
                owner_boot,
                connection_epoch,
                ..
            }) => RegistrationConnection {
                owner_boot,
                connection_epoch,
            },
            _ => {
                for entry in &mut self.desired {
                    entry.authority_observed = false;
                    entry.status.state = State::Unavailable;
                    entry.status.ready_until = None;
                }
                return false;
            }
        };
        *owner_boot = connection.owner_boot.clone();
        let mut verified = true;
        let mut removed = Vec::new();
        for entry in &mut self.desired {
            // An activation reply may have been lost after the serial owner
            // retained its handle. Recover that handle for fenced cleanup.
            if entry.status.role_generation.is_none() {
                entry.status.role_generation = channel
                    .roles()
                    .into_iter()
                    .find(|role| {
                        role.consumer_registration.as_ref().is_some_and(|version| {
                            version.registration_key == entry.update.registration_key
                        })
                    })
                    .map(|role| role.role_generation);
            }
            let new_connection = entry.attempted_connection.as_ref() != Some(&connection);
            if entry.remove_requested || entry.update_pending && entry.retire_before_update {
                entry.authority_observed = false;
                entry.status.state = State::Unavailable;
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
                    removed.push(entry.update.registration_key.clone());
                    continue;
                }
                match channel.control(Command::Status {registration_key: entry.update.registration_key.clone()}).await {
                    Ok(Response::Registered {receipt}) if receipt.version.owner_boot == connection.owner_boot && receipt.version.connection_epoch == connection.connection_epoch => {
                        if matches!(channel.control(Command::Remove {version: receipt.version}).await, Ok(Response::Removed {..})) {removed.push(entry.update.registration_key.clone());}
                        else {verified = false;}
                    },
                    Ok(Response::Rejected {code: kish_lingshu_event_dispatch_contract::ConsumerRegistrationRejection::NotFound}) => removed.push(entry.update.registration_key.clone()),
                    _ => verified = false,
                }
                continue;
            }
            let response = if !new_connection && entry.update_pending {
                if !entry.update_sent {
                    // Persist attempted state before I/O. A lost outcome is only looked up.
                    entry.update_sent = true;
                    channel
                        .control(Command::Declare {
                            previous_connection_epoch: None,
                            update: entry.update.clone(),
                        })
                        .await
                } else {
                    channel
                        .control(Command::Lookup {
                            registration_key: entry.update.registration_key.clone(),
                            operation_id: entry.update.operation_id.clone(),
                        })
                        .await
                }
            } else if new_connection {
                // One declaration replaces the platform connection for every
                // sibling. Compare every entry with the negotiated epoch,
                // rather than depending on each Status returning StaleConnection.
                entry.authority_observed = false;
                entry.status.state = State::Unavailable;
                entry.status.ready_until = None;
                if let Some(generation) = &entry.status.role_generation {
                    if channel
                        .roles()
                        .iter()
                        .any(|role| &role.role_generation == generation)
                        && channel.retire(generation.clone()).await.is_err()
                    {
                        verified = false;
                        continue;
                    }
                    entry.status.role_generation = None;
                }
                let previous_connection_epoch = entry
                    .attempted_connection
                    .as_ref()
                    .map(|old| old.connection_epoch.clone());
                entry.attempted_connection = Some(connection.clone());
                entry.receipt = None;
                entry.update_pending = false;
                entry.update_sent = false;
                entry.attempted_activation = None;
                entry.update.expected_revision = None;
                entry.update.operation_id = uuid::Uuid::new_v4().to_string();
                match channel
                    .control(Command::Declare {
                        previous_connection_epoch,
                        update: entry.update.clone(),
                    })
                    .await
                {
                    Err(ChannelSessionError::Transport) => {
                        channel
                            .control(Command::Lookup {
                                registration_key: entry.update.registration_key.clone(),
                                operation_id: entry.update.operation_id.clone(),
                            })
                            .await
                    }
                    other => other,
                }
            } else {
                channel
                    .control(Command::Status {
                        registration_key: entry.update.registration_key.clone(),
                    })
                    .await
            };
            let receipt = match response {
                Ok(Response::Registered { receipt })
                    if receipt.version.owner_boot == connection.owner_boot
                        && receipt.version.connection_epoch == connection.connection_epoch
                        && receipt.declaration_digest
                            == entry
                                .update
                                .declaration
                                .digest()
                                .expect("validated declaration") =>
                {
                    receipt
                }
                _ => {
                    entry.authority_observed = false;
                    entry.status.state = State::Unavailable;
                    entry.status.ready_until = None;
                    verified = false;
                    continue;
                }
            };
            entry.update_pending = false;
            entry.update_sent = false;
            entry.authority_observed = receipt.state == ConsumerActivationState::Active;
            if let Some(generation) = &entry.status.role_generation {
                let role = channel
                    .roles()
                    .into_iter()
                    .find(|role| &role.role_generation == generation);
                if role
                    .as_ref()
                    .is_some_and(|role| role.state == super::RoleLifecycleState::Active)
                    && receipt.state == ConsumerActivationState::Active
                    && entry
                        .receipt
                        .as_ref()
                        .is_some_and(|old| receipt.version.preserves_route_from(&old.version))
                {
                    let role = channel
                        .roles()
                        .into_iter()
                        .find(|role| &role.role_generation == generation);
                    entry.status.state = if role.as_ref().is_some_and(|role| role.route_confirmed())
                    {
                        State::Active
                    } else {
                        State::Unavailable
                    };
                    entry.status.ready_until =
                        role.map(|role| role.authorization_deadline.min(role.route_deadline));
                    entry.receipt = Some(receipt);
                    continue;
                }
                if let Err(error) = channel.retire(generation.clone()).await {
                    entry.status.state = State::Unavailable;
                    entry.status.last_error = Some(error);
                    verified = false;
                    continue;
                }
                entry.status.role_generation = None;
                entry.status.ready_until = None;
                entry.authority_observed = false;
                entry.status.state = State::Unavailable;
                // Retirement advances the server's activation version. Read
                // the resulting offer on its hint; never prepare the stale one.
                entry.receipt = Some(receipt);
                verified = false;
                continue;
            }
            entry.receipt = Some(receipt.clone());
            match receipt.state {
                ConsumerActivationState::Activating => {
                    // A failed activation can have mutated the remote owner or
                    // retained a local role. Repeated hints must not replay it.
                    if entry.attempted_activation.as_ref() == Some(&receipt.version) {
                        entry.status.state = State::Unavailable;
                        entry.status.ready_until = None;
                        continue;
                    }
                    entry.attempted_activation = Some(receipt.version.clone());
                    let result = channel
                        .activate(
                            receipt.version,
                            entry.update.declaration.consumer.group_key.clone(),
                            entry.node_id.clone(),
                            entry.update.declaration.consumer.maximum_concurrency,
                            self.registry.clone(),
                            self.budget.clone(),
                        )
                        .await;
                    match result {
                        Ok(role) => {
                            entry.authority_observed = true;
                            entry.status.state = if role.route_confirmed() {
                                State::Active
                            } else {
                                State::Unavailable
                            };
                            entry.status.role_generation = Some(role.role_generation);
                            entry.status.ready_until =
                                Some(role.authorization_deadline.min(role.route_deadline));
                            entry.status.last_error = role.last_error;
                        }
                        Err(error) => {
                            verified = false;
                            entry.status.state = State::Failed;
                            entry.status.last_error = Some(error);
                        }
                    }
                }
                ConsumerActivationState::WaitingForCatalog => {
                    entry.status.state = State::WaitingForCatalog
                }
                ConsumerActivationState::Active => entry.status.state = State::Unavailable,
                _ => entry.status.state = State::Unavailable,
            }
        }
        self.desired
            .retain(|entry| !removed.contains(&entry.update.registration_key));
        verified
    }
}

#[cfg(test)]
#[path = "registered_consumers/tests.rs"]
mod tests;

#[cfg(all(test, feature = "event-publication-zenoh", feature = "http-client"))]
#[path = "registered_consumers/host_tests.rs"]
mod host_tests;
