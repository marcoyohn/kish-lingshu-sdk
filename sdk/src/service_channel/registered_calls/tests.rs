use super::*;
use crate::service_channel::{ChannelRoleStatus, RoleLifecycleState};
use kish_lingshu_runtime_contract::service::{InstanceCapability, OperationRef};
use std::{
    collections::{BTreeSet, VecDeque},
    sync::Mutex,
    time::Duration,
};
fn plan(count: usize) -> RegisteredCallPlan {
    let registry = Arc::new(
        crate::services::ServiceRegistryBuilder::empty("app")
            .unwrap()
            .build()
            .unwrap(),
    );
    let desired = (0..count)
        .map(|i| Desired {
            update_pending: false,
            update_sent: false,
            expected_revision: None,
            remove_requested: false,
            retire_before_update: false,
            declaration: CallDeclaration {
                node_id: format!("node-{i}"),
                maximum_in_flight: 8,
                lane_count: 1,
                capabilities: vec![InstanceCapability {
                    operation: OperationRef {
                        service_key: "service".into(),
                        operation_key: format!("op-{i}"),
                        version: "v1".into(),
                        contract_digest: "a".repeat(64),
                    },
                    call: true,
                    events: BTreeSet::new(),
                }],
            },
            operation: format!("op-{i}"),
            connection: None,
            receipt: None,
            attempted: None,
            registry: registry.clone(),
            verified: false,
            status: Status {
                key: format!("call-{i}"),
                state: Activation::Pending,
                role_generation: None,
                ready_until: None,
                last_error: None,
            },
        })
        .collect();
    let (update_sender, updates) = tokio::sync::mpsc::channel(1);
    RegisteredCallPlan {
        registry,
        update_sender,
        updates,
        application: "app".into(),
        desired,
        budget: crate::ServiceExecutionBudget::new(8).unwrap(),
        asynchronous: false,
        status: watch::channel(vec![]).0,
    }
}
fn supported(epoch: &str) -> Response {
    Response::Supported {
        owner_boot: "call-owner".into(),
        connection_epoch: epoch.into(),
        connection_lifecycle: false,
    }
}
fn receipt(declaration: &CallDeclaration, epoch: &str, state: State) -> Response {
    Response::Registered {
        receipt: CallRegistrationReceipt {
            version: CallActivationVersion {
                owner_boot: "call-owner".into(),
                connection_epoch: epoch.into(),
                node_id: declaration.node_id.clone(),
                revision: 1,
                activation_revision: 1,
            },
            declaration: declaration.clone(),
            state,
        },
    }
}
#[derive(Default)]
struct Scripted {
    responses: Mutex<VecDeque<Result<Response, ChannelSessionError>>>,
    commands: Mutex<Vec<Command>>,
    roles: Mutex<Vec<ChannelRoleStatus>>,
    activations: Mutex<Vec<CallActivationVersion>>,
    fail_activation: bool,
}
impl Scripted {
    fn push(&self, responses: impl IntoIterator<Item = Response>) {
        self.responses
            .lock()
            .unwrap()
            .extend(responses.into_iter().map(Ok));
    }
}
impl RegistrationChannel for Scripted {
    async fn control(&self, command: Command) -> Result<Response, ChannelSessionError> {
        self.commands.lock().unwrap().push(command);
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected request")
    }
    fn roles(&self) -> Vec<ChannelRoleStatus> {
        self.roles.lock().unwrap().clone()
    }
    async fn retire(&self, generation: String) -> Result<(), ChannelSessionError> {
        self.roles
            .lock()
            .unwrap()
            .retain(|r| r.role_generation != generation);
        Ok(())
    }
    async fn activate(
        &self,
        version: CallActivationVersion,
        _: Arc<crate::services::ServiceRegistry>,
        _: crate::ServiceExecutionBudget,
        _: bool,
    ) -> Result<ChannelRoleStatus, ChannelSessionError> {
        self.activations.lock().unwrap().push(version.clone());
        if self.fail_activation {
            return Err(ChannelSessionError::Transport);
        }
        let role = ChannelRoleStatus {
            call_registration: Some(version.clone()),
            consumer_registration: None,
            role_generation: format!("role-{}", version.node_id),
            state: RoleLifecycleState::Active,
            authorization_deadline: tokio::time::Instant::now() + Duration::from_secs(30),
            route_deadline: tokio::time::Instant::now() + Duration::from_secs(30),
            last_renewal_status: None,
            last_error: None,
            remote_deregistered: false,
        };
        self.roles.lock().unwrap().push(role.clone());
        Ok(role)
    }
}
#[tokio::test]
async fn unpublished_call_waits_on_original_registration_and_siblings_activate_independently() {
    let mut plan = plan(2);
    let channel = Scripted::default();
    let a = plan.desired[0].declaration.clone();
    let b = plan.desired[1].declaration.clone();
    channel.push([
        supported("host:1"),
        receipt(&a, "host:1", State::WaitingForCatalog),
        receipt(&b, "host:1", State::Activating),
    ]);
    assert!(plan.reconcile(&channel).await);
    assert_eq!(plan.desired[0].status.state, Activation::WaitingForCatalog);
    assert_eq!(plan.desired[1].status.state, Activation::Active);
    channel.push([
        supported("host:1"),
        receipt(&a, "host:1", State::Activating),
        receipt(&b, "host:1", State::Active),
    ]);
    assert!(plan.reconcile(&channel).await);
    assert!(plan
        .desired
        .iter()
        .all(|d| d.status.state == Activation::Active));
    assert_eq!(
        channel
            .commands
            .lock()
            .unwrap()
            .iter()
            .filter(|c| matches!(c, Command::Declare { .. }))
            .count(),
        2
    );
    assert_eq!(channel.activations.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn every_call_redeclares_on_connection_replacement_with_previous_epoch_fence() {
    let mut plan = plan(2);
    let channel = Scripted::default();
    let declarations: Vec<_> = plan.desired.iter().map(|d| d.declaration.clone()).collect();
    for epoch in ["host:1", "host:2"] {
        channel.push([
            supported(epoch),
            receipt(&declarations[0], epoch, State::Activating),
            receipt(&declarations[1], epoch, State::Activating),
        ]);
        assert!(plan.reconcile(&channel).await);
    }
    assert_eq!(channel.activations.lock().unwrap().len(), 4);
    let commands = channel.commands.lock().unwrap();
    assert_eq!(commands.iter().filter(|c|matches!(c,Command::Declare{previous_connection_epoch:Some(epoch),..} if epoch=="host:1")).count(),2);
    assert_eq!(channel.roles().len(), 2);
}
#[tokio::test]
async fn lost_declare_looks_up_and_unknown_activation_is_not_replayed() {
    let mut plan = plan(1);
    let channel = Scripted {
        fail_activation: true,
        ..Default::default()
    };
    let declaration = plan.desired[0].declaration.clone();
    channel.push([supported("host:1")]);
    channel
        .responses
        .lock()
        .unwrap()
        .push_back(Err(ChannelSessionError::Transport));
    channel.push([receipt(&declaration, "host:1", State::Activating)]);
    plan.reconcile(&channel).await;
    assert!(matches!(
        channel.commands.lock().unwrap().last(),
        Some(Command::Lookup { .. })
    ));
    channel.push([
        supported("host:1"),
        receipt(&declaration, "host:1", State::Activating),
    ]);
    plan.reconcile(&channel).await;
    assert_eq!(channel.activations.lock().unwrap().len(), 1);
    assert_eq!(plan.desired[0].status.state, Activation::Unavailable);
    channel
        .responses
        .lock()
        .unwrap()
        .push_back(Err(ChannelSessionError::Transport));
    assert!(!plan.reconcile(&channel).await);
    assert!(!plan.desired[0].verified);
    assert!(plan.desired[0].status.ready_until.is_none());
}
#[test]
fn plan_future_is_send() {
    fn check(plan: RegisteredCallPlan, channel: &ManagedRoleChannel) {
        fn send<T: Send>(_: T) {}
        send(plan.run(channel));
    }
    let _ = check;
}

#[tokio::test]
async fn failed_initial_negotiation_remains_recoverable_without_reenrolling_waiting_calls() {
    let mut plan = plan(1);
    let channel = Scripted::default();
    channel
        .responses
        .lock()
        .unwrap()
        .push_back(Err(ChannelSessionError::Transport));
    assert!(!plan.reconcile(&channel).await);
    assert!(plan.needs_recovery());
    let declaration = plan.desired[0].declaration.clone();
    channel.push([
        supported("host:1"),
        receipt(&declaration, "host:1", State::WaitingForCatalog),
    ]);
    assert!(plan.reconcile(&channel).await);
    assert!(
        !plan.needs_recovery(),
        "waiting for import must not poll declarations"
    );
}

#[tokio::test]
async fn call_update_uses_cas_and_recovers_only_the_original_lost_operation() {
    let mut current = plan(1);
    let channel = Scripted::default();
    let declaration = current.desired[0].declaration.clone();
    channel.push([
        supported("host:1"),
        receipt(&declaration, "host:1", State::Activating),
    ]);
    assert!(current.reconcile(&channel).await);
    let mut next = plan(1);
    next.desired[0].declaration.maximum_in_flight = 4;
    next.desired[0].operation = "update".into();
    current.replace_desired(next).unwrap();
    channel.push([supported("host:1")]);
    channel
        .responses
        .lock()
        .unwrap()
        .push_back(Err(ChannelSessionError::Transport));
    assert!(!current.reconcile(&channel).await);
    assert!(
        channel.roles().is_empty(),
        "old handler must retire before changing execution declarations"
    );
    assert!(current.replace_desired(plan(1)).is_err());
    let mut updated = receipt(&current.desired[0].declaration, "host:1", State::Activating);
    if let Response::Registered { receipt } = &mut updated {
        receipt.version.revision = 3;
    }
    channel.push([supported("host:1"), updated]);
    assert!(current.reconcile(&channel).await);
    assert!(!current.desired[0].update_pending);
    let commands = channel.commands.lock().unwrap();
    assert_eq!(commands.iter().filter(|c| matches!(c, Command::Declare { operation_id, expected_revision: Some(1), .. } if operation_id == "update")).count(), 1);
    assert!(
        matches!(commands.last(), Some(Command::Lookup { operation_id, .. }) if operation_id == "update")
    );
    assert_eq!(channel.activations.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn removing_call_keeps_unchanged_sibling_and_recovers_lost_remove() {
    let mut current = plan(2);
    let channel = Scripted::default();
    let a = current.desired[0].declaration.clone();
    let b = current.desired[1].declaration.clone();
    channel.push([
        supported("host:1"),
        receipt(&a, "host:1", State::Activating),
        receipt(&b, "host:1", State::Activating),
    ]);
    assert!(current.reconcile(&channel).await);
    let mut next = plan(1);
    next.registry = current.registry.clone();
    current.replace_desired(next).unwrap();
    channel.push([
        supported("host:1"),
        receipt(&a, "host:1", State::Active),
        receipt(&b, "host:1", State::WaitingForCatalog),
    ]);
    channel
        .responses
        .lock()
        .unwrap()
        .push_back(Err(ChannelSessionError::Transport));
    assert!(!current.reconcile(&channel).await);
    channel.push([
        supported("host:1"),
        receipt(&a, "host:1", State::Active),
        Response::Rejected {
            code: kish_lingshu_runtime_contract::service::CallRegistrationRejection::NotFound,
        },
    ]);
    assert!(current.reconcile(&channel).await);
    assert_eq!(current.desired.len(), 1);
    assert_eq!(channel.roles().len(), 1);
    assert_eq!(channel.activations.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn logical_replacement_preserves_updates_but_fences_uncertain_old_operations() {
    let mut plan = plan(2);
    let statuses = plan.subscribe();
    let updates = plan.updates();
    plan.desired[0].remove_requested = true;
    plan.desired[1].update_pending = true;
    plan.desired[1].update_sent = true;
    plan.desired[1].declaration.maximum_in_flight = 3;
    plan.desired[1].status.role_generation = Some("old-role".into());
    let operation = plan.desired[1].operation.clone();
    plan.reset_after_close();
    assert_eq!(plan.desired.len(), 1);
    assert_eq!(plan.desired[0].declaration.maximum_in_flight, 3);
    assert_ne!(plan.desired[0].operation, operation);
    assert_eq!(statuses.borrow()[0].role_generation, None);
    assert_eq!(statuses.borrow()[0].state, Activation::Pending);
    assert!(!updates.sender.is_closed());
    let channel = Scripted::default();
    channel.push([
        supported("replacement:1"),
        receipt(
            &plan.desired[0].declaration,
            "replacement:1",
            State::WaitingForCatalog,
        ),
    ]);
    assert!(plan.reconcile(&channel).await);
    assert!(matches!(
        channel.commands.lock().unwrap().get(1),
        Some(Command::Declare {
            expected_revision: None,
            ..
        })
    ));
}
