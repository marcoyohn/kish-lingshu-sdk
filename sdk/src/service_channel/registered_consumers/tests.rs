use super::*;
use kish_lingshu_event_dispatch_contract::*;
use std::{collections::VecDeque, sync::Mutex};

pub(super) fn declaration() -> ConsumerDeclaration {
    serde_json::from_value(serde_json::json!({
        "provider_key":"orders", "producer":{"package_name":"orders-app","package_version":"1.0.0"},
        "consumer":{"key":"orders.created.consumer","group_key":"order-workers",
            "selectors":[{"event_key":"orders.created","topic":"orders","event_type":"created"}],
            "delivery_mode":"sync", "maximum_concurrency":8,
            "policy":{"rate_limit":null,
                "retry":{"maximum_failure_attempts":3,"initial_delay_milliseconds":100,"maximum_delay_milliseconds":1000,"multiplier":2.0,"jitter_ratio":0.1},
                "throttle":{"minimum_cooldown_milliseconds":10,"maximum_cooldown_milliseconds":1000,"maximum_throttle_duration_milliseconds":10000,"half_open_probe_limit":1},
                "timeout":{"invocation_milliseconds":1000,"completion_milliseconds":1000,"maximum_completion_milliseconds":10000},
                "dispatch":{"ordering_scope":"partition","mode":"serial"},"pause":"retain"}},
        "events":[{"key":"orders.created","topic":"orders","event_type":"created","schema_version":"1",
            "payload_schema":{"type":"object"},"topic_defaults":{"name":"Orders","partition_count":8,"consumption_order":"partition_ordered"}}]
    })).unwrap()
}

fn plan(count: usize) -> RegisteredConsumerPlan {
    let desired = (0..count)
        .map(|i| Desired {
            update_pending: false,
            update_sent: false,
            remove_requested: false,
            retire_before_update: false,
            attempted_connection: None,
            update: ConsumerDeclarationUpdate {
                protocol: ConsumerRegistrationProtocol::V1,
                registration_key: format!("consumer-{i}"),
                operation_id: format!("initial-{i}"),
                expected_revision: None,
                declaration: declaration(),
            },
            node_id: format!("node-{i}"),
            receipt: None,
            authority_observed: false,
            attempted_activation: None,
            status: Status {
                key: format!("consumer-{i}"),
                state: State::Pending,
                role_generation: None,
                ready_until: None,
                last_error: None,
            },
        })
        .collect();
    let (status, _) = watch::channel(vec![]);
    let (update_sender, updates) = tokio::sync::mpsc::channel(1);
    RegisteredConsumerPlan {
        update_sender,
        updates,
        application: "app".into(),
        desired,
        registry: Arc::new(crate::event_dispatch::ConsumerRegistry::new("app").unwrap()),
        budget: crate::ServiceExecutionBudget::new(8).unwrap(),
        status,
    }
}
fn supported(boot: &str, epoch: &str) -> Response {
    Response::Supported {
        protocol: ConsumerRegistrationProtocol::V1,
        owner_boot: boot.into(),
        connection_epoch: epoch.into(),
        connection_lifecycle: false,
    }
}
fn receipt(i: usize, boot: &str, epoch: &str, state: ConsumerActivationState) -> Response {
    Response::Registered {
        receipt: ConsumerRegistrationReceipt {
            protocol: ConsumerRegistrationProtocol::V1,
            version: ConsumerActivationVersion {
                owner_boot: boot.into(),
                connection_epoch: epoch.into(),
                registration_key: format!("consumer-{i}"),
                declaration_revision: 1,
                catalog_revision: 1,
                activation_revision: 1,
            },
            declaration_digest: declaration().digest().unwrap(),
            state,
            directory_revision: 1,
        },
    }
}
#[derive(Default)]
struct Scripted {
    replies: Mutex<VecDeque<Result<Response, ChannelSessionError>>>,
    commands: Mutex<Vec<Command>>,
    activations: Mutex<Vec<ConsumerActivationVersion>>,
    roles: Mutex<Vec<super::super::ChannelRoleStatus>>,
    fail_activation: bool,
}
impl Scripted {
    fn push(&self, replies: impl IntoIterator<Item = Response>) {
        self.replies
            .lock()
            .unwrap()
            .extend(replies.into_iter().map(Ok));
    }
    fn count_declarations(&self) -> usize {
        self.commands
            .lock()
            .unwrap()
            .iter()
            .filter(|c| matches!(c, Command::Declare { .. }))
            .count()
    }
}
impl RegistrationChannel for Scripted {
    async fn control(&self, command: Command) -> Result<Response, ChannelSessionError> {
        self.commands.lock().unwrap().push(command);
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected control request")
    }
    fn roles(&self) -> Vec<super::super::ChannelRoleStatus> {
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
        version: ConsumerActivationVersion,
        _: String,
        _: String,
        _: u32,
        _: Arc<crate::event_dispatch::ConsumerRegistry>,
        _: crate::ServiceExecutionBudget,
    ) -> Result<super::super::ChannelRoleStatus, ChannelSessionError> {
        self.activations.lock().unwrap().push(version.clone());
        if self.fail_activation {
            return Err(ChannelSessionError::Transport);
        }
        let status = super::super::ChannelRoleStatus {
            call_registration: None,
            consumer_registration: Some(version.clone()),
            role_generation: format!("role-{}", version.registration_key),
            state: super::super::RoleLifecycleState::Active,
            authorization_deadline: tokio::time::Instant::now()
                + std::time::Duration::from_secs(30),
            route_deadline: tokio::time::Instant::now() + std::time::Duration::from_secs(30),
            last_renewal_status: None,
            last_error: None,
            remote_deregistered: false,
        };
        self.roles.lock().unwrap().push(status.clone());
        Ok(status)
    }
}

#[test]
fn registration_plan_can_run_in_a_spawned_runtime_task() {
    fn check(plan: RegisteredConsumerPlan, channel: &ManagedRoleChannel) {
        fn require_send<T: Send>(_: T) {}
        require_send(plan.run(channel));
    }
    let _ = check;
}

#[tokio::test]
async fn reconnect_and_owner_restart_redeclare_every_sibling_once() {
    let mut plan = plan(2);
    let channel = Scripted::default();
    let mut boot = String::new();
    for (owner, epoch) in [
        ("owner", "host:1"),
        ("owner", "host:2"),
        ("restarted", "host:2"),
    ] {
        channel.push([
            supported(owner, epoch),
            receipt(0, owner, epoch, ConsumerActivationState::WaitingForCatalog),
            receipt(1, owner, epoch, ConsumerActivationState::WaitingForCatalog),
        ]);
        assert!(plan.reconcile(&channel, &mut boot).await);
        for entry in &plan.desired {
            let received = entry.receipt.as_ref().unwrap();
            assert_eq!(received.version.connection_epoch, epoch);
            assert_eq!(received.version.owner_boot, owner);
        }
    }
    assert_eq!(channel.count_declarations(), 6);
    channel.push([
        supported("restarted", "host:2"),
        receipt(
            0,
            "restarted",
            "host:2",
            ConsumerActivationState::WaitingForCatalog,
        ),
        receipt(
            1,
            "restarted",
            "host:2",
            ConsumerActivationState::WaitingForCatalog,
        ),
    ]);
    assert!(plan.reconcile(&channel, &mut boot).await);
    assert_eq!(
        channel.count_declarations(),
        6,
        "ordinary hint must not re-enroll"
    );
}

#[tokio::test]
async fn lost_declaration_looks_up_original_operation_without_replay() {
    let mut plan = plan(1);
    let channel = Scripted::default();
    channel.push([supported("owner", "host:1")]);
    channel
        .replies
        .lock()
        .unwrap()
        .push_back(Err(ChannelSessionError::Transport));
    channel.push([receipt(
        0,
        "owner",
        "host:1",
        ConsumerActivationState::WaitingForCatalog,
    )]);
    assert!(plan.reconcile(&channel, &mut String::new()).await);
    let commands = channel.commands.lock().unwrap();
    let Command::Declare { update, .. } = &commands[1] else {
        panic!("expected Declare")
    };
    let Command::Lookup {
        registration_key,
        operation_id,
    } = &commands[2]
    else {
        panic!("expected Lookup")
    };
    assert_eq!(registration_key, &update.registration_key);
    assert_eq!(operation_id, &update.operation_id);
    assert_eq!(plan.desired[0].status.state, State::WaitingForCatalog);
}

#[tokio::test]
async fn failed_declaration_does_not_block_compatible_sibling() {
    let mut plan = plan(2);
    let channel = Scripted::default();
    channel.push([
        supported("owner", "host:1"),
        Response::Rejected {
            code: ConsumerRegistrationRejection::SourceConflict,
        },
        receipt(1, "owner", "host:1", ConsumerActivationState::Activating),
    ]);
    assert!(!plan.reconcile(&channel, &mut String::new()).await);
    assert_eq!(plan.desired[0].status.state, State::Unavailable);
    assert!(plan.desired[1].status.ready());
    assert_eq!(channel.activations.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn unknown_activation_is_not_replayed_by_repeated_hints() {
    let mut plan = plan(1);
    let channel = Scripted {
        fail_activation: true,
        ..Default::default()
    };
    for _ in 0..2 {
        channel.push([
            supported("owner", "host:1"),
            receipt(0, "owner", "host:1", ConsumerActivationState::Activating),
        ]);
        plan.reconcile(&channel, &mut String::new()).await;
    }
    assert_eq!(channel.count_declarations(), 1);
    assert_eq!(channel.activations.lock().unwrap().len(), 1);
    assert!(!plan.desired[0].status.ready());
}

#[tokio::test]
async fn receipt_from_old_connection_never_activates_even_if_digest_matches() {
    let mut plan = plan(1);
    let channel = Scripted::default();
    channel.push([
        supported("owner", "host:2"),
        receipt(0, "owner", "host:1", ConsumerActivationState::Activating),
    ]);
    assert!(!plan.reconcile(&channel, &mut String::new()).await);
    assert!(channel.activations.lock().unwrap().is_empty());
    assert!(!plan.desired[0].status.ready());
}

#[tokio::test]
async fn authority_query_failure_closes_previously_active_readiness() {
    let mut plan = plan(1);
    let channel = Scripted::default();
    channel.push([
        supported("owner", "host:1"),
        receipt(0, "owner", "host:1", ConsumerActivationState::Activating),
    ]);
    assert!(plan.reconcile(&channel, &mut String::new()).await);
    assert!(plan.desired[0].status.ready());
    channel
        .replies
        .lock()
        .unwrap()
        .push_back(Err(ChannelSessionError::Transport));
    assert!(!plan.reconcile(&channel, &mut String::new()).await);
    assert!(!plan.desired[0].authority_observed);
    assert!(!plan.desired[0].status.ready());
}

#[tokio::test]
async fn retained_stopped_activation_is_retired_before_using_a_new_offer() {
    let mut plan = plan(1);
    let channel = Scripted::default();
    channel.push([
        supported("owner", "host:1"),
        receipt(0, "owner", "host:1", ConsumerActivationState::Activating),
    ]);
    assert!(plan.reconcile(&channel, &mut String::new()).await);
    // Simulate a lost ACK followed by the serial owner's conservative stop.
    channel.roles.lock().unwrap()[0].state = super::super::RoleLifecycleState::Stopped;
    channel.push([
        supported("owner", "host:1"),
        receipt(0, "owner", "host:1", ConsumerActivationState::Active),
    ]);
    assert!(!plan.reconcile(&channel, &mut String::new()).await);
    assert!(channel.roles().is_empty());
    assert!(!plan.desired[0].status.ready());
    assert_eq!(channel.activations.lock().unwrap().len(), 1);
    let mut fresh = receipt(0, "owner", "host:1", ConsumerActivationState::Activating);
    let Response::Registered { receipt } = &mut fresh else {
        unreachable!()
    };
    receipt.version.activation_revision = 2;
    receipt.directory_revision = 2;
    channel.push([supported("owner", "host:1"), fresh]);
    assert!(plan.reconcile(&channel, &mut String::new()).await);
    assert!(plan.desired[0].status.ready());
    assert_eq!(channel.count_declarations(), 1);
    assert_eq!(channel.activations.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn declaration_metadata_update_uses_cas_and_lost_reply_is_only_looked_up() {
    let mut original = plan(1);
    let channel = Scripted::default();
    let mut boot = String::new();
    channel.push([
        supported("owner", "host:1"),
        receipt(
            0,
            "owner",
            "host:1",
            ConsumerActivationState::WaitingForCatalog,
        ),
    ]);
    assert!(original.reconcile(&channel, &mut boot).await);
    let mut next = plan(1);
    next.registry = original.registry.clone();
    next.desired[0].update.declaration.producer.package_version = "1.0.1".into();
    original.replace_desired(next).unwrap();
    channel.push([supported("owner", "host:1")]);
    channel
        .replies
        .lock()
        .unwrap()
        .push_back(Err(ChannelSessionError::Transport));
    assert!(!original.reconcile(&channel, &mut boot).await);
    assert!(original.desired[0].update_pending);
    assert_eq!(original.desired[0].update.expected_revision, Some(1));
    let Response::Registered { mut receipt } = receipt(
        0,
        "owner",
        "host:1",
        ConsumerActivationState::WaitingForCatalog,
    ) else {
        unreachable!()
    };
    receipt.version.declaration_revision = 2;
    receipt.declaration_digest = original.desired[0].update.declaration.digest().unwrap();
    channel.push([
        supported("owner", "host:1"),
        Response::Registered { receipt },
    ]);
    assert!(original.reconcile(&channel, &mut boot).await);
    assert_eq!(
        channel.count_declarations(),
        2,
        "one initial Declare and one update, never an uncertain replay"
    );
    assert!(matches!(
        channel.commands.lock().unwrap().last(),
        Some(Command::Lookup { .. })
    ));
    assert!(!original.desired[0].update_pending);
}
#[tokio::test]
async fn replacement_keeps_unchanged_siblings_and_removes_only_current_version() {
    let mut original = plan(2);
    let channel = Scripted::default();
    let mut boot = String::new();
    channel.push([
        supported("owner", "host:1"),
        receipt(
            0,
            "owner",
            "host:1",
            ConsumerActivationState::WaitingForCatalog,
        ),
        receipt(
            1,
            "owner",
            "host:1",
            ConsumerActivationState::WaitingForCatalog,
        ),
    ]);
    assert!(original.reconcile(&channel, &mut boot).await);
    let before = original.desired[0].update.operation_id.clone();
    let mut next = plan(1);
    next.registry = original.registry.clone();
    original.replace_desired(next).unwrap();
    assert_eq!(original.desired[0].update.operation_id, before);
    assert!(!original.desired[0].update_pending);
    let Response::Registered { receipt: removed } = receipt(
        1,
        "owner",
        "host:1",
        ConsumerActivationState::WaitingForCatalog,
    ) else {
        unreachable!()
    };
    channel.push([
        supported("owner", "host:1"),
        receipt(
            0,
            "owner",
            "host:1",
            ConsumerActivationState::WaitingForCatalog,
        ),
        Response::Registered {
            receipt: removed.clone(),
        },
        Response::Removed {
            version: removed.version.clone(),
        },
    ]);
    assert!(original.reconcile(&channel, &mut boot).await);
    assert_eq!(original.desired.len(), 1);
    assert_eq!(channel.count_declarations(), 2);
    assert!(
        matches!(channel.commands.lock().unwrap().last(), Some(Command::Remove {version}) if version == &removed.version)
    );
}
#[test]
fn replacement_validation_is_atomic_and_cannot_overwrite_unknown_registration() {
    let mut original = plan(2);
    let mut next = plan(2);
    next.desired[0].update.declaration.producer.package_version = "new".into();
    next.desired[1].node_id = "different-node".into();
    assert_eq!(
        original.replace_desired(next),
        Err(ChannelSessionError::InvalidConfig)
    );
    assert_eq!(
        original.desired[0]
            .update
            .declaration
            .producer
            .package_version,
        "1.0.0"
    );
    original.desired[0].attempted_connection = Some(RegistrationConnection {
        owner_boot: "owner".into(),
        connection_epoch: "host:1".into(),
    });
    assert_eq!(
        original.replace_desired(plan(2)),
        Err(ChannelSessionError::ControlRejected)
    );
}

#[tokio::test]
async fn logical_replacement_retains_desired_update_and_does_not_resurrect_removed_sibling() {
    let mut plan = plan(2);
    let statuses = plan.subscribe();
    let updates = plan.updates();
    plan.desired[1].remove_requested = true;
    plan.desired[0].update_pending = true;
    plan.desired[0].update_sent = true;
    plan.desired[0].update.expected_revision = Some(4);
    plan.desired[0].update.declaration.producer.package_version = "2.0.0".into();
    plan.desired[0].status.role_generation = Some("old-role".into());
    let operation = plan.desired[0].update.operation_id.clone();
    plan.reset_after_close();
    assert_eq!(plan.desired.len(), 1);
    assert_eq!(
        plan.desired[0].update.declaration.producer.package_version,
        "2.0.0"
    );
    assert_ne!(plan.desired[0].update.operation_id, operation);
    assert_eq!(plan.desired[0].update.expected_revision, None);
    assert_eq!(statuses.borrow()[0].role_generation, None);
    assert_eq!(statuses.borrow()[0].state, State::Pending);
    assert!(!updates.sender.is_closed());
    let channel = Scripted::default();
    let mut reply = receipt(
        0,
        "new-owner",
        "replacement:1",
        ConsumerActivationState::WaitingForCatalog,
    );
    if let Response::Registered { receipt } = &mut reply {
        receipt.declaration_digest = plan.desired[0].update.declaration.digest().unwrap();
    }
    channel.push([supported("new-owner", "replacement:1"), reply]);
    assert!(plan.reconcile(&channel, &mut String::new()).await);
    assert!(
        matches!(channel.commands.lock().unwrap().get(1), Some(Command::Declare { update, .. }) if update.expected_revision.is_none())
    );
}

#[test]
fn deployment_bindings_select_only_bound_groups_and_can_only_lower_concurrency() {
    let binding = |maximum| RegisteredConsumerBinding {
        group_key: "order-workers".into(),
        node_id: "deployment-a".into(),
        maximum_in_flight: maximum,
    };
    let selected = plan(1).with_bindings(vec![binding(2)]).unwrap();
    assert_eq!(
        selected.desired[0]
            .update
            .declaration
            .consumer
            .maximum_concurrency,
        2
    );
    let node = selected.desired[0].node_id.clone();
    assert_eq!(
        plan(1).with_bindings(vec![binding(100)]).unwrap().desired[0]
            .update
            .declaration
            .consumer
            .maximum_concurrency,
        8
    );
    assert_eq!(
        plan(1).with_bindings(vec![binding(2)]).unwrap().desired[0].node_id,
        node
    );
    assert!(plan(1).with_bindings(vec![binding(0)]).is_err());
    assert!(plan(1).with_bindings(vec![binding(2), binding(2)]).is_err());
    let mut foreign = binding(2);
    foreign.group_key = "unbound".into();
    assert!(plan(1).with_bindings(vec![foreign]).is_err());
    let empty = plan(1).with_bindings(vec![]).unwrap();
    assert!(empty.desired.is_empty() && empty.subscribe().borrow().is_empty());
}
