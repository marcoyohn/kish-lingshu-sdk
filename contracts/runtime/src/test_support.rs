//! Shared contract harness for runtime binding implementations.
//!
//! This module is compiled only for this crate's tests or when a consumer
//! enables `test-support`. The deterministic in-memory binding is not a
//! production runtime; it lets direct and transport-backed adapters run the
//! exact same lifecycle assertions before the SQLite/Service E2E layer.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use async_trait::async_trait;
use futures::{stream, StreamExt};
use serde_json::{json, Map, Value};

use crate::*;

mod event_publisher;
mod product_runtime;
mod user_task;

pub use event_publisher::{
    assert_event_publisher_contract, ContractEventPublisher, EventPublisherContractReport,
};
pub use product_runtime::{
    assert_product_runtime_contract, ContractProductRuntimeFixture, ProductRuntimeContractReport,
};
pub use user_task::{
    assert_user_task_runtime_contract, ContractUserTaskRuntime, UserTaskRuntimeContractReport,
    CONTRACT_READ_TASK_ID, CONTRACT_TODO_TASK_ID,
};

const CONTRACT_WORKFLOW_ID: WorkflowId = WorkflowId(42);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowRuntimeContractReport {
    pub workflow_id: WorkflowId,
    pub workflow_instance_id: WorkflowInstanceId,
    pub signal_cursor: EventCursor,
    pub resume_cursor: EventCursor,
    pub termination_cursor: EventCursor,
}

/// Run one observable lifecycle contract against a runtime binding.
///
/// The configured Workflow fixture is expected to accept the business signal
/// `contract_suspend` by entering a resumable suspension. The included
/// `ContractWorkflowRuntime` supplies those deterministic semantics; later E2E
/// fixtures can expose an equivalent published Workflow through the same
/// harness.
pub async fn assert_workflow_runtime_contract(
    runtime: Arc<dyn WorkflowRuntime>,
    context: RequestContext,
) -> WorkflowRuntimeContractReport {
    let handle = start_contract_run(&runtime, &context, Some(SessionId(7))).await;
    assert_eq!(handle.workflow_id, CONTRACT_WORKFLOW_ID);
    assert_eq!(
        handle.root_workflow_instance_id.0,
        handle.workflow_instance_id.0
    );
    assert_eq!(
        handle.input_message_id,
        Some(MessageId(handle.workflow_instance_id.0))
    );

    let initial = runtime
        .snapshot(WorkflowSnapshotRequest {
            context: context.clone(),
            workflow_instance_id: handle.workflow_instance_id,
        })
        .await
        .expect("started Workflow must have a snapshot");
    assert_eq!(initial.state, WorkflowState::Running);

    let initial_events = collect_contract_events(
        &runtime,
        &context,
        handle.workflow_instance_id,
        Some(handle.subscription_anchor.clone()),
    )
    .await;
    assert_eq!(initial_events.len(), 1);
    assert!(matches!(
        initial_events[0].kind,
        WorkflowEventKind::WorkflowState(WorkflowStateEvent {
            state: WorkflowState::Running,
            ..
        })
    ));
    let initial_cursor = initial_events[0].cursor.clone();

    signal_contract(
        &runtime,
        &context,
        handle.workflow_instance_id,
        "contract_events",
        Value::Null,
    )
    .await;
    let taxonomy_events = collect_contract_events(
        &runtime,
        &context,
        handle.workflow_instance_id,
        Some(initial_cursor),
    )
    .await;
    let taxonomy = taxonomy_events
        .iter()
        .map(contract_event_kind)
        .collect::<HashSet<_>>();
    assert_eq!(
        taxonomy,
        HashSet::from([
            "execution_state",
            "message",
            "tool_call",
            "plan",
            "client_tool_request",
            "permission_request",
            "usage",
            "compaction",
            "contract_future_event",
        ])
    );
    assert!(taxonomy_events.iter().all(|event| {
        event.workflow_instance_id == handle.workflow_instance_id
            && event.root_workflow_instance_id.0 == handle.workflow_instance_id.0
            && event.event_id.as_ref() != event.cursor.as_ref()
    }));
    let message_phases = taxonomy_events
        .iter()
        .filter_map(|event| match &event.kind {
            WorkflowEventKind::Message(message) => Some(message.phase.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        message_phases,
        vec![
            MessagePhase::Started,
            MessagePhase::Delta,
            MessagePhase::Completed
        ]
    );
    assert!(taxonomy_events.iter().any(|event| matches!(
        &event.kind,
        WorkflowEventKind::Extension { kind, payload }
            if kind == "contract_future_event" && payload == &json!({"version": 1})
    )));

    let signal = runtime
        .signal(SignalWorkflowRequest {
            context: context.clone(),
            workflow_instance_id: handle.workflow_instance_id,
            signal: WorkflowSignal::Input {
                input: WorkflowInput::structured(json!("first signal")),
            },
        })
        .await
        .expect("user-input signal must be accepted");
    assert_eq!(signal.disposition, CommandDisposition::Accepted);
    let signal_cursor = signal
        .cursor
        .expect("signal acknowledgement needs a cursor");

    let suspended = runtime
        .signal(SignalWorkflowRequest {
            context: context.clone(),
            workflow_instance_id: handle.workflow_instance_id,
            signal: WorkflowSignal::Business {
                name: "contract_suspend".to_string(),
                payload: json!({"question": "continue?"}),
            },
        })
        .await
        .expect("contract suspension signal must be accepted");
    let suspension_cursor = suspended
        .cursor
        .expect("suspension acknowledgement needs a cursor");
    let suspended_snapshot = runtime
        .snapshot(WorkflowSnapshotRequest {
            context: context.clone(),
            workflow_instance_id: handle.workflow_instance_id,
        })
        .await
        .expect("suspended Workflow must have a snapshot");
    assert_eq!(suspended_snapshot.state, WorkflowState::Suspended);
    let suspension = suspended_snapshot
        .suspension
        .expect("suspended Workflow must expose an opaque handle");

    let resume_signal = SignalWorkflowRequest {
        context: context.clone(),
        workflow_instance_id: handle.workflow_instance_id,
        signal: WorkflowSignal::SuspensionResponse {
            suspension: suspension.clone(),
            response: ResumePayload::UserAnswer {
                value: json!("yes"),
            },
        },
    };
    let resumed = runtime
        .signal(resume_signal.clone())
        .await
        .expect("valid suspension must resume");
    assert_eq!(resumed.disposition, CommandDisposition::Accepted);
    let resume_cursor = resumed
        .cursor
        .expect("resume acknowledgement needs a cursor");

    let after_resume = runtime
        .signal(SignalWorkflowRequest {
            context: context.clone(),
            workflow_instance_id: handle.workflow_instance_id,
            signal: WorkflowSignal::Business {
                name: "contract_progress".to_string(),
                payload: json!({"step": 2}),
            },
        })
        .await
        .expect("post-resume signal must be accepted");
    let after_resume_cursor = after_resume
        .cursor
        .expect("post-resume acknowledgement needs a cursor");

    let replayed = runtime
        .subscribe(SubscribeWorkflowRequest {
            context: context.clone(),
            workflow_instance_id: handle.workflow_instance_id,
            after: Some(signal_cursor.clone()),
        })
        .await
        .expect("cursor subscription must attach")
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<RuntimeResult<Vec<_>>>()
        .expect("replayed events must be valid");
    assert_eq!(replayed.len(), 4);
    assert!(matches!(replayed[0].kind, WorkflowEventKind::Suspension(_)));
    assert_eq!(replayed[1].cursor, suspension_cursor);
    assert!(matches!(
        replayed[1].kind,
        WorkflowEventKind::WorkflowState(WorkflowStateEvent {
            state: WorkflowState::Suspended,
            ..
        })
    ));
    assert_eq!(replayed[2].cursor, resume_cursor);
    assert_eq!(replayed[3].cursor, after_resume_cursor);

    let stale = runtime
        .signal(resume_signal)
        .await
        .expect_err("settled suspension must not resume twice");
    assert_eq!(stale.code, RuntimeErrorCode::StaleSuspension);

    let resumed_snapshot = runtime
        .snapshot(WorkflowSnapshotRequest {
            context: context.clone(),
            workflow_instance_id: handle.workflow_instance_id,
        })
        .await
        .expect("resumed Workflow must have a snapshot");
    assert_eq!(resumed_snapshot.state, WorkflowState::Running);

    let disconnected = runtime
        .subscribe(SubscribeWorkflowRequest {
            context: context.clone(),
            workflow_instance_id: handle.workflow_instance_id,
            after: Some(after_resume_cursor.clone()),
        })
        .await
        .expect("subscriber must attach before disconnect");
    drop(disconnected);
    let after_disconnect = signal_contract(
        &runtime,
        &context,
        handle.workflow_instance_id,
        "contract_after_disconnect",
        json!({"step": 3}),
    )
    .await
    .cursor
    .expect("post-disconnect signal needs a cursor");
    let reattached = collect_contract_events(
        &runtime,
        &context,
        handle.workflow_instance_id,
        Some(after_resume_cursor),
    )
    .await;
    assert_eq!(reattached.len(), 1);
    assert_eq!(reattached[0].cursor, after_disconnect);

    let terminated = runtime
        .terminate(TerminateWorkflowRequest {
            context: context.clone(),
            workflow_instance_id: handle.workflow_instance_id,
            reason: Some("contract complete".to_string()),
            wait_timeout_ms: None,
        })
        .await
        .expect("termination must be accepted");
    assert_eq!(terminated.disposition, CommandDisposition::Terminated);
    let termination_cursor = terminated
        .cursor
        .expect("termination acknowledgement needs a cursor");

    let termination_replay = runtime
        .subscribe(SubscribeWorkflowRequest {
            context: context.clone(),
            workflow_instance_id: handle.workflow_instance_id,
            after: Some(after_disconnect),
        })
        .await
        .expect("termination cursor subscription must attach")
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<RuntimeResult<Vec<_>>>()
        .expect("termination replay must be valid");
    assert_eq!(termination_replay.len(), 1);
    assert_eq!(termination_replay[0].cursor, termination_cursor);

    let terminal_snapshot = runtime
        .snapshot(WorkflowSnapshotRequest {
            context: context.clone(),
            workflow_instance_id: handle.workflow_instance_id,
        })
        .await
        .expect("terminated Workflow must have a snapshot");
    assert_eq!(terminal_snapshot.state, WorkflowState::Terminated);

    let completed_handle = start_contract_run(&runtime, &context, None).await;
    signal_contract(
        &runtime,
        &context,
        completed_handle.workflow_instance_id,
        "contract_complete",
        json!({"answer": 42}),
    )
    .await;
    let completed_events = collect_contract_events(
        &runtime,
        &context,
        completed_handle.workflow_instance_id,
        Some(completed_handle.subscription_anchor.clone()),
    )
    .await;
    assert!(matches!(
        completed_events[completed_events.len() - 2].kind,
        WorkflowEventKind::Output(_)
    ));
    assert!(matches!(
        completed_events.last().map(|event| &event.kind),
        Some(WorkflowEventKind::WorkflowState(WorkflowStateEvent {
            state: WorkflowState::Completed,
            ..
        }))
    ));
    let completed_result = project_workflow_result(
        completed_handle.clone(),
        runtime
            .subscribe(SubscribeWorkflowRequest {
                context: context.clone(),
                workflow_instance_id: completed_handle.workflow_instance_id,
                after: Some(completed_handle.subscription_anchor.clone()),
            })
            .await
            .expect("completed run subscription must attach"),
    )
    .await
    .expect("completed events must project a result");
    assert!(matches!(
        completed_result,
        WorkflowRunResult::Completed { output, .. } if output == json!({"answer": 42})
    ));

    let failed_handle = start_contract_run(&runtime, &context, None).await;
    signal_contract(
        &runtime,
        &context,
        failed_handle.workflow_instance_id,
        "contract_fail",
        json!({"cause": "fixture"}),
    )
    .await;
    let failed_result = project_workflow_result(
        failed_handle.clone(),
        runtime
            .subscribe(SubscribeWorkflowRequest {
                context: context.clone(),
                workflow_instance_id: failed_handle.workflow_instance_id,
                after: Some(failed_handle.subscription_anchor.clone()),
            })
            .await
            .expect("failed run subscription must attach"),
    )
    .await
    .expect("failed events must project a result");
    assert!(matches!(
        failed_result,
        WorkflowRunResult::Failed { failure, .. }
            if failure.code == "contract_failure"
                && failure.details == Some(json!({"cause": "fixture"}))
    ));

    let replay_handle = start_contract_run(&runtime, &context, None).await;
    signal_contract(
        &runtime,
        &context,
        replay_handle.workflow_instance_id,
        "contract_events",
        Value::Null,
    )
    .await;
    signal_contract(
        &runtime,
        &context,
        replay_handle.workflow_instance_id,
        "contract_replay_gap",
        Value::Null,
    )
    .await;
    let replay_error = subscribe_error(
        &runtime,
        &context,
        replay_handle.workflow_instance_id,
        replay_handle.subscription_anchor,
    )
    .await;
    assert_eq!(replay_error.code, RuntimeErrorCode::ReplayUnavailable);
    assert!(replay_error.details.is_some());

    let malformed_error = subscribe_error(
        &runtime,
        &context,
        replay_handle.workflow_instance_id,
        EventCursor::from("malformed"),
    )
    .await;
    assert_eq!(malformed_error.code, RuntimeErrorCode::InvalidRequest);
    let stale_generation = subscribe_error(
        &runtime,
        &context,
        replay_handle.workflow_instance_id,
        EventCursor::from("contract-other:0"),
    )
    .await;
    assert_eq!(stale_generation.code, RuntimeErrorCode::ReplayUnavailable);

    let missing = runtime
        .snapshot(WorkflowSnapshotRequest {
            context,
            workflow_instance_id: WorkflowInstanceId(u64::MAX),
        })
        .await
        .expect_err("unknown Workflow instance must fail");
    assert_eq!(missing.code, RuntimeErrorCode::NotFound);

    WorkflowRuntimeContractReport {
        workflow_id: handle.workflow_id,
        workflow_instance_id: handle.workflow_instance_id,
        signal_cursor,
        resume_cursor,
        termination_cursor,
    }
}

async fn start_contract_run(
    runtime: &Arc<dyn WorkflowRuntime>,
    context: &RequestContext,
    session_id: Option<SessionId>,
) -> WorkflowRunHandle {
    runtime
        .start(StartWorkflowRequest {
            context: WorkflowContext::new(context.clone(), CONTRACT_WORKFLOW_ID),
            session_id,
            input: WorkflowInput::structured(json!({"prompt": "contract"})),
            bindings: RuntimeBindings::default(),
        })
        .await
        .expect("contract start must be accepted")
}

async fn signal_contract(
    runtime: &Arc<dyn WorkflowRuntime>,
    context: &RequestContext,
    workflow_instance_id: WorkflowInstanceId,
    name: &str,
    payload: Value,
) -> CommandAck {
    runtime
        .signal(SignalWorkflowRequest {
            context: context.clone(),
            workflow_instance_id,
            signal: WorkflowSignal::Business {
                name: name.to_string(),
                payload,
            },
        })
        .await
        .expect("contract business signal must be accepted")
}

async fn collect_contract_events(
    runtime: &Arc<dyn WorkflowRuntime>,
    context: &RequestContext,
    workflow_instance_id: WorkflowInstanceId,
    after: Option<EventCursor>,
) -> Vec<WorkflowEvent> {
    runtime
        .subscribe(SubscribeWorkflowRequest {
            context: context.clone(),
            workflow_instance_id,
            after,
        })
        .await
        .expect("contract subscription must attach")
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<RuntimeResult<Vec<_>>>()
        .expect("contract event stream must be valid")
}

async fn subscribe_error(
    runtime: &Arc<dyn WorkflowRuntime>,
    context: &RequestContext,
    workflow_instance_id: WorkflowInstanceId,
    after: EventCursor,
) -> RuntimeError {
    match runtime
        .subscribe(SubscribeWorkflowRequest {
            context: context.clone(),
            workflow_instance_id,
            after: Some(after),
        })
        .await
    {
        Ok(_) => panic!("invalid replay cursor unexpectedly attached"),
        Err(error) => error,
    }
}

fn contract_event_kind(event: &WorkflowEvent) -> &str {
    match &event.kind {
        WorkflowEventKind::WorkflowState(_) => "workflow_state",
        WorkflowEventKind::ExecutionState(_) => "execution_state",
        WorkflowEventKind::Message(_) => "message",
        WorkflowEventKind::ToolCall(_) => "tool_call",
        WorkflowEventKind::Plan(_) => "plan",
        WorkflowEventKind::Suspension(_) => "suspension",
        WorkflowEventKind::ClientToolRequest(_) => "client_tool_request",
        WorkflowEventKind::PermissionRequest(_) => "permission_request",
        WorkflowEventKind::Output(_) => "output",
        WorkflowEventKind::Usage(_) => "usage",
        WorkflowEventKind::Failure(_) => "failure",
        WorkflowEventKind::Compaction(_) => "compaction",
        WorkflowEventKind::Extension { kind, .. } => kind,
    }
}

pub fn contract_context(source: InvocationSource) -> RequestContext {
    if source == InvocationSource::Internal {
        contract_principal_context("contract-actor", PrincipalKind::Internal, source)
    } else {
        contract_user_context(source)
    }
}

pub fn contract_service_context(source: InvocationSource) -> RequestContext {
    contract_principal_context("contract-service", PrincipalKind::Service, source)
}

pub fn contract_user_context(source: InvocationSource) -> RequestContext {
    contract_principal_context("contract-actor", PrincipalKind::User, source)
}

fn contract_principal_context(
    actor_id: &str,
    principal: PrincipalKind,
    source: InvocationSource,
) -> RequestContext {
    TrustedContextFactory::new(
        actor_id,
        principal,
        Some("contract-app".to_string()),
        source,
    )
    .expect("contract authority is valid")
    .request_context("runtime-contract-request", "runtime-contract")
    .expect("contract invocation context is valid")
}

pub(crate) fn contract_context_for_application(
    context: &RequestContext,
    application_id: &str,
) -> RequestContext {
    RequestContext::builder(
        context.actor_id(),
        context.principal(),
        context.source().clone(),
        format!("{}-other-application", context.request_id()),
        context.correlation_id().clone(),
    )
    .application_id(application_id)
    .build()
    .expect("contract invocation context is valid")
}

#[derive(Default)]
pub struct ContractWorkflowRuntime {
    next_instance_id: AtomicU64,
    runs: Mutex<HashMap<WorkflowInstanceId, ContractRun>>,
    starts: Mutex<HashMap<String, (String, WorkflowRunHandle)>>,
}

struct ContractRun {
    workflow_id: WorkflowId,
    session_id: Option<SessionId>,
    state: WorkflowState,
    output: Option<Value>,
    suspension: Option<SuspensionHandle>,
    events: Vec<WorkflowEvent>,
    next_sequence: u64,
    oldest_sequence: u64,
}

impl ContractWorkflowRuntime {
    fn run_mut(
        runs: &mut HashMap<WorkflowInstanceId, ContractRun>,
        workflow_instance_id: WorkflowInstanceId,
    ) -> RuntimeResult<&mut ContractRun> {
        runs.get_mut(&workflow_instance_id).ok_or_else(|| {
            RuntimeError::new(
                RuntimeErrorCode::NotFound,
                format!("Workflow instance {} was not found", workflow_instance_id.0),
            )
        })
    }

    fn validate_context(context: &RequestContext, _run: &ContractRun) -> RuntimeResult<()> {
        if context.application_id() != Some("contract-app") {
            return Err(RuntimeError::new(
                RuntimeErrorCode::Forbidden,
                "request Application does not own this instance",
            ));
        }
        Ok(())
    }

    fn push_event(
        run: &mut ContractRun,
        workflow_instance_id: WorkflowInstanceId,
        kind: WorkflowEventKind,
    ) -> EventCursor {
        Self::push_scoped_event(run, workflow_instance_id, kind, None, None, None, None)
    }

    fn push_scoped_event(
        run: &mut ContractRun,
        workflow_instance_id: WorkflowInstanceId,
        kind: WorkflowEventKind,
        execution_id: Option<ExecutionId>,
        message_id: Option<MessageId>,
        tool_call_id: Option<ToolCallId>,
        plan_id: Option<PlanId>,
    ) -> EventCursor {
        run.next_sequence += 1;
        let sequence = run.next_sequence;
        let cursor = contract_cursor(workflow_instance_id, sequence);
        run.events.push(WorkflowEvent {
            event_id: EventId::from(format!(
                "contract-event-{}-{sequence}",
                workflow_instance_id.0
            )),
            cursor: cursor.clone(),
            workflow_id: run.workflow_id,
            workflow_instance_id,
            root_workflow_instance_id: RootWorkflowInstanceId(workflow_instance_id.0),
            execution_id,
            execution_path: execution_id.map(|id| format!("root/{}", id.0)),
            session_id: run.session_id,
            message_id,
            tool_call_id,
            plan_id,
            timestamp: format!("contract-{sequence}"),
            kind,
        });
        cursor
    }

    fn push_event_suite(
        run: &mut ContractRun,
        workflow_instance_id: WorkflowInstanceId,
    ) -> RuntimeResult<EventCursor> {
        let execution_id = ExecutionId(501);
        Self::push_scoped_event(
            run,
            workflow_instance_id,
            WorkflowEventKind::ExecutionState(ExecutionStateEvent {
                state: "running".to_string(),
                node_id: Some("contract-node".to_string()),
                node_type: Some("contract".to_string()),
            }),
            Some(execution_id),
            None,
            None,
            None,
        );
        for (phase, content) in [
            (MessagePhase::Started, None),
            (MessagePhase::Delta, Some("hello".to_string())),
            (MessagePhase::Completed, None),
        ] {
            Self::push_scoped_event(
                run,
                workflow_instance_id,
                WorkflowEventKind::Message(MessageEvent {
                    phase: phase.clone(),
                    role: "assistant".to_string(),
                    content,
                    reasoning_content: None,
                }),
                Some(execution_id),
                (phase == MessagePhase::Completed).then_some(MessageId(601)),
                None,
                None,
            );
        }
        Self::push_scoped_event(
            run,
            workflow_instance_id,
            WorkflowEventKind::ToolCall(ToolCallEvent {
                state: "completed".to_string(),
                name: "contract_tool".to_string(),
                arguments: Some(json!({"value": 1})),
                result: Some(json!({"ok": true})),
            }),
            Some(execution_id),
            None,
            Some(ToolCallId::from("tool-1")),
            None,
        );
        Self::push_scoped_event(
            run,
            workflow_instance_id,
            WorkflowEventKind::Plan(PlanEvent {
                version: 1,
                overview: Some("contract plan".to_string()),
                items: vec![PlanItem {
                    step: "verify".to_string(),
                    status: "in_progress".to_string(),
                }],
            }),
            Some(execution_id),
            None,
            None,
            Some(PlanId(701)),
        );
        let client_suspension =
            SuspensionHandle::issue(format!("contract-client-tool-{}", workflow_instance_id.0))?;
        Self::push_scoped_event(
            run,
            workflow_instance_id,
            WorkflowEventKind::ClientToolRequest(ClientToolRequestEvent {
                suspension: client_suspension,
                name: "contract_tool".to_string(),
                arguments: json!({"path": "README.md"}),
                execution: ClientToolExecutionContext {
                    client_instance_id: Some(ClientInstanceId::from("contract-client")),
                    ..Default::default()
                },
            }),
            Some(execution_id),
            None,
            Some(ToolCallId::from("client-tool-1")),
            None,
        );
        let permission_suspension =
            SuspensionHandle::issue(format!("contract-permission-{}", workflow_instance_id.0))?;
        Self::push_scoped_event(
            run,
            workflow_instance_id,
            WorkflowEventKind::PermissionRequest(PermissionRequestEvent {
                suspension: Some(permission_suspension),
                request_id: "permission-1".to_string(),
                action: "Run contract tool".to_string(),
                options: vec!["allow_once".to_string(), "reject".to_string()],
                details: Some(json!({"reason": "contract"})),
            }),
            Some(execution_id),
            None,
            Some(ToolCallId::from("permission-tool-1")),
            None,
        );
        Self::push_event(
            run,
            workflow_instance_id,
            WorkflowEventKind::Usage(UsageEvent {
                input_tokens: 10,
                output_tokens: 5,
                cost: Some("0.01".to_string()),
            }),
        );
        Self::push_event(
            run,
            workflow_instance_id,
            WorkflowEventKind::Compaction(CompactionEvent {
                phase: CompactionPhase::Succeeded,
                trigger: CompactionTrigger::Automatic,
                suspension: None,
                input_tokens_before: Some(100),
                input_tokens_after: Some(50),
                summary_tokens: Some(10),
                error: None,
            }),
        );
        Ok(Self::push_event(
            run,
            workflow_instance_id,
            WorkflowEventKind::Extension {
                kind: "contract_future_event".to_string(),
                payload: json!({"version": 1}),
            },
        ))
    }
}

#[async_trait]
impl ConversationService for ContractWorkflowRuntime {
    async fn create_session(&self, request: CreateSessionRequest) -> RuntimeResult<SessionHandle> {
        Ok(SessionHandle {
            session_id: SessionId(7),
            workflow_id: request.context.workflow_id(),
        })
    }

    async fn load_session(&self, request: LoadSessionRequest) -> RuntimeResult<SessionSnapshot> {
        Ok(SessionSnapshot {
            session_id: request.session_id,
            workflow_id: CONTRACT_WORKFLOW_ID,
            created_at: None,
            updated_at: None,
            actor_id: request.context.actor_id().to_string(),
            name: "contract session".to_string(),
            description: None,
            session_type: "contract".to_string(),
            application_id: request.context.application_id().map(str::to_string),
            active_workflow_instance_id: None,
            deleted: false,
            extensions: Map::new(),
        })
    }

    async fn load_messages(&self, _request: LoadMessagesRequest) -> RuntimeResult<MessagePage> {
        Ok(MessagePage {
            messages: Vec::new(),
            next_before: None,
        })
    }
}

#[async_trait]
impl WorkflowRuntime for ContractWorkflowRuntime {
    async fn start(&self, request: StartWorkflowRequest) -> RuntimeResult<WorkflowRunHandle> {
        let workflow_id = request.context.workflow_id();
        if workflow_id.0 == 0 {
            return Err(RuntimeError::new(
                RuntimeErrorCode::NotFound,
                "contract Workflow was not found",
            ));
        }
        let start_identity = request
            .context
            .request()
            .idempotency_key()
            .map(|idempotency_key| {
                format!(
                    "{}|{}|{:?}|{}|{}",
                    request
                        .context
                        .request()
                        .application_id()
                        .unwrap_or_default(),
                    request.context.request().actor_id(),
                    request.context.request().principal(),
                    workflow_id.0,
                    idempotency_key.as_str(),
                )
            });
        let start_digest = serde_json::to_string(&(
            workflow_id,
            request.session_id,
            &request.input,
            &request.bindings,
        ))
        .expect("contract Workflow start is serializable");
        if let Some(identity) = &start_identity {
            if let Some((digest, handle)) = self.starts.lock().unwrap().get(identity) {
                if digest == &start_digest {
                    return Ok(handle.clone());
                }
                return Err(RuntimeError::new(
                    RuntimeErrorCode::Conflict,
                    "Workflow start idempotency key is bound to another request",
                ));
            }
        }
        let workflow_instance_id =
            WorkflowInstanceId(self.next_instance_id.fetch_add(1, Ordering::Relaxed) + 100);
        let mut run = ContractRun {
            workflow_id,
            session_id: request.session_id,
            state: WorkflowState::Running,
            output: None,
            suspension: None,
            events: Vec::new(),
            next_sequence: 0,
            oldest_sequence: 1,
        };
        Self::push_event(
            &mut run,
            workflow_instance_id,
            WorkflowEventKind::WorkflowState(WorkflowStateEvent {
                state: WorkflowState::Running,
                reason: None,
            }),
        );
        self.runs.lock().unwrap().insert(workflow_instance_id, run);
        let handle = WorkflowRunHandle {
            workflow_id,
            workflow_instance_id,
            root_workflow_instance_id: RootWorkflowInstanceId(workflow_instance_id.0),
            session_id: request.session_id,
            input_message_id: request
                .session_id
                .map(|_| MessageId(workflow_instance_id.0)),
            subscription_anchor: contract_cursor(workflow_instance_id, 0),
        };
        if let Some(identity) = start_identity {
            self.starts
                .lock()
                .unwrap()
                .insert(identity, (start_digest, handle.clone()));
        }
        Ok(handle)
    }

    async fn signal(&self, request: SignalWorkflowRequest) -> RuntimeResult<CommandAck> {
        let mut runs = self.runs.lock().unwrap();
        let run = Self::run_mut(&mut runs, request.workflow_instance_id)?;
        Self::validate_context(&request.context, run)?;
        if run.state.is_terminal() {
            return Err(RuntimeError::new(
                RuntimeErrorCode::Conflict,
                "terminal Workflow cannot receive a signal",
            ));
        }

        if let WorkflowSignal::SuspensionResponse {
            suspension,
            response: _,
        } = &request.signal
        {
            if run.state != WorkflowState::Suspended || run.suspension.as_ref() != Some(suspension)
            {
                return Err(RuntimeError::new(
                    RuntimeErrorCode::StaleSuspension,
                    "suspension is stale or already settled",
                ));
            }
            run.state = WorkflowState::Running;
            run.suspension = None;
            let cursor = Self::push_event(
                run,
                request.workflow_instance_id,
                WorkflowEventKind::WorkflowState(WorkflowStateEvent {
                    state: WorkflowState::Running,
                    reason: Some("resumed".to_string()),
                }),
            );
            return Ok(accepted(request.workflow_instance_id, cursor));
        }

        let kind = match request.signal {
            WorkflowSignal::Business { name, .. } if name == "contract_events" => {
                let cursor = Self::push_event_suite(run, request.workflow_instance_id)?;
                return Ok(accepted(request.workflow_instance_id, cursor));
            }
            WorkflowSignal::Business { name, payload } if name == "contract_complete" => {
                run.output = Some(payload.clone());
                Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::Output(payload),
                );
                run.state = WorkflowState::Completed;
                let cursor = Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::WorkflowState(WorkflowStateEvent {
                        state: WorkflowState::Completed,
                        reason: None,
                    }),
                );
                return Ok(accepted(request.workflow_instance_id, cursor));
            }
            WorkflowSignal::Business { name, payload } if name == "contract_fail" => {
                let failure = FailureEvent {
                    code: "contract_failure".to_string(),
                    message: "contract Workflow failed".to_string(),
                    retryable: false,
                    details: Some(payload),
                };
                Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::Failure(failure),
                );
                run.state = WorkflowState::Failed;
                let cursor = Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::WorkflowState(WorkflowStateEvent {
                        state: WorkflowState::Failed,
                        reason: None,
                    }),
                );
                return Ok(accepted(request.workflow_instance_id, cursor));
            }
            WorkflowSignal::Business { name, .. } if name == "contract_replay_gap" => {
                run.events.clear();
                run.oldest_sequence = run.next_sequence + 1;
                let cursor = Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::Extension {
                        kind: "contract_after_compaction".to_string(),
                        payload: Value::Null,
                    },
                );
                return Ok(accepted(request.workflow_instance_id, cursor));
            }
            WorkflowSignal::Business { name, .. } if name == "contract_duplicate_events" => {
                Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::Extension {
                        kind: "contract_duplicate_source".to_string(),
                        payload: json!({"position": "first"}),
                    },
                );
                let duplicate_event_id = run
                    .events
                    .last()
                    .expect("the source event was just appended")
                    .event_id
                    .clone();
                Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::Extension {
                        kind: "contract_duplicate_source".to_string(),
                        payload: json!({"position": "replayed"}),
                    },
                );
                run.events
                    .last_mut()
                    .expect("the duplicate event was just appended")
                    .event_id = duplicate_event_id;
                let cursor = Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::Extension {
                        kind: "contract_after_duplicate".to_string(),
                        payload: Value::Null,
                    },
                );
                return Ok(accepted(request.workflow_instance_id, cursor));
            }
            WorkflowSignal::Business { name, .. } if name == "contract_complete_without_output" => {
                run.state = WorkflowState::Completed;
                let cursor = Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::WorkflowState(WorkflowStateEvent {
                        state: WorkflowState::Completed,
                        reason: None,
                    }),
                );
                return Ok(accepted(request.workflow_instance_id, cursor));
            }
            WorkflowSignal::Business { name, .. } if name == "contract_multiple_outputs" => {
                Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::Output(json!({"position": "first"})),
                );
                Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::Output(json!({"position": "second"})),
                );
                run.state = WorkflowState::Completed;
                let cursor = Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::WorkflowState(WorkflowStateEvent {
                        state: WorkflowState::Completed,
                        reason: None,
                    }),
                );
                return Ok(accepted(request.workflow_instance_id, cursor));
            }
            WorkflowSignal::Business { name, payload } if name == "contract_suspend" => {
                let handle = SuspensionHandle::issue(format!(
                    "contract-{}-{}",
                    request.workflow_instance_id.0,
                    run.next_sequence + 1
                ))?;
                run.state = WorkflowState::Suspended;
                run.suspension = Some(handle.clone());
                Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::Suspension(SuspensionEvent {
                        handle,
                        reason: "contract suspension".to_string(),
                        payload: Some(payload),
                    }),
                );
                let cursor = Self::push_event(
                    run,
                    request.workflow_instance_id,
                    WorkflowEventKind::WorkflowState(WorkflowStateEvent {
                        state: WorkflowState::Suspended,
                        reason: None,
                    }),
                );
                return Ok(accepted(request.workflow_instance_id, cursor));
            }
            signal => WorkflowEventKind::Extension {
                kind: "contract_signal".to_string(),
                payload: serde_json::to_value(signal).map_err(|error| {
                    RuntimeError::new(RuntimeErrorCode::Internal, error.to_string())
                })?,
            },
        };
        let cursor = Self::push_event(run, request.workflow_instance_id, kind);
        Ok(accepted(request.workflow_instance_id, cursor))
    }

    async fn terminate(&self, request: TerminateWorkflowRequest) -> RuntimeResult<CommandAck> {
        let mut runs = self.runs.lock().unwrap();
        let run = Self::run_mut(&mut runs, request.workflow_instance_id)?;
        Self::validate_context(&request.context, run)?;
        run.state = WorkflowState::Terminated;
        run.suspension = None;
        let cursor = Self::push_event(
            run,
            request.workflow_instance_id,
            WorkflowEventKind::WorkflowState(WorkflowStateEvent {
                state: WorkflowState::Terminated,
                reason: request.reason,
            }),
        );
        Ok(CommandAck {
            workflow_instance_id: request.workflow_instance_id,
            disposition: CommandDisposition::Terminated,
            cursor: Some(cursor),
            extensions: Map::new(),
        })
    }

    async fn snapshot(&self, request: WorkflowSnapshotRequest) -> RuntimeResult<WorkflowSnapshot> {
        let runs = self.runs.lock().unwrap();
        let run = runs
            .get(&request.workflow_instance_id)
            .ok_or_else(|| RuntimeError::new(RuntimeErrorCode::NotFound, "instance not found"))?;
        Self::validate_context(&request.context, run)?;
        Ok(WorkflowSnapshot {
            workflow_id: run.workflow_id,
            workflow_instance_id: request.workflow_instance_id,
            root_workflow_instance_id: RootWorkflowInstanceId(request.workflow_instance_id.0),
            active_execution_id: None,
            session_id: run.session_id,
            state: run.state.clone(),
            output: run.output.clone(),
            suspension: run.suspension.clone(),
            latest_cursor: run.events.last().map(|event| event.cursor.clone()),
        })
    }

    async fn subscribe(
        &self,
        request: SubscribeWorkflowRequest,
    ) -> RuntimeResult<WorkflowEventStream> {
        let runs = self.runs.lock().unwrap();
        let run = runs
            .get(&request.workflow_instance_id)
            .ok_or_else(|| RuntimeError::new(RuntimeErrorCode::NotFound, "instance not found"))?;
        Self::validate_context(&request.context, run)?;
        let after = request
            .after
            .as_ref()
            .map(|cursor| parse_contract_cursor(request.workflow_instance_id, cursor))
            .transpose()?
            .unwrap_or_default();
        if after > run.next_sequence || after.saturating_add(1) < run.oldest_sequence {
            return Err(RuntimeError::new(
                RuntimeErrorCode::ReplayUnavailable,
                "contract replay cursor is outside the retained range",
            )
            .with_details(json!({
                "oldest_cursor": contract_cursor(
                    request.workflow_instance_id,
                    run.oldest_sequence,
                ),
                "latest_cursor": contract_cursor(
                    request.workflow_instance_id,
                    run.next_sequence,
                ),
            })));
        }
        let events = run
            .events
            .iter()
            .filter(|event| {
                parse_contract_cursor(request.workflow_instance_id, &event.cursor)
                    .is_ok_and(|sequence| sequence > after)
            })
            .cloned()
            .map(Ok)
            .collect::<Vec<_>>();
        Ok(Box::pin(stream::iter(events)))
    }
}

fn accepted(workflow_instance_id: WorkflowInstanceId, cursor: EventCursor) -> CommandAck {
    CommandAck {
        workflow_instance_id,
        disposition: CommandDisposition::Accepted,
        cursor: Some(cursor),
        extensions: Map::new(),
    }
}

fn contract_cursor(workflow_instance_id: WorkflowInstanceId, sequence: u64) -> EventCursor {
    EventCursor::from(format!("contract-{}:{sequence}", workflow_instance_id.0))
}

fn parse_contract_cursor(
    workflow_instance_id: WorkflowInstanceId,
    cursor: &EventCursor,
) -> RuntimeResult<u64> {
    let (generation, sequence) = cursor
        .as_ref()
        .split_once(':')
        .ok_or_else(|| RuntimeError::invalid_request("invalid contract event cursor"))?;
    let sequence = sequence
        .parse::<u64>()
        .map_err(|_| RuntimeError::invalid_request("invalid contract event cursor"))?;
    let expected_generation = format!("contract-{}", workflow_instance_id.0);
    if generation != expected_generation {
        return Err(RuntimeError::new(
            RuntimeErrorCode::ReplayUnavailable,
            "contract event cursor belongs to another generation",
        ));
    }
    Ok(sequence)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shared_contract_harness_passes_direct_embedded_binding() {
        let runtime: Arc<dyn WorkflowRuntime> = Arc::new(ContractWorkflowRuntime::default());
        let report = assert_workflow_runtime_contract(
            runtime,
            contract_context(InvocationSource::EmbeddedSdk),
        )
        .await;

        assert_eq!(report.workflow_id, CONTRACT_WORKFLOW_ID);
    }

    #[tokio::test]
    async fn product_runtime_contract_harness_passes_direct_in_process_facade() {
        let fixture = ContractProductRuntimeFixture::default();
        let report = assert_product_runtime_contract(
            &fixture.facade,
            contract_service_context(InvocationSource::EmbeddedSdk),
            contract_user_context(InvocationSource::EmbeddedSdk),
        )
        .await;

        assert_eq!(report.events.event_id.get(), 8_101);
        assert_eq!(report.workflow.workflow_id, CONTRACT_WORKFLOW_ID);
        assert_eq!(report.user_tasks.todo_task_id, CONTRACT_TODO_TASK_ID);
    }
}
