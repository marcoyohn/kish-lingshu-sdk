use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, StreamExt};
use kish_lingshu_runtime_contract::*;
use serde_json::json;

fn context() -> RequestContext {
    RequestContext::builder(
        "actor-1",
        PrincipalKind::User,
        InvocationSource::EmbeddedSdk,
        "request-1",
        "correlation-1",
    )
    .application_id("app-1")
    .build()
    .unwrap()
}

#[test]
fn identities_are_transparently_serialized() {
    assert_eq!(serde_json::to_value(SessionId(7)).unwrap(), json!(7));
    assert_eq!(
        serde_json::to_value(ToolCallId::from("call-1")).unwrap(),
        json!("call-1")
    );
    assert_eq!(
        serde_json::from_value::<WorkflowInstanceId>(json!(9)).unwrap(),
        WorkflowInstanceId(9)
    );
}

#[test]
fn common_application_problem_fixtures_preserve_unknown_codes() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/application_problems.json")).unwrap();
    let problems: Vec<ApplicationProblem> = serde_json::from_value(fixture.clone()).unwrap();

    assert_eq!(problems[0].code.as_str(), INVALID_REQUEST_PROBLEM);
    assert_eq!(problems[1].code.as_str(), "future_domain.limit_changed");
    assert!(problems[1].retryable);
    assert_eq!(serde_json::to_value(problems).unwrap(), fixture);
}

#[test]
fn workflow_command_fixtures_round_trip_without_trusted_coordinates() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/workflow_commands.json")).unwrap();
    let start: StartWorkflowRequest = serde_json::from_value(fixture["start"].clone()).unwrap();
    let signal: SignalWorkflowRequest = serde_json::from_value(fixture["signal"].clone()).unwrap();
    let terminate: TerminateWorkflowRequest =
        serde_json::from_value(fixture["terminate"].clone()).unwrap();

    assert_eq!(start.context.workflow_id(), WorkflowId(42));
    assert_eq!(signal.workflow_instance_id, WorkflowInstanceId(9001));
    assert_eq!(terminate.reason.as_deref(), Some("operator_cancelled"));
    assert_eq!(serde_json::to_value(start).unwrap(), fixture["start"]);
    assert_eq!(serde_json::to_value(signal).unwrap(), fixture["signal"]);
    assert_eq!(
        serde_json::to_value(terminate).unwrap(),
        fixture["terminate"]
    );

    let wire = fixture.to_string();
    assert!(!wire.contains("execution_id"));
    assert!(!wire.contains("execution_seq"));
    assert!(!wire.contains("suspension_id"));
}

#[test]
fn user_task_fixtures_cover_queries_values_actions_and_receipts() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/user_tasks.json")).unwrap();

    let application_query: ApplicationTaskQuery =
        serde_json::from_value(fixture["application_query"].clone()).unwrap();
    let current_query: CurrentUserTaskQuery =
        serde_json::from_value(fixture["current_user_query"].clone()).unwrap();
    let task: UserTask = serde_json::from_value(fixture["task"].clone()).unwrap();
    let claim: ClaimTask = serde_json::from_value(fixture["actions"]["claim"].clone()).unwrap();
    let mark_read: MarkTaskRead =
        serde_json::from_value(fixture["actions"]["mark_read"].clone()).unwrap();
    let save_draft: SaveTaskDraft =
        serde_json::from_value(fixture["actions"]["save_draft"].clone()).unwrap();
    let submit: SubmitTask = serde_json::from_value(fixture["actions"]["submit"].clone()).unwrap();
    let receipt: TaskActionReceipt = serde_json::from_value(fixture["receipt"].clone()).unwrap();

    assert_eq!(application_query.pagination.page_size, 50);
    assert_eq!(
        current_query.participant_state,
        Some(UserTaskParticipantState::Unknown(
            "awaiting_delegate".into()
        ))
    );
    assert_eq!(task.summary.revision.get(), 7);
    assert_eq!(claim.precondition.expected_revision.get(), 6);
    assert_eq!(mark_read.precondition.task_id.get(), 7002);
    assert_eq!(save_draft.draft, json!({"approved": true}));
    assert_eq!(submit.submission["comment"], "verified");
    assert_eq!(
        receipt.observation_cursor.as_ref().unwrap().as_ref(),
        "run-9001:21"
    );

    assert_eq!(
        serde_json::to_value(application_query).unwrap(),
        fixture["application_query"]
    );
    assert_eq!(
        serde_json::to_value(current_query).unwrap(),
        fixture["current_user_query"]
    );
    assert_eq!(serde_json::to_value(task).unwrap(), fixture["task"]);
    assert_eq!(serde_json::to_value(receipt).unwrap(), fixture["receipt"]);
}

#[test]
fn client_mcp_binding_serializes_only_schema_and_logical_identity() {
    let bindings = RuntimeBindings {
        workspace_bindings: None,
        client_tools: vec![ClientToolBinding {
            id: "mcp__ide__format".to_string(),
            name: "format".to_string(),
            description: "Format one document".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {"uri": {"type": "string"}}
            }),
            provider: ClientToolProvider::Mcp {
                server_name: "ide".to_string(),
                tool_name: "format".to_string(),
            },
        }],
        trusted_start: None,
        internal_published_start: None,
    };

    let wire = serde_json::to_value(bindings).unwrap();
    assert_eq!(wire["client_tools"][0]["provider"]["type"], "mcp");
    assert_eq!(wire["client_tools"][0]["provider"]["server_name"], "ide");
    assert_eq!(wire["client_tools"][0]["provider"]["tool_name"], "format");
    let wire = wire.to_string();
    for forbidden in [
        "command",
        "url",
        "headers",
        "environment",
        "credentials",
        "physical_cwd",
        "/Users/private/project",
    ] {
        assert!(
            !wire.contains(forbidden),
            "leaked forbidden field: {forbidden}"
        );
    }

    let injected = json!({
        "id": "mcp__ide__format",
        "name": "format",
        "description": "Format one document",
        "input_schema": {"type": "object"},
        "provider": {
            "type": "mcp",
            "server_name": "ide",
            "tool_name": "format"
        },
        "command": "/usr/bin/ide-mcp"
    });
    assert!(serde_json::from_value::<ClientToolBinding>(injected).is_err());
}

#[test]
fn unknown_event_kind_is_preserved() {
    let wire = json!({
        "type": "future_node_progress",
        "data": {"percent": 25, "label": "indexing"}
    });
    let kind: WorkflowEventKind = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(
        kind,
        WorkflowEventKind::Extension {
            kind: "future_node_progress".to_owned(),
            payload: json!({"percent": 25, "label": "indexing"}),
        }
    );
    assert_eq!(serde_json::to_value(kind).unwrap(), wire);
}

#[test]
fn workflow_event_serializes_kind_at_the_top_level() {
    let event = WorkflowEvent {
        event_id: EventId::from("run-1:1"),
        cursor: EventCursor::from("run-1:1"),
        workflow_id: WorkflowId(42),
        workflow_instance_id: WorkflowInstanceId(10),
        root_workflow_instance_id: RootWorkflowInstanceId(10),
        execution_id: None,
        execution_path: None,
        session_id: None,
        message_id: None,
        tool_call_id: None,
        plan_id: None,
        timestamp: "2026-08-22T00:00:00Z".to_string(),
        kind: WorkflowEventKind::Message(MessageEvent {
            phase: MessagePhase::Delta,
            role: "assistant".to_string(),
            content: Some("hello".to_string()),
            reasoning_content: None,
        }),
    };

    let wire = serde_json::to_value(event).unwrap();
    assert_eq!(wire["type"], "message");
    assert_eq!(wire["data"]["phase"], "delta");
    assert!(wire.get("kind").is_none());
    assert!(wire.get("source_event").is_none());
    assert!(wire.get("extensions").is_none());
}

#[test]
fn async_start_fixture_round_trips_as_a_direct_run_handle() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/async_start.json")).unwrap();
    let handle: WorkflowRunHandle = serde_json::from_value(fixture.clone()).unwrap();

    assert_eq!(handle.workflow_instance_id, WorkflowInstanceId(9001));
    assert_eq!(handle.input_message_id, Some(MessageId(501)));
    assert_eq!(serde_json::to_value(handle).unwrap(), fixture);
}

#[test]
fn every_canonical_event_fixture_round_trips_flat() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/events.json")).unwrap();
    let events: Vec<WorkflowEvent> = serde_json::from_value(fixture.clone()).unwrap();
    let kinds = events
        .iter()
        .map(|event| match &event.kind {
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
        })
        .collect::<Vec<_>>();

    for required in [
        "workflow_state",
        "execution_state",
        "message",
        "tool_call",
        "plan",
        "suspension",
        "client_tool_request",
        "permission_request",
        "output",
        "usage",
        "failure",
        "compaction",
        "future_node_progress",
    ] {
        assert!(
            kinds.contains(&required),
            "missing event fixture: {required}"
        );
    }
    assert_ne!(events[0].event_id.as_ref(), events[0].cursor.as_ref());
    let first_wire = serde_json::to_value(&events[0]).unwrap();
    for optional in [
        "execution_id",
        "execution_path",
        "message_id",
        "tool_call_id",
        "plan_id",
    ] {
        assert!(first_wire.get(optional).is_none());
    }
    assert_eq!(serde_json::to_value(events).unwrap(), fixture);
}

#[test]
fn synchronous_result_fixtures_enforce_state_payloads() {
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/run_results.json")).unwrap();
    for name in ["completed", "failed", "suspended", "terminated"] {
        let fixture = fixtures[name].clone();
        let result: WorkflowRunResult = serde_json::from_value(fixture.clone()).unwrap();
        assert_eq!(serde_json::to_value(result).unwrap(), fixture, "{name}");
    }

    assert!(serde_json::from_value::<WorkflowRunResult>(json!({
        "run": fixtures["completed"]["run"],
        "state": "completed"
    }))
    .is_err());
    assert!(serde_json::from_value::<WorkflowRunResult>(json!({
        "run": fixtures["failed"]["run"],
        "state": "failed",
        "failure": fixtures["failed"]["failure"],
        "output": null
    }))
    .is_err());
    assert!(serde_json::from_value::<WorkflowRunResult>(json!({
        "run": fixtures["suspended"]["run"],
        "state": "suspended",
        "suspension": fixtures["suspended"]["suspension"],
        "failure": fixtures["failed"]["failure"]
    }))
    .is_err());
    assert!(serde_json::from_value::<WorkflowRunResult>(json!({
        "run": fixtures["terminated"]["run"],
        "state": "terminated",
        "output": {"unexpected": true}
    }))
    .is_err());
}

#[test]
fn runtime_error_fixtures_are_direct_and_have_stable_http_mappings() {
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/runtime_errors.json")).unwrap();
    for (name, expected_status, expected_code) in [
        ("invalid_request", 400, RuntimeErrorCode::InvalidRequest),
        ("stale_generation", 410, RuntimeErrorCode::ReplayUnavailable),
        (
            "replay_buffer_gap",
            410,
            RuntimeErrorCode::ReplayUnavailable,
        ),
    ] {
        assert_eq!(fixtures[name]["http_status"], expected_status);
        let body = fixtures[name]["body"].clone();
        let error: RuntimeError = serde_json::from_value(body.clone()).unwrap();
        assert_eq!(error.code, expected_code);
        assert_eq!(serde_json::to_value(error).unwrap(), body);
        assert!(body.get("status").is_none());
        assert!(body.get("data").is_none());
        assert!(body.get("runtime_error").is_none());
    }
}

#[test]
fn sse_replay_fixture_uses_cursor_as_frame_id() {
    let frames = include_str!("fixtures/sse_replay.txt")
        .split("\n\n")
        .filter(|frame| !frame.trim().is_empty())
        .collect::<Vec<_>>();
    assert_eq!(frames.len(), 3);
    assert_eq!(frames[1].trim(), ": heartbeat");

    for frame in [frames[0], frames[2]] {
        let id = frame
            .lines()
            .find_map(|line| line.strip_prefix("id: "))
            .unwrap();
        assert!(frame.lines().any(|line| line == "event: workflow_event"));
        let data = frame
            .lines()
            .find_map(|line| line.strip_prefix("data: "))
            .unwrap();
        let event: WorkflowEvent = serde_json::from_str(data).unwrap();
        assert_eq!(event.cursor.as_ref(), id);
    }
}

#[test]
fn suspension_handle_checks_version_without_exposing_payload() {
    let handle = SuspensionHandle::issue("signed-opaque-value").unwrap();
    assert_eq!(handle.version().unwrap(), SUSPENSION_HANDLE_VERSION);
    assert_eq!(handle.as_str(), "v1.signed-opaque-value");
    assert_eq!(SuspensionHandle::parse(handle.as_str()).unwrap(), handle);

    let error = SuspensionHandle::parse("v2.future-value").unwrap_err();
    assert_eq!(error.code, RuntimeErrorCode::Unsupported);
}

#[test]
fn suspension_signal_routes_by_public_instance_without_exposing_internal_coordinates() {
    let request = SignalWorkflowRequest {
        context: context(),
        workflow_instance_id: WorkflowInstanceId(10),
        signal: WorkflowSignal::SuspensionResponse {
            suspension: SuspensionHandle::issue("signed-opaque-value").unwrap(),
            response: ResumePayload::UserAnswer {
                value: json!("yes"),
            },
        },
    };
    let wire = serde_json::to_value(request).unwrap();

    assert_eq!(wire["workflow_instance_id"], 10);
    assert_eq!(
        wire["signal"]["data"]["suspension"],
        "v1.signed-opaque-value"
    );
    assert!(wire.get("execution_id").is_none());
    assert!(wire.get("execution_seq").is_none());
    assert!(wire.get("resume_event").is_none());
}

#[test]
fn invocation_context_has_no_policy_or_placement_surface() {
    let value = serde_json::to_value(context()).unwrap();
    assert_eq!(value["actor_id"], "actor-1");
    assert_eq!(value["principal"], "user");
    assert_eq!(value["request_id"], "request-1");
    assert!(value.get("entrypoint").is_none());
    assert!(value.get("workflow_id").is_none());
    assert!(value.get("model").is_none());
    assert!(value.get("draft").is_none());
    assert!(value.get("permission_policy").is_none());
    assert!(value.get("placement").is_none());
}

#[test]
fn workflow_start_request_has_no_runtime_binding_selector() {
    let value = serde_json::to_value(StartWorkflowRequest {
        context: WorkflowContext::new(context(), WorkflowId(42)),
        session_id: Some(SessionId(3)),
        input: WorkflowInput::structured(json!({"prompt": "hello"})),
        bindings: RuntimeBindings::default(),
    })
    .unwrap();

    assert!(value.get("runtime_binding").is_none());
    assert!(value.get("placement").is_none());
    assert!(value.get("deployment_id").is_none());
    assert!(value.get("execution_tier").is_none());
}

#[test]
fn workflow_event_route_contract_contains_no_catalog_or_target_identity() {
    let value = serde_json::to_value(RouteWorkflowEventRequest {
        event: WorkflowRouteEvent {
            event_id: 1,
            topic: "orders".into(),
            event_type: "order.created".into(),
            schema_version: "1".into(),
            source: "order-service".into(),
            subject: None,
            occurred_at: "2026-08-28T10:00:00Z".into(),
            published_at: "2026-08-28T10:00:01Z".into(),
            not_before: "2026-08-28T10:00:01Z".into(),
            partition_key: Some("order-42".into()),
            correlation_id: None,
            causation_id: None,
            headers: Default::default(),
            payload: json!({"order_id": "42"}),
            schedule: None,
        },
        consumption: WorkflowRouteConsumption {
            subscription_id: 2,
            group_id: 3,
            subscription_epoch: 2,
            queue_epoch: 1,
            queue_id: 3,
            queue_offset: 9,
            consumption_id: Some(4),
            invocation_id: 5,
            attempt_generation: 1,
        },
        delivery: WorkflowRouteDelivery {
            mode: WorkflowRouteDeliveryMode::Sync,
            idempotency_key: "event-consumption/2/2/1/3/9".into(),
            completion: None,
        },
    })
    .unwrap();

    let wire = value.to_string();
    assert_eq!(value["event"]["topic"], "orders");
    assert_eq!(value["event"]["event_type"], "order.created");
    assert!(!wire.contains("namespace"));
    assert!(!wire.contains("workflow_id"));
    assert!(!wire.contains("workflow_define_id"));
    assert!(!wire.contains("start_node_id"));
    assert!(!wire.contains("catalog_version"));
}

#[test]
fn client_workspace_binding_uses_only_provider_neutral_runtime_paths() {
    let bindings = RuntimeBindings {
        workspace_bindings: Some(WorkspaceBindings::Client {
            workspace_id: None,
            client_instance_id: ClientInstanceId::from("client-1"),
            application_cache_root: CLIENT_TEMPORARY_RUNTIME_ROOT.to_string(),
            shell_os: Some("macos".to_string()),
            persistent: Some(ClientPersistentWorkspaceBinding {
                root_path: CLIENT_PERSISTENT_RUNTIME_ROOT.to_string(),
                mode: WorkspaceBindingMode::ReadWrite,
            }),
        }),
        ..RuntimeBindings::default()
    };
    let value = serde_json::to_value(bindings).unwrap();

    assert_eq!(value["workspace_bindings"]["provider"], "client");
    assert_eq!(
        value["workspace_bindings"]["application_cache_root"],
        CLIENT_TEMPORARY_RUNTIME_ROOT
    );
    assert_eq!(
        value["workspace_bindings"]["persistent"]["root_path"],
        CLIENT_PERSISTENT_RUNTIME_ROOT
    );
    assert!(!value.to_string().contains("/Users/"));
}

struct StubRuntime;

#[async_trait]
impl WorkflowRuntime for StubRuntime {
    async fn start(&self, request: StartWorkflowRequest) -> RuntimeResult<WorkflowRunHandle> {
        Ok(WorkflowRunHandle {
            workflow_id: request.context.workflow_id(),
            workflow_instance_id: WorkflowInstanceId(10),
            root_workflow_instance_id: RootWorkflowInstanceId(10),
            session_id: request.session_id,
            input_message_id: request.session_id.map(|_| MessageId(11)),
            subscription_anchor: EventCursor::from("0"),
        })
    }

    async fn signal(&self, request: SignalWorkflowRequest) -> RuntimeResult<CommandAck> {
        Ok(accepted(request.workflow_instance_id))
    }

    async fn terminate(&self, request: TerminateWorkflowRequest) -> RuntimeResult<CommandAck> {
        Ok(CommandAck {
            workflow_instance_id: request.workflow_instance_id,
            disposition: CommandDisposition::Terminated,
            cursor: None,
            extensions: Default::default(),
        })
    }

    async fn snapshot(&self, request: WorkflowSnapshotRequest) -> RuntimeResult<WorkflowSnapshot> {
        Ok(WorkflowSnapshot {
            workflow_id: WorkflowId(42),
            workflow_instance_id: request.workflow_instance_id,
            root_workflow_instance_id: RootWorkflowInstanceId(request.workflow_instance_id.0),
            active_execution_id: None,
            session_id: None,
            state: WorkflowState::Running,
            output: None,
            suspension: None,
            latest_cursor: None,
        })
    }

    async fn subscribe(
        &self,
        _request: SubscribeWorkflowRequest,
    ) -> RuntimeResult<WorkflowEventStream> {
        Ok(Box::pin(stream::empty()))
    }
}

fn accepted(workflow_instance_id: WorkflowInstanceId) -> CommandAck {
    CommandAck {
        workflow_instance_id,
        disposition: CommandDisposition::Accepted,
        cursor: None,
        extensions: Default::default(),
    }
}

#[tokio::test]
async fn workflow_runtime_is_object_safe_and_stream_is_usable() {
    let runtime: Arc<dyn WorkflowRuntime> = Arc::new(StubRuntime);
    let run = runtime
        .start(StartWorkflowRequest {
            context: WorkflowContext::new(context(), WorkflowId(42)),
            session_id: Some(SessionId(3)),
            input: WorkflowInput::structured(json!({"prompt": "hello"})),
            bindings: RuntimeBindings::default(),
        })
        .await
        .unwrap();
    assert_eq!(run.workflow_id, WorkflowId(42));
    assert_eq!(run.workflow_instance_id, WorkflowInstanceId(10));

    let mut events = runtime
        .subscribe(SubscribeWorkflowRequest {
            context: context(),
            workflow_instance_id: run.workflow_instance_id,
            after: Some(run.subscription_anchor),
        })
        .await
        .unwrap();
    assert!(events.next().await.is_none());
}
