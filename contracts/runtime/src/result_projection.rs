use futures::StreamExt;
use serde_json::Value;

use crate::{
    EventCursor, RuntimeError, RuntimeErrorCode, RuntimeResult, SuspensionEvent, WorkflowEvent,
    WorkflowEventKind, WorkflowEventStream, WorkflowRunHandle, WorkflowRunResult, WorkflowState,
    WorkflowWaitMode, WorkflowWaitOptions,
};

/// Projects one root Workflow boundary from the canonical ordered event stream.
pub struct WorkflowResultProjector {
    run: WorkflowRunHandle,
    mode: WorkflowWaitMode,
    last_state: WorkflowState,
    safe_cursor: EventCursor,
    output: Option<Value>,
    failure: Option<crate::FailureEvent>,
    suspension: Option<SuspensionEvent>,
}

impl WorkflowResultProjector {
    pub fn new(run: WorkflowRunHandle) -> Self {
        Self::with_mode(run, WorkflowWaitMode::UntilAction)
    }

    pub fn with_mode(run: WorkflowRunHandle, mode: WorkflowWaitMode) -> Self {
        Self {
            safe_cursor: run.subscription_anchor.clone(),
            run,
            mode,
            last_state: WorkflowState::Pending,
            output: None,
            failure: None,
            suspension: None,
        }
    }

    pub fn cursor(&self) -> &EventCursor {
        &self.safe_cursor
    }

    pub fn with_cursor(mut self, cursor: EventCursor) -> Self {
        self.safe_cursor = cursor;
        self
    }

    pub fn pending(&self) -> WorkflowRunResult {
        WorkflowRunResult::Pending {
            run: self.run.clone(),
            last_state: self.last_state.clone(),
            cursor: self.safe_cursor.clone(),
        }
    }

    /// Shared bounded observer for runtime and SDK streams. Transport errors
    /// retain their original type; only projection/EOF errors use map_error.
    pub async fn wait<S, E>(
        mut self,
        budget: std::time::Duration,
        subscribe: impl std::future::Future<Output = Result<S, E>>,
        mut commit_cursor: impl FnMut(&EventCursor),
        map_error: impl Fn(RuntimeError) -> E,
    ) -> Result<WorkflowRunResult, E>
    where
        S: futures::Stream<Item = Result<WorkflowEvent, E>> + Unpin,
    {
        if budget.is_zero() {
            return Ok(self.pending());
        }
        let deadline = tokio::time::Instant::now() + budget;
        let mut events = match tokio::time::timeout_at(deadline, subscribe).await {
            Ok(result) => result?,
            Err(_) => return Ok(self.pending()),
        };
        let mut seen = std::collections::HashSet::new();
        let mut recent = std::collections::VecDeque::new();
        loop {
            // Ready streams must not starve the observation deadline. Keep
            // duplicate tracking bounded even for a high-volume model stream.
            if tokio::time::Instant::now() >= deadline {
                return Ok(self.pending());
            }
            let event = match tokio::time::timeout_at(deadline, events.next()).await {
                Err(_) => return Ok(self.pending()),
                Ok(Some(event)) => event?,
                Ok(None) => return self.finish().map_err(&map_error),
            };
            if !seen.insert(event.event_id.clone()) {
                self.validate_identity(&event).map_err(&map_error)?;
                continue;
            }
            recent.push_back(event.event_id.clone());
            if recent.len() > 4096 {
                if let Some(expired) = recent.pop_front() {
                    seen.remove(&expired);
                }
            }
            let result = self.observe(&event).map_err(&map_error)?;
            commit_cursor(self.cursor());
            if let Some(result) = result {
                return Ok(result);
            }
        }
    }

    pub fn observe(&mut self, event: &WorkflowEvent) -> RuntimeResult<Option<WorkflowRunResult>> {
        self.validate_identity(event)?;
        let result = self.observe_root(event)?;
        // Do not commit the cursor after a detail until its matching state is consumed.
        if self.output.is_none() && self.failure.is_none() && self.suspension.is_none() {
            self.safe_cursor = event.cursor.clone();
        }
        Ok(result)
    }

    fn validate_identity(&self, event: &WorkflowEvent) -> RuntimeResult<()> {
        if event.event_id.as_ref().is_empty()
            || event.cursor.as_ref().is_empty()
            || event.root_workflow_instance_id != self.run.root_workflow_instance_id
        {
            return Err(invariant(
                "Workflow event identities do not match attached run",
            ));
        }
        Ok(())
    }

    fn observe_root(&mut self, event: &WorkflowEvent) -> RuntimeResult<Option<WorkflowRunResult>> {
        if event.workflow_instance_id != self.run.workflow_instance_id {
            return Ok(None);
        }
        match &event.kind {
            WorkflowEventKind::Output(output) => {
                if self.output.replace(output.clone()).is_some() {
                    return Err(invariant("root Workflow emitted more than one output"));
                }
            }
            WorkflowEventKind::Failure(failure) => {
                if self.failure.replace(failure.clone()).is_some() {
                    return Err(invariant(
                        "root Workflow emitted more than one failure before a boundary",
                    ));
                }
            }
            WorkflowEventKind::Suspension(suspension) => {
                if self.suspension.replace(suspension.clone()).is_some() {
                    return Err(invariant(
                        "root Workflow emitted more than one suspension before a boundary",
                    ));
                }
            }
            WorkflowEventKind::ClientToolRequest(request) => {
                self.set_suspension(SuspensionEvent {
                    kind: Default::default(),
                    handle: request.suspension.clone(),
                    reason: format!("client tool '{}' requires execution", request.name),
                    payload: Some(request.arguments.clone()),
                })?;
            }
            WorkflowEventKind::PermissionRequest(request) => {
                if let Some(handle) = request.suspension.clone() {
                    self.set_suspension(SuspensionEvent {
                        kind: Default::default(),
                        handle,
                        reason: request.action.clone(),
                        payload: request.details.clone(),
                    })?;
                }
            }
            WorkflowEventKind::WorkflowState(state) => {
                self.last_state = state.state.clone();
                return match state.state {
                    WorkflowState::Completed => self.completed().map(Some),
                    WorkflowState::Failed => self.failed().map(Some),
                    WorkflowState::Suspended => {
                        let result = self.suspended()?;
                        match &result {
                            WorkflowRunResult::Suspended { suspension, .. }
                                if suspension.kind.is_automatic()
                                    && self.mode == WorkflowWaitMode::UntilAction =>
                            {
                                Ok(None)
                            }
                            _ => Ok(Some(result)),
                        }
                    }
                    WorkflowState::RequiresAction => self.suspended().map(Some),
                    WorkflowState::Terminated => self.terminated().map(Some),
                    WorkflowState::Pending | WorkflowState::Running => Ok(None),
                };
            }
            WorkflowEventKind::ExecutionState(_)
            | WorkflowEventKind::Message(_)
            | WorkflowEventKind::ToolCall(_)
            | WorkflowEventKind::Plan(_)
            | WorkflowEventKind::Usage(_)
            | WorkflowEventKind::Compaction(_)
            | WorkflowEventKind::Extension { .. } => {}
        }
        Ok(None)
    }

    pub fn finish(self) -> RuntimeResult<WorkflowRunResult> {
        Err(RuntimeError::new(
            RuntimeErrorCode::Connectivity,
            "workflow event stream ended before a result boundary",
        )
        .with_retryable(true))
    }

    fn set_suspension(&mut self, suspension: SuspensionEvent) -> RuntimeResult<()> {
        if self.suspension.replace(suspension).is_some() {
            return Err(invariant(
                "root Workflow emitted more than one suspension before a boundary",
            ));
        }
        Ok(())
    }

    fn completed(&mut self) -> RuntimeResult<WorkflowRunResult> {
        if self.failure.is_some() || self.suspension.is_some() {
            return Err(invariant(
                "completed Workflow contains failure or suspension detail",
            ));
        }
        let output = self
            .output
            .take()
            .ok_or_else(|| invariant("completed Workflow has no preceding output"))?;
        Ok(WorkflowRunResult::Completed {
            run: self.run.clone(),
            output,
        })
    }

    fn failed(&mut self) -> RuntimeResult<WorkflowRunResult> {
        if self.output.is_some() || self.suspension.is_some() {
            return Err(invariant(
                "failed Workflow contains output or suspension detail",
            ));
        }
        let failure = self
            .failure
            .take()
            .ok_or_else(|| invariant("failed Workflow has no preceding failure detail"))?;
        Ok(WorkflowRunResult::Failed {
            run: self.run.clone(),
            failure,
        })
    }

    fn suspended(&mut self) -> RuntimeResult<WorkflowRunResult> {
        if self.output.is_some() || self.failure.is_some() {
            return Err(invariant(
                "suspended Workflow contains output or failure detail",
            ));
        }
        let suspension = self
            .suspension
            .take()
            .ok_or_else(|| invariant("suspended Workflow has no preceding suspension detail"))?;
        Ok(WorkflowRunResult::Suspended {
            run: self.run.clone(),
            suspension,
        })
    }

    fn terminated(&mut self) -> RuntimeResult<WorkflowRunResult> {
        if self.output.is_some() || self.failure.is_some() {
            return Err(invariant(
                "terminated Workflow contains output or failure detail",
            ));
        }
        self.suspension = None;
        Ok(WorkflowRunResult::Terminated {
            run: self.run.clone(),
        })
    }
}

pub async fn project_workflow_result(
    run: WorkflowRunHandle,
    events: WorkflowEventStream,
) -> RuntimeResult<WorkflowRunResult> {
    wait_for_workflow_result(run, WorkflowWaitOptions::default(), async { Ok(events) }).await
}

/// Observe an existing run. Cancelling or timing out this future only drops the
/// subscription; it never issues a Workflow mutation. The budget includes attach.
pub async fn wait_for_workflow_result(
    run: WorkflowRunHandle,
    options: WorkflowWaitOptions,
    subscribe: impl std::future::Future<Output = RuntimeResult<WorkflowEventStream>>,
) -> RuntimeResult<WorkflowRunResult> {
    options.validate()?;
    WorkflowResultProjector::with_mode(run, options.mode)
        .wait(
            std::time::Duration::from_millis(options.timeout_ms),
            subscribe,
            |_| {},
            |error| error,
        )
        .await
}

fn invariant(message: &str) -> RuntimeError {
    RuntimeError::new(
        RuntimeErrorCode::Internal,
        format!("workflow result projection invariant violated: {message}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EventCursor, EventId, FailureEvent, MessageEvent, MessagePhase, RootWorkflowInstanceId,
        SessionId, SuspensionHandle, WorkflowId, WorkflowInstanceId, WorkflowStateEvent,
    };
    use serde_json::json;

    fn run() -> WorkflowRunHandle {
        WorkflowRunHandle {
            workflow_id: WorkflowId(42),
            workflow_instance_id: WorkflowInstanceId(9001),
            root_workflow_instance_id: RootWorkflowInstanceId(9001),
            session_id: Some(SessionId(100)),
            input_message_id: None,
            subscription_anchor: EventCursor::from("run-9001:0"),
        }
    }

    fn event(instance_id: u64, sequence: u64, kind: WorkflowEventKind) -> WorkflowEvent {
        WorkflowEvent {
            event_id: EventId::from(format!("event-{sequence}")),
            cursor: EventCursor::from(format!("run-9001:{sequence}")),
            workflow_id: WorkflowId(42),
            workflow_instance_id: WorkflowInstanceId(instance_id),
            root_workflow_instance_id: RootWorkflowInstanceId(9001),
            execution_id: None,
            execution_path: None,
            session_id: Some(SessionId(100)),
            message_id: None,
            tool_call_id: None,
            plan_id: None,
            timestamp: "2026-09-06T10:20:30+08:00".to_string(),
            kind,
        }
    }

    fn state(value: WorkflowState) -> WorkflowEventKind {
        WorkflowEventKind::WorkflowState(WorkflowStateEvent {
            state: value,
            reason: None,
        })
    }

    #[test]
    fn projects_completed_null_and_structured_outputs_exactly() {
        for output in [Value::Null, json!({"answer": 42})] {
            let mut projector = WorkflowResultProjector::new(run());
            assert!(projector
                .observe(&event(9002, 1, WorkflowEventKind::Output(json!("child"))))
                .unwrap()
                .is_none());
            assert!(projector
                .observe(&event(
                    9001,
                    2,
                    WorkflowEventKind::Message(MessageEvent {
                        finalization: Default::default(),
                        phase: MessagePhase::Completed,
                        role: "assistant".to_string(),
                        content: Some("presentation only".to_string()),
                        reasoning_content: None,
                    }),
                ))
                .unwrap()
                .is_none());
            projector
                .observe(&event(9001, 3, WorkflowEventKind::Output(output.clone())))
                .unwrap();
            let result = projector
                .observe(&event(9001, 4, state(WorkflowState::Completed)))
                .unwrap()
                .unwrap();
            assert_eq!(result, WorkflowRunResult::Completed { run: run(), output });
        }
    }

    #[test]
    fn projects_failed_suspended_and_terminated_boundaries() {
        let failure = FailureEvent {
            code: "tool_failed".to_string(),
            message: "tool failed".to_string(),
            retryable: false,
            details: Some(json!({"tool": "Search"})),
        };
        let mut failed = WorkflowResultProjector::new(run());
        failed
            .observe(&event(9001, 1, WorkflowEventKind::Failure(failure.clone())))
            .unwrap();
        assert_eq!(
            failed
                .observe(&event(9001, 2, state(WorkflowState::Failed)))
                .unwrap(),
            Some(WorkflowRunResult::Failed {
                run: run(),
                failure,
            })
        );

        let suspension = SuspensionEvent {
            kind: Default::default(),
            handle: SuspensionHandle::issue("projection-test").unwrap(),
            reason: "input required".to_string(),
            payload: Some(json!({"field": "approval"})),
        };
        let mut suspended = WorkflowResultProjector::new(run());
        suspended
            .observe(&event(
                9001,
                1,
                WorkflowEventKind::Suspension(suspension.clone()),
            ))
            .unwrap();
        assert_eq!(
            suspended
                .observe(&event(9001, 2, state(WorkflowState::Suspended)))
                .unwrap(),
            Some(WorkflowRunResult::Suspended {
                run: run(),
                suspension,
            })
        );

        let mut terminated = WorkflowResultProjector::new(run());
        assert_eq!(
            terminated
                .observe(&event(9001, 1, state(WorkflowState::Terminated)))
                .unwrap(),
            Some(WorkflowRunResult::Terminated { run: run() })
        );
    }

    #[test]
    fn user_termination_discards_outstanding_suspension_without_successful_output() {
        let mut projector = WorkflowResultProjector::new(run());
        projector
            .observe(&event(
                9001,
                1,
                WorkflowEventKind::Suspension(SuspensionEvent {
                    kind: Default::default(),
                    handle: SuspensionHandle::issue("stop-test").unwrap(),
                    reason: "waiting for tool".into(),
                    payload: None,
                }),
            ))
            .unwrap();
        assert_eq!(
            projector
                .observe(&event(9001, 2, state(WorkflowState::Terminated)))
                .unwrap(),
            Some(WorkflowRunResult::Terminated { run: run() })
        );
    }

    #[test]
    fn rejects_missing_or_duplicate_boundary_payloads() {
        let malformed = [
            vec![state(WorkflowState::Completed)],
            vec![state(WorkflowState::Failed)],
            vec![state(WorkflowState::Suspended)],
            vec![
                WorkflowEventKind::Output(Value::Null),
                WorkflowEventKind::Output(json!("duplicate")),
            ],
        ];
        for events in malformed {
            let mut projector = WorkflowResultProjector::new(run());
            let mut error = None;
            for (index, kind) in events.into_iter().enumerate() {
                match projector.observe(&event(9001, index as u64 + 1, kind)) {
                    Ok(_) => {}
                    Err(value) => {
                        error = Some(value);
                        break;
                    }
                }
            }
            assert_eq!(error.unwrap().code, RuntimeErrorCode::Internal);
        }
    }

    #[test]
    fn premature_eof_is_retryable_connectivity_error() {
        let error = WorkflowResultProjector::new(run()).finish().unwrap_err();
        assert_eq!(error.code, RuntimeErrorCode::Connectivity);
        assert!(error.retryable);
    }

    #[tokio::test]
    async fn runtime_stream_errors_are_returned_without_reinterpretation() {
        let stream_error =
            RuntimeError::new(RuntimeErrorCode::Connectivity, "stream lost").with_retryable(true);
        let events: WorkflowEventStream =
            Box::pin(futures::stream::iter(vec![Err(stream_error.clone())]));

        assert_eq!(
            project_workflow_result(run(), events).await.unwrap_err(),
            stream_error
        );
    }
    fn automatic(kind: crate::SuspensionKind) -> WorkflowEventKind {
        WorkflowEventKind::Suspension(SuspensionEvent {
            handle: crate::SuspensionHandle::issue("automatic").unwrap(),
            kind,
            reason: "capacity".into(),
            payload: None,
        })
    }

    #[tokio::test]
    async fn automatic_waits_continue_to_result_and_boundary_mode_remains_explicit() {
        for kind in [
            crate::SuspensionKind::Admission,
            crate::SuspensionKind::Retry,
            crate::SuspensionKind::Service,
        ] {
            let sequence = vec![
                automatic(kind),
                state(WorkflowState::Suspended),
                state(WorkflowState::Running),
                automatic(kind),
                state(WorkflowState::Suspended),
                state(WorkflowState::Running),
                WorkflowEventKind::Output(json!({"answer":42})),
                state(WorkflowState::Completed),
            ];
            let events: WorkflowEventStream = Box::pin(futures::stream::iter(
                sequence
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(i, k)| Ok(event(9001, i as u64 + 1, k)))
                    .collect::<Vec<_>>(),
            ));
            assert!(
                matches!(project_workflow_result(run(),events).await.unwrap(), WorkflowRunResult::Completed { output, .. } if output == json!({"answer":42}))
            );
            let mut boundary =
                WorkflowResultProjector::with_mode(run(), WorkflowWaitMode::Boundary);
            boundary.observe(&event(9001, 1, automatic(kind))).unwrap();
            assert!(matches!(
                boundary
                    .observe(&event(9001, 2, state(WorkflowState::Suspended)))
                    .unwrap(),
                Some(WorkflowRunResult::Suspended { .. })
            ));
        }
    }

    #[tokio::test]
    async fn timeout_cursor_replays_partial_boundary_and_does_not_cancel_producer() {
        let first = event(9001, 1, state(WorkflowState::Running));
        let detail = event(9001, 2, WorkflowEventKind::Output(json!(42)));
        let events: WorkflowEventStream = Box::pin(
            futures::stream::iter(vec![Ok(first.clone()), Ok(detail.clone())])
                .chain(futures::stream::pending()),
        );
        let result = wait_for_workflow_result(
            run(),
            WorkflowWaitOptions {
                timeout_ms: 5,
                ..Default::default()
            },
            async { Ok(events) },
        )
        .await
        .unwrap();
        let WorkflowRunResult::Pending {
            cursor, last_state, ..
        } = result
        else {
            panic!("expected observation timeout")
        };
        assert_eq!(cursor, first.cursor);
        assert_eq!(last_state, WorkflowState::Running);
        let mut resumed = WorkflowResultProjector::new(run()).with_cursor(cursor);
        resumed.observe(&detail).unwrap();
        assert!(
            matches!(resumed.observe(&event(9001,3,state(WorkflowState::Completed))).unwrap(),Some(WorkflowRunResult::Completed { output, .. }) if output == json!(42))
        );
    }

    #[tokio::test]
    async fn continuously_ready_events_cannot_starve_the_deadline() {
        let events: WorkflowEventStream = Box::pin(futures::stream::repeat_with(|| {
            Ok(event(9001, 1, state(WorkflowState::Running)))
        }));
        let result = wait_for_workflow_result(
            run(),
            WorkflowWaitOptions {
                timeout_ms: 5,
                ..Default::default()
            },
            async { Ok(events) },
        )
        .await
        .unwrap();
        assert!(matches!(
            result,
            WorkflowRunResult::Pending {
                last_state: WorkflowState::Running,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn observation_budget_includes_subscription_and_validates_bounds() {
        let pending = wait_for_workflow_result(
            run(),
            WorkflowWaitOptions {
                timeout_ms: 5,
                ..Default::default()
            },
            futures::future::pending(),
        )
        .await
        .unwrap();
        assert!(
            matches!(pending,WorkflowRunResult::Pending {cursor,..} if cursor == run().subscription_anchor)
        );
        for timeout_ms in [0, 300001, u64::MAX] {
            assert!(wait_for_workflow_result(
                run(),
                WorkflowWaitOptions {
                    timeout_ms,
                    ..Default::default()
                },
                futures::future::pending()
            )
            .await
            .is_err());
        }
    }

    #[test]
    fn legacy_suspension_is_external_and_requires_action_never_becomes_automatic() {
        let legacy: SuspensionEvent =
            serde_json::from_value(json!({"handle":"v1.legacy","reason":"input"})).unwrap();
        assert_eq!(legacy.kind, crate::SuspensionKind::External);
        let mut projector = WorkflowResultProjector::new(run());
        projector
            .observe(&event(9001, 1, automatic(crate::SuspensionKind::Admission)))
            .unwrap();
        assert!(matches!(
            projector
                .observe(&event(9001, 2, state(WorkflowState::RequiresAction)))
                .unwrap(),
            Some(WorkflowRunResult::Suspended { .. })
        ));
    }
}
