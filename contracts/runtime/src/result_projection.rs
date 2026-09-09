use futures::StreamExt;
use serde_json::Value;

use crate::{
    RuntimeError, RuntimeErrorCode, RuntimeResult, SuspensionEvent, WorkflowEvent,
    WorkflowEventKind, WorkflowEventStream, WorkflowRunHandle, WorkflowRunResult, WorkflowState,
};

/// Projects one root Workflow boundary from the canonical ordered event stream.
pub struct WorkflowResultProjector {
    run: WorkflowRunHandle,
    output: Option<Value>,
    failure: Option<crate::FailureEvent>,
    suspension: Option<SuspensionEvent>,
}

impl WorkflowResultProjector {
    pub fn new(run: WorkflowRunHandle) -> Self {
        Self {
            run,
            output: None,
            failure: None,
            suspension: None,
        }
    }

    pub fn observe(&mut self, event: &WorkflowEvent) -> RuntimeResult<Option<WorkflowRunResult>> {
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
                    handle: request.suspension.clone(),
                    reason: format!("client tool '{}' requires execution", request.name),
                    payload: Some(request.arguments.clone()),
                })?;
            }
            WorkflowEventKind::PermissionRequest(request) => {
                if let Some(handle) = request.suspension.clone() {
                    self.set_suspension(SuspensionEvent {
                        handle,
                        reason: request.action.clone(),
                        payload: request.details.clone(),
                    })?;
                }
            }
            WorkflowEventKind::WorkflowState(state) => {
                return match state.state {
                    WorkflowState::Completed => self.completed().map(Some),
                    WorkflowState::Failed => self.failed().map(Some),
                    WorkflowState::Suspended | WorkflowState::RequiresAction => {
                        self.suspended().map(Some)
                    }
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

    fn terminated(&self) -> RuntimeResult<WorkflowRunResult> {
        if self.output.is_some() || self.failure.is_some() || self.suspension.is_some() {
            return Err(invariant(
                "terminated Workflow contains output, failure, or suspension detail",
            ));
        }
        Ok(WorkflowRunResult::Terminated {
            run: self.run.clone(),
        })
    }
}

pub async fn project_workflow_result(
    run: WorkflowRunHandle,
    mut events: WorkflowEventStream,
) -> RuntimeResult<WorkflowRunResult> {
    let mut projector = WorkflowResultProjector::new(run);
    while let Some(event) = events.next().await {
        let event = event?;
        if let Some(result) = projector.observe(&event)? {
            return Ok(result);
        }
    }
    projector.finish()
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
}
