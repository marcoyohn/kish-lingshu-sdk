use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use futures::StreamExt;
use kish_lingshu_event_dispatch_contract::{
    DeliveryTime, DynamicEvent, EventId, EventRoute, EventVisibility, PublishEvent,
};
use kish_lingshu_runtime_contract::{
    ApplicationProblem, ApplicationResult, ApplicationTaskQuery, ApplicationTaskSummary, ClaimTask,
    CommandDisposition, CurrentUserTaskQuery, EventCursor, EventId as WorkflowEventId,
    IdempotencyKey, InvocationSource, MarkTaskRead, MutationDisposition, MutationReceipt, Page,
    PrincipalKind, ProblemCode, ProductRuntimeFacade, RequestContext, RetryUserTaskCompletion,
    RootWorkflowInstanceId, RuntimeBindings, RuntimeError, RuntimeErrorCode, RuntimeResult,
    SaveTaskDraft, SessionId, SignalWorkflowRequest, StartWorkflowRequest, SubmitTask,
    SubscribeWorkflowRequest, TaskActionPrecondition, TaskActionReceipt, TerminateWorkflowRequest,
    TrustedContextFactory, UserTask, UserTaskAction, UserTaskCompletionDetail,
    UserTaskCompletionExecution, UserTaskCompletionProblem, UserTaskCompletionRetryReceipt,
    UserTaskId, UserTaskParticipantState, UserTaskPermissions, UserTaskRevision, UserTaskRuntime,
    UserTaskState, UserTaskSummary, WorkflowContext, WorkflowEvent, WorkflowEventKind, WorkflowId,
    WorkflowInput, WorkflowInstanceId, WorkflowSignal, WorkflowSnapshotRequest, CONFLICT_PROBLEM,
    FORBIDDEN_PROBLEM, NOT_FOUND_PROBLEM, STALE_TASK_REVISION_PROBLEM,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{BindingRef, HttpBinding, ProductBinding, ProductRuntimeBinding};
use crate::{
    auth::ClientCredential, ClientConfig, MutationOptions, RequestOptions, ServiceCredential,
    UserCredential,
};

const TODO_TASK_ID: u64 = 7_001;
const READ_TASK_ID: u64 = 7_002;
const COMPLETION_REJECT_APPLY_TASK_ID: u64 = 7_101;
const COMPLETION_FAILED_TASK_ID: u64 = 7_102;

#[derive(Debug, Eq, PartialEq)]
struct BindingContractReport {
    event_id: u64,
    workflow_id: u64,
    workflow_instance_id: u64,
    signal_cursor: String,
    task_revision: u64,
    completed_tasks: u64,
}

#[tokio::test]
async fn http_and_product_runtime_bindings_pass_the_same_capability_contract() {
    let in_process_fixture =
        kish_lingshu_runtime_contract::test_support::ContractProductRuntimeFixture::default();
    let in_process = binding_pair(in_process_fixture.facade);
    let expected = assert_binding_contract(in_process.0, in_process.1).await;

    let http_fixture =
        kish_lingshu_runtime_contract::test_support::ContractProductRuntimeFixture::default();
    let server = MockProductRuntimeServer::start(http_fixture.facade).await;
    let remote = http_binding_pair(server.endpoint());
    let actual = assert_binding_contract(remote.0, remote.1).await;

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn http_and_product_runtime_bindings_observe_the_same_completion_lifecycle() {
    let expected = assert_completion_binding_contract(binding_pair(completion_facade())).await;

    let server = MockProductRuntimeServer::start(completion_facade()).await;
    let actual = assert_completion_binding_contract(http_binding_pair(server.endpoint())).await;

    assert_eq!(actual, expected);
}

#[derive(Debug, Eq, PartialEq)]
struct CompletionBindingContractReport {
    applied_revision: u64,
    failed_revision: u64,
    retried_revision: u64,
    applied_invocation_id: String,
    retried_invocation_id: String,
}

async fn assert_completion_binding_contract(
    (service, user): (BindingRef, BindingRef),
) -> CompletionBindingContractReport {
    let rejected_action = completion_submit(
        COMPLETION_REJECT_APPLY_TASK_ID,
        1,
        "contract/completion/reject",
        "reject",
    );
    let rejected = user
        .act_on_user_task(
            rejected_action.clone(),
            MutationOptions::new("contract/completion/reject").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rejected.state, UserTaskState::Completing);
    assert_eq!(rejected.revision.get(), 2);

    let replayed_rejection = user
        .act_on_user_task(
            rejected_action,
            MutationOptions::new("contract/completion/reject").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replayed_rejection.state, UserTaskState::Completing);
    assert_eq!(replayed_rejection.revision.get(), 2);
    assert_eq!(
        replayed_rejection.mutation.disposition,
        MutationDisposition::Duplicate
    );

    let reopened = user
        .get_current_user_task(
            UserTaskId::new(COMPLETION_REJECT_APPLY_TASK_ID).unwrap(),
            RequestOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(reopened.summary.state, UserTaskState::PendingClaimed);
    assert_eq!(reopened.summary.revision.get(), 3);
    assert_eq!(reopened.payloads.submission, None);
    assert_eq!(reopened.payloads.draft, Some(json!({"decision": "reject"})));
    assert!(reopened.permissions.can_submit);
    assert_eq!(
        reopened
            .completion
            .as_ref()
            .unwrap()
            .problem
            .as_ref()
            .unwrap()
            .code,
        "decision_rejected"
    );

    let applied_action = completion_submit(
        COMPLETION_REJECT_APPLY_TASK_ID,
        3,
        "contract/completion/apply",
        "approve",
    );
    let applied = user
        .act_on_user_task(
            applied_action.clone(),
            MutationOptions::new("contract/completion/apply").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(applied.state, UserTaskState::Completing);
    assert_eq!(applied.revision.get(), 4);
    let replayed_apply = user
        .act_on_user_task(
            applied_action,
            MutationOptions::new("contract/completion/apply").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replayed_apply.revision, applied.revision);
    assert_eq!(
        replayed_apply.mutation.disposition,
        MutationDisposition::Duplicate
    );

    let completing = service
        .get_application_task(
            UserTaskId::new(COMPLETION_REJECT_APPLY_TASK_ID).unwrap(),
            RequestOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(completing.summary.state, UserTaskState::Completing);
    assert_eq!(completing.summary.revision.get(), 4);
    let applied_invocation_id = completing
        .completion
        .as_ref()
        .and_then(|completion| completion.execution.as_ref())
        .unwrap()
        .invocation_id
        .clone();
    let completed = service
        .get_application_task(
            UserTaskId::new(COMPLETION_REJECT_APPLY_TASK_ID).unwrap(),
            RequestOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(completed.summary.state, UserTaskState::Completed);
    assert_eq!(completed.summary.revision.get(), 5);
    assert_eq!(
        completed
            .completion
            .as_ref()
            .and_then(|completion| completion.execution.as_ref())
            .and_then(|execution| execution.outcome.as_deref()),
        Some("applied")
    );

    let failed = user
        .act_on_user_task(
            completion_submit(
                COMPLETION_FAILED_TASK_ID,
                1,
                "contract/completion/fail",
                "fail",
            ),
            MutationOptions::new("contract/completion/fail").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(failed.state, UserTaskState::Completing);
    assert_eq!(failed.revision.get(), 2);
    let failed_detail = service
        .get_application_task(
            UserTaskId::new(COMPLETION_FAILED_TASK_ID).unwrap(),
            RequestOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(failed_detail.summary.state, UserTaskState::CompletionFailed);
    assert_eq!(failed_detail.summary.revision.get(), 3);
    let failed_execution = failed_detail
        .completion
        .as_ref()
        .and_then(|completion| completion.execution.as_ref())
        .unwrap();
    let failed_invocation_id = failed_execution.invocation_id.clone();
    assert_eq!(failed_execution.outcome.as_deref(), Some("failed"));

    let retry = service
        .retry_user_task_completion(
            RetryUserTaskCompletion {
                task_id: UserTaskId::new(COMPLETION_FAILED_TASK_ID).unwrap(),
                expected_revision: UserTaskRevision::new(3).unwrap(),
            },
            RequestOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(retry.state, UserTaskState::Completing);
    assert_eq!(retry.revision.get(), 4);
    assert_eq!(retry.invocation_id, failed_invocation_id);

    let retried = service
        .get_application_task(
            UserTaskId::new(COMPLETION_FAILED_TASK_ID).unwrap(),
            RequestOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(retried.summary.state, UserTaskState::Completing);
    assert_eq!(
        retried
            .completion
            .as_ref()
            .and_then(|completion| completion.execution.as_ref())
            .unwrap()
            .invocation_id,
        failed_invocation_id
    );
    let retry_completed = service
        .get_application_task(
            UserTaskId::new(COMPLETION_FAILED_TASK_ID).unwrap(),
            RequestOptions::new(),
        )
        .await
        .unwrap();
    assert_eq!(retry_completed.summary.state, UserTaskState::Completed);
    assert_eq!(retry_completed.summary.revision.get(), 5);

    CompletionBindingContractReport {
        applied_revision: completed.summary.revision.get(),
        failed_revision: failed_detail.summary.revision.get(),
        retried_revision: retry_completed.summary.revision.get(),
        applied_invocation_id,
        retried_invocation_id: retry.invocation_id,
    }
}

fn completion_submit(
    task_id: u64,
    revision: u64,
    idempotency_key: &str,
    decision: &str,
) -> UserTaskAction {
    UserTaskAction::Submit(SubmitTask {
        precondition: task_precondition(
            UserTaskId::new(task_id).unwrap(),
            revision,
            idempotency_key,
        ),
        submission: json!({"decision": decision}),
    })
}

fn completion_facade() -> ProductRuntimeFacade {
    let workflow =
        Arc::new(kish_lingshu_runtime_contract::test_support::ContractWorkflowRuntime::default());
    ProductRuntimeFacade::new(workflow.clone(), workflow)
        .with_user_tasks(Arc::new(CompletionConformanceRuntime::new()))
}

#[derive(Clone)]
struct CompletionTestCommand {
    action: UserTaskAction,
    receipt: TaskActionReceipt,
}

struct CompletionTestTask {
    task: UserTask,
    complete_after_observation: bool,
}

struct CompletionTestState {
    tasks: HashMap<UserTaskId, CompletionTestTask>,
    commands: HashMap<(UserTaskId, String, String), CompletionTestCommand>,
}

struct CompletionConformanceRuntime {
    state: Mutex<CompletionTestState>,
}

impl CompletionConformanceRuntime {
    fn new() -> Self {
        let template =
            kish_lingshu_runtime_contract::test_support::ContractUserTaskRuntime::default()
                .task(UserTaskId::new(TODO_TASK_ID).unwrap())
                .unwrap();
        let tasks = [
            completion_test_task(template.clone(), COMPLETION_REJECT_APPLY_TASK_ID),
            completion_test_task(template, COMPLETION_FAILED_TASK_ID),
        ]
        .into_iter()
        .map(|task| {
            (
                task.summary.id,
                CompletionTestTask {
                    task,
                    complete_after_observation: false,
                },
            )
        })
        .collect();
        Self {
            state: Mutex::new(CompletionTestState {
                tasks,
                commands: HashMap::new(),
            }),
        }
    }

    fn require_application<'a>(
        context: &'a RequestContext,
        principal: PrincipalKind,
    ) -> ApplicationResult<&'a str> {
        if context.principal() != principal {
            return Err(completion_problem(
                context,
                FORBIDDEN_PROBLEM,
                "principal is not authorized for this completion operation",
            ));
        }
        context.application_id().ok_or_else(|| {
            completion_problem(
                context,
                FORBIDDEN_PROBLEM,
                "completion operation requires an Application scope",
            )
        })
    }

    fn observe_task(&self, task_id: UserTaskId) -> Option<UserTask> {
        let mut state = self.state.lock().unwrap();
        let entry = state.tasks.get_mut(&task_id)?;
        let observed = entry.task.clone();
        if entry.complete_after_observation {
            complete_test_task(&mut entry.task);
            entry.complete_after_observation = false;
        }
        Some(observed)
    }
}

#[async_trait]
impl UserTaskRuntime for CompletionConformanceRuntime {
    async fn list_application(
        &self,
        context: RequestContext,
        query: ApplicationTaskQuery,
    ) -> ApplicationResult<Page<UserTaskSummary>> {
        let application_id = Self::require_application(&context, PrincipalKind::Service)?;
        let state = self.state.lock().unwrap();
        let items = state
            .tasks
            .values()
            .filter(|entry| entry.task.summary.application_id == application_id)
            .map(|entry| application_completion_task(entry.task.clone()).summary)
            .collect::<Vec<_>>();
        Ok(Page {
            total: items.len() as u64,
            items,
            page: query.pagination.page,
            page_size: query.pagination.page_size,
        })
    }

    async fn summarize_application(
        &self,
        context: RequestContext,
        _: ApplicationTaskQuery,
    ) -> ApplicationResult<ApplicationTaskSummary> {
        let application_id = Self::require_application(&context, PrincipalKind::Service)?;
        let state = self.state.lock().unwrap();
        let mut summary = ApplicationTaskSummary::default();
        for entry in state
            .tasks
            .values()
            .filter(|entry| entry.task.summary.application_id == application_id)
        {
            summary.total += 1;
            if entry.task.summary.state == UserTaskState::Completed {
                summary.completed += 1;
            } else {
                summary.pending_todo += 1;
            }
        }
        Ok(summary)
    }

    async fn get_application(
        &self,
        context: RequestContext,
        task_id: UserTaskId,
    ) -> ApplicationResult<UserTask> {
        let application_id = Self::require_application(&context, PrincipalKind::Service)?;
        self.observe_task(task_id)
            .filter(|task| task.summary.application_id == application_id)
            .map(application_completion_task)
            .ok_or_else(|| {
                completion_problem(&context, NOT_FOUND_PROBLEM, "completion task was not found")
            })
    }

    async fn retry_completion(
        &self,
        context: RequestContext,
        request: RetryUserTaskCompletion,
    ) -> ApplicationResult<UserTaskCompletionRetryReceipt> {
        let application_id = Self::require_application(&context, PrincipalKind::Service)?;
        let mut state = self.state.lock().unwrap();
        let entry = state.tasks.get_mut(&request.task_id).ok_or_else(|| {
            completion_problem(&context, NOT_FOUND_PROBLEM, "completion task was not found")
        })?;
        if entry.task.summary.application_id != application_id {
            return Err(completion_problem(
                &context,
                NOT_FOUND_PROBLEM,
                "completion task was not found",
            ));
        }
        if entry.task.summary.state != UserTaskState::CompletionFailed
            || entry.task.summary.revision != request.expected_revision
        {
            return Err(completion_problem(
                &context,
                CONFLICT_PROBLEM,
                "completion task is not retryable",
            ));
        }
        let completion = entry.task.completion.as_mut().unwrap();
        completion.problem = None;
        let execution = completion.execution.as_mut().unwrap();
        execution.state = "effect_pending".to_string();
        execution.outcome = None;
        execution.problem = None;
        execution.completed_at = None;
        execution.attempt_count += 1;
        execution.lease_generation += 1;
        entry.task.summary.state = UserTaskState::Completing;
        entry.task.summary.revision =
            UserTaskRevision::new(entry.task.summary.revision.get() + 1).unwrap();
        entry.complete_after_observation = true;
        Ok(UserTaskCompletionRetryReceipt {
            task_id: request.task_id,
            revision: entry.task.summary.revision,
            state: UserTaskState::Completing,
            command_id: execution.command_id,
            invocation_id: execution.invocation_id.clone(),
        })
    }

    async fn list_mine(
        &self,
        context: RequestContext,
        query: CurrentUserTaskQuery,
    ) -> ApplicationResult<Page<UserTaskSummary>> {
        let application_id = Self::require_application(&context, PrincipalKind::User)?;
        let state = self.state.lock().unwrap();
        let items = state
            .tasks
            .values()
            .filter(|entry| entry.task.summary.application_id == application_id)
            .map(|entry| participant_completion_task(entry.task.clone()).summary)
            .collect::<Vec<_>>();
        Ok(Page {
            total: items.len() as u64,
            items,
            page: query.pagination.page,
            page_size: query.pagination.page_size,
        })
    }

    async fn get_mine(
        &self,
        context: RequestContext,
        task_id: UserTaskId,
    ) -> ApplicationResult<UserTask> {
        let application_id = Self::require_application(&context, PrincipalKind::User)?;
        self.observe_task(task_id)
            .filter(|task| task.summary.application_id == application_id)
            .map(participant_completion_task)
            .ok_or_else(|| {
                completion_problem(&context, NOT_FOUND_PROBLEM, "completion task was not found")
            })
    }

    async fn act(
        &self,
        context: RequestContext,
        action: UserTaskAction,
    ) -> ApplicationResult<TaskActionReceipt> {
        let application_id = Self::require_application(&context, PrincipalKind::User)?;
        let precondition = action.precondition();
        let command_key = (
            precondition.task_id,
            context.actor_id().to_string(),
            precondition.idempotency_key.to_string(),
        );
        let mut state = self.state.lock().unwrap();
        if let Some(command) = state.commands.get(&command_key) {
            if command.action != action {
                return Err(completion_problem(
                    &context,
                    CONFLICT_PROBLEM,
                    "completion idempotency key is bound to another request",
                ));
            }
            let mut receipt = command.receipt.clone();
            receipt.mutation.disposition = MutationDisposition::Duplicate;
            return Ok(receipt);
        }
        let submission = match &action {
            UserTaskAction::Submit(request) => request.submission.clone(),
            _ => {
                return Err(completion_problem(
                    &context,
                    CONFLICT_PROBLEM,
                    "completion fixture only supports submit",
                ))
            }
        };
        let entry = state.tasks.get_mut(&precondition.task_id).ok_or_else(|| {
            completion_problem(&context, NOT_FOUND_PROBLEM, "completion task was not found")
        })?;
        if entry.task.summary.application_id != application_id {
            return Err(completion_problem(
                &context,
                NOT_FOUND_PROBLEM,
                "completion task was not found",
            ));
        }
        if entry.task.summary.revision != precondition.expected_revision {
            return Err(completion_problem(
                &context,
                STALE_TASK_REVISION_PROBLEM,
                "completion task revision is stale",
            ));
        }
        if entry.task.summary.state != UserTaskState::PendingClaimed {
            return Err(completion_problem(
                &context,
                CONFLICT_PROBLEM,
                "completion task is not submit-ready",
            ));
        }

        stage_test_completion(&mut entry.task, submission.clone());
        let receipt = TaskActionReceipt {
            mutation: MutationReceipt::accepted(
                context.request_id().clone(),
                entry.task.summary.timestamps.updated_at,
            ),
            task_id: entry.task.summary.id,
            action: action.kind(),
            revision: entry.task.summary.revision,
            state: UserTaskState::Completing,
            workflow_id: entry.task.summary.workflow.workflow_id,
            workflow_instance_id: entry.task.summary.workflow.workflow_instance_id,
            observation_cursor: Some(EventCursor::from(format!(
                "completion:{}:{}",
                entry.task.summary.id, entry.task.summary.revision
            ))),
        };
        match submission
            .get("decision")
            .and_then(serde_json::Value::as_str)
        {
            Some("reject") => reject_test_completion(&mut entry.task, submission),
            Some("fail") => fail_test_completion(&mut entry.task),
            _ => entry.complete_after_observation = true,
        }
        state.commands.insert(
            command_key,
            CompletionTestCommand {
                action,
                receipt: receipt.clone(),
            },
        );
        Ok(receipt)
    }
}

fn completion_test_task(mut task: UserTask, id: u64) -> UserTask {
    task.summary.id = UserTaskId::new(id).unwrap();
    task.summary.title = format!("Completion contract {id}");
    task.summary.state = UserTaskState::PendingClaimed;
    task.summary.revision = UserTaskRevision::new(1).unwrap();
    task.summary.claimed_by = Some("contract-actor".to_string());
    task.summary.current_user_participant_state = Some(UserTaskParticipantState::PendingClaimed);
    task.summary.workflow.workflow_instance_id = WorkflowInstanceId(id + 1_000);
    task.summary.workflow.root_workflow_instance_id = Some(RootWorkflowInstanceId(id + 1_000));
    task.summary.timestamps.claimed_at = Some(task.summary.timestamps.created_at);
    task.participants[0].state = UserTaskParticipantState::PendingClaimed;
    task.permissions = UserTaskPermissions {
        can_save_draft: true,
        can_submit: true,
        ..Default::default()
    };
    task.completion = Some(UserTaskCompletionDetail {
        binding_key: "default".to_string(),
        problem: None,
        execution: None,
    });
    task
}

fn stage_test_completion(task: &mut UserTask, submission: serde_json::Value) {
    let now = task.summary.timestamps.updated_at + chrono::TimeDelta::seconds(1);
    task.summary.revision = UserTaskRevision::new(task.summary.revision.get() + 1).unwrap();
    task.summary.state = UserTaskState::Completing;
    task.summary.timestamps.updated_at = now;
    task.summary.timestamps.submitted_at = Some(now);
    task.payloads.submission = Some(submission);
    task.permissions = UserTaskPermissions::default();
    let command_id = task.summary.id.get() + 10_000;
    task.completion = Some(UserTaskCompletionDetail {
        binding_key: "default".to_string(),
        problem: None,
        execution: Some(UserTaskCompletionExecution {
            command_id,
            invocation_id: format!("user-task-completion/{command_id}"),
            state: "effect_pending".to_string(),
            outcome: None,
            binding_key: "default".to_string(),
            service_key: "kishee-ykm".to_string(),
            operation_key: "user-task.complete.v1".to_string(),
            attempt_count: 1,
            lease_generation: 1,
            lease_expires_at: None,
            accepted_at: Some(now),
            completed_at: None,
            updated_at: now,
            problem: None,
            attempts: Vec::new(),
        }),
    });
}

fn reject_test_completion(task: &mut UserTask, submission: serde_json::Value) {
    let problem = UserTaskCompletionProblem::new("decision_rejected", "decision can be edited");
    finish_test_completion(task, UserTaskState::PendingClaimed, "rejected", &problem);
    task.payloads.draft = Some(submission);
    task.payloads.submission = None;
    task.summary.timestamps.submitted_at = None;
    task.permissions = UserTaskPermissions {
        can_save_draft: true,
        can_submit: true,
        ..Default::default()
    };
}

fn fail_test_completion(task: &mut UserTask) {
    let problem = UserTaskCompletionProblem::new("handler_failed", "operator retry is required");
    finish_test_completion(task, UserTaskState::CompletionFailed, "failed", &problem);
}

fn finish_test_completion(
    task: &mut UserTask,
    state: UserTaskState,
    outcome: &str,
    problem: &UserTaskCompletionProblem,
) {
    let now = task.summary.timestamps.updated_at + chrono::TimeDelta::seconds(1);
    task.summary.revision = UserTaskRevision::new(task.summary.revision.get() + 1).unwrap();
    task.summary.state = state;
    task.summary.timestamps.updated_at = now;
    let completion = task.completion.as_mut().unwrap();
    completion.problem = Some(problem.clone());
    let execution = completion.execution.as_mut().unwrap();
    execution.state = outcome.to_string();
    execution.outcome = Some(outcome.to_string());
    execution.problem = Some(problem.clone());
    execution.completed_at = Some(now);
    execution.updated_at = now;
}

fn complete_test_task(task: &mut UserTask) {
    let now = task.summary.timestamps.updated_at + chrono::TimeDelta::seconds(1);
    task.summary.revision = UserTaskRevision::new(task.summary.revision.get() + 1).unwrap();
    task.summary.state = UserTaskState::Completed;
    task.summary.timestamps.updated_at = now;
    task.summary.timestamps.completed_at = Some(now);
    task.permissions = UserTaskPermissions::default();
    task.participants[0].state = UserTaskParticipantState::Completed;
    task.participants[0].acted_at = Some(now);
    let completion = task.completion.as_mut().unwrap();
    completion.problem = None;
    let execution = completion.execution.as_mut().unwrap();
    execution.state = "accepted".to_string();
    execution.outcome = Some("applied".to_string());
    execution.problem = None;
    execution.completed_at = Some(now);
    execution.updated_at = now;
}

fn application_completion_task(mut task: UserTask) -> UserTask {
    task.summary.current_user_participant_state = None;
    task.permissions = UserTaskPermissions::default();
    task
}

fn participant_completion_task(mut task: UserTask) -> UserTask {
    task.summary.current_user_participant_state = Some(task.participants[0].state.clone());
    if task.summary.state != UserTaskState::PendingClaimed {
        task.permissions = UserTaskPermissions::default();
    }
    task
}

fn completion_problem(
    context: &RequestContext,
    code: &'static str,
    message: impl Into<String>,
) -> ApplicationProblem {
    ApplicationProblem::new(
        ProblemCode::known(code),
        message,
        context.request_id().clone(),
    )
}

#[tokio::test]
async fn public_http_workflow_start_replays_and_rejects_changed_input() {
    let fixture =
        kish_lingshu_runtime_contract::test_support::ContractProductRuntimeFixture::default();
    let server = MockProductRuntimeServer::start(fixture.facade).await;
    let client = crate::ClientBuilder::new(
        ClientConfig::new(server.endpoint()).with_selected_application("contract-app"),
    )
    .user_credential(UserCredential::new("user-secret").unwrap())
    .connect()
    .unwrap();
    let workflow = client.workflows().select(WorkflowId(42)).unwrap();

    let first = workflow
        .start(
            crate::workflow::WorkflowStart::structured(json!({"prompt": "same"}))
                .in_session(SessionId(7)),
            MutationOptions::new("public-http/workflow/42/start").unwrap(),
        )
        .await
        .unwrap();
    let replay = workflow
        .start(
            crate::workflow::WorkflowStart::structured(json!({"prompt": "same"}))
                .in_session(SessionId(7)),
            MutationOptions::new("public-http/workflow/42/start").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replay.handle(), first.handle());

    let error = match workflow
        .start(
            crate::workflow::WorkflowStart::structured(json!({"prompt": "changed"}))
                .in_session(SessionId(7)),
            MutationOptions::new("public-http/workflow/42/start").unwrap(),
        )
        .await
    {
        Ok(_) => panic!("changed HTTP start unexpectedly replayed the original Workflow"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        crate::Error::Application(failure) if failure.problem.code.as_str() == "conflict"
    ));
}

#[tokio::test]
async fn public_http_event_publication_replays_and_rejects_changed_payload() {
    let fixture =
        kish_lingshu_runtime_contract::test_support::ContractProductRuntimeFixture::default();
    let server = MockProductRuntimeServer::start(fixture.facade).await;
    let client = crate::ClientBuilder::new(ClientConfig::new(server.endpoint()))
        .service_credential(ServiceCredential::new("contract-app", "service-secret").unwrap())
        .connect()
        .unwrap();
    let events = client.event_dispatch();
    let route = EventRoute::new("contract.orders", "contract.order.created", "1").unwrap();
    let mut event = PublishEvent::dynamic(
        "contract-checkout",
        DynamicEvent::new(route, json!({"order_id": 42})).unwrap(),
    );
    event.delivery = DeliveryTime::after(std::time::Duration::from_secs(5));

    let accepted = events
        .publish(
            event.clone(),
            MutationOptions::new("public-http/event/order-42").unwrap(),
        )
        .await
        .unwrap();
    let replay = events
        .publish(
            event.clone(),
            MutationOptions::new("public-http/event/order-42").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replay.event_id, accepted.event_id);
    assert_eq!(replay.mutation.disposition, MutationDisposition::Duplicate);

    let observed = events
        .get(accepted.event_id, RequestOptions::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.payload, json!({"order_id": 42}));

    event.payload = json!({"order_id": 43});
    let error = events
        .publish(
            event,
            MutationOptions::new("public-http/event/order-42").unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        crate::Error::Application(failure) if failure.problem.code.as_str() == "conflict"
    ));
}

#[tokio::test]
async fn http_sse_reattaches_after_the_last_accepted_cursor_with_stable_logical_ids() {
    let state = Arc::new(ReconnectServerState::default());
    let app = Router::new()
        .route(
            "/api/user/workflow-instances/{instance_id}/notifications/sse",
            get(reconnecting_sse),
        )
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let config = ClientConfig::new(format!("http://{address}/"))
        .with_selected_application("contract-app")
        .with_retry_limit(2);
    let binding = HttpBinding::new(
        &config,
        ClientCredential::User(UserCredential::new("user-secret").unwrap()),
        PrincipalKind::User,
    )
    .unwrap();
    let events = binding
        .subscribe_workflow(
            WorkflowInstanceId(9_001),
            Some(EventCursor::from("reconnect:0")),
            RequestOptions::new(),
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    task.abort();

    assert_eq!(
        events
            .iter()
            .map(|event| event.cursor.as_ref())
            .collect::<Vec<_>>(),
        ["reconnect:1", "reconnect:2"]
    );
    let requests = state.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].last_event_id, "reconnect:0");
    assert_eq!(requests[1].last_event_id, "reconnect:1");
    assert_eq!(requests[0].request_id, requests[1].request_id);
    assert_eq!(requests[0].correlation_id, requests[1].correlation_id);
    assert_eq!(requests[0].attempt, 1);
    assert_eq!(requests[1].attempt, 2);
}

#[derive(Default)]
struct ReconnectServerState {
    connections: AtomicUsize,
    requests: Mutex<Vec<ReconnectRequest>>,
}

struct ReconnectRequest {
    last_event_id: String,
    request_id: String,
    correlation_id: String,
    attempt: u32,
}

async fn reconnecting_sse(
    State(state): State<Arc<ReconnectServerState>>,
    headers: HeaderMap,
    Path(instance_id): Path<u64>,
) -> Response {
    assert_eq!(instance_id, 9_001);
    request_context(&headers, PrincipalKind::User);
    let connection = state.connections.fetch_add(1, Ordering::SeqCst) + 1;
    let last_event_id = headers
        .get("last-event-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    state.requests.lock().unwrap().push(ReconnectRequest {
        last_event_id,
        request_id: headers
            .get("x-request-id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string(),
        correlation_id: headers
            .get("x-correlation-id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string(),
        attempt: headers
            .get("x-kish-transport-attempt")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap(),
    });
    let event = WorkflowEvent {
        event_id: WorkflowEventId::from(format!("reconnect-event-{connection}")),
        cursor: EventCursor::from(format!("reconnect:{connection}")),
        workflow_id: WorkflowId(42),
        workflow_instance_id: WorkflowInstanceId(instance_id),
        root_workflow_instance_id: RootWorkflowInstanceId(instance_id),
        execution_id: None,
        execution_path: None,
        session_id: None,
        message_id: None,
        tool_call_id: None,
        plan_id: None,
        timestamp: "2026-09-06T08:00:00Z".to_string(),
        kind: WorkflowEventKind::Extension {
            kind: "reconnect_test".to_string(),
            payload: json!({"connection": connection}),
        },
    };
    let mut body = format!("data: {}\n\n", serde_json::to_string(&event).unwrap());
    if connection == 2 {
        body.push_str("data: [DONE]\n\n");
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(body))
        .unwrap()
}

fn binding_pair(facade: ProductRuntimeFacade) -> (BindingRef, BindingRef) {
    let service = Arc::new(ProductRuntimeBinding::new(
        facade.clone(),
        context_factory(PrincipalKind::Service),
    ));
    let user = Arc::new(ProductRuntimeBinding::new(
        facade,
        context_factory(PrincipalKind::User),
    ));
    (service, user)
}

fn http_binding_pair(endpoint: String) -> (BindingRef, BindingRef) {
    let service_config = ClientConfig::new(&endpoint).with_retry_limit(1);
    let service = HttpBinding::new(
        &service_config,
        ClientCredential::Service(
            ServiceCredential::new("contract-app", "service-secret").unwrap(),
        ),
        PrincipalKind::Service,
    )
    .unwrap();
    let user_config = ClientConfig::new(endpoint)
        .with_selected_application("contract-app")
        .with_retry_limit(1);
    let user = HttpBinding::new(
        &user_config,
        ClientCredential::User(UserCredential::new("user-secret").unwrap()),
        PrincipalKind::User,
    )
    .unwrap();
    (Arc::new(service), Arc::new(user))
}

fn context_factory(principal: PrincipalKind) -> TrustedContextFactory {
    TrustedContextFactory::new(
        match principal {
            PrincipalKind::Service => "contract-service",
            PrincipalKind::User => "contract-actor",
            PrincipalKind::Internal => unreachable!(),
        },
        principal,
        Some("contract-app".to_string()),
        InvocationSource::EmbeddedSdk,
    )
    .unwrap()
}

async fn assert_binding_contract(service: BindingRef, user: BindingRef) -> BindingContractReport {
    let mut event = PublishEvent::dynamic(
        "contract-checkout",
        DynamicEvent::new(
            EventRoute::new("contract.orders", "contract.order.created", "1").unwrap(),
            json!({"order_id": 42}),
        )
        .unwrap(),
    );
    event.delivery = DeliveryTime::after(std::time::Duration::from_secs(5));
    let accepted = service
        .publish_event(
            event.clone(),
            MutationOptions::new("contract/event/order-42").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.visibility, EventVisibility::Delayed);
    let observed = service
        .get_event(accepted.event_id, RequestOptions::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.payload, json!({"order_id": 42}));
    let duplicate = service
        .publish_event(
            event.clone(),
            MutationOptions::new("contract/event/order-42").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        duplicate.mutation.disposition,
        MutationDisposition::Duplicate
    );
    event.payload = json!({"order_id": 43});
    let conflict = service
        .publish_event(
            event,
            MutationOptions::new("contract/event/order-42").unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        crate::Error::Application(failure) if failure.problem.code.as_str() == "conflict"
    ));

    let run = user
        .start_workflow(
            WorkflowId(42),
            Some(SessionId(7)),
            WorkflowInput::structured(json!({"prompt": "contract"})),
            RuntimeBindings::default(),
            MutationOptions::new("contract/workflow/start").unwrap(),
        )
        .await
        .unwrap();
    let replayed_run = user
        .start_workflow(
            WorkflowId(42),
            Some(SessionId(7)),
            WorkflowInput::structured(json!({"prompt": "contract"})),
            RuntimeBindings::default(),
            MutationOptions::new("contract/workflow/start").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replayed_run, run);
    let start_conflict = user
        .start_workflow(
            WorkflowId(42),
            Some(SessionId(7)),
            WorkflowInput::structured(json!({"prompt": "changed"})),
            RuntimeBindings::default(),
            MutationOptions::new("contract/workflow/start").unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        start_conflict,
        crate::Error::Application(failure) if failure.problem.code.as_str() == "conflict"
    ));
    let snapshot = user
        .workflow_snapshot(run.workflow_instance_id, RequestOptions::new())
        .await
        .unwrap();
    assert_eq!(snapshot.workflow_id, WorkflowId(42));
    let initial_events = user
        .subscribe_workflow(
            run.workflow_instance_id,
            Some(run.subscription_anchor.clone()),
            RequestOptions::new(),
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert!(initial_events.into_iter().all(|event| event.is_ok()));
    let signal = user
        .signal_workflow(
            run.workflow_instance_id,
            WorkflowSignal::Business {
                name: "contract_progress".to_string(),
                payload: json!({"step": 1}),
            },
            MutationOptions::new("contract/workflow/signal").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(signal.disposition, CommandDisposition::Accepted);
    let signal_cursor = signal.cursor.unwrap();
    let events = user
        .subscribe_workflow(
            run.workflow_instance_id,
            Some(run.subscription_anchor.clone()),
            RequestOptions::new(),
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(events.last().unwrap().cursor, signal_cursor);
    let terminated = user
        .terminate_workflow(
            run.workflow_instance_id,
            Some("contract complete".to_string()),
            None,
            MutationOptions::new("contract/workflow/terminate").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(terminated.disposition, CommandDisposition::Terminated);

    let initial_tasks = service
        .list_application_tasks(ApplicationTaskQuery::default(), RequestOptions::new())
        .await
        .unwrap();
    assert_eq!(initial_tasks.total, 2);
    let todo_id = UserTaskId::new(TODO_TASK_ID).unwrap();
    let application_task = service
        .get_application_task(todo_id, RequestOptions::new())
        .await
        .unwrap();
    assert_eq!(application_task.summary.revision.get(), 1);
    let retry_error = service
        .retry_user_task_completion(
            RetryUserTaskCompletion {
                task_id: todo_id,
                expected_revision: UserTaskRevision::new(4).unwrap(),
            },
            RequestOptions::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        retry_error,
        crate::Error::Application(failure)
            if failure.problem.code.as_str() == "conflict"
                && failure.problem.message.contains("no failed completion command")
    ));
    let current_tasks = user
        .list_current_user_tasks(CurrentUserTaskQuery::default(), RequestOptions::new())
        .await
        .unwrap();
    assert_eq!(current_tasks.total, 2);
    let mine = user
        .get_current_user_task(todo_id, RequestOptions::new())
        .await
        .unwrap();
    assert!(mine.permissions.can_claim);
    let claimed = user
        .act_on_user_task(
            UserTaskAction::Claim(ClaimTask {
                precondition: task_precondition(todo_id, 1, "contract/task/7001/claim"),
            }),
            MutationOptions::new("contract/task/7001/claim").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(claimed.revision.get(), 2);
    let drafted = user
        .act_on_user_task(
            UserTaskAction::SaveDraft(SaveTaskDraft {
                precondition: task_precondition(todo_id, 2, "contract/task/7001/draft"),
                draft: json!({"approved": true}),
            }),
            MutationOptions::new("contract/task/7001/draft").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(drafted.revision.get(), 3);
    let submitted = user
        .act_on_user_task(
            UserTaskAction::Submit(SubmitTask {
                precondition: task_precondition(todo_id, 3, "contract/task/7001/submit"),
                submission: json!({"approved": true}),
            }),
            MutationOptions::new("contract/task/7001/submit").unwrap(),
        )
        .await
        .unwrap();
    assert!(submitted.observation_cursor.is_some());
    user.act_on_user_task(
        UserTaskAction::MarkRead(MarkTaskRead {
            precondition: task_precondition(
                UserTaskId::new(READ_TASK_ID).unwrap(),
                1,
                "contract/task/7002/read",
            ),
        }),
        MutationOptions::new("contract/task/7002/read").unwrap(),
    )
    .await
    .unwrap();
    let summary = service
        .summarize_application_tasks(ApplicationTaskQuery::default(), RequestOptions::new())
        .await
        .unwrap();
    assert_eq!(summary.completed, 2);

    BindingContractReport {
        event_id: accepted.event_id.get(),
        workflow_id: run.workflow_id.0,
        workflow_instance_id: run.workflow_instance_id.0,
        signal_cursor: signal_cursor.to_string(),
        task_revision: submitted.revision.get(),
        completed_tasks: summary.completed,
    }
}

fn task_precondition(task_id: UserTaskId, revision: u64, key: &str) -> TaskActionPrecondition {
    TaskActionPrecondition {
        task_id,
        expected_revision: UserTaskRevision::new(revision).unwrap(),
        idempotency_key: IdempotencyKey::new(key).unwrap(),
    }
}

struct MockProductRuntimeServer {
    address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl MockProductRuntimeServer {
    async fn start(facade: ProductRuntimeFacade) -> Self {
        let app = Router::new()
            .route("/openapi/event-dispatch/v1/events", post(publish_event))
            .route(
                "/openapi/event-dispatch/v1/events/{event_id}",
                get(get_event),
            )
            .route(
                "/api/user/workflows/{workflow_id}/run/async",
                post(start_workflow),
            )
            .route(
                "/api/user/workflow-instances/{instance_id}",
                get(workflow_snapshot),
            )
            .route(
                "/api/user/workflow-instances/{instance_id}/append-message",
                post(signal_workflow),
            )
            .route(
                "/api/user/workflow-instances/{instance_id}/terminate",
                post(terminate_workflow),
            )
            .route(
                "/api/user/workflow-instances/{instance_id}/notifications/sse",
                get(subscribe_workflow),
            )
            .route("/openapi/user-tasks", get(list_application_tasks))
            .route(
                "/openapi/user-tasks/summary",
                get(summarize_application_tasks),
            )
            .route("/openapi/user-tasks/{task_id}", get(get_application_task))
            .route(
                "/openapi/user-tasks/{task_id}/completion/retry",
                post(retry_user_task_completion),
            )
            .route("/api/user/user-tasks", get(list_current_user_tasks))
            .route("/api/user/user-tasks/{task_id}", get(get_current_user_task))
            .route("/api/user/user-tasks/{task_id}/claim", post(claim_task))
            .route("/api/user/user-tasks/{task_id}/read", post(read_task))
            .route("/api/user/user-tasks/{task_id}/draft", put(save_draft))
            .route("/api/user/user-tasks/{task_id}/submit", post(submit_task))
            .with_state(facade);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self { address, task }
    }

    fn endpoint(&self) -> String {
        format!("http://{}/", self.address)
    }
}

impl Drop for MockProductRuntimeServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn request_context(headers: &HeaderMap, principal: PrincipalKind) -> RequestContext {
    match principal {
        PrincipalKind::Service => {
            let authorization = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap();
            assert_eq!(authorization, "Bearer service-secret");
            assert_eq!(
                headers
                    .get("x-kish-app-id")
                    .and_then(|value| value.to_str().ok()),
                Some("contract%2Dapp")
            );
        }
        PrincipalKind::User => {
            assert_eq!(
                headers
                    .get("x-kish-token-key")
                    .and_then(|value| value.to_str().ok()),
                Some("x-token")
            );
            assert_eq!(
                headers.get("x-token").and_then(|value| value.to_str().ok()),
                Some("user-secret")
            );
            assert!(headers.contains_key("x-kish-app-id"));
        }
        PrincipalKind::Internal => unreachable!(),
    }
    let request_id = headers.get("x-request-id").unwrap().to_str().unwrap();
    let correlation_id = headers.get("x-correlation-id").unwrap().to_str().unwrap();
    TrustedContextFactory::new(
        match principal {
            PrincipalKind::Service => "contract-service",
            PrincipalKind::User => "contract-actor",
            PrincipalKind::Internal => unreachable!(),
        },
        principal,
        Some("contract-app".to_string()),
        InvocationSource::Http,
    )
    .unwrap()
    .request_context(request_id, correlation_id)
    .unwrap()
}

fn status_for_runtime(error: &RuntimeError) -> StatusCode {
    match error.code {
        RuntimeErrorCode::InvalidRequest => StatusCode::BAD_REQUEST,
        RuntimeErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
        RuntimeErrorCode::Forbidden => StatusCode::FORBIDDEN,
        RuntimeErrorCode::NotFound => StatusCode::NOT_FOUND,
        RuntimeErrorCode::Conflict => StatusCode::CONFLICT,
        RuntimeErrorCode::StaleSuspension | RuntimeErrorCode::ReplayUnavailable => StatusCode::GONE,
        RuntimeErrorCode::Unsupported => StatusCode::UNPROCESSABLE_ENTITY,
        RuntimeErrorCode::Connectivity | RuntimeErrorCode::Unavailable => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        RuntimeErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn status_for_problem(error: &ApplicationProblem) -> StatusCode {
    match error.code.as_str() {
        "invalid_request" => StatusCode::BAD_REQUEST,
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "forbidden" => StatusCode::FORBIDDEN,
        "not_found" => StatusCode::NOT_FOUND,
        "conflict" | "stale_task_revision" => StatusCode::CONFLICT,
        "unsupported" => StatusCode::UNPROCESSABLE_ENTITY,
        "unavailable" => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn runtime_response<T: Serialize>(result: RuntimeResult<T>) -> Response {
    match result {
        Ok(value) => Json(value).into_response(),
        Err(error) => (status_for_runtime(&error), Json(error)).into_response(),
    }
}

fn application_response<T: Serialize>(result: ApplicationResult<T>) -> Response {
    match result {
        Ok(value) => Json(value).into_response(),
        Err(error) => (status_for_problem(&error), Json(error)).into_response(),
    }
}

fn require_idempotency(headers: &HeaderMap, expected: &IdempotencyKey) {
    assert_eq!(
        headers
            .get("idempotency-key")
            .and_then(|value| value.to_str().ok()),
        Some(expected.as_str())
    );
}

async fn publish_event(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Json(event): Json<PublishEvent>,
) -> Response {
    let context = request_context(&headers, PrincipalKind::Service);
    let key =
        IdempotencyKey::new(headers.get("idempotency-key").unwrap().to_str().unwrap()).unwrap();
    application_response(facade.event_publisher().publish(context, event, key).await)
}

async fn get_event(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(event_id): Path<u64>,
) -> Response {
    let context = request_context(&headers, PrincipalKind::Service);
    application_response(
        facade
            .event_publisher()
            .get(context, EventId::new(event_id).unwrap())
            .await,
    )
}

#[derive(Deserialize)]
struct StartWorkflowBody {
    input: WorkflowInput,
    #[serde(default)]
    bindings: RuntimeBindings,
}

async fn start_workflow(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(workflow_id): Path<u64>,
    Json(body): Json<StartWorkflowBody>,
) -> Response {
    let idempotency_key =
        IdempotencyKey::new(headers.get("idempotency-key").unwrap().to_str().unwrap()).unwrap();
    let context =
        request_context(&headers, PrincipalKind::User).with_idempotency_key(idempotency_key);
    runtime_response(
        facade
            .workflow()
            .start(StartWorkflowRequest {
                context: WorkflowContext::new(context, WorkflowId(workflow_id)),
                session_id: Some(SessionId(7)),
                input: body.input,
                bindings: body.bindings,
            })
            .await,
    )
}

async fn workflow_snapshot(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(instance_id): Path<u64>,
) -> Response {
    let context = request_context(&headers, PrincipalKind::User);
    runtime_response(
        facade
            .workflow()
            .snapshot(WorkflowSnapshotRequest {
                context,
                workflow_instance_id: WorkflowInstanceId(instance_id),
            })
            .await,
    )
}

#[derive(Deserialize)]
struct SignalWorkflowBody {
    signal: WorkflowSignal,
}

async fn signal_workflow(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(instance_id): Path<u64>,
    Json(body): Json<SignalWorkflowBody>,
) -> Response {
    let idempotency_key =
        IdempotencyKey::new(headers.get("idempotency-key").unwrap().to_str().unwrap()).unwrap();
    let context =
        request_context(&headers, PrincipalKind::User).with_idempotency_key(idempotency_key);
    runtime_response(
        facade
            .workflow()
            .signal(SignalWorkflowRequest {
                context,
                workflow_instance_id: WorkflowInstanceId(instance_id),
                signal: body.signal,
            })
            .await,
    )
}

#[derive(Deserialize)]
struct TerminateWorkflowBody {
    reason: Option<String>,
    wait_timeout_ms: Option<u64>,
}

async fn terminate_workflow(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(instance_id): Path<u64>,
    Json(body): Json<TerminateWorkflowBody>,
) -> Response {
    let idempotency_key =
        IdempotencyKey::new(headers.get("idempotency-key").unwrap().to_str().unwrap()).unwrap();
    let context =
        request_context(&headers, PrincipalKind::User).with_idempotency_key(idempotency_key);
    runtime_response(
        facade
            .workflow()
            .terminate(TerminateWorkflowRequest {
                context,
                workflow_instance_id: WorkflowInstanceId(instance_id),
                reason: body.reason,
                wait_timeout_ms: body.wait_timeout_ms,
            })
            .await,
    )
}

async fn subscribe_workflow(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(instance_id): Path<u64>,
) -> Response {
    let context = request_context(&headers, PrincipalKind::User);
    let after = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .map(EventCursor::from);
    let stream = match facade
        .workflow()
        .subscribe(SubscribeWorkflowRequest {
            context,
            workflow_instance_id: WorkflowInstanceId(instance_id),
            after,
        })
        .await
    {
        Ok(stream) => stream,
        Err(error) => return runtime_response::<()>(Err(error)),
    };
    let events = match stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<RuntimeResult<Vec<_>>>()
    {
        Ok(events) => events,
        Err(error) => return runtime_response::<()>(Err(error)),
    };
    let mut body = String::new();
    for event in events {
        body.push_str("id: ");
        body.push_str(event.cursor.as_ref());
        body.push_str("\ndata: ");
        body.push_str(&serde_json::to_string(&event).unwrap());
        body.push_str("\n\n");
    }
    body.push_str("data: [DONE]\n\n");
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(body))
        .unwrap()
}

async fn list_application_tasks(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
) -> Response {
    let context = request_context(&headers, PrincipalKind::Service);
    application_response(
        facade
            .user_tasks()
            .list_application(context, ApplicationTaskQuery::default())
            .await,
    )
}

async fn summarize_application_tasks(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
) -> Response {
    let context = request_context(&headers, PrincipalKind::Service);
    application_response(
        facade
            .user_tasks()
            .summarize_application(context, ApplicationTaskQuery::default())
            .await,
    )
}

async fn get_application_task(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(task_id): Path<u64>,
) -> Response {
    let context = request_context(&headers, PrincipalKind::Service);
    application_response(
        facade
            .user_tasks()
            .get_application(context, UserTaskId::new(task_id).unwrap())
            .await,
    )
}

async fn retry_user_task_completion(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(task_id): Path<u64>,
    Json(request): Json<RetryUserTaskCompletion>,
) -> Response {
    assert_eq!(request.task_id.get(), task_id);
    let context = request_context(&headers, PrincipalKind::Service);
    application_response(facade.user_tasks().retry_completion(context, request).await)
}

async fn list_current_user_tasks(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
) -> Response {
    let context = request_context(&headers, PrincipalKind::User);
    application_response(
        facade
            .user_tasks()
            .list_mine(context, CurrentUserTaskQuery::default())
            .await,
    )
}

async fn get_current_user_task(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(task_id): Path<u64>,
) -> Response {
    let context = request_context(&headers, PrincipalKind::User);
    application_response(
        facade
            .user_tasks()
            .get_mine(context, UserTaskId::new(task_id).unwrap())
            .await,
    )
}

async fn task_action<T, F>(
    facade: ProductRuntimeFacade,
    headers: HeaderMap,
    task_id: u64,
    request: T,
    into_action: F,
) -> Response
where
    T: Serialize,
    F: FnOnce(T) -> UserTaskAction,
{
    let action = into_action(request);
    assert_eq!(action.precondition().task_id.get(), task_id);
    require_idempotency(&headers, &action.precondition().idempotency_key);
    let context = request_context(&headers, PrincipalKind::User);
    application_response(facade.user_tasks().act(context, action).await)
}

async fn claim_task(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(task_id): Path<u64>,
    Json(request): Json<ClaimTask>,
) -> Response {
    task_action(facade, headers, task_id, request, UserTaskAction::Claim).await
}

async fn read_task(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(task_id): Path<u64>,
    Json(request): Json<MarkTaskRead>,
) -> Response {
    task_action(facade, headers, task_id, request, UserTaskAction::MarkRead).await
}

async fn save_draft(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(task_id): Path<u64>,
    Json(request): Json<SaveTaskDraft>,
) -> Response {
    task_action(facade, headers, task_id, request, UserTaskAction::SaveDraft).await
}

async fn submit_task(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(task_id): Path<u64>,
    Json(request): Json<SubmitTask>,
) -> Response {
    task_action(facade, headers, task_id, request, UserTaskAction::Submit).await
}
