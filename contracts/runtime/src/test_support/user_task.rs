use std::{collections::HashMap, sync::Mutex};

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use serde_json::json;

use crate::{
    ApplicationProblem, ApplicationResult, ApplicationTaskQuery, ApplicationTaskSummary, ClaimTask,
    CurrentUserTaskQuery, EventCursor, IdempotencyKey, MarkTaskRead, MutationDisposition,
    MutationReceipt, Page, PageRequest, PrincipalKind, ProblemCode, RequestContext, SaveTaskDraft,
    SubmitTask, TaskAction, TaskActionPrecondition, TaskActionReceipt, UserTask, UserTaskAction,
    UserTaskId, UserTaskInitiator, UserTaskMode, UserTaskPage, UserTaskPageSource,
    UserTaskParticipant, UserTaskParticipantRole, UserTaskParticipantState, UserTaskPayloads,
    UserTaskPermissions, UserTaskRevision, UserTaskRuntime, UserTaskState, UserTaskSummary,
    UserTaskTimestamps, UserTaskWorkflowReference, WorkflowId, WorkflowInstanceId,
    CONFLICT_PROBLEM, FORBIDDEN_PROBLEM, NOT_FOUND_PROBLEM, STALE_TASK_REVISION_PROBLEM,
};

pub const CONTRACT_TODO_TASK_ID: u64 = 7_001;
pub const CONTRACT_READ_TASK_ID: u64 = 7_002;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserTaskRuntimeContractReport {
    pub todo_task_id: u64,
    pub todo_final_revision: u64,
    pub read_task_id: u64,
    pub read_final_revision: u64,
    pub observation_cursor: EventCursor,
}

/// Run application observation and all current-participant actions against a port.
///
/// The runtime fixture must expose the two contract tasks identified by
/// [`CONTRACT_TODO_TASK_ID`] and [`CONTRACT_READ_TASK_ID`] in the supplied
/// Application. `ContractUserTaskRuntime` provides this state by default.
pub async fn assert_user_task_runtime_contract(
    runtime: std::sync::Arc<dyn UserTaskRuntime>,
    service_context: RequestContext,
    user_context: RequestContext,
) -> UserTaskRuntimeContractReport {
    assert_eq!(service_context.principal(), PrincipalKind::Service);
    assert_eq!(user_context.principal(), PrincipalKind::User);
    assert_eq!(
        service_context.application_id(),
        user_context.application_id()
    );

    let initial = runtime
        .list_application(service_context.clone(), ApplicationTaskQuery::default())
        .await
        .expect("Application task list must be observable");
    assert_eq!(initial.total, 2);
    assert_eq!(initial.items.len(), 2);
    assert!(initial
        .items
        .iter()
        .all(|task| task.current_user_participant_state.is_none()));

    let paged = runtime
        .list_application(
            service_context.clone(),
            ApplicationTaskQuery {
                pagination: PageRequest::new(1, 1).unwrap(),
                ..Default::default()
            },
        )
        .await
        .expect("Application pagination must succeed");
    assert_eq!(paged.total, 2);
    assert_eq!(paged.items.len(), 1);

    let initial_summary = runtime
        .summarize_application(service_context.clone(), ApplicationTaskQuery::default())
        .await
        .expect("Application task summary must be observable");
    assert_eq!(initial_summary.pending_todo, 1);
    assert_eq!(initial_summary.pending_read, 1);
    assert_eq!(initial_summary.completed, 0);
    assert_eq!(initial_summary.total, 2);

    let todo_id = UserTaskId::new(CONTRACT_TODO_TASK_ID).unwrap();
    let read_id = UserTaskId::new(CONTRACT_READ_TASK_ID).unwrap();
    let application_todo = runtime
        .get_application(service_context.clone(), todo_id)
        .await
        .expect("Application task detail must be observable");
    assert_eq!(application_todo.summary.revision.get(), 1);
    assert_eq!(application_todo.permissions, UserTaskPermissions::default());

    let mine = runtime
        .list_mine(user_context.clone(), CurrentUserTaskQuery::default())
        .await
        .expect("current-user task list must be observable");
    assert_eq!(mine.total, 2);
    let mine_todo = runtime
        .get_mine(user_context.clone(), todo_id)
        .await
        .expect("current-user task detail must be observable");
    assert!(mine_todo.permissions.can_claim);

    let claim = UserTaskAction::Claim(ClaimTask {
        precondition: precondition(todo_id, 1, "contract/task/7001/claim"),
    });
    let claimed = runtime
        .act(user_context.clone(), claim.clone())
        .await
        .expect("eligible participant must be able to claim");
    assert_eq!(claimed.revision.get(), 2);
    assert_eq!(claimed.state, UserTaskState::PendingClaimed);
    assert_eq!(claimed.mutation.disposition, MutationDisposition::Accepted);

    let replayed_claim = runtime
        .act(user_context.clone(), claim)
        .await
        .expect("same claim command must replay");
    assert_eq!(replayed_claim.revision, claimed.revision);
    assert_eq!(
        replayed_claim.mutation.disposition,
        MutationDisposition::Duplicate
    );

    let conflicting_claim = runtime
        .act(
            user_context.clone(),
            UserTaskAction::Claim(ClaimTask {
                precondition: precondition(todo_id, 2, "contract/task/7001/claim"),
            }),
        )
        .await
        .expect_err("changed request under the same action key must conflict");
    assert_eq!(conflicting_claim.code.as_str(), CONFLICT_PROBLEM);

    let stale_draft = runtime
        .act(
            user_context.clone(),
            UserTaskAction::SaveDraft(SaveTaskDraft {
                precondition: precondition(todo_id, 1, "contract/task/7001/stale-draft"),
                draft: json!({"approved": false}),
            }),
        )
        .await
        .expect_err("stale observed task revision must be rejected");
    assert_eq!(stale_draft.code.as_str(), STALE_TASK_REVISION_PROBLEM);

    let drafted = runtime
        .act(
            user_context.clone(),
            UserTaskAction::SaveDraft(SaveTaskDraft {
                precondition: precondition(todo_id, 2, "contract/task/7001/draft"),
                draft: json!({"approved": true}),
            }),
        )
        .await
        .expect("claimed participant must be able to save a draft");
    assert_eq!(drafted.revision.get(), 3);
    let after_draft = runtime
        .get_mine(user_context.clone(), todo_id)
        .await
        .expect("saved draft must be observable");
    assert_eq!(after_draft.payloads.draft, Some(json!({"approved": true})));

    let submitted = runtime
        .act(
            user_context.clone(),
            UserTaskAction::Submit(SubmitTask {
                precondition: precondition(todo_id, 3, "contract/task/7001/submit"),
                submission: json!({"approved": true, "comment": "contract"}),
            }),
        )
        .await
        .expect("claimed participant must be able to submit");
    assert_eq!(submitted.revision.get(), 4);
    assert_eq!(submitted.state, UserTaskState::Completed);
    let observation_cursor = submitted
        .observation_cursor
        .clone()
        .expect("submission must expose its Workflow observation cursor");

    let marked_read = runtime
        .act(
            user_context.clone(),
            UserTaskAction::MarkRead(MarkTaskRead {
                precondition: precondition(read_id, 1, "contract/task/7002/read"),
            }),
        )
        .await
        .expect("eligible reader must be able to mark a task read");
    assert_eq!(marked_read.revision.get(), 2);
    assert_eq!(marked_read.state, UserTaskState::Completed);

    let final_summary = runtime
        .summarize_application(service_context, ApplicationTaskQuery::default())
        .await
        .expect("mutated task summary must remain observable");
    assert_eq!(final_summary.pending_todo, 0);
    assert_eq!(final_summary.pending_read, 0);
    assert_eq!(final_summary.completed, 2);
    assert_eq!(final_summary.total, 2);

    UserTaskRuntimeContractReport {
        todo_task_id: todo_id.get(),
        todo_final_revision: submitted.revision.get(),
        read_task_id: read_id.get(),
        read_final_revision: marked_read.revision.get(),
        observation_cursor,
    }
}

struct ContractTaskCommand {
    action: UserTaskAction,
    receipt: TaskActionReceipt,
}

struct UserTaskStateStore {
    tasks: HashMap<UserTaskId, UserTask>,
    commands: HashMap<(UserTaskId, String, String, String), ContractTaskCommand>,
}

/// Seeded in-memory User Task runtime for SDK and adapter contract tests.
pub struct ContractUserTaskRuntime {
    state: Mutex<UserTaskStateStore>,
}

impl Default for ContractUserTaskRuntime {
    fn default() -> Self {
        Self {
            state: Mutex::new(UserTaskStateStore {
                tasks: [contract_todo_task(), contract_read_task()]
                    .into_iter()
                    .map(|task| (task.summary.id, task))
                    .collect(),
                commands: HashMap::new(),
            }),
        }
    }
}

impl ContractUserTaskRuntime {
    pub fn task(&self, task_id: UserTaskId) -> Option<UserTask> {
        self.state.lock().unwrap().tasks.get(&task_id).cloned()
    }

    fn require_application<'a>(
        context: &'a RequestContext,
        principal: PrincipalKind,
    ) -> ApplicationResult<&'a str> {
        if context.principal() != principal {
            return Err(problem(
                context,
                FORBIDDEN_PROBLEM,
                "principal is not authorized for this User Task operation",
            ));
        }
        context.application_id().ok_or_else(|| {
            problem(
                context,
                FORBIDDEN_PROBLEM,
                "User Task operation requires an Application scope",
            )
        })
    }
}

#[async_trait]
impl UserTaskRuntime for ContractUserTaskRuntime {
    async fn list_application(
        &self,
        context: RequestContext,
        query: ApplicationTaskQuery,
    ) -> ApplicationResult<Page<UserTaskSummary>> {
        let application_id = Self::require_application(&context, PrincipalKind::Service)?;
        let tasks = self.state.lock().unwrap();
        let summaries = tasks
            .tasks
            .values()
            .filter(|task| {
                task.summary.application_id == application_id
                    && matches_application_query(task, &query)
            })
            .map(application_summary)
            .collect();
        Ok(page(summaries, query.pagination))
    }

    async fn summarize_application(
        &self,
        context: RequestContext,
        query: ApplicationTaskQuery,
    ) -> ApplicationResult<ApplicationTaskSummary> {
        let application_id = Self::require_application(&context, PrincipalKind::Service)?;
        let tasks = self.state.lock().unwrap();
        let filtered = tasks.tasks.values().filter(|task| {
            task.summary.application_id == application_id && matches_application_query(task, &query)
        });
        let mut summary = ApplicationTaskSummary::default();
        for task in filtered {
            summary.total += 1;
            if task.summary.state == UserTaskState::Completed {
                summary.completed += 1;
            } else if task.summary.mode == UserTaskMode::Todo {
                summary.pending_todo += 1;
            } else if task.summary.mode == UserTaskMode::Read {
                summary.pending_read += 1;
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
        self.state
            .lock()
            .unwrap()
            .tasks
            .get(&task_id)
            .filter(|task| task.summary.application_id == application_id)
            .cloned()
            .map(application_task)
            .ok_or_else(|| not_found(&context, task_id))
    }

    async fn retry_completion(
        &self,
        context: RequestContext,
        _: crate::RetryUserTaskCompletion,
    ) -> ApplicationResult<crate::UserTaskCompletionRetryReceipt> {
        Self::require_application(&context, PrincipalKind::Service)?;
        Err(problem(
            &context,
            crate::CONFLICT_PROBLEM,
            "contract fixture has no failed completion command",
        ))
    }

    async fn list_mine(
        &self,
        context: RequestContext,
        query: CurrentUserTaskQuery,
    ) -> ApplicationResult<Page<UserTaskSummary>> {
        let application_id = Self::require_application(&context, PrincipalKind::User)?;
        let tasks = self.state.lock().unwrap();
        let summaries = tasks
            .tasks
            .values()
            .filter(|task| {
                task.summary.application_id == application_id
                    && is_participant(task, context.actor_id())
                    && matches_current_user_query(task, context.actor_id(), &query)
            })
            .cloned()
            .map(|task| current_user_task(task, context.actor_id()).summary)
            .collect();
        Ok(page(summaries, query.pagination))
    }

    async fn get_mine(
        &self,
        context: RequestContext,
        task_id: UserTaskId,
    ) -> ApplicationResult<UserTask> {
        let application_id = Self::require_application(&context, PrincipalKind::User)?;
        self.state
            .lock()
            .unwrap()
            .tasks
            .get(&task_id)
            .filter(|task| {
                task.summary.application_id == application_id
                    && is_participant(task, context.actor_id())
            })
            .cloned()
            .map(|task| current_user_task(task, context.actor_id()))
            .ok_or_else(|| not_found(&context, task_id))
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
            task_action_name(action.kind()).to_string(),
            precondition.idempotency_key.to_string(),
        );
        let mut state = self.state.lock().unwrap();
        if let Some(command) = state.commands.get(&command_key) {
            if command.action != action {
                return Err(problem(
                    &context,
                    CONFLICT_PROBLEM,
                    "idempotency key is already bound to a different User Task action",
                ));
            }
            let mut receipt = command.receipt.clone();
            receipt.mutation.disposition = MutationDisposition::Duplicate;
            return Ok(receipt);
        }

        let task = state
            .tasks
            .get_mut(&precondition.task_id)
            .filter(|task| {
                task.summary.application_id == application_id
                    && is_participant(task, context.actor_id())
            })
            .ok_or_else(|| not_found(&context, precondition.task_id))?;
        if task.summary.revision != precondition.expected_revision {
            return Err(problem(
                &context,
                STALE_TASK_REVISION_PROBLEM,
                "User Task revision is stale",
            )
            .with_details(json!({
                "expected": precondition.expected_revision,
                "actual": task.summary.revision,
            })));
        }

        apply_action(task, context.actor_id(), &action, &context)?;
        let receipt = TaskActionReceipt {
            mutation: MutationReceipt::accepted(
                context.request_id().clone(),
                task.summary.timestamps.updated_at,
            ),
            task_id: task.summary.id,
            action: action.kind(),
            revision: task.summary.revision,
            state: task.summary.state.clone(),
            workflow_id: task.summary.workflow.workflow_id,
            workflow_instance_id: task.summary.workflow.workflow_instance_id,
            observation_cursor: (action.kind() == TaskAction::Submit).then(|| {
                EventCursor::from(format!(
                    "contract-task-{}:{}",
                    task.summary.id, task.summary.revision
                ))
            }),
        };
        state.commands.insert(
            command_key,
            ContractTaskCommand {
                action,
                receipt: receipt.clone(),
            },
        );
        Ok(receipt)
    }
}

fn apply_action(
    task: &mut UserTask,
    actor_id: &str,
    action: &UserTaskAction,
    context: &RequestContext,
) -> ApplicationResult<()> {
    let accepted_at = contract_task_time()
        + TimeDelta::seconds(i64::try_from(task.summary.revision.get()).unwrap());
    match action {
        UserTaskAction::Claim(_) => {
            if task.summary.mode != UserTaskMode::Todo
                || task.summary.state != UserTaskState::PendingUnclaimed
            {
                return Err(problem(
                    context,
                    CONFLICT_PROBLEM,
                    "User Task cannot be claimed in its current state",
                ));
            }
            let participant = participant_mut(task, actor_id, context)?;
            participant.state = UserTaskParticipantState::PendingClaimed;
            participant.acted_at = Some(accepted_at);
            task.summary.state = UserTaskState::PendingClaimed;
            task.summary.claimed_by = Some(actor_id.to_string());
            task.summary.timestamps.claimed_at = Some(accepted_at);
        }
        UserTaskAction::MarkRead(_) => {
            if task.summary.mode != UserTaskMode::Read
                || task.summary.state != UserTaskState::PendingRead
            {
                return Err(problem(
                    context,
                    CONFLICT_PROBLEM,
                    "User Task cannot be marked read in its current state",
                ));
            }
            let participant = participant_mut(task, actor_id, context)?;
            participant.state = UserTaskParticipantState::Read;
            participant.acted_at = Some(accepted_at);
            task.summary.state = UserTaskState::Completed;
            task.summary.timestamps.completed_at = Some(accepted_at);
        }
        UserTaskAction::SaveDraft(request) => {
            require_claim(task, actor_id, context)?;
            task.payloads.draft = Some(request.draft.clone());
        }
        UserTaskAction::Submit(request) => {
            require_claim(task, actor_id, context)?;
            task.payloads.submission = Some(request.submission.clone());
            task.summary.state = UserTaskState::Completed;
            task.summary.timestamps.completed_at = Some(accepted_at);
            task.summary.timestamps.submitted_at = Some(accepted_at);
            let participant = participant_mut(task, actor_id, context)?;
            participant.state = UserTaskParticipantState::Completed;
            participant.acted_at = Some(accepted_at);
        }
    }
    task.summary.revision = UserTaskRevision::new(task.summary.revision.get() + 1).unwrap();
    task.summary.timestamps.updated_at = accepted_at;
    Ok(())
}

fn require_claim(
    task: &UserTask,
    actor_id: &str,
    context: &RequestContext,
) -> ApplicationResult<()> {
    if task.summary.mode != UserTaskMode::Todo
        || task.summary.state != UserTaskState::PendingClaimed
        || task.summary.claimed_by.as_deref() != Some(actor_id)
    {
        return Err(problem(
            context,
            FORBIDDEN_PROBLEM,
            "User Task action requires the current claim owner",
        ));
    }
    Ok(())
}

fn participant_mut<'a>(
    task: &'a mut UserTask,
    actor_id: &str,
    context: &RequestContext,
) -> ApplicationResult<&'a mut UserTaskParticipant> {
    task.participants
        .iter_mut()
        .find(|participant| participant.user_id == actor_id)
        .ok_or_else(|| {
            problem(
                context,
                FORBIDDEN_PROBLEM,
                "current user is not a task participant",
            )
        })
}

fn application_summary(task: &UserTask) -> UserTaskSummary {
    let mut summary = task.summary.clone();
    summary.current_user_participant_state = None;
    summary
}

fn application_task(mut task: UserTask) -> UserTask {
    task.summary.current_user_participant_state = None;
    task.permissions = UserTaskPermissions::default();
    task
}

fn current_user_task(mut task: UserTask, actor_id: &str) -> UserTask {
    let participant_state = task
        .participants
        .iter()
        .find(|participant| participant.user_id == actor_id)
        .map(|participant| participant.state.clone());
    task.summary.current_user_participant_state = participant_state.clone();
    task.permissions = UserTaskPermissions {
        can_claim: task.summary.mode == UserTaskMode::Todo
            && task.summary.state == UserTaskState::PendingUnclaimed
            && participant_state == Some(UserTaskParticipantState::PendingUnclaimed),
        can_mark_read: task.summary.mode == UserTaskMode::Read
            && task.summary.state == UserTaskState::PendingRead
            && participant_state == Some(UserTaskParticipantState::Unread),
        can_save_draft: task.summary.mode == UserTaskMode::Todo
            && task.summary.state == UserTaskState::PendingClaimed
            && task.summary.claimed_by.as_deref() == Some(actor_id),
        can_submit: task.summary.mode == UserTaskMode::Todo
            && task.summary.state == UserTaskState::PendingClaimed
            && task.summary.claimed_by.as_deref() == Some(actor_id),
    };
    task
}

fn matches_application_query(task: &UserTask, query: &ApplicationTaskQuery) -> bool {
    query
        .state
        .as_ref()
        .is_none_or(|state| &task.summary.state == state)
        && query
            .mode
            .as_ref()
            .is_none_or(|mode| &task.summary.mode == mode)
        && query
            .workflow_id
            .is_none_or(|workflow_id| task.summary.workflow.workflow_id == workflow_id)
        && query.participant_user_id.as_deref().is_none_or(|user_id| {
            task.participants
                .iter()
                .any(|participant| participant.user_id == user_id)
        })
        && query
            .participant_state
            .as_ref()
            .is_none_or(|state| task.participants.iter().any(|p| &p.state == state))
        && query
            .claimed_by
            .as_deref()
            .is_none_or(|claimed_by| task.summary.claimed_by.as_deref() == Some(claimed_by))
        && query
            .created
            .created_from
            .is_none_or(|from| task.summary.timestamps.created_at >= from)
        && query
            .created
            .created_before
            .is_none_or(|before| task.summary.timestamps.created_at < before)
}

fn matches_current_user_query(
    task: &UserTask,
    actor_id: &str,
    query: &CurrentUserTaskQuery,
) -> bool {
    query.participant_state.as_ref().is_none_or(|state| {
        task.participants
            .iter()
            .any(|participant| participant.user_id == actor_id && &participant.state == state)
    }) && query
        .mode
        .as_ref()
        .is_none_or(|mode| &task.summary.mode == mode)
        && query
            .created
            .created_from
            .is_none_or(|from| task.summary.timestamps.created_at >= from)
        && query
            .created
            .created_before
            .is_none_or(|before| task.summary.timestamps.created_at < before)
}

fn is_participant(task: &UserTask, actor_id: &str) -> bool {
    task.participants
        .iter()
        .any(|participant| participant.user_id == actor_id)
}

fn page<T>(items: Vec<T>, request: PageRequest) -> Page<T> {
    let total = u64::try_from(items.len()).unwrap();
    let offset = (request.page - 1).saturating_mul(request.page_size);
    let items = items
        .into_iter()
        .skip(usize::try_from(offset).unwrap_or(usize::MAX))
        .take(usize::try_from(request.page_size).unwrap_or(usize::MAX))
        .collect();
    Page {
        items,
        total,
        page: request.page,
        page_size: request.page_size,
    }
}

fn precondition(
    task_id: UserTaskId,
    expected_revision: u64,
    idempotency_key: &str,
) -> TaskActionPrecondition {
    TaskActionPrecondition {
        task_id,
        expected_revision: UserTaskRevision::new(expected_revision).unwrap(),
        idempotency_key: IdempotencyKey::new(idempotency_key).unwrap(),
    }
}

fn contract_todo_task() -> UserTask {
    let created_at = contract_task_time();
    UserTask {
        summary: UserTaskSummary {
            id: UserTaskId::new(CONTRACT_TODO_TASK_ID).unwrap(),
            revision: UserTaskRevision::new(1).unwrap(),
            application_id: "contract-app".to_string(),
            title: "Approve contract order".to_string(),
            state: UserTaskState::PendingUnclaimed,
            mode: UserTaskMode::Todo,
            task_type: "contract_approval".to_string(),
            workflow: contract_workflow_reference(9_001, "approve-contract"),
            initiator: contract_initiator(),
            current_user_participant_state: Some(UserTaskParticipantState::PendingUnclaimed),
            claimed_by: None,
            timestamps: UserTaskTimestamps {
                created_at,
                updated_at: created_at,
                claimed_at: None,
                completed_at: None,
                submitted_at: None,
            },
        },
        page: UserTaskPage {
            source: UserTaskPageSource::InlineHtml,
            value: "<form data-contract=\"approval\"></form>".to_string(),
        },
        payloads: UserTaskPayloads {
            display: Some(json!({"order_id": 42})),
            result_schema: Some(json!({
                "type": "object",
                "required": ["approved"]
            })),
            ..Default::default()
        },
        participants: vec![UserTaskParticipant {
            user_id: "contract-actor".to_string(),
            role: UserTaskParticipantRole::TodoCandidate,
            state: UserTaskParticipantState::PendingUnclaimed,
            acted_at: None,
        }],
        permissions: UserTaskPermissions {
            can_claim: true,
            ..Default::default()
        },
        completion: None,
    }
}

fn contract_read_task() -> UserTask {
    let created_at = contract_task_time() + TimeDelta::seconds(1);
    UserTask {
        summary: UserTaskSummary {
            id: UserTaskId::new(CONTRACT_READ_TASK_ID).unwrap(),
            revision: UserTaskRevision::new(1).unwrap(),
            application_id: "contract-app".to_string(),
            title: "Read contract notice".to_string(),
            state: UserTaskState::PendingRead,
            mode: UserTaskMode::Read,
            task_type: "contract_notice".to_string(),
            workflow: contract_workflow_reference(9_002, "read-contract"),
            initiator: contract_initiator(),
            current_user_participant_state: Some(UserTaskParticipantState::Unread),
            claimed_by: None,
            timestamps: UserTaskTimestamps {
                created_at,
                updated_at: created_at,
                claimed_at: None,
                completed_at: None,
                submitted_at: None,
            },
        },
        page: UserTaskPage {
            source: UserTaskPageSource::InlineHtml,
            value: "<article data-contract=\"notice\"></article>".to_string(),
        },
        payloads: UserTaskPayloads {
            display: Some(json!({"notice": "contract"})),
            ..Default::default()
        },
        participants: vec![UserTaskParticipant {
            user_id: "contract-actor".to_string(),
            role: UserTaskParticipantRole::Reader,
            state: UserTaskParticipantState::Unread,
            acted_at: None,
        }],
        permissions: UserTaskPermissions {
            can_mark_read: true,
            ..Default::default()
        },
        completion: None,
    }
}

fn contract_workflow_reference(
    workflow_instance_id: u64,
    flow_node_id: &str,
) -> UserTaskWorkflowReference {
    UserTaskWorkflowReference {
        workflow_id: WorkflowId(42),
        workflow_instance_id: WorkflowInstanceId(workflow_instance_id),
        root_workflow_instance_id: Some(crate::RootWorkflowInstanceId(workflow_instance_id)),
        workflow_name: "Contract Workflow".to_string(),
        flow_node_id: flow_node_id.to_string(),
        flow_node_name: "Contract User Task".to_string(),
    }
}

fn contract_initiator() -> UserTaskInitiator {
    UserTaskInitiator {
        user_id: "contract-initiator".to_string(),
        display_name: "Contract Initiator".to_string(),
    }
}

fn contract_task_time() -> DateTime<Utc> {
    "2026-09-06T08:00:00Z".parse().unwrap()
}

fn task_action_name(action: TaskAction) -> &'static str {
    match action {
        TaskAction::Claim => "claim",
        TaskAction::MarkRead => "mark_read",
        TaskAction::SaveDraft => "save_draft",
        TaskAction::Submit => "submit",
    }
}

fn not_found(context: &RequestContext, task_id: UserTaskId) -> ApplicationProblem {
    problem(
        context,
        NOT_FOUND_PROBLEM,
        format!("User Task {task_id} was not found"),
    )
}

fn problem(
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
