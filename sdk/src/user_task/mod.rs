//! Application-scoped User Task observation and current-user actions.

use std::{fmt, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::{
    client::ClientInner, Error, MutationOptions, ProtocolDirection, ProtocolError, RequestOptions,
};

#[cfg(feature = "user-task-completion")]
pub mod completion;

pub use kish_lingshu_runtime_contract::{
    ApplicationTaskQuery, ApplicationTaskSummary, ClaimTask, CurrentUserTaskQuery,
    InvalidPageRequest, InvalidUserTaskIdentity, InvalidUserTaskValue, MarkTaskRead, Page,
    PageRequest, RetryUserTaskCompletion, SaveTaskDraft, SubmitTask, TaskAction,
    TaskActionPrecondition, TaskActionReceipt, UserTask, UserTaskAction, UserTaskCompletionDetail,
    UserTaskCompletionExecution, UserTaskCompletionInvocationV1, UserTaskCompletionOutcomeV1,
    UserTaskCompletionProblem, UserTaskCompletionRetryReceipt, UserTaskCompletionTaskV1,
    UserTaskCompletionWorkflowV1, UserTaskId, UserTaskInitiator, UserTaskMode, UserTaskPage,
    UserTaskPageSource, UserTaskParticipant, UserTaskParticipantRole, UserTaskParticipantState,
    UserTaskPayloads, UserTaskPermissions, UserTaskRevision, UserTaskState, UserTaskSummary,
    UserTaskTimeRange, UserTaskTimestamps, UserTaskWorkflowReference,
};

#[cfg(feature = "user-task-completion")]
pub use kish_lingshu_sdk_macros::{completion_handler, user_task_handlers};

#[derive(Clone)]
pub struct ApplicationUserTasks {
    pub(crate) inner: Arc<ClientInner>,
}

impl ApplicationUserTasks {
    pub(crate) fn new(inner: Arc<ClientInner>) -> Self {
        Self { inner }
    }

    /// Lists tasks within the Application bound to the service credential.
    pub async fn list(
        &self,
        query: ApplicationTaskQuery,
        options: RequestOptions,
    ) -> Result<Page<UserTaskSummary>, Error> {
        validate_application_query(&query)?;
        self.inner
            .binding
            .list_application_tasks(query, options)
            .await
    }

    /// Summarizes tasks using exactly the supplied list filters.
    pub async fn summary(
        &self,
        query: ApplicationTaskQuery,
        options: RequestOptions,
    ) -> Result<ApplicationTaskSummary, Error> {
        validate_application_query(&query)?;
        self.inner
            .binding
            .summarize_application_tasks(query, options)
            .await
    }

    /// Reads one task within the authenticated Application scope.
    pub async fn get(
        &self,
        task_id: UserTaskId,
        options: RequestOptions,
    ) -> Result<UserTask, Error> {
        self.inner
            .binding
            .get_application_task(task_id, options)
            .await
    }

    /// Reactivates the original fatal completion command without allocating a
    /// new business-effect identity. Refetch the task if this request's
    /// response is lost; this mutation is intentionally not transport-retried.
    pub async fn retry_completion(
        &self,
        task_id: UserTaskId,
        expected_revision: UserTaskRevision,
        options: RequestOptions,
    ) -> Result<UserTaskCompletionRetryReceipt, Error> {
        self.inner
            .binding
            .retry_user_task_completion(
                RetryUserTaskCompletion {
                    task_id,
                    expected_revision,
                },
                options,
            )
            .await
    }
}

#[derive(Clone)]
pub struct CurrentUserTasks {
    pub(crate) inner: Arc<ClientInner>,
}

impl CurrentUserTasks {
    pub(crate) fn new(inner: Arc<ClientInner>) -> Self {
        Self { inner }
    }

    /// Lists tasks in which the authenticated user is a participant.
    pub async fn list(
        &self,
        query: CurrentUserTaskQuery,
        options: RequestOptions,
    ) -> Result<Page<UserTaskSummary>, Error> {
        validate_current_user_query(&query)?;
        self.inner
            .binding
            .list_current_user_tasks(query, options)
            .await
    }

    /// Reads current-user detail and returns an action handle bound to its revision.
    pub async fn get(
        &self,
        task_id: UserTaskId,
        options: RequestOptions,
    ) -> Result<UserTaskHandle, Error> {
        let task = self
            .inner
            .binding
            .get_current_user_task(task_id, options)
            .await?;
        validate_task_identity(task_id, &task)?;
        Ok(UserTaskHandle {
            inner: self.inner.clone(),
            task,
        })
    }
}

/// Current-user task detail plus actions bound to the observed task revision.
///
/// A handle is an optimistic-concurrency snapshot. The server remains
/// authoritative, so an action from a stale handle returns a stale-revision
/// application problem. Fetch the task again to obtain a fresh handle.
#[derive(Clone)]
pub struct UserTaskHandle {
    inner: Arc<ClientInner>,
    task: UserTask,
}

impl fmt::Debug for UserTaskHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UserTaskHandle")
            .field("task", &self.task)
            .finish_non_exhaustive()
    }
}

impl UserTaskHandle {
    pub fn task(&self) -> &UserTask {
        &self.task
    }

    pub fn into_task(self) -> UserTask {
        self.task
    }

    pub fn id(&self) -> UserTaskId {
        self.task.summary.id
    }

    pub fn revision(&self) -> UserTaskRevision {
        self.task.summary.revision
    }

    /// Reloads the task and returns a handle bound to its latest revision.
    pub async fn refresh(&self, options: RequestOptions) -> Result<Self, Error> {
        let task = self
            .inner
            .binding
            .get_current_user_task(self.id(), options)
            .await?;
        validate_task_identity(self.id(), &task)?;
        Ok(Self {
            inner: self.inner.clone(),
            task,
        })
    }

    pub async fn claim(&self, options: MutationOptions) -> Result<TaskActionReceipt, Error> {
        self.act(
            UserTaskAction::Claim(ClaimTask {
                precondition: self.precondition(&options),
            }),
            options,
        )
        .await
    }

    pub async fn mark_read(&self, options: MutationOptions) -> Result<TaskActionReceipt, Error> {
        self.act(
            UserTaskAction::MarkRead(MarkTaskRead {
                precondition: self.precondition(&options),
            }),
            options,
        )
        .await
    }

    /// Saves an already encoded JSON draft.
    pub async fn save_draft_json(
        &self,
        draft: Value,
        options: MutationOptions,
    ) -> Result<TaskActionReceipt, Error> {
        self.act(
            UserTaskAction::SaveDraft(SaveTaskDraft {
                precondition: self.precondition(&options),
                draft,
            }),
            options,
        )
        .await
    }

    /// Serializes and saves a typed draft before performing any I/O.
    pub async fn save_draft<T: Serialize + ?Sized>(
        &self,
        draft: &T,
        options: MutationOptions,
    ) -> Result<TaskActionReceipt, Error> {
        let draft = encode_payload("draft", draft, options.request().request_id())?;
        self.save_draft_json(draft, options).await
    }

    /// Submits an already encoded JSON result.
    pub async fn submit_json(
        &self,
        submission: Value,
        options: MutationOptions,
    ) -> Result<TaskActionReceipt, Error> {
        self.act(
            UserTaskAction::Submit(SubmitTask {
                precondition: self.precondition(&options),
                submission,
            }),
            options,
        )
        .await
    }

    /// Serializes and submits a typed result before performing any I/O.
    pub async fn submit<T: Serialize + ?Sized>(
        &self,
        submission: &T,
        options: MutationOptions,
    ) -> Result<TaskActionReceipt, Error> {
        let submission = encode_payload("submission", submission, options.request().request_id())?;
        self.submit_json(submission, options).await
    }

    fn precondition(&self, options: &MutationOptions) -> TaskActionPrecondition {
        TaskActionPrecondition {
            task_id: self.id(),
            expected_revision: self.revision(),
            idempotency_key: options.idempotency_key().clone(),
        }
    }

    async fn act(
        &self,
        action: UserTaskAction,
        options: MutationOptions,
    ) -> Result<TaskActionReceipt, Error> {
        let receipt = self.inner.binding.act_on_user_task(action, options).await?;
        if receipt.task_id != self.id() {
            return Err(Error::contract(
                "User Task action receipt identity does not match its handle",
                Some(receipt.mutation.request_id.clone()),
            ));
        }
        Ok(receipt)
    }
}

/// Local typed decoding helpers while retaining access to the original JSON.
pub trait UserTaskPayloadExt {
    fn display_json(&self) -> Option<&Value>;
    fn draft_json(&self) -> Option<&Value>;
    fn submission_json(&self) -> Option<&Value>;
    fn result_schema_json(&self) -> Option<&Value>;

    fn decode_display<T: DeserializeOwned>(&self) -> Result<Option<T>, Error> {
        decode_payload("display", self.display_json())
    }

    fn decode_draft<T: DeserializeOwned>(&self) -> Result<Option<T>, Error> {
        decode_payload("draft", self.draft_json())
    }

    fn decode_submission<T: DeserializeOwned>(&self) -> Result<Option<T>, Error> {
        decode_payload("submission", self.submission_json())
    }

    fn decode_result_schema<T: DeserializeOwned>(&self) -> Result<Option<T>, Error> {
        decode_payload("result_schema", self.result_schema_json())
    }
}

impl UserTaskPayloadExt for UserTask {
    fn display_json(&self) -> Option<&Value> {
        self.payloads.display.as_ref()
    }

    fn draft_json(&self) -> Option<&Value> {
        self.payloads.draft.as_ref()
    }

    fn submission_json(&self) -> Option<&Value> {
        self.payloads.submission.as_ref()
    }

    fn result_schema_json(&self) -> Option<&Value> {
        self.payloads.result_schema.as_ref()
    }
}

impl UserTaskPayloadExt for UserTaskHandle {
    fn display_json(&self) -> Option<&Value> {
        self.task.display_json()
    }

    fn draft_json(&self) -> Option<&Value> {
        self.task.draft_json()
    }

    fn submission_json(&self) -> Option<&Value> {
        self.task.submission_json()
    }

    fn result_schema_json(&self) -> Option<&Value> {
        self.task.result_schema_json()
    }
}

fn validate_application_query(query: &ApplicationTaskQuery) -> Result<(), Error> {
    validate_page(query.pagination)?;
    validate_time_range(&query.created)?;
    if query
        .workflow_id
        .is_some_and(|workflow_id| workflow_id.0 == 0)
    {
        return Err(Error::configuration(
            "query.workflow_id",
            "value must be positive",
        ));
    }
    validate_optional_filter("query.participant_user_id", &query.participant_user_id)?;
    validate_optional_filter("query.claimed_by", &query.claimed_by)
}

fn validate_current_user_query(query: &CurrentUserTaskQuery) -> Result<(), Error> {
    validate_page(query.pagination)?;
    validate_time_range(&query.created)
}

fn validate_page(page: PageRequest) -> Result<(), Error> {
    PageRequest::new(page.page, page.page_size)
        .map(|_| ())
        .map_err(|error| Error::configuration("query.pagination", error.to_string()))
}

fn validate_time_range(range: &UserTaskTimeRange) -> Result<(), Error> {
    if matches!(
        (range.created_from, range.created_before),
        (Some(from), Some(before)) if from >= before
    ) {
        return Err(Error::configuration(
            "query.created",
            "created_from must be earlier than created_before",
        ));
    }
    Ok(())
}

fn validate_optional_filter(field: &'static str, value: &Option<String>) -> Result<(), Error> {
    if value
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(Error::configuration(field, "value must not be empty"));
    }
    Ok(())
}

fn validate_task_identity(requested: UserTaskId, task: &UserTask) -> Result<(), Error> {
    if task.summary.id != requested {
        return Err(Error::contract(
            "User Task detail identity does not match the requested task",
            None,
        ));
    }
    Ok(())
}

fn encode_payload<T: Serialize + ?Sized>(
    field: &'static str,
    value: &T,
    request_id: &kish_lingshu_runtime_contract::RequestId,
) -> Result<Value, Error> {
    serde_json::to_value(value).map_err(|error| {
        Error::Protocol(ProtocolError {
            direction: ProtocolDirection::EncodeRequest,
            message: format!("cannot encode User Task {field}: {error}"),
            request_id: Some(request_id.clone()),
        })
    })
}

fn decode_payload<T: DeserializeOwned>(
    field: &'static str,
    value: Option<&Value>,
) -> Result<Option<T>, Error> {
    value
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| {
            Error::Protocol(ProtocolError {
                direction: ProtocolDirection::DecodeResponse,
                message: format!("cannot decode User Task {field}: {error}"),
                request_id: None,
            })
        })
}
