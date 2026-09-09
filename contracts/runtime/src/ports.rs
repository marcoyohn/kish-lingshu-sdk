use async_trait::async_trait;
use kish_lingshu_event_dispatch_contract::{EventId, EventRecord, PublishEvent, PublishReceipt};

use crate::{
    ApplicationResult, ApplicationTaskQuery, ApplicationTaskSummary, CurrentUserTaskQuery,
    IdempotencyKey, Page, RequestContext, RetryUserTaskCompletion, TaskActionReceipt, UserTask,
    UserTaskAction, UserTaskCompletionRetryReceipt, UserTaskId, UserTaskSummary,
};

/// Application-scoped outbound Event publication and receipt lookup.
#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(
        &self,
        context: RequestContext,
        event: PublishEvent,
        idempotency_key: IdempotencyKey,
    ) -> ApplicationResult<PublishReceipt>;

    async fn get(
        &self,
        context: RequestContext,
        event_id: EventId,
    ) -> ApplicationResult<Option<EventRecord>>;
}

/// User Task observation and current-participant action boundary.
#[async_trait]
pub trait UserTaskRuntime: Send + Sync {
    async fn list_application(
        &self,
        context: RequestContext,
        query: ApplicationTaskQuery,
    ) -> ApplicationResult<Page<UserTaskSummary>>;

    async fn summarize_application(
        &self,
        context: RequestContext,
        query: ApplicationTaskQuery,
    ) -> ApplicationResult<ApplicationTaskSummary>;

    async fn get_application(
        &self,
        context: RequestContext,
        task_id: UserTaskId,
    ) -> ApplicationResult<UserTask>;

    async fn retry_completion(
        &self,
        context: RequestContext,
        request: RetryUserTaskCompletion,
    ) -> ApplicationResult<UserTaskCompletionRetryReceipt>;

    async fn list_mine(
        &self,
        context: RequestContext,
        query: CurrentUserTaskQuery,
    ) -> ApplicationResult<Page<UserTaskSummary>>;

    async fn get_mine(
        &self,
        context: RequestContext,
        task_id: UserTaskId,
    ) -> ApplicationResult<UserTask>;

    async fn act(
        &self,
        context: RequestContext,
        action: UserTaskAction,
    ) -> ApplicationResult<TaskActionReceipt>;
}
