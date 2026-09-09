//! Private adapter implementations shared by public domain handles.
use kish_lingshu_runtime_contract::WorkspaceApprovalMode;

use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use futures::Stream;
use kish_lingshu_event_dispatch_contract::{EventId, EventRecord, PublishEvent, PublishReceipt};
use kish_lingshu_runtime_contract::{
    ApplicationTaskQuery, ApplicationTaskSummary, CommandAck, CurrentUserTaskQuery, EventCursor,
    Page, RetryUserTaskCompletion, RuntimeBindings, SessionHandle, SessionId, SessionSnapshot,
    TaskActionReceipt, UserTask, UserTaskAction, UserTaskCompletionRetryReceipt, UserTaskId,
    UserTaskSummary, WorkflowEvent, WorkflowId, WorkflowInput, WorkflowInstanceId,
    WorkflowSnapshot,
};

use kish_lingshu_runtime_contract::{AssetRef, AssetUpload};

use crate::workflow::{
    AgentTaskPlan, ConversationScope, SessionMessage, SessionRecord, WorkflowRecord,
};
use crate::AuthenticatedUser;
use crate::{Error, MutationOptions, RequestOptions};

#[cfg(feature = "http-client")]
mod http;
#[cfg(all(test, feature = "http-client", feature = "event-consumer-http"))]
mod http_conformance;
#[cfg(feature = "http-client")]
mod retry;
mod runtime;
#[cfg(feature = "http-client")]
mod sse;

#[cfg(feature = "http-client")]
pub(crate) use http::HttpBinding;
pub(crate) use runtime::ProductRuntimeBinding;

pub(crate) type WorkflowEventStream =
    Pin<Box<dyn Stream<Item = Result<WorkflowEvent, Error>> + Send + 'static>>;

/// Transport-neutral operations consumed by every public capability handle.
///
/// The trait is deliberately private: callers select capabilities and a
/// principal, while construction selects HTTP or an in-process runtime.
#[async_trait]
pub(crate) trait ProductBinding: Send + Sync {
    fn name(&self) -> &'static str;

    async fn authenticated_user(&self, options: RequestOptions)
        -> Result<AuthenticatedUser, Error>;

    async fn allocate_workspace_id(&self, options: RequestOptions) -> Result<u64, Error>;

    async fn update_workspace_approval_mode(
        &self,
        application_id: &str,
        workspace_id: u64,
        approval_mode: WorkspaceApprovalMode,
        options: RequestOptions,
    ) -> Result<WorkspaceApprovalMode, Error>;

    async fn list_workflow_records(
        &self,
        limit: u64,
        options: RequestOptions,
    ) -> Result<Vec<WorkflowRecord>, Error>;

    async fn get_workflow_record(
        &self,
        workflow_id: u64,
        options: RequestOptions,
    ) -> Result<Option<WorkflowRecord>, Error>;

    async fn list_session_records(
        &self,
        workflow_id: Option<u64>,
        limit: u64,
        name: Option<&str>,
        options: RequestOptions,
    ) -> Result<Vec<SessionRecord>, Error>;

    async fn create_session_record(
        &self,
        workflow_id: u64,
        name: String,
        options: RequestOptions,
    ) -> Result<SessionRecord, Error>;

    async fn rename_session_record(
        &self,
        session_id: u64,
        description: String,
        options: RequestOptions,
    ) -> Result<SessionRecord, Error>;

    async fn delete_session_record(
        &self,
        session_id: u64,
        options: RequestOptions,
    ) -> Result<(), Error>;

    async fn load_session_message_records(
        &self,
        session_id: u64,
        before_id: Option<u64>,
        limit: u64,
        conversation_scope_id: Option<u64>,
        options: RequestOptions,
    ) -> Result<Vec<SessionMessage>, Error>;

    async fn list_conversation_scope_records(
        &self,
        session_id: u64,
        options: RequestOptions,
    ) -> Result<Vec<ConversationScope>, Error>;

    async fn load_agent_task_plan_records(
        &self,
        workflow_instance_id: u64,
        options: RequestOptions,
    ) -> Result<Vec<AgentTaskPlan>, Error>;

    async fn download_image(
        &self,
        source_url: &str,
        options: RequestOptions,
    ) -> Result<Vec<u8>, Error>;

    async fn upload_asset(
        &self,
        upload: AssetUpload,
        options: RequestOptions,
    ) -> Result<AssetRef, Error>;

    async fn publish_event(
        &self,
        event: PublishEvent,
        options: MutationOptions,
    ) -> Result<PublishReceipt, Error>;

    async fn get_event(
        &self,
        event_id: EventId,
        options: RequestOptions,
    ) -> Result<Option<EventRecord>, Error>;

    async fn start_workflow(
        &self,
        workflow_id: WorkflowId,
        session_id: Option<SessionId>,
        input: WorkflowInput,
        bindings: RuntimeBindings,
        options: MutationOptions,
    ) -> Result<kish_lingshu_runtime_contract::WorkflowRunHandle, Error>;

    async fn create_session(
        &self,
        workflow_id: WorkflowId,
        name: String,
        session_type: String,
        get_or_create: bool,
        options: RequestOptions,
    ) -> Result<SessionHandle, Error>;

    async fn load_session(
        &self,
        session_id: SessionId,
        options: RequestOptions,
    ) -> Result<SessionSnapshot, Error>;

    async fn signal_workflow(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        signal: kish_lingshu_runtime_contract::WorkflowSignal,
        options: MutationOptions,
    ) -> Result<CommandAck, Error>;

    async fn terminate_workflow(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        reason: Option<String>,
        wait_timeout_ms: Option<u64>,
        options: MutationOptions,
    ) -> Result<CommandAck, Error>;

    async fn workflow_snapshot(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        options: RequestOptions,
    ) -> Result<WorkflowSnapshot, Error>;

    async fn subscribe_workflow(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        after: Option<EventCursor>,
        options: RequestOptions,
    ) -> Result<WorkflowEventStream, Error>;

    async fn list_application_tasks(
        &self,
        query: ApplicationTaskQuery,
        options: RequestOptions,
    ) -> Result<Page<UserTaskSummary>, Error>;

    async fn summarize_application_tasks(
        &self,
        query: ApplicationTaskQuery,
        options: RequestOptions,
    ) -> Result<ApplicationTaskSummary, Error>;

    async fn get_application_task(
        &self,
        task_id: UserTaskId,
        options: RequestOptions,
    ) -> Result<UserTask, Error>;

    async fn retry_user_task_completion(
        &self,
        request: RetryUserTaskCompletion,
        options: RequestOptions,
    ) -> Result<UserTaskCompletionRetryReceipt, Error>;

    async fn list_current_user_tasks(
        &self,
        query: CurrentUserTaskQuery,
        options: RequestOptions,
    ) -> Result<Page<UserTaskSummary>, Error>;

    async fn get_current_user_task(
        &self,
        task_id: UserTaskId,
        options: RequestOptions,
    ) -> Result<UserTask, Error>;

    async fn act_on_user_task(
        &self,
        action: UserTaskAction,
        options: MutationOptions,
    ) -> Result<TaskActionReceipt, Error>;
}

pub(crate) type BindingRef = Arc<dyn ProductBinding>;
