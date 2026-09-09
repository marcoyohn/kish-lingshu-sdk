//! In-process Product Runtime facade binding implementation.
use kish_lingshu_runtime_contract::WorkspaceApprovalMode;

use async_trait::async_trait;
use futures::StreamExt;
use kish_lingshu_event_dispatch_contract::{EventId, EventRecord, PublishEvent, PublishReceipt};
use kish_lingshu_runtime_contract::{
    ApplicationProblem, ApplicationTaskQuery, ApplicationTaskSummary, CommandAck,
    CreateSessionRequest, CurrentUserTaskQuery, LoadSessionRequest, Page, ProblemCode,
    ProductRuntimeFacade, RequestContext, RetryUserTaskCompletion, RuntimeBindings, RuntimeError,
    RuntimeErrorCode, SessionHandle, SessionId, SessionSnapshot, SignalWorkflowRequest,
    StartWorkflowRequest, SubscribeWorkflowRequest, TaskActionReceipt, TerminateWorkflowRequest,
    TrustedContextFactory, UserTask, UserTaskAction, UserTaskCompletionRetryReceipt, UserTaskId,
    UserTaskSummary, WorkflowContext, WorkflowId, WorkflowInput, WorkflowInstanceId,
    WorkflowRunHandle, WorkflowSnapshot, WorkflowSnapshotRequest,
};

use kish_lingshu_runtime_contract::{AssetRef, AssetUpload};

use super::{ProductBinding, WorkflowEventStream};

use crate::workflow::{
    AgentTaskPlan, ConversationScope, SessionMessage, SessionRecord, WorkflowRecord,
};
use crate::{ApplicationFailure, AuthenticatedUser, Error, MutationOptions, RequestOptions};

pub(crate) struct ProductRuntimeBinding {
    facade: ProductRuntimeFacade,
    context_factory: TrustedContextFactory,
}

impl ProductRuntimeBinding {
    pub(crate) fn new(
        facade: ProductRuntimeFacade,
        context_factory: TrustedContextFactory,
    ) -> Self {
        Self {
            facade,
            context_factory,
        }
    }

    fn request_context(&self, options: &RequestOptions) -> Result<RequestContext, Error> {
        self.request_context_with_idempotency(options, None)
    }

    fn request_context_with_idempotency(
        &self,
        options: &RequestOptions,
        idempotency_key: Option<&kish_lingshu_runtime_contract::IdempotencyKey>,
    ) -> Result<RequestContext, Error> {
        let mut builder = RequestContext::builder(
            self.context_factory.actor_id(),
            self.context_factory.principal(),
            self.context_factory.source().clone(),
            options.request_id().clone(),
            options.correlation_id().clone(),
        );
        if let Some(application_id) = self.context_factory.application_id() {
            builder = builder.application_id(application_id);
        }
        if let Some(trace) = options.trace() {
            builder = builder.trace(trace.clone());
        }
        if let Some(deadline) = options.deadline() {
            builder = builder.deadline(deadline);
        }
        if let Some(idempotency_key) = idempotency_key {
            builder = builder.idempotency_key(idempotency_key.clone());
        }
        builder
            .build()
            .map_err(|error| Error::contract(error.message, Some(options.request_id().clone())))
    }

    fn workflow_context(
        &self,
        workflow_id: WorkflowId,
        options: &MutationOptions,
    ) -> Result<WorkflowContext, Error> {
        self.request_context_with_idempotency(options.request(), Some(options.idempotency_key()))
            .map(|context| WorkflowContext::new(context, workflow_id))
    }
}

#[async_trait]
impl ProductBinding for ProductRuntimeBinding {
    fn name(&self) -> &'static str {
        "product-runtime"
    }

    async fn authenticated_user(
        &self,
        _options: RequestOptions,
    ) -> Result<AuthenticatedUser, Error> {
        Ok(AuthenticatedUser {
            user_type: "user".to_string(),
            user_id: self.context_factory.actor_id().to_string(),
            user_name: self.context_factory.actor_id().to_string(),
            nick_name: self.context_factory.actor_id().to_string(),
            real_name: self.context_factory.actor_id().to_string(),
            application_id: self
                .context_factory
                .application_id()
                .unwrap_or_default()
                .to_string(),
            is_admin: false,
            runtime_authority_id: None,
        })
    }

    async fn allocate_workspace_id(&self, options: RequestOptions) -> Result<u64, Error> {
        Err(unsupported_client_operation(&options))
    }

    async fn update_workspace_approval_mode(
        &self,
        _application_id: &str,
        _workspace_id: u64,
        _approval_mode: WorkspaceApprovalMode,
        options: RequestOptions,
    ) -> Result<WorkspaceApprovalMode, Error> {
        Err(unsupported_client_operation(&options))
    }

    async fn list_workflow_records(
        &self,
        _limit: u64,
        options: RequestOptions,
    ) -> Result<Vec<WorkflowRecord>, Error> {
        Err(unsupported_client_operation(&options))
    }

    async fn get_workflow_record(
        &self,
        _workflow_id: u64,
        options: RequestOptions,
    ) -> Result<Option<WorkflowRecord>, Error> {
        Err(unsupported_client_operation(&options))
    }

    async fn list_session_records(
        &self,
        _workflow_id: Option<u64>,
        _limit: u64,
        _name: Option<&str>,
        options: RequestOptions,
    ) -> Result<Vec<SessionRecord>, Error> {
        Err(unsupported_client_operation(&options))
    }

    async fn create_session_record(
        &self,
        _workflow_id: u64,
        _name: String,
        options: RequestOptions,
    ) -> Result<SessionRecord, Error> {
        Err(unsupported_client_operation(&options))
    }

    async fn rename_session_record(
        &self,
        _session_id: u64,
        _description: String,
        options: RequestOptions,
    ) -> Result<SessionRecord, Error> {
        Err(unsupported_client_operation(&options))
    }

    async fn delete_session_record(
        &self,
        _session_id: u64,
        options: RequestOptions,
    ) -> Result<(), Error> {
        Err(unsupported_client_operation(&options))
    }

    async fn load_session_message_records(
        &self,
        _session_id: u64,
        _before_id: Option<u64>,
        _limit: u64,
        _conversation_scope_id: Option<u64>,
        options: RequestOptions,
    ) -> Result<Vec<SessionMessage>, Error> {
        Err(unsupported_client_operation(&options))
    }

    async fn list_conversation_scope_records(
        &self,
        _session_id: u64,
        options: RequestOptions,
    ) -> Result<Vec<ConversationScope>, Error> {
        Err(unsupported_client_operation(&options))
    }

    async fn load_agent_task_plan_records(
        &self,
        _workflow_instance_id: u64,
        options: RequestOptions,
    ) -> Result<Vec<AgentTaskPlan>, Error> {
        Err(unsupported_client_operation(&options))
    }

    async fn download_image(
        &self,
        _source_url: &str,
        options: RequestOptions,
    ) -> Result<Vec<u8>, Error> {
        Err(unsupported_client_operation(&options))
    }

    async fn upload_asset(
        &self,
        upload: AssetUpload,
        options: RequestOptions,
    ) -> Result<AssetRef, Error> {
        let context = self.request_context(&options)?;
        self.facade
            .assets()
            .upload(context, upload)
            .await
            .map_err(|error| runtime_error(error, options.request_id().clone()))
    }

    async fn publish_event(
        &self,
        event: PublishEvent,
        options: MutationOptions,
    ) -> Result<PublishReceipt, Error> {
        let context = self.request_context(options.request())?;
        self.facade
            .event_publisher()
            .publish(context, event, options.idempotency_key().clone())
            .await
            .map_err(application_error)
    }

    async fn get_event(
        &self,
        event_id: EventId,
        options: RequestOptions,
    ) -> Result<Option<EventRecord>, Error> {
        let context = self.request_context(&options)?;
        self.facade
            .event_publisher()
            .get(context, event_id)
            .await
            .map_err(application_error)
    }

    async fn start_workflow(
        &self,
        workflow_id: WorkflowId,
        session_id: Option<SessionId>,
        input: WorkflowInput,
        bindings: RuntimeBindings,
        options: MutationOptions,
    ) -> Result<WorkflowRunHandle, Error> {
        let context = self.workflow_context(workflow_id, &options)?;
        self.facade
            .workflow()
            .start(StartWorkflowRequest {
                context,
                session_id,
                input,
                bindings,
            })
            .await
            .map_err(|error| runtime_error(error, options.request().request_id().clone()))
    }

    async fn create_session(
        &self,
        workflow_id: WorkflowId,
        name: String,
        session_type: String,
        get_or_create: bool,
        options: RequestOptions,
    ) -> Result<SessionHandle, Error> {
        let request_id = options.request_id().clone();
        let context = WorkflowContext::new(self.request_context(&options)?, workflow_id);
        self.facade
            .conversation()
            .create_session(CreateSessionRequest {
                context,
                name,
                description: None,
                session_type,
                get_or_create,
            })
            .await
            .map_err(|error| runtime_error(error, request_id))
    }

    async fn load_session(
        &self,
        session_id: SessionId,
        options: RequestOptions,
    ) -> Result<SessionSnapshot, Error> {
        let request_id = options.request_id().clone();
        let context = self.request_context(&options)?;
        self.facade
            .conversation()
            .load_session(LoadSessionRequest {
                context,
                session_id,
            })
            .await
            .map_err(|error| runtime_error(error, request_id))
    }

    async fn signal_workflow(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        signal: kish_lingshu_runtime_contract::WorkflowSignal,
        options: MutationOptions,
    ) -> Result<CommandAck, Error> {
        let context = self
            .request_context_with_idempotency(options.request(), Some(options.idempotency_key()))?;
        self.facade
            .workflow()
            .signal(SignalWorkflowRequest {
                context,
                workflow_instance_id,
                signal,
            })
            .await
            .map_err(|error| runtime_error(error, options.request().request_id().clone()))
    }

    async fn terminate_workflow(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        reason: Option<String>,
        wait_timeout_ms: Option<u64>,
        options: MutationOptions,
    ) -> Result<CommandAck, Error> {
        let context = self
            .request_context_with_idempotency(options.request(), Some(options.idempotency_key()))?;
        self.facade
            .workflow()
            .terminate(TerminateWorkflowRequest {
                context,
                workflow_instance_id,
                reason,
                wait_timeout_ms,
            })
            .await
            .map_err(|error| runtime_error(error, options.request().request_id().clone()))
    }

    async fn workflow_snapshot(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        options: RequestOptions,
    ) -> Result<WorkflowSnapshot, Error> {
        let context = self.request_context(&options)?;
        self.facade
            .workflow()
            .snapshot(WorkflowSnapshotRequest {
                context,
                workflow_instance_id,
            })
            .await
            .map_err(|error| runtime_error(error, options.request_id().clone()))
    }

    async fn subscribe_workflow(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        after: Option<kish_lingshu_runtime_contract::EventCursor>,
        options: RequestOptions,
    ) -> Result<WorkflowEventStream, Error> {
        let context = self.request_context(&options)?;
        let request_id = options.request_id().clone();
        let stream = self
            .facade
            .workflow()
            .subscribe(SubscribeWorkflowRequest {
                context,
                workflow_instance_id,
                after,
            })
            .await
            .map_err(|error| runtime_error(error, request_id.clone()))?;
        Ok(Box::pin(stream.map(move |item| {
            item.map_err(|error| runtime_error(error, request_id.clone()))
        })))
    }

    async fn list_application_tasks(
        &self,
        query: ApplicationTaskQuery,
        options: RequestOptions,
    ) -> Result<Page<UserTaskSummary>, Error> {
        let context = self.request_context(&options)?;
        self.facade
            .user_tasks()
            .list_application(context, query)
            .await
            .map_err(application_error)
    }

    async fn summarize_application_tasks(
        &self,
        query: ApplicationTaskQuery,
        options: RequestOptions,
    ) -> Result<ApplicationTaskSummary, Error> {
        let context = self.request_context(&options)?;
        self.facade
            .user_tasks()
            .summarize_application(context, query)
            .await
            .map_err(application_error)
    }

    async fn get_application_task(
        &self,
        task_id: UserTaskId,
        options: RequestOptions,
    ) -> Result<UserTask, Error> {
        let context = self.request_context(&options)?;
        self.facade
            .user_tasks()
            .get_application(context, task_id)
            .await
            .map_err(application_error)
    }

    async fn retry_user_task_completion(
        &self,
        request: RetryUserTaskCompletion,
        options: RequestOptions,
    ) -> Result<UserTaskCompletionRetryReceipt, Error> {
        let context = self.request_context(&options)?;
        self.facade
            .user_tasks()
            .retry_completion(context, request)
            .await
            .map_err(application_error)
    }

    async fn list_current_user_tasks(
        &self,
        query: CurrentUserTaskQuery,
        options: RequestOptions,
    ) -> Result<Page<UserTaskSummary>, Error> {
        let context = self.request_context(&options)?;
        self.facade
            .user_tasks()
            .list_mine(context, query)
            .await
            .map_err(application_error)
    }

    async fn get_current_user_task(
        &self,
        task_id: UserTaskId,
        options: RequestOptions,
    ) -> Result<UserTask, Error> {
        let context = self.request_context(&options)?;
        self.facade
            .user_tasks()
            .get_mine(context, task_id)
            .await
            .map_err(application_error)
    }

    async fn act_on_user_task(
        &self,
        action: UserTaskAction,
        options: MutationOptions,
    ) -> Result<TaskActionReceipt, Error> {
        if &action.precondition().idempotency_key != options.idempotency_key() {
            return Err(Error::contract(
                "User Task action idempotency key does not match its mutation options",
                Some(options.request().request_id().clone()),
            ));
        }
        let context = self
            .request_context_with_idempotency(options.request(), Some(options.idempotency_key()))?;
        self.facade
            .user_tasks()
            .act(context, action)
            .await
            .map_err(application_error)
    }
}

fn application_error(problem: ApplicationProblem) -> Error {
    Error::Application(ApplicationFailure::new(problem))
}

fn unsupported_client_operation(options: &RequestOptions) -> Error {
    application_error(ApplicationProblem::unsupported(
        "operation is available only through the authenticated HTTP client binding",
        options.request_id().clone(),
    ))
}

pub(super) fn runtime_error(
    error: RuntimeError,
    request_id: kish_lingshu_runtime_contract::RequestId,
) -> Error {
    let code = match error.code {
        RuntimeErrorCode::InvalidRequest => "invalid_request",
        RuntimeErrorCode::Unauthorized => "unauthorized",
        RuntimeErrorCode::Forbidden => "forbidden",
        RuntimeErrorCode::NotFound => "not_found",
        RuntimeErrorCode::Conflict => "conflict",
        RuntimeErrorCode::StaleSuspension => "stale_suspension",
        RuntimeErrorCode::ReplayUnavailable => "replay_unavailable",
        RuntimeErrorCode::Unsupported => "unsupported",
        RuntimeErrorCode::Connectivity => "connectivity",
        RuntimeErrorCode::Unavailable => "unavailable",
        RuntimeErrorCode::Internal => "internal",
    };
    let mut problem = ApplicationProblem::new(ProblemCode::known(code), error.message, request_id)
        .with_retryable(error.retryable);
    if let Some(details) = error.details {
        problem = problem.with_details(details);
    }
    application_error(problem)
}
