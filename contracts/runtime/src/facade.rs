use std::sync::Arc;

use crate::{
    ApplicationProblem, ApplicationResult, AssetRef, AssetStore, AssetUpload, ConversationService,
    EventPublisher, IdempotencyKey, RequestContext, RouteWorkflowEventRequest,
    RouteWorkflowEventResponse, RuntimeError, RuntimeErrorCode, RuntimeResult, SessionDirectory,
    SessionId, SessionQuery, SessionSnapshot, SessionUpdate, WorkflowContext, WorkflowDirectory,
    WorkflowEventRouteService, WorkflowId, WorkflowQuery, WorkflowRouteCallbackToken,
    WorkflowRuntime, WorkflowSummary,
};
use async_trait::async_trait;
use kish_lingshu_event_dispatch_contract::{EventId, EventRecord, PublishEvent, PublishReceipt};

#[derive(Clone)]
pub struct ProductRuntimeFacade {
    conversation: Arc<dyn ConversationService>,
    workflow: Arc<dyn WorkflowRuntime>,
    sessions: Arc<dyn SessionDirectory>,
    workflows: Arc<dyn WorkflowDirectory>,
    assets: Arc<dyn AssetStore>,
    workflow_event_routes: Arc<dyn WorkflowEventRouteService>,
    event_publisher: Arc<dyn EventPublisher>,
    human_tasks: Arc<dyn crate::WorkflowHumanTaskRuntime>,
}

impl ProductRuntimeFacade {
    pub fn new(
        conversation: Arc<dyn ConversationService>,
        workflow: Arc<dyn WorkflowRuntime>,
    ) -> Self {
        Self {
            conversation,
            workflow,
            sessions: Arc::new(UnsupportedSessionDirectory),
            workflows: Arc::new(UnsupportedWorkflowDirectory),
            assets: Arc::new(UnsupportedAssetStore),
            workflow_event_routes: Arc::new(UnsupportedWorkflowEventRouteService),
            event_publisher: Arc::new(UnsupportedEventPublisher),
            human_tasks: Arc::new(UnsupportedWorkflowHumanTasks),
        }
    }

    pub fn conversation(&self) -> &Arc<dyn ConversationService> {
        &self.conversation
    }

    pub fn workflow(&self) -> &Arc<dyn WorkflowRuntime> {
        &self.workflow
    }

    pub fn sessions(&self) -> &Arc<dyn SessionDirectory> {
        &self.sessions
    }
    pub fn workflows(&self) -> &Arc<dyn WorkflowDirectory> {
        &self.workflows
    }
    pub fn assets(&self) -> &Arc<dyn AssetStore> {
        &self.assets
    }

    pub fn workflow_event_routes(&self) -> &Arc<dyn WorkflowEventRouteService> {
        &self.workflow_event_routes
    }

    pub fn event_publisher(&self) -> &Arc<dyn EventPublisher> {
        &self.event_publisher
    }

    pub fn human_tasks(&self) -> &Arc<dyn crate::WorkflowHumanTaskRuntime> {
        &self.human_tasks
    }

    pub fn with_human_tasks(
        mut self,
        human_tasks: Arc<dyn crate::WorkflowHumanTaskRuntime>,
    ) -> Self {
        self.human_tasks = human_tasks;
        self
    }

    pub fn with_product_services(
        mut self,
        sessions: Arc<dyn SessionDirectory>,
        workflows: Arc<dyn WorkflowDirectory>,
        assets: Arc<dyn AssetStore>,
    ) -> Self {
        self.sessions = sessions;
        self.workflows = workflows;
        self.assets = assets;
        self
    }

    pub fn with_workflow_event_routes(
        mut self,
        workflow_event_routes: Arc<dyn WorkflowEventRouteService>,
    ) -> Self {
        self.workflow_event_routes = workflow_event_routes;
        self
    }

    pub fn with_event_publisher(mut self, event_publisher: Arc<dyn EventPublisher>) -> Self {
        self.event_publisher = event_publisher;
        self
    }
}

fn unsupported<T>() -> RuntimeResult<T> {
    Err(RuntimeError::new(
        RuntimeErrorCode::Unsupported,
        "runtime capability is not available",
    ))
}

struct UnsupportedSessionDirectory;

struct UnsupportedWorkflowHumanTasks;
#[async_trait]
impl crate::WorkflowHumanTaskRuntime for UnsupportedWorkflowHumanTasks {
    async fn list(
        &self,
        _: RequestContext,
        _: crate::WorkflowInstanceId,
    ) -> ApplicationResult<Vec<crate::HumanTaskSnapshot>> {
        Err(unsupported_human_task())
    }
    async fn get(
        &self,
        _: RequestContext,
        _: crate::WorkflowInstanceId,
        _: String,
    ) -> ApplicationResult<crate::HumanTaskSnapshot> {
        Err(unsupported_human_task())
    }
    async fn command(
        &self,
        _: RequestContext,
        _: crate::WorkflowInstanceId,
        _: String,
        _: crate::HumanTaskCommand,
    ) -> ApplicationResult<crate::HumanTaskCommandReceipt> {
        Err(unsupported_human_task())
    }
    async fn command_status(
        &self,
        _: RequestContext,
        _: crate::WorkflowInstanceId,
        _: String,
        _: String,
    ) -> ApplicationResult<crate::HumanTaskCommandStatus> {
        Err(unsupported_human_task())
    }
    async fn retry(
        &self,
        _: RequestContext,
        _: crate::WorkflowInstanceId,
        _: String,
        _: String,
    ) -> ApplicationResult<crate::HumanTaskCommandReceipt> {
        Err(unsupported_human_task())
    }
}
fn unsupported_human_task() -> crate::ApplicationProblem {
    crate::ApplicationProblem::new(
        crate::ProblemCode::known(crate::UNSUPPORTED_PROBLEM),
        "workflow human tasks are not available",
        "unsupported",
    )
}
#[async_trait]
impl SessionDirectory for UnsupportedSessionDirectory {
    async fn list(
        &self,
        _: RequestContext,
        _: SessionQuery,
    ) -> RuntimeResult<Vec<SessionSnapshot>> {
        unsupported()
    }
    async fn get_or_create(
        &self,
        _: WorkflowContext,
        _: String,
        _: String,
    ) -> RuntimeResult<crate::SessionHandle> {
        unsupported()
    }
    async fn update(
        &self,
        _: RequestContext,
        _: SessionId,
        _: SessionUpdate,
    ) -> RuntimeResult<SessionSnapshot> {
        unsupported()
    }
    async fn delete(&self, _: RequestContext, _: SessionId) -> RuntimeResult<()> {
        unsupported()
    }
}

struct UnsupportedWorkflowDirectory;
#[async_trait]
impl WorkflowDirectory for UnsupportedWorkflowDirectory {
    async fn list(
        &self,
        _: RequestContext,
        _: WorkflowQuery,
    ) -> RuntimeResult<Vec<WorkflowSummary>> {
        unsupported()
    }
    async fn get(
        &self,
        _: RequestContext,
        _: WorkflowId,
    ) -> RuntimeResult<Option<WorkflowSummary>> {
        unsupported()
    }
}

struct UnsupportedWorkflowEventRouteService;
#[async_trait]
impl WorkflowEventRouteService for UnsupportedWorkflowEventRouteService {
    async fn route(
        &self,
        _: &str,
        _: &str,
        _: RouteWorkflowEventRequest,
        _: Option<WorkflowRouteCallbackToken>,
    ) -> RuntimeResult<RouteWorkflowEventResponse> {
        unsupported()
    }
}

struct UnsupportedAssetStore;
#[async_trait]
impl AssetStore for UnsupportedAssetStore {
    async fn upload(&self, _: RequestContext, _: AssetUpload) -> RuntimeResult<AssetRef> {
        unsupported()
    }
    async fn download(&self, _: RequestContext, _: AssetRef) -> RuntimeResult<Vec<u8>> {
        unsupported()
    }
}

fn unsupported_application<T>(context: &RequestContext) -> ApplicationResult<T> {
    Err(ApplicationProblem::unsupported(
        "runtime capability is not available",
        context.request_id().clone(),
    ))
}

struct UnsupportedEventPublisher;
#[async_trait]
impl EventPublisher for UnsupportedEventPublisher {
    async fn publish(
        &self,
        context: RequestContext,
        _: PublishEvent,
        _: IdempotencyKey,
    ) -> ApplicationResult<PublishReceipt> {
        unsupported_application(&context)
    }

    async fn get(
        &self,
        context: RequestContext,
        _: EventId,
    ) -> ApplicationResult<Option<EventRecord>> {
        unsupported_application(&context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{test_support::ContractWorkflowRuntime, InvocationSource, UNSUPPORTED_PROBLEM};

    fn facade() -> ProductRuntimeFacade {
        let runtime = Arc::new(ContractWorkflowRuntime::default());
        ProductRuntimeFacade::new(runtime.clone(), runtime)
    }

    fn context() -> RequestContext {
        crate::test_support::contract_context(InvocationSource::EmbeddedSdk)
    }

    #[tokio::test]
    async fn optional_event_ports_return_the_common_unsupported_problem() {
        let facade = facade();
        let event_error = facade
            .event_publisher()
            .get(context(), EventId::new(1).unwrap())
            .await
            .unwrap_err();

        for error in [event_error] {
            assert_eq!(error.code.as_str(), UNSUPPORTED_PROBLEM);
            assert_eq!(error.request_id.as_ref(), "runtime-contract-request");
            assert!(!error.retryable);
        }
    }
}
