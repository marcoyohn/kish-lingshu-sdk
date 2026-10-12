use std::sync::Arc;

use super::{
    assert_event_publisher_contract, assert_workflow_runtime_contract, ContractEventPublisher,
    ContractWorkflowRuntime, EventPublisherContractReport, WorkflowRuntimeContractReport,
};
use crate::{ProductRuntimeFacade, RequestContext};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProductRuntimeContractReport {
    pub events: EventPublisherContractReport,
    pub workflow: WorkflowRuntimeContractReport,
}

/// Run all Product Runtime capability contracts through one composed facade.
pub async fn assert_product_runtime_contract(
    facade: &ProductRuntimeFacade,
    service_context: RequestContext,
    user_context: RequestContext,
) -> ProductRuntimeContractReport {
    let events =
        assert_event_publisher_contract(facade.event_publisher().clone(), service_context.clone())
            .await;
    let workflow =
        assert_workflow_runtime_contract(facade.workflow().clone(), user_context.clone()).await;
    ProductRuntimeContractReport { events, workflow }
}

/// Fully in-process Product Runtime fixture with inspectable contract fakes.
pub struct ContractProductRuntimeFixture {
    pub facade: ProductRuntimeFacade,
    pub workflow: Arc<ContractWorkflowRuntime>,
    pub event_publisher: Arc<ContractEventPublisher>,
}

impl Default for ContractProductRuntimeFixture {
    fn default() -> Self {
        let workflow = Arc::new(ContractWorkflowRuntime::default());
        let event_publisher = Arc::new(ContractEventPublisher::default());
        let facade = ProductRuntimeFacade::new(workflow.clone(), workflow.clone())
            .with_event_publisher(event_publisher.clone());
        Self {
            facade,
            workflow,
            event_publisher,
        }
    }
}
