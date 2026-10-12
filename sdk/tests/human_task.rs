#![cfg(feature = "service-manifest")]
use kish_lingshu_sdk::{
    human_task::{HumanTaskContext, HumanTaskResult},
    lingshu_service,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
#[derive(Deserialize, JsonSchema)]
struct Input {
    business_id: String,
}
#[derive(Serialize, JsonSchema)]
struct Output {
    external_ref: String,
}
struct Provider;
#[lingshu_service(key = "human")]
impl Provider {
    #[human_task_handler(task_type="approval.v1",action="create",node_kinds=["todo","draft"],operation="create",version="v1",modes=["sync","async"],idempotent=true)]
    async fn create(&self, context: HumanTaskContext, input: Input) -> HumanTaskResult<Output> {
        assert_eq!(
            context.action(),
            kish_lingshu_sdk::human_task::HumanTaskAction::Create
        );
        Ok(Output {
            external_ref: input.business_id,
        })
    }
}
#[test]
fn typed_lifecycle_declaration_binds_native_handler_without_legacy_identity() {
    use kish_lingshu_sdk::services::*;
    let definition = Provider::lingshu_service_definition();
    let operation = &definition.operations[0];
    assert!(operation.human_task.is_some());
    operation.validate().unwrap();
    let mut builder = ServiceRegistryBuilder::new(ServiceManifest {
        contract_version: 1,
        application_id: "app".into(),
        services: vec![definition],
    })
    .unwrap();
    Arc::new(Provider)
        .bind_lingshu_services(&mut builder)
        .unwrap();
    assert!(builder.build().is_ok());
}
