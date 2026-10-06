#![cfg(feature = "service-manifest")]

use std::sync::Arc;

use kish_lingshu_sdk::{
    lingshu_service,
    user_task::completion::{CompletionContext, CompletionResult},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JsonSchema)]
struct Submission;

#[derive(JsonSchema, Serialize)]
struct Output;

struct First;

#[lingshu_service(key = "reviews")]
impl First {
    #[user_task_completion_handler(task_type = "duplicate.approval.v1", operation="complete",version="v1",modes=["sync"],idempotent=true)]
    async fn complete(
        &self,
        _context: CompletionContext,
        _submission: Submission,
    ) -> CompletionResult<Output> {
        Ok(Output)
    }
}

struct Second;

#[lingshu_service(key = "reviews")]
impl Second {
    #[user_task_completion_handler(task_type = "duplicate.approval.v1", operation="complete",version="v1",modes=["sync"],idempotent=true)]
    async fn complete(
        &self,
        _context: CompletionContext,
        _submission: Submission,
    ) -> CompletionResult<Output> {
        Ok(Output)
    }
}

#[test]
fn duplicate_operations_are_rejected_before_readiness_and_export() {
    use kish_lingshu_sdk::services::*;
    let mut builder = ServiceRegistryBuilder::new(ServiceManifest {
        contract_version: 1,
        application_id: "app".into(),
        services: vec![First::lingshu_service_definition()],
    })
    .unwrap();
    Arc::new(First).bind_lingshu_services(&mut builder).unwrap();
    assert!(Arc::new(Second)
        .bind_lingshu_services(&mut builder)
        .is_err());
    assert!(export_manifest("app").is_err());
}
