#![cfg(feature = "user-task-completion")]

use std::sync::Arc;

use kish_lingshu_sdk::{
    user_task::completion::{
        CompletionContext, CompletionRegistry, CompletionRegistryError, CompletionResult,
        CompletionSourceCatalog,
    },
    user_task_handlers,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JsonSchema)]
struct Submission;

#[derive(JsonSchema, Serialize)]
struct Output;

struct First;

#[user_task_handlers]
impl First {
    #[completion_handler(task_type = "duplicate.approval.v1")]
    async fn complete(
        &self,
        _context: CompletionContext,
        _submission: Submission,
    ) -> CompletionResult<Output> {
        Ok(Output)
    }
}

struct Second;

#[user_task_handlers]
impl Second {
    #[completion_handler(task_type = "duplicate.approval.v1")]
    async fn complete(
        &self,
        _context: CompletionContext,
        _submission: Submission,
    ) -> CompletionResult<Output> {
        Ok(Output)
    }
}

#[test]
fn duplicate_task_types_are_rejected_at_registry_and_source_collection() {
    let mut builder = CompletionRegistry::builder("approval-app").unwrap();
    builder.bind(Arc::new(First)).unwrap();
    builder.bind(Arc::new(Second)).unwrap();
    let error = match builder.build() {
        Ok(_) => panic!("registry unexpectedly accepted duplicate task types"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        CompletionRegistryError::DuplicateTaskType { ref task_type, .. }
            if task_type == "duplicate.approval.v1"
    ));

    let error = CompletionSourceCatalog::collect().unwrap_err();
    assert!(matches!(
        error,
        CompletionRegistryError::DuplicateTaskType { ref task_type, .. }
            if task_type == "duplicate.approval.v1"
    ));
}
