use kish_lingshu_sdk::{
    user_task::completion::{CompletionContext, CompletionResult},
    user_task_handlers,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JsonSchema)]
struct Submission;

#[derive(JsonSchema, Serialize)]
struct Output;

struct Handlers;

#[user_task_handlers]
impl Handlers {
    #[completion_handler(task_type = "approval.invalid.v1")]
    fn complete(
        &self,
        _context: CompletionContext,
        _submission: Submission,
    ) -> CompletionResult<Output> {
        Ok(Output)
    }
}

fn main() {}
