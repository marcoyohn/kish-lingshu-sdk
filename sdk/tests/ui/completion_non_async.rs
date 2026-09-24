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

struct Handlers;

#[lingshu_service(key = "reviews")]
impl Handlers {
    #[user_task_completion_handler(task_type = "approval.invalid.v1", operation="complete", version="v1", modes=["sync"])]
    fn complete(
        &self,
        _context: CompletionContext,
        _submission: Submission,
    ) -> CompletionResult<Output> {
        Ok(Output)
    }
}

fn main() {}
