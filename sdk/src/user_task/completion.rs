//! Typed User Task business context and outcomes for native Service handlers.
pub use kish_lingshu_runtime_contract::{
    UserTaskCompletionDetail, UserTaskCompletionTaskV1, UserTaskCompletionWorkflowV1,
};
use kish_lingshu_runtime_contract::{
    UserTaskCompletionInvocationV1, UserTaskCompletionOutcomeV1, UserTaskCompletionProblem,
};
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;

pub type CompletionResult<T> = Result<T, CompletionError>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum CompletionError {
    #[error("{problem:?}")]
    Rejected { problem: UserTaskCompletionProblem },
    #[error("{problem:?}")]
    Retryable {
        problem: UserTaskCompletionProblem,
        retry_after: Option<Duration>,
    },
    #[error("{problem:?}")]
    Failed { problem: UserTaskCompletionProblem },
}

impl CompletionError {
    pub fn rejected(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Rejected {
            problem: bounded_problem(UserTaskCompletionProblem::new(code, message)),
        }
    }

    pub fn rejected_problem(problem: UserTaskCompletionProblem) -> Self {
        Self::Rejected {
            problem: bounded_problem(problem),
        }
    }

    pub fn retryable(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Retryable {
            problem: bounded_problem(UserTaskCompletionProblem::new(code, message)),
            retry_after: None,
        }
    }

    pub fn retryable_after(
        code: impl Into<String>,
        message: impl Into<String>,
        retry_after: Duration,
    ) -> Self {
        Self::Retryable {
            problem: bounded_problem(UserTaskCompletionProblem::new(code, message)),
            retry_after: Some(retry_after),
        }
    }

    pub fn failed(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Failed {
            problem: bounded_problem(UserTaskCompletionProblem::new(code, message)),
        }
    }

    fn into_outcome(self) -> UserTaskCompletionOutcomeV1 {
        match self {
            Self::Rejected { problem } => UserTaskCompletionOutcomeV1::Rejected { problem },
            Self::Retryable {
                problem,
                retry_after,
            } => UserTaskCompletionOutcomeV1::Retry {
                problem,
                retry_after_milliseconds: retry_after
                    .map(|value| value.as_millis().min(u128::from(u64::MAX)) as u64),
            },
            Self::Failed { problem } => UserTaskCompletionOutcomeV1::Failed { problem },
        }
    }
}

fn bounded_problem(mut problem: UserTaskCompletionProblem) -> UserTaskCompletionProblem {
    problem.code = bounded(problem.code, 128);
    problem.message = bounded(problem.message, 1_024);
    problem.field_errors = problem
        .field_errors
        .into_iter()
        .take(64)
        .map(|(field, message)| (bounded(field, 255), bounded(message, 512)))
        .collect();
    problem
}

fn bounded(value: String, maximum_chars: usize) -> String {
    value.chars().take(maximum_chars).collect()
}

#[derive(Clone, Debug)]
pub struct CompletionContext {
    invocation: UserTaskCompletionInvocationV1,
}

impl CompletionContext {
    fn new(invocation: &UserTaskCompletionInvocationV1) -> Self {
        Self {
            invocation: invocation.clone(),
        }
    }

    pub fn invocation_id(&self) -> &str {
        &self.invocation.invocation_id
    }

    pub fn idempotency_key(&self) -> &str {
        &self.invocation.idempotency_key
    }

    pub fn application_id(&self) -> &str {
        &self.invocation.application_id
    }

    pub fn actor_user_id(&self) -> &str {
        &self.invocation.actor_user_id
    }

    pub fn task(&self) -> &kish_lingshu_runtime_contract::UserTaskCompletionTaskV1 {
        &self.invocation.task
    }

    pub fn workflow(&self) -> &kish_lingshu_runtime_contract::UserTaskCompletionWorkflowV1 {
        &self.invocation.workflow
    }

    pub fn request_id(&self) -> &kish_lingshu_runtime_contract::RequestId {
        &self.invocation.request_id
    }

    pub fn correlation_id(&self) -> &kish_lingshu_runtime_contract::CorrelationId {
        &self.invocation.correlation_id
    }

    pub fn invocation_deadline(&self) -> chrono::DateTime<chrono::Utc> {
        self.invocation.invocation_deadline
    }

    pub fn trace(&self) -> Option<&kish_lingshu_runtime_contract::TraceContext> {
        self.invocation.trace.as_ref()
    }
}

#[doc(hidden)]
pub mod __private {
    pub use serde_json;
}

/// Generated Service adapter entrypoint. Transport validates the trusted Call context first.
pub fn encode_outcome<T: Serialize>(
    result: CompletionResult<T>,
) -> Result<Value, kish_lingshu_runtime_contract::service::ServiceError> {
    let outcome = match result {
        Ok(output) => UserTaskCompletionOutcomeV1::Applied {
            output: serde_json::to_value(output).map_err(|e| {
                kish_lingshu_runtime_contract::service::ServiceError::rejected(
                    "invalid_output",
                    e.to_string(),
                )
            })?,
        },
        Err(error) => error.into_outcome(),
    };
    serde_json::to_value(outcome).map_err(|e| {
        kish_lingshu_runtime_contract::service::ServiceError::rejected(
            "invalid_output",
            e.to_string(),
        )
    })
}

pub fn outcome_schema<T: schemars::JsonSchema>() -> Value {
    let mut output = serde_json::to_value(schemars::schema_for!(T)).expect("output schema");
    let definitions = output.as_object_mut().and_then(|o| o.remove("$defs"));
    let mut schema = serde_json::json!({"oneOf":[
        {"type":"object","required":["outcome","output"],"properties":{"outcome":{"const":"applied"},"output":output},"additionalProperties":false},
        {"type":"object","required":["outcome","problem"],"properties":{"outcome":{"enum":["rejected","retry","failed"]},"problem":{"type":"object"},"retry_after_milliseconds":{"type":["integer","null"]}},"additionalProperties":false}
    ]});
    if let Some(definitions) = definitions {
        schema["$defs"] = definitions;
    }
    schema
}

impl CompletionContext {
    pub fn from_service(
        context: &kish_lingshu_runtime_contract::service::ServiceContext,
    ) -> Result<Self, kish_lingshu_runtime_contract::service::ServiceError> {
        use kish_lingshu_runtime_contract::service::*;
        let InvocationRole::Call(call) = &context.invocation else {
            return Err(ServiceError::rejected(
                "invalid_completion_context",
                "Completion requires a Call",
            ));
        };
        let invocation = call.user_task_completion.as_ref().ok_or_else(|| {
            ServiceError::rejected(
                "invalid_completion_context",
                "Missing trusted User Task context",
            )
        })?;
        if invocation.application_id != context.application_id
            || invocation.idempotency_key != context.idempotency_key
            || call.caller != "user_task"
        {
            return Err(ServiceError::rejected(
                "invalid_completion_context",
                "Completion identity mismatch",
            ));
        }
        Ok(Self::new(invocation))
    }
}
