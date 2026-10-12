//! Typed external human lifecycle handlers. Business writes and idempotency belong to the provider.
pub use kish_lingshu_runtime_contract::human_task::*;
use kish_lingshu_runtime_contract::service::*;
#[doc(hidden)]
pub use serde_json;

#[derive(Debug, Clone)]
pub struct HumanTaskContext {
    invocation: HumanTaskInvocation,
}

impl HumanTaskContext {
    /// Generated adapters call this only after registry and transport validation.
    pub fn from_service(context: &ServiceContext) -> Result<Self, ServiceError> {
        let InvocationRole::Call(call) = &context.invocation else {
            return Err(ServiceError::rejected(
                "invalid_human_context",
                "Human lifecycle requires a Call",
            ));
        };
        let wire = call.human_task.as_ref().ok_or_else(|| {
            ServiceError::rejected("invalid_human_context", "Missing trusted task context")
        })?;
        if wire.application_id != context.application_id
            || wire.idempotency_key != context.idempotency_key
            || call.caller != "human_task"
            || call.workflow.as_ref() != Some(&wire.workflow)
        {
            return Err(ServiceError::rejected(
                "invalid_human_context",
                "Human task context identity mismatch",
            ));
        }
        Ok(Self {
            invocation: wire.as_ref().clone(),
        })
    }
    pub fn invocation(&self) -> &HumanTaskInvocation {
        &self.invocation
    }
    pub fn application_id(&self) -> &str {
        &self.invocation.application_id
    }
    pub fn task_id(&self) -> &str {
        &self.invocation.task_id
    }
    pub fn actor_id(&self) -> &str {
        &self.invocation.actor_id
    }
    pub fn idempotency_key(&self) -> &str {
        &self.invocation.idempotency_key
    }
    pub fn action(&self) -> HumanTaskAction {
        self.invocation.action
    }
    pub fn business_ref(&self) -> &serde_json::Value {
        &self.invocation.business_ref
    }
    pub fn external_ref(&self) -> Option<&str> {
        self.invocation.external_ref.as_deref()
    }
}

#[doc(hidden)]
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;

pub type HumanTaskResult<T> = Result<T, HumanTaskError>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum HumanTaskError {
    #[error("{problem:?}")]
    Rejected { problem: HumanTaskProblem },
    #[error("{problem:?}")]
    Retryable {
        problem: HumanTaskProblem,
        retry_after: Option<Duration>,
    },
    #[error("{problem:?}")]
    Failed { problem: HumanTaskProblem },
}

impl HumanTaskError {
    pub fn rejected(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Rejected {
            problem: bounded_problem(HumanTaskProblem::new(code, message)),
        }
    }

    pub fn rejected_problem(problem: HumanTaskProblem) -> Self {
        Self::Rejected {
            problem: bounded_problem(problem),
        }
    }

    pub fn retryable(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Retryable {
            problem: bounded_problem(HumanTaskProblem::new(code, message)),
            retry_after: None,
        }
    }

    pub fn retryable_after(
        code: impl Into<String>,
        message: impl Into<String>,
        retry_after: Duration,
    ) -> Self {
        Self::Retryable {
            problem: bounded_problem(HumanTaskProblem::new(code, message)),
            retry_after: Some(retry_after),
        }
    }

    pub fn failed(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Failed {
            problem: bounded_problem(HumanTaskProblem::new(code, message)),
        }
    }

    fn into_outcome(self) -> HumanTaskOutcome {
        match self {
            Self::Rejected { problem } => HumanTaskOutcome::Rejected { problem },
            Self::Retryable {
                problem,
                retry_after,
            } => HumanTaskOutcome::Retry {
                problem,
                retry_after_milliseconds: retry_after
                    .map(|value| value.as_millis().min(u128::from(u64::MAX)) as u64),
            },
            Self::Failed { problem } => HumanTaskOutcome::Failed { problem },
        }
    }
}

fn bounded_problem(mut problem: HumanTaskProblem) -> HumanTaskProblem {
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

/// Generated Service adapter entrypoint. Transport validates the trusted Call context first.
pub fn encode_outcome<T: Serialize>(
    result: HumanTaskResult<T>,
) -> Result<Value, kish_lingshu_runtime_contract::service::ServiceError> {
    let outcome = match result {
        Ok(output) => HumanTaskOutcome::Applied {
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
