use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    CorrelationId, RequestId, RootWorkflowInstanceId, TraceContext, UserTaskId, UserTaskRevision,
    WorkflowId, WorkflowInstanceId,
};

pub const USER_TASK_COMPLETION_CONTRACT_VERSION: &str = "1.0";
pub const USER_TASK_COMPLETION_PATH: &str = "/internal/lingshu/v1/user-task-completions";
pub const USER_TASK_COMPLETION_IDEMPOTENCY_HEADER: &str = "Idempotency-Key";
pub const USER_TASK_COMPLETION_INVOCATION_ID_HEADER: &str =
    "X-Lingshu-User-Task-Completion-Invocation-Id";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskCompletionTaskV1 {
    pub id: UserTaskId,
    pub task_type: String,
    pub expected_revision: UserTaskRevision,
    pub staged_revision: UserTaskRevision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskCompletionWorkflowV1 {
    pub workflow_id: WorkflowId,
    pub workflow_instance_id: WorkflowInstanceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_workflow_instance_id: Option<RootWorkflowInstanceId>,
    pub flow_node_id: String,
}

/// Trusted envelope sent by Lingshu after a participant submission is staged.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskCompletionInvocationV1 {
    pub contract_version: String,
    pub invocation_id: String,
    pub idempotency_key: String,
    pub application_id: String,
    pub task: UserTaskCompletionTaskV1,
    pub actor_user_id: String,
    pub workflow: UserTaskCompletionWorkflowV1,
    pub submission: Value,
    pub request_id: RequestId,
    pub correlation_id: CorrelationId,
    pub invocation_deadline: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<TraceContext>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskCompletionProblem {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub field_errors: BTreeMap<String, String>,
}

impl UserTaskCompletionProblem {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            field_errors: BTreeMap::new(),
        }
    }

    pub fn with_field_error(
        mut self,
        field: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        self.field_errors.insert(field.into(), message.into());
        self
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum UserTaskCompletionOutcomeV1 {
    Applied {
        #[serde(default)]
        output: Value,
    },
    Rejected {
        problem: UserTaskCompletionProblem,
    },
    Retry {
        problem: UserTaskCompletionProblem,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_after_milliseconds: Option<u64>,
    },
    Failed {
        problem: UserTaskCompletionProblem,
    },
}

/// Bounded operational summary for the latest durable completion command.
///
/// The summary deliberately omits catalog credentials and invocation payloads.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskCompletionExecution {
    pub command_id: u64,
    pub invocation_id: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    pub binding_key: String,
    pub service_key: String,
    pub operation_key: String,
    pub attempt_count: u64,
    pub lease_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub problem: Option<UserTaskCompletionProblem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempts: Vec<UserTaskCompletionAttempt>,
}

/// One bounded, credential-free invocation attempt nested under its owning
/// User Task completion command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskCompletionAttempt {
    pub attempt: u64,
    pub lease_generation: u64,
    pub started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_milliseconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub problem: Option<UserTaskCompletionProblem>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskCompletionDetail {
    pub binding_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub problem: Option<UserTaskCompletionProblem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<UserTaskCompletionExecution>,
}

/// Optimistic precondition for an operator-authorized retry of a terminal
/// completion command. The original action command and invocation remain the
/// durable identity of the remote business effect.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RetryUserTaskCompletion {
    pub task_id: UserTaskId,
    pub expected_revision: UserTaskRevision,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskCompletionRetryReceipt {
    pub task_id: UserTaskId,
    pub revision: UserTaskRevision,
    pub state: crate::UserTaskState,
    pub command_id: u64,
    pub invocation_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_outcomes_have_distinct_wire_discriminants() {
        let outcomes = [
            UserTaskCompletionOutcomeV1::Applied {
                output: serde_json::json!({"case_revision": 8}),
            },
            UserTaskCompletionOutcomeV1::Rejected {
                problem: UserTaskCompletionProblem::new("invalid_decision", "invalid decision")
                    .with_field_error("decision", "unsupported value"),
            },
            UserTaskCompletionOutcomeV1::Retry {
                problem: UserTaskCompletionProblem::new("temporarily_unavailable", "retry later"),
                retry_after_milliseconds: Some(1_000),
            },
            UserTaskCompletionOutcomeV1::Failed {
                problem: UserTaskCompletionProblem::new(
                    "unsupported_task_type",
                    "handler is not registered",
                ),
            },
        ];
        let discriminants = outcomes
            .iter()
            .map(|outcome| {
                let encoded = serde_json::to_value(outcome).unwrap();
                let decoded: UserTaskCompletionOutcomeV1 =
                    serde_json::from_value(encoded.clone()).unwrap();
                assert_eq!(&decoded, outcome);
                encoded["outcome"].as_str().unwrap().to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(discriminants, ["applied", "rejected", "retry", "failed"]);
    }

    #[test]
    fn invocation_rejects_unrecognized_trusted_fields() {
        let result = serde_json::from_value::<UserTaskCompletionInvocationV1>(serde_json::json!({
            "contract_version": USER_TASK_COMPLETION_CONTRACT_VERSION,
            "invocation_id": "user-task-completion/9001",
            "idempotency_key": "user-task-completion/9001",
            "application_id": "app-a",
            "task": {
                "id": 1,
                "task_type": "approval.v1",
                "expected_revision": 1,
                "staged_revision": 2
            },
            "actor_user_id": "user-a",
            "workflow": {
                "workflow_id": 2,
                "workflow_instance_id": 3,
                "flow_node_id": "approval"
            },
            "submission": {},
            "request_id": "req-a",
            "correlation_id": "corr-a",
            "invocation_deadline": "2026-09-07T10:00:00Z",
            "credential": "must-not-be-accepted"
        }));
        assert!(result.is_err());
    }

    #[test]
    fn execution_observation_omits_payloads_and_credentials() {
        let detail = UserTaskCompletionDetail {
            binding_key: "default".to_string(),
            problem: None,
            execution: Some(UserTaskCompletionExecution {
                command_id: 9001,
                invocation_id: "user-task-completion/9001".to_string(),
                state: "effect_pending".to_string(),
                outcome: None,
                binding_key: "default".to_string(),
                service_key: "kishee-ykm".to_string(),
                operation_key: "user-task.complete.v1".to_string(),
                attempt_count: 2,
                lease_generation: 2,
                lease_expires_at: None,
                accepted_at: None,
                completed_at: None,
                updated_at: "2026-09-07T10:00:00Z".parse().unwrap(),
                problem: None,
                attempts: Vec::new(),
            }),
        };
        let encoded = serde_json::to_string(&detail).unwrap();
        assert!(encoded.contains("user-task-completion/9001"));
        for forbidden in ["credential", "submission", "request_json", "response_json"] {
            assert!(!encoded.contains(forbidden));
        }
    }
}
