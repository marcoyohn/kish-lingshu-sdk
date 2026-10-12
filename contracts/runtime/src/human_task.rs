//! External human work: execution identity belongs to Workflow, business data to the provider.
use crate::{service::*, ApplicationResult, IdempotencyKey, RequestContext, WorkflowInstanceId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub const HUMAN_TASK_PROTOCOL: &str = "external_v1";
pub const HUMAN_TASK_EVENT: &str = "human_task";
pub const MAX_HUMAN_TASK_INPUT_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanTaskAction {
    Create,
    Claim,
    Save,
    Complete,
    Cancel,
    Withdraw,
}

impl HumanTaskAction {
    pub fn hook(self) -> Self {
        if self == Self::Withdraw {
            Self::Cancel
        } else {
            self
        }
    }
    pub fn is_command(self) -> bool {
        matches!(
            self,
            Self::Claim | Self::Save | Self::Complete | Self::Withdraw
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanTaskKind {
    Todo,
    Read,
    Draft,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanTaskClaimPolicy {
    #[default]
    None,
    Required,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanTaskDefinition {
    pub task_type: String,
    pub action: HumanTaskAction,
    pub node_kinds: BTreeSet<HumanTaskKind>,
}

impl HumanTaskDefinition {
    pub fn validate(&self) -> Result<(), ServiceError> {
        if !valid_key(&self.task_type)
            || self.action == HumanTaskAction::Withdraw
            || self.node_kinds.is_empty()
            || (self.node_kinds.contains(&HumanTaskKind::Read)
                && !matches!(
                    self.action,
                    HumanTaskAction::Create | HumanTaskAction::Cancel
                ))
        {
            return Err(ServiceError::rejected(
                "invalid_human_binding",
                "Invalid human task type, action or node kind",
            ));
        }
        Ok(())
    }
}

/// Deployment-independent hooks. Binding validation includes the native catalog metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanTaskConfig {
    pub protocol: String,
    pub task_type: String,
    #[serde(default)]
    pub claim_policy: HumanTaskClaimPolicy,
    pub hooks: BTreeMap<HumanTaskAction, HumanTaskHookBinding>,
    #[serde(default)]
    pub withdrawal: Option<HumanTaskWithdrawal>,
    #[serde(default)]
    pub input_schema: BTreeMap<HumanTaskAction, Value>,
    #[serde(default)]
    pub result_schema: Option<Value>,
    #[serde(default)]
    pub outcome_edges: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanTaskWithdrawal {
    pub draft_node_id: String,
    pub edge_id: String,
}

impl HumanTaskConfig {
    pub fn validate(&self, kind: HumanTaskKind) -> Result<(), ServiceError> {
        if self.protocol != HUMAN_TASK_PROTOCOL || !valid_key(&self.task_type) {
            return Err(ServiceError::rejected(
                "invalid_human_protocol",
                "Explicit external_v1 and task_type are required",
            ));
        }
        let mut required = vec![HumanTaskAction::Create, HumanTaskAction::Cancel];
        if kind != HumanTaskKind::Read {
            required.push(HumanTaskAction::Complete);
        }
        if kind == HumanTaskKind::Draft {
            required.push(HumanTaskAction::Save);
        }
        if self.claim_policy == HumanTaskClaimPolicy::Required {
            required.push(HumanTaskAction::Claim);
        }
        if required
            .iter()
            .any(|action| !self.hooks.contains_key(action))
            || self.hooks.contains_key(&HumanTaskAction::Withdraw)
            || (kind == HumanTaskKind::Read
                && (self.claim_policy != HumanTaskClaimPolicy::None
                    || self.hooks.keys().any(|action| {
                        !matches!(action, HumanTaskAction::Create | HumanTaskAction::Cancel)
                    })))
            || (self.withdrawal.is_some() && kind != HumanTaskKind::Todo)
            || self
                .withdrawal
                .as_ref()
                .is_some_and(|w| w.draft_node_id.trim().is_empty() || w.edge_id.trim().is_empty())
        {
            return Err(ServiceError::rejected(
                "invalid_human_hooks",
                "Missing or incompatible lifecycle Hook",
            ));
        }
        for binding in self.hooks.values() {
            binding.validate()?;
        }
        for schema in self.input_schema.values().chain(self.result_schema.iter()) {
            if (!schema.is_object() && !schema.is_boolean()) || !local_schema_refs(schema) {
                return Err(ServiceError::rejected(
                    "invalid_schema",
                    "Human schemas must use local references",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanTaskInvocation {
    pub protocol: String,
    pub application_id: String,
    pub task_id: String,
    pub task_type: String,
    pub node_kind: HumanTaskKind,
    pub action: HumanTaskAction,
    pub task_revision: u64,
    pub operation_id: String,
    pub idempotency_key: String,
    pub request_digest: String,
    pub workflow: WorkflowCallTarget,
    pub actor_id: String,
    pub principal: crate::PrincipalKind,
    pub initiator_id: String,
    pub business_ref: Value,
    #[serde(default)]
    pub external_ref: Option<String>,
    #[serde(default)]
    pub expected_business_revision: Option<String>,
    #[serde(default)]
    pub cancel_reason: Option<String>,
    pub request_id: String,
    pub correlation_id: String,
}

impl HumanTaskInvocation {
    pub fn validate(
        &self,
        context: &ServiceContext,
        call: &CallContext,
        definition: &HumanTaskDefinition,
    ) -> Result<(), ServiceError> {
        if self.protocol != HUMAN_TASK_PROTOCOL
            || self.application_id != context.application_id
            || self.idempotency_key != context.idempotency_key
            || self.task_type != definition.task_type
            || self.action != definition.action
            || !definition.node_kinds.contains(&self.node_kind)
            || call.caller != "human_task"
            || call.workflow.as_ref() != Some(&self.workflow)
            || self.task_id.is_empty()
            || self.operation_id.is_empty()
            || self.operation_id != call.call_id
            || self.workflow.execution_seq == 0
            || self.actor_id.is_empty()
            || self.task_revision == 0
            || self.request_digest.len() != 64
        {
            return Err(ServiceError::rejected(
                "invalid_human_context",
                "Human task authority does not match its operation",
            ));
        }
        Ok(())
    }
}

// The lifecycle uses the same no-effect rejection and unknown-result semantics as completion,
// while its invocation carries no legacy task identity.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HumanTaskProblem {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub field_errors: BTreeMap<String, String>,
}

impl HumanTaskProblem {
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
pub enum HumanTaskOutcome {
    Applied {
        #[serde(default)]
        output: Value,
    },
    Rejected {
        problem: HumanTaskProblem,
    },
    Retry {
        problem: HumanTaskProblem,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_after_milliseconds: Option<u64>,
    },
    Failed {
        problem: HumanTaskProblem,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanTaskPhase {
    Creating,
    Waiting,
    Cancelling,
    Completed,
    Cancelled,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanTaskCommandState {
    Accepted,
    Running,
    RetryWait,
    Applied,
    Rejected,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanTaskCommand {
    pub action: HumanTaskAction,
    pub expected_revision: u64,
    pub idempotency_key: IdempotencyKey,
    pub input: Value,
    #[serde(default)]
    pub expected_business_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanTaskCommandReceipt {
    pub application_id: String,
    pub workflow_instance_id: WorkflowInstanceId,
    /// Workflow occurrence identity shared by SDK, HTTP and lifecycle Hooks.
    pub task_id: String,
    pub command_id: String,
    pub operation_id: String,
    pub task_revision: u64,
    pub state: HumanTaskCommandState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanTaskCommandStatus {
    pub receipt: HumanTaskCommandReceipt,
    pub state: HumanTaskCommandState,
    pub action: HumanTaskAction,
    pub problem: Option<HumanTaskProblem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanTaskSnapshot {
    pub application_id: String,
    pub workflow_instance_id: WorkflowInstanceId,
    /// Workflow occurrence identity, not an external business inbox record ID.
    pub task_id: String,
    pub task_type: String,
    pub node_kind: HumanTaskKind,
    pub node_id: String,
    pub execution_seq: u64,
    pub task_revision: u64,
    pub phase: HumanTaskPhase,
    pub external_ref: Option<String>,
    pub active_command_id: Option<String>,
    #[serde(default)]
    pub active_operation_id: Option<String>,
    #[serde(default)]
    pub active_command_state: Option<HumanTaskCommandState>,
    pub supported_actions: BTreeSet<HumanTaskAction>,
    pub problem: Option<HumanTaskProblem>,
}

#[async_trait]
pub trait WorkflowHumanTaskRuntime: Send + Sync {
    async fn list(
        &self,
        context: RequestContext,
        instance: WorkflowInstanceId,
    ) -> ApplicationResult<Vec<HumanTaskSnapshot>>;
    async fn get(
        &self,
        context: RequestContext,
        instance: WorkflowInstanceId,
        task_id: String,
    ) -> ApplicationResult<HumanTaskSnapshot>;
    async fn command(
        &self,
        context: RequestContext,
        instance: WorkflowInstanceId,
        task_id: String,
        command: HumanTaskCommand,
    ) -> ApplicationResult<HumanTaskCommandReceipt>;
    async fn command_status(
        &self,
        context: RequestContext,
        instance: WorkflowInstanceId,
        task_id: String,
        command: String,
    ) -> ApplicationResult<HumanTaskCommandStatus>;
    async fn retry(
        &self,
        context: RequestContext,
        instance: WorkflowInstanceId,
        task_id: String,
        command: String,
    ) -> ApplicationResult<HumanTaskCommandReceipt>;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn read_handlers_cannot_claim_or_complete() {
        let definition = HumanTaskDefinition {
            task_type: "approval.v1".into(),
            action: HumanTaskAction::Complete,
            node_kinds: [HumanTaskKind::Read].into_iter().collect(),
        };
        assert!(definition.validate().is_err());
    }
    #[test]
    fn forged_actor_is_not_a_command_field() {
        assert!(serde_json::from_value::<HumanTaskCommand>(serde_json::json!({"action":"complete","expected_revision":1,"idempotency_key":"a","input":{},"actor_id":"other"})).is_err());
    }

    #[test]
    fn human_task_snapshot_uses_task_wire_identity() {
        let wire = serde_json::json!({
            "application_id": "app", "workflow_instance_id": 42,
            "task_id": "draft-round-2", "task_type": "approval.v1",
            "node_kind": "draft", "node_id": "draft", "execution_seq": 2,
            "task_revision": 3, "phase": "waiting", "external_ref": "edit-2",
            "active_command_id": null, "active_operation_id": null,
            "active_command_state": null, "supported_actions": ["save", "complete"],
            "problem": null
        });
        let task: HumanTaskSnapshot = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(task.task_id, "draft-round-2");
        assert_eq!(task.task_type, "approval.v1");
        assert_eq!(serde_json::to_value(task).unwrap(), wire);
        let mut retired_wire = wire;
        let fields = retired_wire.as_object_mut().unwrap();
        let id = fields.remove("task_id").unwrap();
        fields.insert("interaction_id".into(), id);
        assert!(serde_json::from_value::<HumanTaskSnapshot>(retired_wire).is_err());
    }

    #[test]
    fn retired_lifecycle_metadata_is_rejected() {
        let definition = serde_json::json!({
            "interaction_type": "approval.v1", "action": "create", "node_kinds": ["draft"]
        });
        assert!(serde_json::from_value::<HumanTaskDefinition>(definition).is_err());
    }
}
