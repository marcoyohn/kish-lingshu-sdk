use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct SessionRecord {
    pub id: u64,
    pub workflow_id: u64,
    pub name: String,
    pub description: Option<String>,
    pub workflow_session_type: String,
    pub app_id: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

/// Summary returned by Workflow discovery operations.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct WorkflowRecord {
    pub id: u64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub app_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskPlanItemStatus {
    Pending,
    InProgress,
    Completed,
}

impl TaskPlanItemStatus {
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TaskPlanItem {
    pub step: String,
    pub status: TaskPlanItemStatus,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct AgentTaskPlan {
    #[serde(default)]
    pub workflow_instance_id: u64,
    pub execution_id: u64,
    #[serde(default)]
    pub flow_node_id: String,
    #[serde(default)]
    pub conversation_scope_id: Option<u64>,
    #[serde(default)]
    pub active: bool,
    pub version: u64,
    pub overview: Option<String>,
    pub updated_at: Option<String>,
    #[serde(rename = "plan", alias = "items")]
    pub items: Vec<TaskPlanItem>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ConversationScope {
    pub id: u64,
    pub session_id: u64,
    pub parent_scope_id: Option<u64>,
    pub scope_type: String,
    pub resource_id: String,
    pub resource_name: Option<String>,
    pub scope_key: String,
    pub memory_scope_kind: Option<String>,
    pub active_workflow_instance_id: Option<u64>,
    pub active_execution_id: Option<u64>,
    pub status: String,
    #[serde(default)]
    pub is_deleted: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct SessionMessage {
    pub id: u64,
    pub session_id: Option<u64>,
    pub workflow_id: Option<u64>,
    pub workflow_instance_id: Option<u64>,
    pub message_type: String,
    pub role: Option<String>,
    pub busi_type: Option<String>,
    pub content: Option<String>,
    pub reasoning_content: Option<String>,
    pub tool_calls: Option<String>,
    pub tool_responses: Option<String>,
    pub connection_id: Option<String>,
    pub meta: Option<String>,
    pub created_at: String,
    pub updated_at: Option<String>,
    pub is_user_stop: Option<bool>,
    pub status: Option<bool>,
    pub conversation_scope_id: Option<u64>,
}
