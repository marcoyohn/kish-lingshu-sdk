use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    MessageId, RequestContext, RuntimeResult, SessionId, WorkflowContext, WorkflowId,
    WorkflowInstanceId,
};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CreateSessionRequest {
    pub context: WorkflowContext,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub session_type: String,
    #[serde(default)]
    pub get_or_create: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LoadSessionRequest {
    pub context: RequestContext,
    pub session_id: SessionId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LoadMessagesRequest {
    pub context: RequestContext,
    pub session_id: SessionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<MessageId>,
    pub limit: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionHandle {
    pub session_id: SessionId,
    pub workflow_id: WorkflowId,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SessionSnapshot {
    pub session_id: SessionId,
    pub workflow_id: WorkflowId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    pub actor_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub session_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_workflow_instance_id: Option<WorkflowInstanceId>,
    #[serde(default)]
    pub deleted: bool,
    /// Small non-authoritative extension fields. Implementations must not put
    /// a second complete Session representation in this map.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extensions: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConversationMessage {
    pub message_id: MessageId,
    pub session_id: SessionId,
    pub workflow_id: WorkflowId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_instance_id: Option<WorkflowInstanceId>,
    pub message_type: String,
    pub updated_at: String,
    pub savepoint_version: i32,
    pub actor_id: String,
    pub connection_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipient: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub business_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_responses: Option<String>,
    #[serde(default)]
    pub user_stopped: bool,
    #[serde(default)]
    pub like_count: i32,
    #[serde(default)]
    pub dislike_count: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    #[serde(default)]
    pub status: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_id: Option<String>,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_scope_id: Option<u64>,
    pub created_at: String,
    /// Small non-authoritative extension fields. Implementations must not put
    /// a second complete persisted Message representation in this map.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extensions: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MessagePage {
    pub messages: Vec<ConversationMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_before: Option<MessageId>,
}

#[async_trait]
pub trait ConversationService: Send + Sync {
    async fn create_session(&self, request: CreateSessionRequest) -> RuntimeResult<SessionHandle>;

    async fn load_session(&self, request: LoadSessionRequest) -> RuntimeResult<SessionSnapshot>;

    async fn load_messages(&self, request: LoadMessagesRequest) -> RuntimeResult<MessagePage>;
}
