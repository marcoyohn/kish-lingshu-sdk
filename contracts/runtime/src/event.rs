use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::{
    ClientToolExecutionContext, EventCursor, EventId, ExecutionId, MessageId, PlanId,
    RootWorkflowInstanceId, SessionId, SuspensionHandle, ToolCallId, WorkflowId,
    WorkflowInstanceId, WorkflowState,
};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowEvent {
    pub event_id: EventId,
    pub cursor: EventCursor,
    pub workflow_id: WorkflowId,
    pub workflow_instance_id: WorkflowInstanceId,
    pub root_workflow_instance_id: RootWorkflowInstanceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<MessageId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<ToolCallId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<PlanId>,
    pub timestamp: String,
    #[serde(flatten)]
    pub kind: WorkflowEventKind,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowStateEvent {
    pub state: WorkflowState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecutionStateEvent {
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_type: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MessageEvent {
    pub phase: MessagePhase,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessagePhase {
    Started,
    Delta,
    Completed,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolCallEvent {
    pub state: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PlanEvent {
    pub version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overview: Option<String>,
    pub items: Vec<PlanItem>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlanItem {
    pub step: String,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SuspensionEvent {
    pub handle: SuspensionHandle,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ClientToolRequestEvent {
    pub suspension: SuspensionHandle,
    pub name: String,
    pub arguments: Value,
    pub execution: ClientToolExecutionContext,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PermissionRequestEvent {
    /// Opaque resume authority for this exact pending authorization. Older
    /// canonical transports may omit it; adapters can recover the current
    /// suspension from a snapshot but must never synthesize one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspension: Option<SuspensionHandle>,
    pub request_id: String,
    pub action: String,
    pub options: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct UsageEvent {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FailureEvent {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CompactionEvent {
    pub phase: CompactionPhase,
    pub trigger: CompactionTrigger,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspension: Option<SuspensionHandle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens_before: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens_after: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionPhase {
    Started,
    Required,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionTrigger {
    Automatic,
    Manual,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowEventKind {
    WorkflowState(WorkflowStateEvent),
    ExecutionState(ExecutionStateEvent),
    Message(MessageEvent),
    ToolCall(ToolCallEvent),
    Plan(PlanEvent),
    Suspension(SuspensionEvent),
    ClientToolRequest(ClientToolRequestEvent),
    PermissionRequest(PermissionRequestEvent),
    Output(Value),
    Usage(UsageEvent),
    Failure(FailureEvent),
    Compaction(CompactionEvent),
    Extension { kind: String, payload: Value },
}

#[derive(Deserialize, Serialize)]
struct EventKindWire {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    data: Value,
}

impl Serialize for WorkflowEventKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let (kind, data) = match self {
            Self::WorkflowState(value) => ("workflow_state", to_value(value)),
            Self::ExecutionState(value) => ("execution_state", to_value(value)),
            Self::Message(value) => ("message", to_value(value)),
            Self::ToolCall(value) => ("tool_call", to_value(value)),
            Self::Plan(value) => ("plan", to_value(value)),
            Self::Suspension(value) => ("suspension", to_value(value)),
            Self::ClientToolRequest(value) => ("client_tool_request", to_value(value)),
            Self::PermissionRequest(value) => ("permission_request", to_value(value)),
            Self::Output(value) => ("output", value.clone()),
            Self::Usage(value) => ("usage", to_value(value)),
            Self::Failure(value) => ("failure", to_value(value)),
            Self::Compaction(value) => ("compaction", to_value(value)),
            Self::Extension { kind, payload } => {
                return EventKindWire {
                    kind: kind.clone(),
                    data: payload.clone(),
                }
                .serialize(serializer)
            }
        };
        EventKindWire {
            kind: kind.to_owned(),
            data,
        }
        .serialize(serializer)
    }
}

fn to_value<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("event payloads are serializable")
}

impl<'de> Deserialize<'de> for WorkflowEventKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = EventKindWire::deserialize(deserializer)?;
        let invalid = |error: serde_json::Error| serde::de::Error::custom(error);
        match wire.kind.as_str() {
            "workflow_state" => serde_json::from_value(wire.data)
                .map(Self::WorkflowState)
                .map_err(invalid),
            "execution_state" => serde_json::from_value(wire.data)
                .map(Self::ExecutionState)
                .map_err(invalid),
            "message" => serde_json::from_value(wire.data)
                .map(Self::Message)
                .map_err(invalid),
            "tool_call" => serde_json::from_value(wire.data)
                .map(Self::ToolCall)
                .map_err(invalid),
            "plan" => serde_json::from_value(wire.data)
                .map(Self::Plan)
                .map_err(invalid),
            "suspension" => serde_json::from_value(wire.data)
                .map(Self::Suspension)
                .map_err(invalid),
            "client_tool_request" => serde_json::from_value(wire.data)
                .map(Self::ClientToolRequest)
                .map_err(invalid),
            "permission_request" => serde_json::from_value(wire.data)
                .map(Self::PermissionRequest)
                .map_err(invalid),
            "output" => Ok(Self::Output(wire.data)),
            "usage" => serde_json::from_value(wire.data)
                .map(Self::Usage)
                .map_err(invalid),
            "failure" => serde_json::from_value(wire.data)
                .map(Self::Failure)
                .map_err(invalid),
            "compaction" => serde_json::from_value(wire.data)
                .map(Self::Compaction)
                .map_err(invalid),
            _ => Ok(Self::Extension {
                kind: wire.kind,
                payload: wire.data,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn compaction_event_round_trips_as_a_first_class_event() {
        let event = WorkflowEventKind::Compaction(CompactionEvent {
            phase: CompactionPhase::Required,
            trigger: CompactionTrigger::Automatic,
            suspension: Some(SuspensionHandle::issue("compact-1").unwrap()),
            input_tokens_before: Some(12_000),
            input_tokens_after: None,
            summary_tokens: None,
            error: None,
        });
        let encoded = serde_json::to_value(&event).unwrap();
        assert_eq!(encoded["type"], json!("compaction"));
        assert_eq!(
            serde_json::from_value::<WorkflowEventKind>(encoded).unwrap(),
            event
        );
    }
}
