use std::{collections::BTreeMap, fmt, pin::Pin};

use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    ClientInstanceId, ClientToolBinding, EventCursor, ExecutionId, FailureEvent, MessageId,
    RequestContext, RootWorkflowInstanceId, RuntimeError, RuntimeErrorCode, RuntimeResult,
    SessionId, SuspensionEvent, SuspensionHandle, WorkflowContext, WorkflowEvent, WorkflowId,
    WorkflowInstanceId,
};

pub type WorkflowEventStream =
    Pin<Box<dyn Stream<Item = RuntimeResult<WorkflowEvent>> + Send + 'static>>;

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct RuntimeBindings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_bindings: Option<WorkspaceBindings>,
    /// Session-scoped client tool schemas. Provider connection details never
    /// enter this contract; calls execute through Workflow suspension on the
    /// client that supplied the binding.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub client_tools: Vec<ClientToolBinding>,
    /// Trusted host-only identity for an idempotent event-dispatch start.
    /// Public transports must strip this unless the caller is an authorized
    /// event-dispatch service principal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trusted_start: Option<TrustedWorkflowStart>,
    /// Internal domain orchestration may select an immutable published
    /// definition while still invoking the transport-neutral Runtime facade.
    /// Runtime implementations must reject this binding for non-internal
    /// invocation contexts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub internal_published_start: Option<InternalPublishedWorkflowStart>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct InternalPublishedWorkflowStart {
    pub workflow_define_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_branch_id: Option<u64>,
    pub source_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TrustedWorkflowStart {
    pub idempotency_scope: String,
    pub idempotency_key: String,
    pub workflow_define_id: u64,
    pub start_node_id: String,
    pub trigger_id: u64,
    pub trigger_generation: u64,
    pub provenance: WorkflowEventProvenance,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowEventProvenance {
    pub event_id: u64,
    pub topic: String,
    pub event_type: String,
    pub subscription_id: u64,
    pub group_id: u64,
    pub subscription_epoch: u64,
    pub queue_epoch: u64,
    pub queue_id: u32,
    pub queue_offset: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumption_id: Option<u64>,
    pub invocation_id: u64,
    pub attempt_generation: u64,
    pub workflow_define_id: u64,
    pub start_node_id: String,
    pub trigger_id: u64,
    pub trigger_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_at: Option<String>,
}

/// Trusted Event Dispatch input for the application-level Workflow Event Router.
/// Workflow target identities are intentionally absent; Workflow Service resolves
/// them from its own published trigger catalog.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RouteWorkflowEventRequest {
    pub event: WorkflowRouteEvent,
    pub consumption: WorkflowRouteConsumption,
    pub delivery: WorkflowRouteDelivery,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowRouteEvent {
    pub event_id: u64,
    pub topic: String,
    pub event_type: String,
    pub schema_version: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    pub occurred_at: String,
    pub published_at: String,
    pub not_before: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, Value>,
    pub payload: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<WorkflowRouteSchedule>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowRouteSchedule {
    pub schedule_id: u64,
    pub scheduled_at: String,
    pub schedule_key: String,
    pub overlap_policy: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowRouteConsumption {
    pub subscription_id: u64,
    pub group_id: u64,
    pub subscription_epoch: u64,
    pub queue_epoch: u64,
    pub queue_id: u32,
    pub queue_offset: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumption_id: Option<u64>,
    pub invocation_id: u64,
    pub attempt_generation: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowRouteDeliveryMode {
    Sync,
    Async,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowRouteDelivery {
    pub mode: WorkflowRouteDeliveryMode,
    pub idempotency_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion: Option<WorkflowRouteCompletion>,
}

/// Server-generated asynchronous callback addresses. The credential is carried
/// only in the dedicated HTTP header and never enters this serializable value.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowRouteCompletion {
    pub url: String,
    pub heartbeat_url: String,
    pub callback_header_name: String,
    pub expires_at: String,
    pub maximum_expires_at: String,
    pub heartbeat_interval_seconds: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowRouteResponseStatus {
    Completed,
    Accepted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RouteWorkflowEventResponse {
    pub status: WorkflowRouteResponseStatus,
    pub matched_targets: usize,
    pub workflow_instance_ids: Vec<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted_at: Option<String>,
}

/// Secret supplied by Event Dispatch outside the serialized route request.
#[derive(Clone)]
pub struct WorkflowRouteCallbackToken(String);

impl WorkflowRouteCallbackToken {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for WorkflowRouteCallbackToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WorkflowRouteCallbackToken([REDACTED])")
    }
}

/// Inward request for resolving application-owned triggers and starting each
/// matching Workflow. Callback delivery remains an outer Adapter concern.
#[derive(Clone, Debug)]
pub struct WorkflowEventRouteStartRequest {
    pub actor_id: String,
    pub app_id: String,
    pub event: WorkflowRouteEvent,
    pub consumption: WorkflowRouteConsumption,
}

#[derive(Clone, Debug)]
pub struct WorkflowEventRouteStartedTarget {
    pub context: WorkflowContext,
    pub workflow_instance_id: WorkflowInstanceId,
}

#[derive(Clone, Debug, Default)]
pub struct WorkflowEventRouteStartResult {
    pub targets: Vec<WorkflowEventRouteStartedTarget>,
}

#[async_trait]
pub trait WorkflowEventRouteService: Send + Sync {
    async fn route(
        &self,
        actor_id: &str,
        app_id: &str,
        request: RouteWorkflowEventRequest,
        callback_token: Option<WorkflowRouteCallbackToken>,
    ) -> RuntimeResult<RouteWorkflowEventResponse>;
}

pub const WORKFLOW_EVENT_ROUTER_ACTOR_ID: &str = "workflow-event-router";

/// Conventional provider-neutral runtime roots for callers that explicitly
/// need a portable execution view.
///
/// The default CLI/ACP SDK binding preserves the client's canonical absolute
/// paths so the model can describe and reuse the same paths without a rewrite.
/// These constants remain available for hosts that intentionally choose a
/// virtual path namespace.
pub const CLIENT_TEMPORARY_RUNTIME_ROOT: &str = "/tmp/work";
pub const CLIENT_PERSISTENT_RUNTIME_ROOT: &str = "/workspace";

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceBindingMode {
    #[default]
    ReadOnly,
    ReadWrite,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackendPersistentWorkspaceBinding {
    pub workspace_id: u64,
    #[serde(default)]
    pub mode: WorkspaceBindingMode,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientPersistentWorkspaceBinding {
    /// Path in the client execution view. For the default SDK binding this is
    /// the client's canonical physical Workspace root. The Server treats it
    /// as an opaque client-owned path and never uses it for backend I/O.
    pub root_path: String,
    #[serde(default)]
    pub mode: WorkspaceBindingMode,
}

/// Root-execution Workspace provider selection.
///
/// Client bindings carry an opaque client identity and client execution paths.
/// Physical paths may cross the runtime command boundary as opaque values so
/// the Workflow/LLM can use the same path vocabulary as the local client.
/// They remain owned and authorized by the client executor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkspaceBindings {
    Backend {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        persistent: Option<BackendPersistentWorkspaceBinding>,
    },
    Client {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workspace_id: Option<u64>,
        client_instance_id: ClientInstanceId,
        application_cache_root: String,
        /// Operating system used by the client-side Shell sandbox. This is a
        /// prompt hint for command selection, not an execution or permission
        /// authority.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        shell_os: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        persistent: Option<ClientPersistentWorkspaceBinding>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum WorkflowInput {
    Structured(Value),
    Conversation(ConversationInput),
}

impl WorkflowInput {
    pub fn structured(value: Value) -> Self {
        Self::Structured(value)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConversationInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub business_type: Option<String>,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ConversationToolCall>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_responses: Option<Vec<ConversationToolResponse>>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub metadata: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConversationToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub index: i32,
    pub function: ConversationFunctionCall,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConversationFunctionCall {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConversationToolResponse {
    pub tool_call_id: String,
    pub role: String,
    pub content: Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct StartWorkflowRequest {
    pub context: WorkflowContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    pub input: WorkflowInput,
    #[serde(default)]
    pub bindings: RuntimeBindings,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SignalWorkflowRequest {
    pub context: RequestContext,
    pub workflow_instance_id: WorkflowInstanceId,
    pub signal: WorkflowSignal,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum WorkflowSignal {
    Input {
        input: WorkflowInput,
    },
    Business {
        name: String,
        payload: Value,
    },
    Control {
        control: WorkflowControl,
    },
    SuspensionResponse {
        suspension: SuspensionHandle,
        response: crate::SuspensionResponse,
    },
    /// Compatibility signal owned by a trusted transport adapter. Runtime
    /// bindings must reject extension kinds they do not explicitly support.
    AdapterExtension {
        kind: String,
        payload: Value,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowControl {
    RequestCompaction,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TerminateWorkflowRequest {
    pub context: RequestContext,
    pub workflow_instance_id: WorkflowInstanceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_timeout_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowSnapshotRequest {
    pub context: RequestContext,
    pub workflow_instance_id: WorkflowInstanceId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SubscribeWorkflowRequest {
    pub context: RequestContext,
    pub workflow_instance_id: WorkflowInstanceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<EventCursor>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowRunHandle {
    pub workflow_id: WorkflowId,
    pub workflow_instance_id: WorkflowInstanceId,
    pub root_workflow_instance_id: RootWorkflowInstanceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_message_id: Option<MessageId>,
    pub subscription_anchor: EventCursor,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkflowRunResult {
    Completed {
        run: WorkflowRunHandle,
        output: Value,
    },
    Failed {
        run: WorkflowRunHandle,
        failure: FailureEvent,
    },
    Suspended {
        run: WorkflowRunHandle,
        suspension: SuspensionEvent,
    },
    Terminated {
        run: WorkflowRunHandle,
    },
}

impl WorkflowRunResult {
    pub fn run(&self) -> &WorkflowRunHandle {
        match self {
            Self::Completed { run, .. }
            | Self::Failed { run, .. }
            | Self::Suspended { run, .. }
            | Self::Terminated { run } => run,
        }
    }

    pub fn state(&self) -> WorkflowState {
        match self {
            Self::Completed { .. } => WorkflowState::Completed,
            Self::Failed { .. } => WorkflowState::Failed,
            Self::Suspended { .. } => WorkflowState::Suspended,
            Self::Terminated { .. } => WorkflowState::Terminated,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandAck {
    pub workflow_instance_id: WorkflowInstanceId,
    pub disposition: CommandDisposition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<EventCursor>,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extensions: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandDisposition {
    Accepted,
    AlreadyApplied,
    TerminationRequested,
    Terminated,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    Pending,
    Running,
    RequiresAction,
    Suspended,
    Completed,
    Failed,
    Terminated,
}

impl WorkflowState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Terminated)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowSnapshot {
    pub workflow_id: WorkflowId,
    pub workflow_instance_id: WorkflowInstanceId,
    pub root_workflow_instance_id: RootWorkflowInstanceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_execution_id: Option<ExecutionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,
    pub state: WorkflowState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspension: Option<SuspensionHandle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_cursor: Option<EventCursor>,
}

#[async_trait]
pub trait WorkflowRuntime: Send + Sync {
    async fn start(&self, request: StartWorkflowRequest) -> RuntimeResult<WorkflowRunHandle>;

    async fn signal(&self, request: SignalWorkflowRequest) -> RuntimeResult<CommandAck>;

    async fn terminate(&self, request: TerminateWorkflowRequest) -> RuntimeResult<CommandAck>;

    async fn snapshot(&self, request: WorkflowSnapshotRequest) -> RuntimeResult<WorkflowSnapshot>;

    async fn start_event_routes(
        &self,
        _request: WorkflowEventRouteStartRequest,
    ) -> RuntimeResult<WorkflowEventRouteStartResult> {
        Err(RuntimeError::new(
            RuntimeErrorCode::Unsupported,
            "Workflow Event routing is not available",
        ))
    }

    async fn subscribe(
        &self,
        request: SubscribeWorkflowRequest,
    ) -> RuntimeResult<WorkflowEventStream>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn workflow_signals_keep_typed_directional_shapes() {
        let control = WorkflowSignal::Control {
            control: WorkflowControl::RequestCompaction,
        };
        assert_eq!(
            serde_json::to_value(&control).unwrap(),
            json!({"type": "control", "data": {"control": "request_compaction"}})
        );

        let business = WorkflowSignal::Business {
            name: "job.completed".to_string(),
            payload: json!({"job_id": 7}),
        };
        assert_eq!(
            serde_json::from_value::<WorkflowSignal>(serde_json::to_value(business).unwrap())
                .unwrap(),
            WorkflowSignal::Business {
                name: "job.completed".to_string(),
                payload: json!({"job_id": 7}),
            }
        );
    }

    #[test]
    fn suspension_response_signal_preserves_opaque_handle_and_decision() {
        let signal = WorkflowSignal::SuspensionResponse {
            suspension: SuspensionHandle::issue("compact-1").unwrap(),
            response: crate::SuspensionResponse::CompactionDecision {
                decision: crate::CompactionDecision::ContinueWithoutCompaction,
            },
        };
        let encoded = serde_json::to_value(&signal).unwrap();
        assert_eq!(encoded["data"]["suspension"], json!("v1.compact-1"));
        assert_eq!(
            encoded["data"]["response"]["type"],
            json!("compaction_decision")
        );
        assert_eq!(
            serde_json::from_value::<WorkflowSignal>(encoded).unwrap(),
            signal
        );
    }
}
