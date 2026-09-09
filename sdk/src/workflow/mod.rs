//! Workflow selection, execution handles, commands, and canonical observation.

use std::{
    collections::HashSet,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex},
};

use futures::{future, Stream, StreamExt};
use serde_json::{Map, Value};

use crate::{
    client::ClientInner, Error, MutationOptions, Principal, RequestOptions, TransportFailure,
    TransportKind,
};

pub use kish_lingshu_runtime_contract::{
    AuthorizationChoice, ClientInstanceId, ClientToolExecutionContext, ClientToolRequestEvent,
    CommandAck, CommandDisposition, CompactionDecision, CompactionEvent, CompactionPhase,
    CompactionTrigger, ConversationFunctionCall, ConversationInput, ConversationToolCall,
    ConversationToolResponse, EventCursor, EventId, ExecutionId, ExecutionStateEvent, FailureEvent,
    MessageEvent, MessageId, MessagePhase, PermissionDecision, PermissionRequestEvent, PlanEvent,
    PlanId, PlanItem, RootWorkflowInstanceId, SessionHandle, SessionId, SessionSnapshot,
    SuspensionEvent, SuspensionHandle, ToolCallEvent, ToolCallId, UsageEvent, WorkflowEvent,
    WorkflowEventKind, WorkflowId, WorkflowInput, WorkflowInstanceId, WorkflowRunHandle,
    WorkflowRunResult, WorkflowSnapshot, WorkflowState, WorkflowStateEvent,
};

use kish_lingshu_runtime_contract::{
    RuntimeBindings, SuspensionResponsePayload, WorkflowResultProjector,
    WorkflowSignal as RuntimeWorkflowSignal,
};

mod records;
pub use records::{
    AgentTaskPlan, ConversationScope, SessionMessage, SessionRecord, TaskPlanItem,
    TaskPlanItemStatus, WorkflowRecord,
};

/// A public Workflow start request containing only application-owned input.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowStart {
    input: WorkflowInput,
    session_id: Option<SessionId>,
    bindings: RuntimeBindings,
}

impl WorkflowStart {
    pub fn structured(input: Value) -> Self {
        Self::new(WorkflowInput::Structured(input))
    }

    pub fn conversation(input: ConversationInput) -> Self {
        Self::new(WorkflowInput::Conversation(input))
    }

    pub fn message(content: impl Into<String>) -> Self {
        Self::conversation(ConversationInput {
            business_type: None,
            role: "user".to_string(),
            content: Some(Value::String(content.into())),
            reasoning_content: None,
            tool_calls: None,
            tool_responses: None,
            metadata: Map::new(),
        })
    }

    pub fn new(input: WorkflowInput) -> Self {
        Self {
            input,
            session_id: None,
            bindings: RuntimeBindings::default(),
        }
    }

    pub fn in_session(mut self, session_id: SessionId) -> Self {
        self.session_id = Some(session_id);
        self
    }

    pub fn input(&self) -> &WorkflowInput {
        &self.input
    }

    pub fn session_id(&self) -> Option<SessionId> {
        self.session_id
    }

    pub(crate) fn with_bindings(mut self, bindings: RuntimeBindings) -> Self {
        self.bindings = bindings;
        self
    }
}

/// Signals application code may send to an existing Workflow run.
#[derive(Clone, Debug, PartialEq)]
pub enum WorkflowSignal {
    Input(WorkflowInput),
    Business { name: String, payload: Value },
    RequestCompaction,
}

impl WorkflowSignal {
    pub fn input(input: WorkflowInput) -> Self {
        Self::Input(input)
    }

    pub fn business(name: impl Into<String>, payload: Value) -> Self {
        Self::Business {
            name: name.into(),
            payload,
        }
    }

    fn into_runtime(self) -> Result<RuntimeWorkflowSignal, Error> {
        match self {
            Self::Input(input) => Ok(RuntimeWorkflowSignal::Input { input }),
            Self::Business { name, payload } => {
                if name.trim().is_empty() {
                    return Err(Error::configuration(
                        "signal.name",
                        "business signal name must not be empty",
                    ));
                }
                Ok(RuntimeWorkflowSignal::Business { name, payload })
            }
            Self::RequestCompaction => Ok(RuntimeWorkflowSignal::Control {
                control: kish_lingshu_runtime_contract::WorkflowControl::RequestCompaction,
            }),
        }
    }
}

/// Supported public responses to an opaque Workflow suspension.
#[derive(Clone, Debug, PartialEq)]
pub enum SuspensionResponse {
    ClientToolResult {
        tool_call_id: ToolCallId,
        status: String,
        result: Option<Value>,
        reason: Option<String>,
    },
    PermissionDecision {
        request_id: String,
        decision: AuthorizationChoice,
    },
    UserAnswer(Value),
    CompactionDecision(CompactionDecision),
}

impl SuspensionResponse {
    fn into_runtime(self) -> SuspensionResponsePayload {
        match self {
            Self::ClientToolResult {
                tool_call_id,
                status,
                result,
                reason,
            } => SuspensionResponsePayload::ClientToolResult {
                tool_call_id,
                status,
                result,
                reason,
            },
            Self::PermissionDecision {
                request_id,
                decision,
            } => SuspensionResponsePayload::PermissionDecision {
                request_id,
                decision,
            },
            Self::UserAnswer(value) => SuspensionResponsePayload::UserAnswer { value },
            Self::CompactionDecision(decision) => {
                SuspensionResponsePayload::CompactionDecision { decision }
            }
        }
    }
}

pub type WorkflowEventStream =
    Pin<Box<dyn Stream<Item = Result<WorkflowEvent, Error>> + Send + 'static>>;

pub struct Workflows<P: Principal> {
    pub(crate) inner: Arc<ClientInner>,
    principal: PhantomData<P>,
}

impl<P: Principal> Clone for Workflows<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            principal: PhantomData,
        }
    }
}

impl<P: Principal> Workflows<P> {
    /// Respond using an observed opaque suspension without fetching a run snapshot.
    pub async fn respond(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        suspension: SuspensionHandle,
        response: SuspensionResponse,
        options: MutationOptions,
    ) -> Result<CommandAck, Error> {
        respond_to_suspension(
            &self.inner,
            workflow_instance_id,
            suspension,
            response,
            options,
        )
        .await
    }

    pub(crate) fn new(inner: Arc<ClientInner>) -> Self {
        Self {
            inner,
            principal: PhantomData,
        }
    }

    pub async fn list_records(
        &self,
        limit: u64,
        options: RequestOptions,
    ) -> Result<Vec<WorkflowRecord>, Error> {
        self.inner
            .binding
            .list_workflow_records(limit, options)
            .await
    }

    pub async fn get_record(
        &self,
        workflow_id: u64,
        options: RequestOptions,
    ) -> Result<Option<WorkflowRecord>, Error> {
        self.inner
            .binding
            .get_workflow_record(workflow_id, options)
            .await
    }

    pub async fn list_session_records(
        &self,
        workflow_id: Option<u64>,
        limit: u64,
        name: Option<&str>,
        options: RequestOptions,
    ) -> Result<Vec<SessionRecord>, Error> {
        self.inner
            .binding
            .list_session_records(workflow_id, limit, name, options)
            .await
    }

    pub async fn create_session_record(
        &self,
        workflow_id: u64,
        name: String,
        options: RequestOptions,
    ) -> Result<SessionRecord, Error> {
        self.inner
            .binding
            .create_session_record(workflow_id, name, options)
            .await
    }

    pub async fn rename_session_record(
        &self,
        session_id: u64,
        description: String,
        options: RequestOptions,
    ) -> Result<SessionRecord, Error> {
        self.inner
            .binding
            .rename_session_record(session_id, description, options)
            .await
    }

    pub async fn delete_session_record(
        &self,
        session_id: u64,
        options: RequestOptions,
    ) -> Result<(), Error> {
        self.inner
            .binding
            .delete_session_record(session_id, options)
            .await
    }

    pub async fn load_session_message_records(
        &self,
        session_id: u64,
        before_id: Option<u64>,
        limit: u64,
        conversation_scope_id: Option<u64>,
        options: RequestOptions,
    ) -> Result<Vec<SessionMessage>, Error> {
        self.inner
            .binding
            .load_session_message_records(
                session_id,
                before_id,
                limit,
                conversation_scope_id,
                options,
            )
            .await
    }

    pub async fn list_conversation_scopes(
        &self,
        session_id: u64,
        options: RequestOptions,
    ) -> Result<Vec<ConversationScope>, Error> {
        self.inner
            .binding
            .list_conversation_scope_records(session_id, options)
            .await
    }

    pub async fn load_agent_task_plans(
        &self,
        workflow_instance_id: u64,
        options: RequestOptions,
    ) -> Result<Vec<AgentTaskPlan>, Error> {
        self.inner
            .binding
            .load_agent_task_plan_records(workflow_instance_id, options)
            .await
    }

    pub fn select(&self, workflow_id: WorkflowId) -> Result<Workflow<P>, Error> {
        validate_workflow_id(workflow_id)?;
        Ok(Workflow {
            inner: self.inner.clone(),
            workflow_id,
            principal: PhantomData,
        })
    }

    pub async fn start(
        &self,
        workflow_id: WorkflowId,
        start: WorkflowStart,
        options: MutationOptions,
    ) -> Result<WorkflowRun, Error> {
        self.select(workflow_id)?.start(start, options).await
    }

    pub fn attach(
        &self,
        handle: WorkflowRunHandle,
        after: Option<EventCursor>,
    ) -> Result<WorkflowRun, Error> {
        WorkflowRun::attached(self.inner.clone(), handle, after)
    }

    pub async fn snapshot(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        options: RequestOptions,
    ) -> Result<WorkflowSnapshot, Error> {
        if workflow_instance_id.0 == 0 {
            return Err(Error::configuration(
                "workflow_instance_id",
                "Workflow Instance identity must be positive",
            ));
        }
        self.inner
            .binding
            .workflow_snapshot(workflow_instance_id, options)
            .await
    }

    /// Reattach using the durable Workflow snapshot without replaying a command.
    pub async fn reattach(
        &self,
        workflow_id: WorkflowId,
        workflow_instance_id: WorkflowInstanceId,
        session_id: Option<SessionId>,
        after: Option<EventCursor>,
        options: RequestOptions,
    ) -> Result<WorkflowRun, Error> {
        validate_workflow_id(workflow_id)?;
        if let Some(session_id) = session_id {
            validate_session_id(session_id)?;
        }
        let request_id = options.request_id().clone();
        let snapshot = self
            .inner
            .binding
            .workflow_snapshot(workflow_instance_id, options)
            .await?;
        if snapshot.workflow_id != workflow_id
            || snapshot.workflow_instance_id != workflow_instance_id
            || snapshot.session_id != session_id
        {
            return Err(Error::contract(
                "Workflow snapshot does not match the requested reattachment identities",
                Some(request_id),
            ));
        }
        let subscription_anchor = after.or(snapshot.latest_cursor).ok_or_else(|| {
            Error::contract(
                "Workflow snapshot has no canonical cursor for reattachment",
                Some(request_id),
            )
        })?;
        WorkflowRun::attached(
            self.inner.clone(),
            WorkflowRunHandle {
                workflow_id,
                workflow_instance_id,
                root_workflow_instance_id: snapshot.root_workflow_instance_id,
                session_id,
                input_message_id: None,
                subscription_anchor: subscription_anchor.clone(),
            },
            Some(subscription_anchor),
        )
    }
}

pub struct Workflow<P: Principal> {
    inner: Arc<ClientInner>,
    workflow_id: WorkflowId,
    principal: PhantomData<P>,
}

impl<P: Principal> Clone for Workflow<P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            workflow_id: self.workflow_id,
            principal: PhantomData,
        }
    }
}

impl<P: Principal> Workflow<P> {
    pub fn id(&self) -> WorkflowId {
        self.workflow_id
    }

    pub fn session(&self, session_id: SessionId) -> Result<WorkflowSession<P>, Error> {
        validate_session_id(session_id)?;
        Ok(WorkflowSession {
            workflow: self.clone(),
            session_id,
        })
    }

    pub async fn create_session(
        &self,
        name: impl Into<String>,
        session_type: impl Into<String>,
        get_or_create: bool,
        options: RequestOptions,
    ) -> Result<WorkflowSession<P>, Error> {
        let handle = self
            .inner
            .binding
            .create_session(
                self.workflow_id,
                name.into(),
                session_type.into(),
                get_or_create,
                options,
            )
            .await?;
        if handle.workflow_id != self.workflow_id {
            return Err(Error::contract(
                "Session creation returned another Workflow identity",
                None,
            ));
        }
        self.session(handle.session_id)
    }

    pub async fn start(
        &self,
        start: WorkflowStart,
        options: MutationOptions,
    ) -> Result<WorkflowRun, Error> {
        if let Some(session_id) = start.session_id {
            validate_session_id(session_id)?;
        }
        let request_id = options.request().request_id().clone();
        let expected_session_id = start.session_id;
        let handle = self
            .inner
            .binding
            .start_workflow(
                self.workflow_id,
                start.session_id,
                start.input,
                start.bindings,
                options,
            )
            .await?;
        validate_started_handle(&handle, self.workflow_id, expected_session_id, request_id)?;
        WorkflowRun::attached(self.inner.clone(), handle, None)
    }

    pub fn attach(
        &self,
        handle: WorkflowRunHandle,
        after: Option<EventCursor>,
    ) -> Result<WorkflowRun, Error> {
        if handle.workflow_id != self.workflow_id {
            return Err(Error::configuration(
                "workflow_run.workflow_id",
                "run handle does not belong to the selected Workflow",
            ));
        }
        WorkflowRun::attached(self.inner.clone(), handle, after)
    }
}

/// A convenience selector for starting one Workflow in an existing Session.
pub struct WorkflowSession<P: Principal> {
    workflow: Workflow<P>,
    session_id: SessionId,
}

impl<P: Principal> Clone for WorkflowSession<P> {
    fn clone(&self) -> Self {
        Self {
            workflow: self.workflow.clone(),
            session_id: self.session_id,
        }
    }
}

impl<P: Principal> WorkflowSession<P> {
    pub fn id(&self) -> SessionId {
        self.session_id
    }

    pub fn workflow_id(&self) -> WorkflowId {
        self.workflow.id()
    }

    pub async fn snapshot(&self, options: RequestOptions) -> Result<SessionSnapshot, Error> {
        let request_id = options.request_id().clone();
        let snapshot = self
            .workflow
            .inner
            .binding
            .load_session(self.session_id, options)
            .await?;
        if snapshot.session_id != self.session_id || snapshot.workflow_id != self.workflow.id() {
            return Err(Error::contract(
                "Session snapshot does not match the selected Workflow Session",
                Some(request_id),
            ));
        }
        Ok(snapshot)
    }

    pub async fn start(
        &self,
        input: WorkflowInput,
        options: MutationOptions,
    ) -> Result<WorkflowRun, Error> {
        self.workflow
            .start(
                WorkflowStart::new(input).in_session(self.session_id),
                options,
            )
            .await
    }

    pub async fn send(
        &self,
        content: impl Into<String>,
        options: MutationOptions,
    ) -> Result<WorkflowRun, Error> {
        self.workflow
            .start(
                WorkflowStart::message(content).in_session(self.session_id),
                options,
            )
            .await
    }

    pub async fn start_with_runtime_bindings(
        &self,
        input: WorkflowInput,
        bindings: RuntimeBindings,
        options: MutationOptions,
    ) -> Result<WorkflowRun, Error> {
        self.workflow
            .start(
                WorkflowStart::new(input)
                    .in_session(self.session_id)
                    .with_bindings(bindings),
                options,
            )
            .await
    }

    /// Continue the Session's resumable Workflow, or start a new run when the
    /// Session has no active non-terminal instance.
    pub async fn start_or_signal_with_runtime_bindings(
        &self,
        input: WorkflowInput,
        bindings: RuntimeBindings,
        options: MutationOptions,
    ) -> Result<WorkflowRun, Error> {
        let session = self.snapshot(RequestOptions::new()).await?;
        if let Some(workflow_instance_id) = session.active_workflow_instance_id {
            let run = self
                .reattach_active(workflow_instance_id, None, RequestOptions::new())
                .await?;
            if !run
                .snapshot(RequestOptions::new())
                .await?
                .state
                .is_terminal()
            {
                match run
                    .signal(WorkflowSignal::Input(input.clone()), options.clone())
                    .await
                {
                    Ok(_) => return Ok(run),
                    Err(error) if error.application_code() == Some("not_found") => {}
                    Err(error) => return Err(error),
                }
            }
        }

        self.start_with_runtime_bindings(input, bindings, options)
            .await
    }

    /// Signal an adapter-validated active run, falling back to a new Session
    /// run only when the transient runner is no longer available.
    pub async fn signal_active_or_start(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        input: WorkflowInput,
        bindings: RuntimeBindings,
        options: MutationOptions,
    ) -> Result<WorkflowRun, Error> {
        let run = self
            .reattach_active(workflow_instance_id, None, RequestOptions::new())
            .await?;
        match run
            .signal(WorkflowSignal::Input(input.clone()), options.clone())
            .await
        {
            Ok(_) => Ok(run),
            Err(error) if error.application_code() == Some("not_found") => {
                self.start_with_runtime_bindings(input, bindings, options)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    pub async fn request_compaction_with_runtime_bindings(
        &self,
        bindings: RuntimeBindings,
        options: MutationOptions,
    ) -> Result<WorkflowRun, Error> {
        let session = self.snapshot(RequestOptions::new()).await?;
        if let Some(workflow_instance_id) = session.active_workflow_instance_id {
            let run = self
                .reattach_active(workflow_instance_id, None, RequestOptions::new())
                .await?;
            run.signal(WorkflowSignal::RequestCompaction, options)
                .await?;
            return Ok(run);
        }

        let input = WorkflowInput::Conversation(ConversationInput {
            business_type: None,
            role: "user".to_string(),
            content: Some(Value::String("/compact".to_string())),
            reasoning_content: None,
            tool_calls: None,
            tool_responses: None,
            metadata: Map::from_iter([(
                "control".to_string(),
                serde_json::json!({"command": "compact"}),
            )]),
        });
        self.start_with_runtime_bindings(input, bindings, options)
            .await
    }

    async fn reattach_active(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        after: Option<EventCursor>,
        options: RequestOptions,
    ) -> Result<WorkflowRun, Error> {
        let request_id = options.request_id().clone();
        let snapshot = self
            .workflow
            .inner
            .binding
            .workflow_snapshot(workflow_instance_id, options)
            .await?;
        if snapshot.workflow_id != self.workflow.id()
            || snapshot.workflow_instance_id != workflow_instance_id
            || snapshot.session_id != Some(self.session_id)
        {
            return Err(Error::contract(
                "active Workflow snapshot does not match the selected Session",
                Some(request_id),
            ));
        }
        let subscription_anchor = after.or(snapshot.latest_cursor).ok_or_else(|| {
            Error::contract(
                "active Workflow snapshot has no canonical cursor",
                Some(request_id),
            )
        })?;
        WorkflowRun::attached(
            self.workflow.inner.clone(),
            WorkflowRunHandle {
                workflow_id: snapshot.workflow_id,
                workflow_instance_id: snapshot.workflow_instance_id,
                root_workflow_instance_id: snapshot.root_workflow_instance_id,
                session_id: snapshot.session_id,
                input_message_id: None,
                subscription_anchor: subscription_anchor.clone(),
            },
            Some(subscription_anchor),
        )
    }
}

#[derive(Clone)]
pub struct WorkflowRun {
    inner: Arc<ClientInner>,
    handle: WorkflowRunHandle,
    observation: Arc<Mutex<ObservationState>>,
}

#[derive(Default)]
struct ObservationState {
    cursor: Option<EventCursor>,
    seen: HashSet<EventId>,
}

impl WorkflowRun {
    fn attached(
        inner: Arc<ClientInner>,
        handle: WorkflowRunHandle,
        after: Option<EventCursor>,
    ) -> Result<Self, Error> {
        validate_attached_handle(&handle)?;
        let cursor = Some(after.unwrap_or_else(|| handle.subscription_anchor.clone()));
        Ok(Self {
            inner,
            handle,
            observation: Arc::new(Mutex::new(ObservationState {
                cursor,
                seen: HashSet::new(),
            })),
        })
    }

    pub fn handle(&self) -> &WorkflowRunHandle {
        &self.handle
    }

    pub fn workflow_id(&self) -> WorkflowId {
        self.handle.workflow_id
    }

    pub fn workflow_instance_id(&self) -> WorkflowInstanceId {
        self.handle.workflow_instance_id
    }

    pub fn root_workflow_instance_id(&self) -> RootWorkflowInstanceId {
        self.handle.root_workflow_instance_id
    }

    pub fn session_id(&self) -> Option<SessionId> {
        self.handle.session_id
    }

    pub fn input_message_id(&self) -> Option<MessageId> {
        self.handle.input_message_id
    }

    pub fn subscription_anchor(&self) -> &EventCursor {
        &self.handle.subscription_anchor
    }

    pub fn cursor(&self) -> EventCursor {
        self.observation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .cursor
            .clone()
            .unwrap_or_else(|| self.handle.subscription_anchor.clone())
    }

    pub async fn snapshot(&self, options: RequestOptions) -> Result<WorkflowSnapshot, Error> {
        let request_id = options.request_id().clone();
        let snapshot = self
            .inner
            .binding
            .workflow_snapshot(self.handle.workflow_instance_id, options)
            .await?;
        if snapshot.workflow_id != self.handle.workflow_id
            || snapshot.workflow_instance_id != self.handle.workflow_instance_id
            || snapshot.root_workflow_instance_id != self.handle.root_workflow_instance_id
        {
            return Err(Error::contract(
                "Workflow snapshot identities do not match the attached run",
                Some(request_id),
            ));
        }
        Ok(snapshot)
    }

    pub async fn signal(
        &self,
        signal: WorkflowSignal,
        options: MutationOptions,
    ) -> Result<CommandAck, Error> {
        let request_id = options.request().request_id().clone();
        let ack = self
            .inner
            .binding
            .signal_workflow(
                self.handle.workflow_instance_id,
                signal.into_runtime()?,
                options,
            )
            .await?;
        validate_ack(&ack, self.handle.workflow_instance_id, request_id)?;
        Ok(ack)
    }

    pub async fn respond(
        &self,
        suspension: SuspensionHandle,
        response: SuspensionResponse,
        options: MutationOptions,
    ) -> Result<CommandAck, Error> {
        respond_to_suspension(
            &self.inner,
            self.handle.workflow_instance_id,
            suspension,
            response,
            options,
        )
        .await
    }

    pub async fn terminate(
        &self,
        reason: Option<String>,
        options: MutationOptions,
    ) -> Result<CommandAck, Error> {
        self.terminate_with_timeout(reason, None, options).await
    }

    pub async fn terminate_with_timeout(
        &self,
        reason: Option<String>,
        wait_timeout_ms: Option<u64>,
        options: MutationOptions,
    ) -> Result<CommandAck, Error> {
        let request_id = options.request().request_id().clone();
        let ack = self
            .inner
            .binding
            .terminate_workflow(
                self.handle.workflow_instance_id,
                reason,
                wait_timeout_ms,
                options,
            )
            .await?;
        validate_ack(&ack, self.handle.workflow_instance_id, request_id)?;
        Ok(ack)
    }

    /// Observe canonical events strictly after the last event accepted by this handle.
    pub async fn events(&self, options: RequestOptions) -> Result<WorkflowEventStream, Error> {
        let after = self.cursor();
        let request_id = options.request_id().clone();
        let stream = self
            .inner
            .binding
            .subscribe_workflow(self.handle.workflow_instance_id, Some(after), options)
            .await?;
        let observation = self.observation.clone();
        let root_workflow_instance_id = self.handle.root_workflow_instance_id;
        Ok(Box::pin(stream.filter_map(move |item| {
            let accepted = match item {
                Ok(event) => {
                    if event.event_id.as_ref().trim().is_empty()
                        || event.cursor.as_ref().trim().is_empty()
                        || event.root_workflow_instance_id != root_workflow_instance_id
                    {
                        Some(Err(Error::contract(
                            "Workflow event identities do not match the attached run",
                            Some(request_id.clone()),
                        )))
                    } else {
                        let mut state = observation
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        state.cursor = Some(event.cursor.clone());
                        state
                            .seen
                            .insert(event.event_id.clone())
                            .then_some(Ok(event))
                    }
                }
                Err(error) => Some(Err(error)),
            };
            future::ready(accepted)
        })))
    }

    /// Wait until the canonical event sequence reaches one Workflow boundary.
    pub async fn wait(&self, options: RequestOptions) -> Result<WorkflowRunResult, Error> {
        let request_id = options.request_id().clone();
        let mut events = self.events(options).await?;
        let mut projector = WorkflowResultProjector::new(self.handle.clone());
        while let Some(event) = events.next().await {
            let event = event?;
            match projector.observe(&event) {
                Ok(Some(result)) => return Ok(result),
                Ok(None) => {}
                Err(error) => {
                    return Err(Error::contract(error.message, Some(request_id)));
                }
            }
        }
        Err(Error::Transport(TransportFailure {
            kind: TransportKind::Stream,
            message: "Workflow event stream ended before a result boundary".to_string(),
            request_id: Some(request_id),
            retryable: true,
        }))
    }

    pub async fn wait_to_boundary(
        &self,
        options: RequestOptions,
    ) -> Result<WorkflowRunResult, Error> {
        self.wait(options).await
    }
}

fn validate_workflow_id(workflow_id: WorkflowId) -> Result<(), Error> {
    if workflow_id.0 == 0 {
        return Err(Error::configuration(
            "workflow_id",
            "Workflow identity must be positive",
        ));
    }
    Ok(())
}

fn validate_session_id(session_id: SessionId) -> Result<(), Error> {
    if session_id.0 == 0 {
        return Err(Error::configuration(
            "session_id",
            "Session identity must be positive",
        ));
    }
    Ok(())
}

fn validate_attached_handle(handle: &WorkflowRunHandle) -> Result<(), Error> {
    validate_workflow_id(handle.workflow_id)?;
    if handle.workflow_instance_id.0 == 0 || handle.root_workflow_instance_id.0 == 0 {
        return Err(Error::configuration(
            "workflow_run",
            "Workflow Instance identities must be positive",
        ));
    }
    if handle.subscription_anchor.as_ref().trim().is_empty() {
        return Err(Error::configuration(
            "workflow_run.subscription_anchor",
            "subscription anchor must not be empty",
        ));
    }
    if let Some(session_id) = handle.session_id {
        validate_session_id(session_id)?;
    }
    Ok(())
}

fn validate_started_handle(
    handle: &WorkflowRunHandle,
    workflow_id: WorkflowId,
    session_id: Option<SessionId>,
    request_id: kish_lingshu_runtime_contract::RequestId,
) -> Result<(), Error> {
    if handle.workflow_id != workflow_id || handle.session_id != session_id {
        return Err(Error::contract(
            "Workflow start response does not match the selected Workflow or Session",
            Some(request_id),
        ));
    }
    validate_attached_handle(handle).map_err(|error| match error {
        Error::Configuration(error) => Error::contract(error.to_string(), Some(request_id)),
        error => error,
    })
}

fn validate_ack(
    ack: &CommandAck,
    workflow_instance_id: WorkflowInstanceId,
    request_id: kish_lingshu_runtime_contract::RequestId,
) -> Result<(), Error> {
    if ack.workflow_instance_id != workflow_instance_id {
        return Err(Error::contract(
            "Workflow command acknowledgement identifies another Workflow Instance",
            Some(request_id),
        ));
    }
    if ack
        .cursor
        .as_ref()
        .is_some_and(|cursor| cursor.as_ref().trim().is_empty())
    {
        return Err(Error::contract(
            "Workflow command acknowledgement contains an empty cursor",
            Some(request_id),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kish_lingshu_runtime_contract::{
        test_support::ContractProductRuntimeFixture, InvocationSource, PrincipalKind,
        TrustedContextFactory,
    };
    use serde_json::json;

    use crate::{ClientBuilder, ClientConfig, ServiceCredential};

    fn client() -> crate::Client<crate::ServicePrincipal> {
        let fixture = ContractProductRuntimeFixture::default();
        ClientBuilder::new(ClientConfig::in_process())
            .service_credential(
                ServiceCredential::new("contract-app", "workflow-test-secret").unwrap(),
            )
            .bind_runtime(
                fixture.facade,
                TrustedContextFactory::new(
                    "contract-service",
                    PrincipalKind::Service,
                    Some("contract-app".to_string()),
                    InvocationSource::EmbeddedSdk,
                )
                .unwrap(),
            )
            .unwrap()
    }

    fn mutation(key: &str) -> MutationOptions {
        MutationOptions::new(key).unwrap()
    }

    #[test]
    fn one_client_selects_and_starts_multiple_workflows() {
        futures::executor::block_on(async {
            let client = client();
            let workflows = client.workflows();
            let first = workflows
                .start(
                    WorkflowId(41),
                    WorkflowStart::structured(json!({"workflow": 41})),
                    mutation("workflow/41/start"),
                )
                .await
                .unwrap();
            let second = workflows
                .select(WorkflowId(42))
                .unwrap()
                .session(SessionId(7))
                .unwrap()
                .send("hello", mutation("workflow/42/session/7/start"))
                .await
                .unwrap();

            assert_eq!(first.workflow_id(), WorkflowId(41));
            assert_eq!(first.session_id(), None);
            assert_eq!(second.workflow_id(), WorkflowId(42));
            assert_eq!(second.session_id(), Some(SessionId(7)));
            assert_ne!(first.workflow_instance_id(), second.workflow_instance_id());
        });
    }

    #[test]
    fn attach_does_not_start_another_workflow_and_preserves_cursor() {
        futures::executor::block_on(async {
            let client = client();
            let workflow = client.workflows().select(WorkflowId(42)).unwrap();
            let started = workflow
                .start(
                    WorkflowStart::structured(json!({"prompt": "start"})),
                    mutation("workflow/42/start"),
                )
                .await
                .unwrap();
            let handle = started.handle().clone();
            let after = EventCursor::from("contract-9001:7");
            let attached = workflow
                .attach(handle.clone(), Some(after.clone()))
                .unwrap();

            assert_eq!(attached.handle(), &handle);
            assert_eq!(attached.cursor(), after);
        });
    }

    #[test]
    fn start_replay_returns_the_original_run_and_changed_input_conflicts() {
        futures::executor::block_on(async {
            let client = client();
            let workflow = client.workflows().select(WorkflowId(42)).unwrap();
            let first = workflow
                .start(
                    WorkflowStart::structured(json!({"prompt": "same"})),
                    mutation("workflow/42/replay"),
                )
                .await
                .unwrap();
            let replay = workflow
                .start(
                    WorkflowStart::structured(json!({"prompt": "same"})),
                    mutation("workflow/42/replay"),
                )
                .await
                .unwrap();
            assert_eq!(replay.handle(), first.handle());

            let error = match workflow
                .start(
                    WorkflowStart::structured(json!({"prompt": "changed"})),
                    mutation("workflow/42/replay"),
                )
                .await
            {
                Ok(_) => panic!("changed input unexpectedly replayed the original start"),
                Err(error) => error,
            };
            assert!(matches!(
                error,
                Error::Application(failure) if failure.problem.code.as_str() == "conflict"
            ));
        });
    }

    #[test]
    fn wait_projects_all_typed_workflow_boundaries() {
        futures::executor::block_on(async {
            let client = client();
            let workflow = client.workflows().select(WorkflowId(42)).unwrap();

            let completed = workflow
                .start(
                    WorkflowStart::structured(json!({"case": "completed"})),
                    mutation("workflow/42/completed/start"),
                )
                .await
                .unwrap();
            completed
                .signal(
                    WorkflowSignal::business("contract_complete", json!({"answer": 42})),
                    mutation("workflow/42/completed/signal"),
                )
                .await
                .unwrap();
            assert!(matches!(
                completed.wait(RequestOptions::new()).await.unwrap(),
                WorkflowRunResult::Completed { output, .. } if output == json!({"answer": 42})
            ));

            let failed = workflow
                .start(
                    WorkflowStart::structured(json!({"case": "failed"})),
                    mutation("workflow/42/failed/start"),
                )
                .await
                .unwrap();
            failed
                .signal(
                    WorkflowSignal::business("contract_fail", json!({"tool": "Search"})),
                    mutation("workflow/42/failed/signal"),
                )
                .await
                .unwrap();
            assert!(matches!(
                failed.wait(RequestOptions::new()).await.unwrap(),
                WorkflowRunResult::Failed { failure, .. } if failure.code == "contract_failure"
            ));

            let suspended = workflow
                .start(
                    WorkflowStart::structured(json!({"case": "suspended"})),
                    mutation("workflow/42/suspended/start"),
                )
                .await
                .unwrap();
            suspended
                .signal(
                    WorkflowSignal::business("contract_suspend", json!({"question": "continue?"})),
                    mutation("workflow/42/suspended/signal"),
                )
                .await
                .unwrap();
            assert!(matches!(
                suspended.wait(RequestOptions::new()).await.unwrap(),
                WorkflowRunResult::Suspended { .. }
            ));

            let terminated = workflow
                .start(
                    WorkflowStart::structured(json!({"case": "terminated"})),
                    mutation("workflow/42/terminated/start"),
                )
                .await
                .unwrap();
            terminated
                .terminate(
                    Some("test complete".to_string()),
                    mutation("workflow/42/terminated/terminate"),
                )
                .await
                .unwrap();
            assert!(matches!(
                terminated.wait(RequestOptions::new()).await.unwrap(),
                WorkflowRunResult::Terminated { .. }
            ));
        });
    }

    #[test]
    fn event_identity_deduplication_survives_stream_reattachment() {
        futures::executor::block_on(async {
            let client = client();
            let run = client
                .workflows()
                .start(
                    WorkflowId(42),
                    WorkflowStart::structured(json!({"case": "deduplication"})),
                    mutation("workflow/42/deduplication/start"),
                )
                .await
                .unwrap();

            let initial = run
                .events(RequestOptions::new())
                .await
                .unwrap()
                .collect::<Vec<_>>()
                .await;
            assert!(initial.into_iter().all(|event| event.is_ok()));

            let ack = run
                .signal(
                    WorkflowSignal::business("contract_duplicate_events", Value::Null),
                    mutation("workflow/42/deduplication/signal"),
                )
                .await
                .unwrap();
            let mut first_connection = run.events(RequestOptions::new()).await.unwrap();
            let first = first_connection.next().await.unwrap().unwrap();
            assert!(matches!(
                first.kind,
                WorkflowEventKind::Extension { ref kind, .. }
                    if kind == "contract_duplicate_source"
            ));
            drop(first_connection);

            let reattached = run
                .events(RequestOptions::new())
                .await
                .unwrap()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(reattached.len(), 1);
            assert!(matches!(
                reattached[0].kind,
                WorkflowEventKind::Extension { ref kind, .. }
                    if kind == "contract_after_duplicate"
            ));
            assert_eq!(run.cursor(), ack.cursor.unwrap());
        });
    }

    #[test]
    fn unknown_events_remain_observable_and_advance_the_cursor() {
        futures::executor::block_on(async {
            let client = client();
            let run = client
                .workflows()
                .start(
                    WorkflowId(42),
                    WorkflowStart::structured(json!({"case": "unknown-event"})),
                    mutation("workflow/42/unknown/start"),
                )
                .await
                .unwrap();
            let ack = run
                .signal(
                    WorkflowSignal::business("contract_events", Value::Null),
                    mutation("workflow/42/unknown/signal"),
                )
                .await
                .unwrap();

            let events = run
                .events(RequestOptions::new())
                .await
                .unwrap()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert!(events.iter().any(|event| matches!(
                event.kind,
                WorkflowEventKind::Extension { ref kind, .. }
                    if kind == "contract_future_event"
            )));
            assert_eq!(run.cursor(), ack.cursor.unwrap());
        });
    }

    #[test]
    fn malformed_workflow_boundaries_are_contract_violations() {
        futures::executor::block_on(async {
            for (case, signal) in [
                ("missing-output", "contract_complete_without_output"),
                ("multiple-outputs", "contract_multiple_outputs"),
            ] {
                let client = client();
                let run = client
                    .workflows()
                    .start(
                        WorkflowId(42),
                        WorkflowStart::structured(json!({"case": case})),
                        mutation(&format!("workflow/42/{case}/start")),
                    )
                    .await
                    .unwrap();
                run.signal(
                    WorkflowSignal::business(signal, Value::Null),
                    mutation(&format!("workflow/42/{case}/signal")),
                )
                .await
                .unwrap();

                assert!(matches!(
                    run.wait(RequestOptions::new()).await,
                    Err(Error::ContractViolation(_))
                ));
            }
        });
    }

    #[test]
    fn public_builders_reject_empty_or_cross_workflow_identities() {
        let client = client();
        assert!(client.workflows().select(WorkflowId(0)).is_err());
        assert!(client
            .workflows()
            .select(WorkflowId(42))
            .unwrap()
            .session(SessionId(0))
            .is_err());
        assert!(WorkflowSignal::business(" ", Value::Null)
            .into_runtime()
            .is_err());
    }
}

async fn respond_to_suspension(
    inner: &ClientInner,
    workflow_instance_id: WorkflowInstanceId,
    suspension: SuspensionHandle,
    response: SuspensionResponse,
    options: MutationOptions,
) -> Result<CommandAck, Error> {
    if workflow_instance_id.0 == 0 {
        return Err(Error::configuration(
            "workflow_instance_id",
            "must be positive",
        ));
    }
    let request_id = options.request().request_id().clone();
    let ack = inner
        .binding
        .signal_workflow(
            workflow_instance_id,
            RuntimeWorkflowSignal::SuspensionResponse {
                suspension,
                response: response.into_runtime(),
            },
            options,
        )
        .await?;
    validate_ack(&ack, workflow_instance_id, request_id)?;
    Ok(ack)
}
