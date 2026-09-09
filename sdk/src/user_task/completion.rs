use std::{
    any::{type_name, Any, TypeId},
    collections::{BTreeSet, HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use kish_lingshu_runtime_contract::{
    UserTaskCompletionInvocationV1, UserTaskCompletionOutcomeV1, UserTaskCompletionProblem,
    USER_TASK_COMPLETION_CONTRACT_VERSION, USER_TASK_COMPLETION_PATH,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[cfg(feature = "user-task-completion-http")]
use axum::{
    extract::{DefaultBodyLimit, Json, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
#[cfg(feature = "user-task-completion-http")]
use chrono::Utc;

pub use kish_lingshu_runtime_contract::{
    UserTaskCompletionDetail, UserTaskCompletionTaskV1, UserTaskCompletionWorkflowV1,
    USER_TASK_COMPLETION_IDEMPOTENCY_HEADER, USER_TASK_COMPLETION_INVOCATION_ID_HEADER,
};

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
pub type CompletionHandlerFuture =
    Pin<Box<dyn Future<Output = CompletionResult<Value>> + Send + 'static>>;

#[doc(hidden)]
pub type CompletionHandlerInvokeFn =
    fn(Arc<dyn Any + Send + Sync>, CompletionContext, Value) -> CompletionHandlerFuture;

#[doc(hidden)]
pub struct CompletionHandlerDescriptor {
    pub task_type: &'static str,
    pub handler_type_id: fn() -> TypeId,
    pub handler_type_name: fn() -> &'static str,
    pub diagnostic_name: &'static str,
    pub submission_schema: fn() -> Result<Value, String>,
    pub output_schema: fn() -> Result<Value, String>,
    pub invoke: CompletionHandlerInvokeFn,
}

inventory::collect!(CompletionHandlerDescriptor);

#[derive(Debug, thiserror::Error)]
pub enum CompletionRegistryError {
    #[error("Application ID must be 1..=255 non-control characters")]
    InvalidApplicationId,
    #[error("invalid task type {task_type}: must be 1..=255 visible ASCII bytes")]
    InvalidTaskType { task_type: String },
    #[error("duplicate completion handler for task type {task_type}: {first} and {second}")]
    DuplicateTaskType {
        task_type: String,
        first: String,
        second: String,
    },
    #[error("missing completion Handler binding for {handler_type}")]
    MissingBinding { handler_type: String },
    #[error("duplicate completion Handler binding for {handler_type}")]
    DuplicateBinding { handler_type: String },
    #[error("unused completion Handler binding for {handler_type}")]
    UnusedBinding { handler_type: String },
    #[error("completion source declaration {declaration} failed: {message}")]
    Descriptor {
        declaration: String,
        message: String,
    },
}

struct BoundCompletionHandler {
    type_name: &'static str,
    instance: Arc<dyn Any + Send + Sync>,
}

struct LinkedCompletionHandler {
    instance: Arc<dyn Any + Send + Sync>,
    invoke: CompletionHandlerInvokeFn,
}

pub struct CompletionRegistry {
    application_id: String,
    handlers: HashMap<String, LinkedCompletionHandler>,
}

impl CompletionRegistry {
    pub fn builder(
        application_id: impl Into<String>,
    ) -> Result<CompletionRegistryBuilder, CompletionRegistryError> {
        let application_id = application_id.into();
        if application_id.trim().is_empty()
            || application_id.len() > 255
            || application_id.chars().any(char::is_control)
        {
            return Err(CompletionRegistryError::InvalidApplicationId);
        }
        Ok(CompletionRegistryBuilder {
            application_id,
            bindings: HashMap::new(),
        })
    }

    pub fn application_id(&self) -> &str {
        &self.application_id
    }

    pub async fn dispatch(
        &self,
        invocation: UserTaskCompletionInvocationV1,
    ) -> UserTaskCompletionOutcomeV1 {
        let Some(handler) = self.handlers.get(&invocation.task.task_type) else {
            return CompletionError::failed(
                "unsupported_task_type",
                format!(
                    "no User Task completion handler is registered for {}",
                    invocation.task.task_type
                ),
            )
            .into_outcome();
        };
        let context = CompletionContext::new(&invocation);
        match (handler.invoke)(handler.instance.clone(), context, invocation.submission).await {
            Ok(output) => UserTaskCompletionOutcomeV1::Applied { output },
            Err(error) => error.into_outcome(),
        }
    }
}

pub struct CompletionRegistryBuilder {
    application_id: String,
    bindings: HashMap<TypeId, BoundCompletionHandler>,
}

impl CompletionRegistryBuilder {
    pub fn bind<T>(&mut self, handler: Arc<T>) -> Result<&mut Self, CompletionRegistryError>
    where
        T: Any + Send + Sync + 'static,
    {
        let handler_type_id = TypeId::of::<T>();
        let handler_type = type_name::<T>();
        if self.bindings.contains_key(&handler_type_id) {
            return Err(CompletionRegistryError::DuplicateBinding {
                handler_type: handler_type.to_string(),
            });
        }
        self.bindings.insert(
            handler_type_id,
            BoundCompletionHandler {
                type_name: handler_type,
                instance: handler,
            },
        );
        Ok(self)
    }

    pub fn build(self) -> Result<CompletionRegistry, CompletionRegistryError> {
        self.build_inner(false)
    }

    pub fn build_bound_handlers(self) -> Result<CompletionRegistry, CompletionRegistryError> {
        self.build_inner(true)
    }

    fn build_inner(
        self,
        bound_handlers_only: bool,
    ) -> Result<CompletionRegistry, CompletionRegistryError> {
        let mut descriptors = inventory::iter::<CompletionHandlerDescriptor>
            .into_iter()
            .collect::<Vec<_>>();
        if bound_handlers_only {
            descriptors
                .retain(|descriptor| self.bindings.contains_key(&(descriptor.handler_type_id)()));
        }
        descriptors.sort_by_key(|descriptor| (descriptor.task_type, descriptor.diagnostic_name));

        let mut handlers = HashMap::new();
        let mut handler_diagnostics = HashMap::<String, String>::new();
        let mut described_types = HashSet::new();
        for descriptor in descriptors {
            validate_task_type(descriptor.task_type)?;
            let handler_type_id = (descriptor.handler_type_id)();
            described_types.insert(handler_type_id);
            let binding = self.bindings.get(&handler_type_id).ok_or_else(|| {
                CompletionRegistryError::MissingBinding {
                    handler_type: (descriptor.handler_type_name)().to_string(),
                }
            })?;
            if let Some(first) = handler_diagnostics.insert(
                descriptor.task_type.to_string(),
                descriptor.diagnostic_name.to_string(),
            ) {
                return Err(CompletionRegistryError::DuplicateTaskType {
                    task_type: descriptor.task_type.to_string(),
                    first,
                    second: descriptor.diagnostic_name.to_string(),
                });
            }
            handlers.insert(
                descriptor.task_type.to_string(),
                LinkedCompletionHandler {
                    instance: binding.instance.clone(),
                    invoke: descriptor.invoke,
                },
            );
        }

        let mut unused = self
            .bindings
            .iter()
            .filter_map(|(type_id, binding)| {
                (!described_types.contains(type_id)).then_some(binding.type_name)
            })
            .collect::<Vec<_>>();
        unused.sort_unstable();
        if let Some(handler_type) = unused.first() {
            return Err(CompletionRegistryError::UnusedBinding {
                handler_type: (*handler_type).to_string(),
            });
        }

        Ok(CompletionRegistry {
            application_id: self.application_id,
            handlers,
        })
    }
}

fn validate_task_type(task_type: &str) -> Result<(), CompletionRegistryError> {
    if task_type.is_empty()
        || task_type.len() > 255
        || !task_type.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(CompletionRegistryError::InvalidTaskType {
            task_type: task_type.to_string(),
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionProducerMetadata {
    pub package_name: String,
    pub package_version: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionOperationDefinition {
    pub operation_key: String,
    pub method: String,
    pub path: String,
    pub contract_version: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionHandlerDefinition {
    pub task_type: String,
    pub submission_schema: Value,
    pub output_schema: Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionSourceContractV1 {
    pub manifest_version: String,
    pub producer: CompletionProducerMetadata,
    pub operation: CompletionOperationDefinition,
    pub handlers: Vec<CompletionHandlerDefinition>,
    pub contract_hash: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompletionSourceCatalog {
    handlers: Vec<CompletionHandlerDefinition>,
}

impl CompletionSourceCatalog {
    pub fn collect() -> Result<Self, CompletionRegistryError> {
        let mut descriptors = inventory::iter::<CompletionHandlerDescriptor>
            .into_iter()
            .collect::<Vec<_>>();
        descriptors.sort_by_key(|descriptor| (descriptor.task_type, descriptor.diagnostic_name));
        let mut seen = BTreeSet::new();
        let mut handlers = Vec::with_capacity(descriptors.len());
        for descriptor in descriptors {
            validate_task_type(descriptor.task_type)?;
            if !seen.insert(descriptor.task_type) {
                let first = inventory::iter::<CompletionHandlerDescriptor>
                    .into_iter()
                    .find(|candidate| candidate.task_type == descriptor.task_type)
                    .map(|candidate| candidate.diagnostic_name)
                    .unwrap_or("unknown");
                return Err(CompletionRegistryError::DuplicateTaskType {
                    task_type: descriptor.task_type.to_string(),
                    first: first.to_string(),
                    second: descriptor.diagnostic_name.to_string(),
                });
            }
            let submission_schema = (descriptor.submission_schema)().map_err(|message| {
                CompletionRegistryError::Descriptor {
                    declaration: descriptor.diagnostic_name.to_string(),
                    message,
                }
            })?;
            let output_schema = (descriptor.output_schema)().map_err(|message| {
                CompletionRegistryError::Descriptor {
                    declaration: descriptor.diagnostic_name.to_string(),
                    message,
                }
            })?;
            jsonschema::meta::validate(&submission_schema).map_err(|error| {
                CompletionRegistryError::Descriptor {
                    declaration: descriptor.diagnostic_name.to_string(),
                    message: format!("invalid submission schema: {error}"),
                }
            })?;
            jsonschema::meta::validate(&output_schema).map_err(|error| {
                CompletionRegistryError::Descriptor {
                    declaration: descriptor.diagnostic_name.to_string(),
                    message: format!("invalid output schema: {error}"),
                }
            })?;
            handlers.push(CompletionHandlerDefinition {
                task_type: descriptor.task_type.to_string(),
                submission_schema,
                output_schema,
            });
        }
        Ok(Self { handlers })
    }

    pub fn handlers(&self) -> &[CompletionHandlerDefinition] {
        &self.handlers
    }

    pub fn contract(
        &self,
        producer: CompletionProducerMetadata,
    ) -> Result<CompletionSourceContractV1, CompletionRegistryError> {
        let operation = CompletionOperationDefinition {
            operation_key: "user-task.complete.v1".to_string(),
            method: "POST".to_string(),
            path: USER_TASK_COMPLETION_PATH.to_string(),
            contract_version: USER_TASK_COMPLETION_CONTRACT_VERSION.to_string(),
        };
        let hash_material = serde_json::to_vec(&("1.0", &producer, &operation, &self.handlers))
            .map_err(|error| CompletionRegistryError::Descriptor {
                declaration: "completion source contract".to_string(),
                message: error.to_string(),
            })?;
        let contract_hash = Sha256::digest(hash_material)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Ok(CompletionSourceContractV1 {
            manifest_version: "1.0".to_string(),
            producer,
            operation,
            handlers: self.handlers.clone(),
            contract_hash,
        })
    }

    pub fn export_json(
        &self,
        producer: CompletionProducerMetadata,
    ) -> Result<Vec<u8>, CompletionRegistryError> {
        serde_json::to_vec(&self.contract(producer)?).map_err(|error| {
            CompletionRegistryError::Descriptor {
                declaration: "completion source contract".to_string(),
                message: error.to_string(),
            }
        })
    }
}

#[cfg(feature = "user-task-completion-http")]
#[derive(Debug, Clone)]
pub struct CompletionHttpConfig {
    pub maximum_request_bytes: usize,
}

#[cfg(feature = "user-task-completion-http")]
impl Default for CompletionHttpConfig {
    fn default() -> Self {
        Self {
            maximum_request_bytes: 1024 * 1024,
        }
    }
}

#[cfg(feature = "user-task-completion-http")]
pub struct CompletionHttpAdapter {
    registry: Arc<CompletionRegistry>,
    config: CompletionHttpConfig,
}

#[cfg(feature = "user-task-completion-http")]
impl CompletionHttpAdapter {
    pub fn new(registry: Arc<CompletionRegistry>) -> Self {
        Self {
            registry,
            config: CompletionHttpConfig::default(),
        }
    }

    pub fn with_config(mut self, config: CompletionHttpConfig) -> Self {
        self.config = config;
        self
    }

    /// Returns the canonical completion route. Service authentication composes outside it.
    pub fn router(self) -> Router {
        Router::new()
            .route(USER_TASK_COMPLETION_PATH, post(complete_user_task))
            .layer(DefaultBodyLimit::max(self.config.maximum_request_bytes))
            .with_state(self.registry)
    }
}

#[cfg(feature = "user-task-completion-http")]
async fn complete_user_task(
    State(registry): State<Arc<CompletionRegistry>>,
    headers: HeaderMap,
    invocation: Result<
        Json<UserTaskCompletionInvocationV1>,
        axum::extract::rejection::JsonRejection,
    >,
) -> Response {
    let Json(invocation) = match invocation {
        Ok(invocation) => invocation,
        Err(error) => {
            return protocol_error(
                StatusCode::BAD_REQUEST,
                "invalid_completion_body",
                error.body_text(),
            )
        }
    };
    if let Err(response) = validate_invocation(&registry, &headers, &invocation) {
        return response;
    }
    let timeout = match (invocation.invocation_deadline - Utc::now()).to_std() {
        Ok(value) if !value.is_zero() => value,
        _ => return completion_deadline_exceeded(),
    };
    match tokio::time::timeout(timeout, registry.dispatch(invocation)).await {
        Ok(outcome) => Json(outcome).into_response(),
        Err(_) => completion_deadline_exceeded(),
    }
}

#[cfg(feature = "user-task-completion-http")]
fn validate_invocation(
    registry: &CompletionRegistry,
    headers: &HeaderMap,
    invocation: &UserTaskCompletionInvocationV1,
) -> Result<(), Response> {
    if invocation.contract_version != USER_TASK_COMPLETION_CONTRACT_VERSION {
        return Err(protocol_error(
            StatusCode::BAD_REQUEST,
            "unsupported_contract_version",
            "unsupported User Task completion contract version",
        ));
    }
    if invocation.application_id != registry.application_id() {
        return Err(protocol_error(
            StatusCode::NOT_FOUND,
            "completion_handler_not_registered",
            "no completion handler registry exists for the Application",
        ));
    }
    if invocation.invocation_id.trim().is_empty()
        || invocation.idempotency_key.trim().is_empty()
        || invocation.actor_user_id.trim().is_empty()
        || invocation.task.task_type.trim().is_empty()
        || invocation.workflow.workflow_id.0 == 0
        || invocation.workflow.workflow_instance_id.0 == 0
        || invocation.workflow.flow_node_id.trim().is_empty()
        || invocation.task.staged_revision.get()
            != invocation.task.expected_revision.get().saturating_add(1)
    {
        return Err(protocol_error(
            StatusCode::BAD_REQUEST,
            "invalid_completion_identity",
            "User Task completion identity is incomplete or inconsistent",
        ));
    }
    let idempotency_key = required_header(
        headers,
        kish_lingshu_runtime_contract::USER_TASK_COMPLETION_IDEMPOTENCY_HEADER,
    )?;
    if idempotency_key != invocation.idempotency_key {
        return Err(protocol_error(
            StatusCode::BAD_REQUEST,
            "idempotency_key_mismatch",
            "Idempotency-Key does not match the invocation body",
        ));
    }
    let invocation_id = required_header(
        headers,
        kish_lingshu_runtime_contract::USER_TASK_COMPLETION_INVOCATION_ID_HEADER,
    )?;
    if invocation_id != invocation.invocation_id {
        return Err(protocol_error(
            StatusCode::BAD_REQUEST,
            "invocation_id_mismatch",
            "completion invocation header does not match the invocation body",
        ));
    }
    if invocation.invocation_deadline <= Utc::now() {
        return Err(completion_deadline_exceeded());
    }
    Ok(())
}

#[cfg(feature = "user-task-completion-http")]
fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String, Response> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            protocol_error(
                StatusCode::BAD_REQUEST,
                "missing_completion_header",
                format!("{name} header is required"),
            )
        })
}

#[cfg(feature = "user-task-completion-http")]
#[derive(Serialize)]
struct CompletionProtocolError {
    code: String,
    message: String,
}

#[cfg(feature = "user-task-completion-http")]
fn completion_deadline_exceeded() -> Response {
    protocol_error(
        StatusCode::REQUEST_TIMEOUT,
        "completion_deadline_exceeded",
        "User Task completion did not finish before its invocation deadline",
    )
}

#[cfg(feature = "user-task-completion-http")]
fn protocol_error(
    status: StatusCode,
    code: impl Into<String>,
    message: impl Into<String>,
) -> Response {
    (
        status,
        Json(CompletionProtocolError {
            code: bounded(code.into(), 128),
            message: bounded(message.into(), 1_024),
        }),
    )
        .into_response()
}

#[doc(hidden)]
pub mod __private {
    pub use super::{CompletionHandlerDescriptor, CompletionHandlerFuture};
    pub use inventory;
    pub use schemars;
    pub use serde_json;
}
