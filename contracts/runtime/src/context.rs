use serde::{Deserialize, Serialize};

use crate::{
    CorrelationId, IdempotencyKey, RequestDeadline, RequestId, RuntimeError, RuntimeResult,
    WorkflowId,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Service,
    User,
    Internal,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InvocationSource {
    Http,
    Cli,
    Acp,
    EmbeddedSdk,
    Internal,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TraceContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
}

/// Trusted identity and telemetry context for one logical Product Runtime call.
///
/// Product payloads must not be deserialized directly into this value. Remote
/// adapters reconstruct it after authentication and route resolution, while
/// in-process composition supplies it through a trusted context factory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RequestContext {
    actor_id: String,
    principal: PrincipalKind,
    application_id: Option<String>,
    source: InvocationSource,
    request_id: RequestId,
    correlation_id: CorrelationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    idempotency_key: Option<IdempotencyKey>,
    trace: Option<TraceContext>,
    deadline: Option<RequestDeadline>,
}

impl RequestContext {
    pub fn builder(
        actor_id: impl Into<String>,
        principal: PrincipalKind,
        source: InvocationSource,
        request_id: impl Into<RequestId>,
        correlation_id: impl Into<CorrelationId>,
    ) -> RequestContextBuilder {
        RequestContextBuilder {
            actor_id: actor_id.into(),
            principal,
            application_id: None,
            source,
            request_id: request_id.into(),
            correlation_id: correlation_id.into(),
            idempotency_key: None,
            trace: None,
            deadline: None,
        }
    }

    pub fn actor_id(&self) -> &str {
        &self.actor_id
    }

    pub fn principal(&self) -> PrincipalKind {
        self.principal
    }

    pub fn application_id(&self) -> Option<&str> {
        self.application_id.as_deref()
    }

    pub fn source(&self) -> &InvocationSource {
        &self.source
    }

    pub fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    pub fn correlation_id(&self) -> &CorrelationId {
        &self.correlation_id
    }

    pub fn idempotency_key(&self) -> Option<&IdempotencyKey> {
        self.idempotency_key.as_ref()
    }

    /// Bind a mutation identity resolved by a trusted transport or SDK binding.
    pub fn with_idempotency_key(mut self, idempotency_key: IdempotencyKey) -> Self {
        self.idempotency_key = Some(idempotency_key);
        self
    }

    pub fn trace(&self) -> Option<&TraceContext> {
        self.trace.as_ref()
    }

    pub fn deadline(&self) -> Option<RequestDeadline> {
        self.deadline
    }
}

#[derive(Clone, Debug)]
pub struct RequestContextBuilder {
    actor_id: String,
    principal: PrincipalKind,
    application_id: Option<String>,
    source: InvocationSource,
    request_id: RequestId,
    correlation_id: CorrelationId,
    idempotency_key: Option<IdempotencyKey>,
    trace: Option<TraceContext>,
    deadline: Option<RequestDeadline>,
}

/// Factory configured by a trusted Adapter or in-process composition root.
///
/// The factory fixes authorization and routing identity once. Individual
/// operations may supply only their logical request identities and, for a
/// Workflow entrypoint, the selected Workflow. This prevents ordinary product
/// payloads from choosing an actor, principal kind, Application, or invocation
/// source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedContextFactory {
    actor_id: String,
    principal: PrincipalKind,
    application_id: Option<String>,
    source: InvocationSource,
}

impl TrustedContextFactory {
    pub fn new(
        actor_id: impl Into<String>,
        principal: PrincipalKind,
        application_id: Option<String>,
        source: InvocationSource,
    ) -> RuntimeResult<Self> {
        let actor_id = actor_id.into();
        validate_non_empty("actor_id", &actor_id)?;
        if application_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(RuntimeError::invalid_request(
                "application_id must not be empty when present",
            ));
        }
        Ok(Self {
            actor_id,
            principal,
            application_id,
            source,
        })
    }

    pub fn actor_id(&self) -> &str {
        &self.actor_id
    }

    pub fn principal(&self) -> PrincipalKind {
        self.principal
    }

    pub fn application_id(&self) -> Option<&str> {
        self.application_id.as_deref()
    }

    pub fn source(&self) -> &InvocationSource {
        &self.source
    }

    /// Construct one general Product Runtime context. Authority fields come
    /// only from this factory; the caller contributes logical operation IDs.
    pub fn request_context(
        &self,
        request_id: impl Into<RequestId>,
        correlation_id: impl Into<CorrelationId>,
    ) -> RuntimeResult<RequestContext> {
        let mut builder = RequestContext::builder(
            self.actor_id.clone(),
            self.principal,
            self.source.clone(),
            request_id,
            correlation_id,
        );
        if let Some(application_id) = &self.application_id {
            builder = builder.application_id(application_id.clone());
        }
        builder.build()
    }

    /// Construct one Workflow entrypoint context without storing Workflow
    /// identity in global Client or Adapter configuration.
    pub fn workflow_context(
        &self,
        workflow_id: WorkflowId,
        request_id: impl Into<RequestId>,
        correlation_id: impl Into<CorrelationId>,
    ) -> RuntimeResult<WorkflowContext> {
        Ok(WorkflowContext::new(
            self.request_context(request_id, correlation_id)?,
            workflow_id,
        ))
    }

    /// Add a per-operation Workflow selection to a request produced by this
    /// factory. This is useful when one logical operation first observes an
    /// existing instance and then conditionally starts a new one.
    pub fn workflow_context_from_request(
        &self,
        request: RequestContext,
        workflow_id: WorkflowId,
    ) -> RuntimeResult<WorkflowContext> {
        if request.actor_id() != self.actor_id
            || request.principal() != self.principal
            || request.application_id() != self.application_id.as_deref()
            || request.source() != &self.source
        {
            return Err(RuntimeError::invalid_request(
                "request context authority does not match trusted context factory",
            ));
        }
        Ok(WorkflowContext::new(request, workflow_id))
    }
}

impl RequestContextBuilder {
    pub fn application_id(mut self, application_id: impl Into<String>) -> Self {
        self.application_id = Some(application_id.into());
        self
    }

    pub fn trace(mut self, trace: TraceContext) -> Self {
        self.trace = Some(trace);
        self
    }

    pub fn idempotency_key(mut self, idempotency_key: IdempotencyKey) -> Self {
        self.idempotency_key = Some(idempotency_key);
        self
    }

    pub fn deadline(mut self, deadline: impl Into<RequestDeadline>) -> Self {
        self.deadline = Some(deadline.into());
        self
    }

    pub fn build(self) -> RuntimeResult<RequestContext> {
        validate_non_empty("actor_id", &self.actor_id)?;
        validate_non_empty("request_id", self.request_id.as_ref())?;
        validate_non_empty("correlation_id", self.correlation_id.as_ref())?;
        if self
            .application_id
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(RuntimeError::invalid_request(
                "application_id must not be empty when present",
            ));
        }
        if let Some(trace) = &self.trace {
            if trace
                .traceparent
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
                || trace
                    .tracestate
                    .as_deref()
                    .is_some_and(|value| value.trim().is_empty())
            {
                return Err(RuntimeError::invalid_request(
                    "trace context values must not be empty when present",
                ));
            }
        }

        Ok(RequestContext {
            actor_id: self.actor_id,
            principal: self.principal,
            application_id: self.application_id,
            source: self.source,
            request_id: self.request_id,
            correlation_id: self.correlation_id,
            idempotency_key: self.idempotency_key,
            trace: self.trace,
            deadline: self.deadline,
        })
    }
}

/// A trusted request context plus the Workflow selected for one entry operation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowContext {
    request: RequestContext,
    workflow_id: WorkflowId,
}

impl WorkflowContext {
    pub fn new(request: RequestContext, workflow_id: WorkflowId) -> Self {
        Self {
            request,
            workflow_id,
        }
    }

    pub fn request(&self) -> &RequestContext {
        &self.request
    }

    pub fn into_request(self) -> RequestContext {
        self.request
    }

    pub fn workflow_id(&self) -> WorkflowId {
        self.workflow_id
    }
}

fn validate_non_empty(field: &str, value: &str) -> RuntimeResult<()> {
    if value.trim().is_empty() {
        return Err(RuntimeError::invalid_request(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_context_requires_logical_request_identity() {
        let error = RequestContext::builder(
            "user-1",
            PrincipalKind::User,
            InvocationSource::Http,
            "",
            "correlation-1",
        )
        .build()
        .unwrap_err();
        assert_eq!(error.message, "request_id must not be empty");
    }

    #[test]
    fn workflow_context_keeps_selection_out_of_general_request_context() {
        let request = RequestContext::builder(
            "service-1",
            PrincipalKind::Service,
            InvocationSource::EmbeddedSdk,
            "request-1",
            "correlation-1",
        )
        .application_id("orders")
        .build()
        .unwrap();
        let workflow = WorkflowContext::new(request, WorkflowId(42));
        assert_eq!(workflow.workflow_id(), WorkflowId(42));
        assert_eq!(workflow.request().principal(), PrincipalKind::Service);
    }

    #[test]
    fn trusted_factory_fixes_authority_but_selects_workflow_per_operation() {
        let factory = TrustedContextFactory::new(
            "user-1",
            PrincipalKind::User,
            Some("orders".to_string()),
            InvocationSource::EmbeddedSdk,
        )
        .unwrap();

        let request = factory
            .request_context("request-1", "correlation-1")
            .unwrap();
        let workflow = factory
            .workflow_context(WorkflowId(42), "request-2", "correlation-1")
            .unwrap();
        let wrapped = factory
            .workflow_context_from_request(request.clone(), WorkflowId(43))
            .unwrap();

        assert_eq!(request.actor_id(), "user-1");
        assert_eq!(request.principal(), PrincipalKind::User);
        assert_eq!(request.application_id(), Some("orders"));
        assert_eq!(workflow.workflow_id(), WorkflowId(42));
        assert_eq!(workflow.request().actor_id(), request.actor_id());
        assert_eq!(workflow.request().principal(), request.principal());
        assert_eq!(
            workflow.request().application_id(),
            request.application_id()
        );
        assert_eq!(wrapped.workflow_id(), WorkflowId(43));
        assert_eq!(wrapped.request(), &request);
    }

    #[test]
    fn trusted_factory_rejects_invalid_fixed_authority() {
        let error = TrustedContextFactory::new(
            "user-1",
            PrincipalKind::User,
            Some(" ".to_string()),
            InvocationSource::EmbeddedSdk,
        )
        .unwrap_err();
        assert_eq!(
            error.message,
            "application_id must not be empty when present"
        );
    }
}
