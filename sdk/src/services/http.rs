use super::execution::{
    CallExecutionCore, CallExecutionError, CallReportError, CallReporter, RejectionKind,
};
use super::*;
use crate::{ServiceAuthError, ServiceConnection};
use axum::{
    extract::{DefaultBodyLimit, Json, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
#[cfg(feature = "service-event-http")]
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicUsize, RwLock},
};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::sync::watch;
#[cfg(feature = "service-event-http")]
use tokio::sync::Semaphore;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceRuntimeStatus {
    pub accepting: bool,
    pub call_accepting: bool,
    pub event_accepting: bool,
    pub active: usize,
    pub unconfirmed_results: u64,
}

struct HttpState {
    execution: Arc<CallExecutionCore>,
    #[cfg(feature = "service-event-http")]
    events: super::event::EventAdmission,
}
impl std::ops::Deref for HttpState {
    type Target = CallExecutionCore;
    fn deref(&self) -> &Self::Target {
        &self.execution
    }
}

/// Keep this handle alive alongside its Router. Dropping it cancels all managed
/// work; platform-owned Workflow waits retain recovery responsibility.
pub struct ServiceHttpAdapter {
    state: Arc<HttpState>,
    cancel: watch::Sender<bool>,
}

impl ServiceHttpAdapter {
    pub fn new(
        registry: Arc<ServiceRegistry>,
        connection: ServiceConnection,
        maximum_in_flight: u32,
    ) -> Result<Self, ServiceError> {
        let budget = crate::ServiceExecutionBudget::new(maximum_in_flight).map_err(|_| {
            ServiceError::rejected("invalid_service_host", "Invalid instance capacity")
        })?;
        Self::with_execution_budget(registry, connection, budget)
    }

    /// Share the same budget with existing Event Consumer adapters in this host.
    pub fn with_execution_budget(
        registry: Arc<ServiceRegistry>,
        connection: ServiceConnection,
        budget: crate::ServiceExecutionBudget,
    ) -> Result<Self, ServiceError> {
        let reporter = Arc::new(HttpCallReporter {
            connection: connection.clone(),
        });
        let (execution, cancel) = CallExecutionCore::new(registry, connection, budget, reporter)?;
        Ok(Self {
            state: Arc::new(HttpState {
                #[cfg(feature = "service-event-http")]
                events: super::event::EventAdmission {
                    cancel: execution.cancel.clone(),
                    connection: execution.connection.clone(),
                    memberships: Arc::new(RwLock::new(BTreeMap::new())),
                    active: Arc::new(AtomicUsize::new(0)),
                },
                execution,
            }),
            cancel,
        })
    }
    /// Register exactly the handlers bound to this adapter and bind admission to
    /// the resulting lease. Keep the returned handle alive beside the adapter.
    pub async fn enroll(
        &self,
        node_id: impl Into<String>,
        invocation_url: impl Into<String>,
    ) -> Result<EnrolledService, ServiceAuthError> {
        let registration = self
            .state
            .connection
            .enroll_service(
                node_id,
                invocation_url,
                self.state.maximum_in_flight,
                self.state.registry.clone(),
            )
            .await?;
        *self
            .state
            .enrollment
            .write()
            .map_err(|_| ServiceAuthError::InvalidNodeConfig)? = Some(registration.subscribe());
        Ok(registration)
    }
    /// Keep the catalog endpoint serving while published dependencies are pending.
    /// Dropping this future cancels activation; authorization errors are terminal.
    pub async fn enroll_when_available(
        &self,
        node_id: &str,
        invocation_url: &str,
    ) -> Result<EnrolledService, ServiceAuthError> {
        self.state
            .connection
            .wait_for_catalog(|| self.enroll(node_id, invocation_url))
            .await
    }

    /// Authentication cannot accidentally be omitted from the public adapter.
    pub fn router(&self, invocation_url: &str) -> Result<Router, ServiceAuthError> {
        let router = Router::new()
            .route("/", post(invoke).delete(cancel_attempt))
            .layer(DefaultBodyLimit::max(MAX_SERVICE_PAYLOAD_BYTES))
            .with_state(self.state.clone());
        self.state.connection.protect(router, invocation_url)
    }
    #[cfg(feature = "service-event-http")]
    pub fn event_router(&self, invocation_url: &str) -> Result<Router, ServiceError> {
        // Each role has its own operation budget; only the instance-wide
        // semaphore is shared. A busy Event role does not consume Call permits.
        let event_operations = self
            .state
            .registry
            .operations
            .values()
            .map(|o| {
                (
                    o.reference.clone(),
                    Arc::new(Semaphore::new(self.state.maximum_in_flight as usize)),
                )
            })
            .collect();
        let registry = Arc::new(self.state.registry.event_registry(
            self.state.events.clone(),
            self.state.total.clone(),
            &event_operations,
        )?);
        self.state
            .connection
            .protect(
                crate::event_dispatch::ConsumerHttpAdapter::new(registry).router(),
                invocation_url,
            )
            .map_err(|_| {
                ServiceError::rejected("invalid_event_url", "Invalid event invocation URL")
            })
    }
    /// Event memberships renew independently from the native Call instance.
    /// A disabled group cannot revoke unrelated Call capabilities.
    #[cfg(feature = "service-event-http")]
    pub async fn enroll_events(
        &self,
        node_id: &str,
        invocation_url: &str,
    ) -> Result<Vec<crate::event_dispatch::EnrolledConsumerNode>, ServiceAuthError> {
        let groups = self
            .state
            .registry
            .manifest
            .services
            .iter()
            .flat_map(|s| &s.operations)
            .flat_map(|o| &o.events)
            .map(|e| e.consumer_group.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let mut memberships = Vec::new();
        for group in groups {
            let membership = self
                .state
                .connection
                .enroll_consumer(crate::event_dispatch::EnrolledConsumerNodeConfig {
                    group_key: group.clone(),
                    node_id: node_id.into(),
                    invocation_url: invocation_url.into(),
                    maximum_in_flight: self.state.maximum_in_flight,
                })
                .await?;
            self.state
                .events
                .memberships
                .write()
                .map_err(|_| ServiceAuthError::InvalidNodeConfig)?
                .insert(group, membership.subscribe());
            memberships.push(membership);
        }
        Ok(memberships)
    }
    pub fn status(&self) -> ServiceRuntimeStatus {
        let open = !*self.state.cancel.borrow() && self.state.connection.ensure_open().is_ok();
        let call_accepting = open && self.state.live_instance().is_some();
        #[cfg(feature = "service-event-http")]
        let event_accepting = open && self.state.events.has_live_membership();
        #[cfg(not(feature = "service-event-http"))]
        let event_accepting = false;
        ServiceRuntimeStatus {
            accepting: call_accepting || event_accepting,
            call_accepting,
            event_accepting,
            active: self.active(),
            unconfirmed_results: self.state.unconfirmed.load(Ordering::Acquire),
        }
    }
    pub async fn shutdown(&self) {
        self.cancel.send_replace(true);
        // Every handler and callback request selects on cancellation; no local
        // task is promoted to durable ownership during shutdown.
        for _ in 0..100 {
            if self.active() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
impl ServiceHttpAdapter {
    fn active(&self) -> usize {
        let calls = self.state.active.load(Ordering::Acquire);
        #[cfg(feature = "service-event-http")]
        {
            calls + self.state.events.active.load(Ordering::Acquire)
        }
        #[cfg(not(feature = "service-event-http"))]
        {
            calls
        }
    }
}
impl Drop for ServiceHttpAdapter {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
    }
}

fn rejection(status: StatusCode, error: ServiceError) -> Response {
    let mut response = (status, Json(error)).into_response();
    // This helper is used only before handler acceptance. Gateways must preserve
    // this evidence; a bare proxy 503 cannot prove that work never started.
    if matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
    ) {
        response
            .headers_mut()
            .insert("x-lingshu-submission-rejected", "true".parse().unwrap());
        response
            .headers_mut()
            .insert("retry-after", "1".parse().unwrap());
    }
    response
}
async fn cancel_attempt(
    State(state): State<Arc<HttpState>>,
    Json(request): Json<ServiceCancellation>,
) -> Response {
    if state.execution.cancel_attempt(request) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        StatusCode::CONFLICT.into_response()
    }
}
async fn invoke(
    State(state): State<Arc<HttpState>>,
    Json(invocation): Json<ServiceInvocation>,
) -> Response {
    let response = state.execution.invoke(invocation).await;
    #[cfg(all(test, feature = "service-zenoh"))]
    let encoding_started = std::time::Instant::now();
    let response = match response {
        Ok(response @ InvocationResponse::Accepted { .. }) => {
            (StatusCode::ACCEPTED, Json(response)).into_response()
        }
        Ok(response) => Json(response).into_response(),
        Err(CallExecutionError::Rejected { kind, error }) => rejection(
            match kind {
                RejectionKind::Invalid => StatusCode::UNPROCESSABLE_ENTITY,
                RejectionKind::Conflict => StatusCode::CONFLICT,
                RejectionKind::NotFound => StatusCode::NOT_FOUND,
                RejectionKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            },
            error,
        ),
        Err(CallExecutionError::OutcomeUnknown) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ServiceError::retryable(
                "outcome_unknown",
                "Service stopped before returning its result",
            )),
        )
            .into_response(),
    };
    #[cfg(all(test, feature = "service-zenoh"))]
    {
        use axum::body::HttpBody;
        metrics::histogram!("lingshu_sdk_call_response_encoding_seconds", "transport" => "http")
            .record(encoding_started.elapsed().as_secs_f64());
        if let Some(bytes) = response.body().size_hint().exact() {
            metrics::histogram!("lingshu_sdk_call_response_encoded_bytes", "transport" => "http")
                .record(bytes as f64);
        }
    }
    response
}
struct HttpCallReporter {
    connection: ServiceConnection,
}
fn report_error(error: ServiceAuthError) -> CallReportError {
    match error {
        ServiceAuthError::Http(401 | 403) => CallReportError::AuthorityUnavailable,
        ServiceAuthError::Http(code)
            if (400..500).contains(&code) && code != 408 && code != 429 =>
        {
            CallReportError::Rejected
        }
        _ => CallReportError::Unavailable,
    }
}
#[async_trait::async_trait]
impl CallReporter for HttpCallReporter {
    async fn progress(
        &self,
        target: &CompletionTarget,
        progress: &ServiceProgress,
    ) -> Result<ProgressDisposition, CallReportError> {
        let request = self
            .connection
            .service_request(reqwest::Method::POST, "progress", Some(&target.token))
            .map_err(|_| CallReportError::AuthorityUnavailable)?;
        crate::service_auth::response_json(request.json(progress))
            .await
            .map_err(report_error)
    }
    async fn heartbeat(
        &self,
        target: &CompletionTarget,
        heartbeat: &ServiceHeartbeat,
    ) -> Result<HeartbeatDisposition, CallReportError> {
        let request = self
            .connection
            .service_request(reqwest::Method::POST, "heartbeats", Some(&target.token))
            .map_err(|_| CallReportError::AuthorityUnavailable)?;
        crate::service_auth::response_json(request.timeout(Duration::from_secs(3)).json(heartbeat))
            .await
            .map_err(report_error)
    }
    async fn complete(
        &self,
        target: &CompletionTarget,
        result: &ServiceCompletion,
        timeout: Option<Duration>,
    ) -> Result<CompletionDisposition, CallReportError> {
        let mut request = self
            .connection
            .service_request(reqwest::Method::POST, "completions", Some(&target.token))
            .map_err(|_| CallReportError::AuthorityUnavailable)?;
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }
        crate::service_auth::response_json(request.json(result))
            .await
            .map_err(report_error)
    }
}
#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
