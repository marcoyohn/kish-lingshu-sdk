use super::*;
use crate::{ServiceAuthError, ServiceConnection};
use axum::{
    extract::{DefaultBodyLimit, Json, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use futures::FutureExt;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};
use tokio::sync::{watch, Semaphore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceRuntimeStatus {
    pub accepting: bool,
    pub call_accepting: bool,
    pub event_accepting: bool,
    pub active: usize,
    pub unconfirmed_results: u64,
}

struct HttpState {
    registry: Arc<ServiceRegistry>,
    connection: ServiceConnection,
    maximum_in_flight: u32,
    admission_policies: Mutex<BTreeMap<OperationRef, ServiceCallAdmission>>,
    total: Arc<Semaphore>,
    operations: BTreeMap<OperationRef, Arc<Semaphore>>,
    cancel: watch::Receiver<bool>,
    enrollment: RwLock<Option<watch::Receiver<ServiceEnrollmentStatus>>>,
    #[cfg(feature = "service-event-http")]
    events: super::event::EventAdmission,
    active: Arc<AtomicUsize>,
    unconfirmed: Arc<AtomicU64>,
    attempts: Mutex<BTreeMap<(OperationRef, String, u32), watch::Sender<bool>>>,
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
        let maximum_in_flight = budget.maximum_in_flight();
        if registry.manifest.application_id != connection.application_id() || maximum_in_flight == 0
        {
            return Err(ServiceError::rejected(
                "invalid_service_host",
                "Application identity and capacity must match",
            ));
        }
        let operations = registry
            .operations
            .values()
            .map(|o| {
                (
                    o.reference.clone(),
                    Arc::new(Semaphore::new(
                        o.definition
                            .call
                            .as_ref()
                            .map_or(maximum_in_flight, |b| b.maximum_concurrency)
                            as usize,
                    )),
                )
            })
            .collect();
        let (cancel, cancel_rx) = watch::channel(false);
        Ok(Self {
            state: Arc::new(HttpState {
                #[cfg(feature = "service-event-http")]
                events: super::event::EventAdmission {
                    cancel: cancel_rx.clone(),
                    connection: connection.clone(),
                    memberships: Arc::new(RwLock::new(BTreeMap::new())),
                    active: Arc::new(AtomicUsize::new(0)),
                },
                registry,
                connection,
                maximum_in_flight,
                admission_policies: Mutex::new(BTreeMap::new()),
                total: budget.semaphore,
                operations,
                cancel: cancel_rx,
                enrollment: RwLock::new(None),
                active: Arc::new(AtomicUsize::new(0)),
                unconfirmed: Arc::new(AtomicU64::new(0)),
                attempts: Mutex::new(BTreeMap::new()),
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

pub(super) struct ActiveGuard(pub(super) Arc<AtomicUsize>);
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn rejection(status: StatusCode, error: ServiceError) -> Response {
    (status, Json(error)).into_response()
}
async fn canceled(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow_and_update() {
            return;
        }
        if receiver.changed().await.is_err() {
            return;
        }
    }
}

struct AttemptGuard {
    state: Arc<HttpState>,
    key: (OperationRef, String, u32),
}
impl Drop for AttemptGuard {
    fn drop(&mut self) {
        self.state
            .attempts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
    }
}

async fn cancel_attempt(
    State(state): State<Arc<HttpState>>,
    Json(request): Json<ServiceCancellation>,
) -> Response {
    if request.contract_version != SERVICE_CONTRACT_VERSION
        || state.live_instance().as_ref() != Some(&request.target_instance)
    {
        return StatusCode::CONFLICT.into_response();
    }
    if let Some(cancel) = state
        .attempts
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&(request.operation, request.call_id, request.attempt))
    {
        cancel.send_replace(true);
    }
    // No tombstone or local recovery queue. A cancellation racing admission is
    // also observed by the SDK's progress check against the durable Workflow.
    StatusCode::NO_CONTENT.into_response()
}

/// Serialize policy observation and permit acquisition. Permit release does not
/// need this lock; reducing a quota drains existing work without canceling it.
fn acquire_call_permits(
    state: &HttpState,
    invocation: &ServiceInvocation,
    budget: &Arc<Semaphore>,
    ceiling: u32,
) -> Result<
    (
        tokio::sync::OwnedSemaphorePermit,
        tokio::sync::OwnedSemaphorePermit,
    ),
    ServiceError,
> {
    let policy = invocation
        .admission
        .as_ref()
        .filter(|p| {
            p.policy_revision > 0 && p.maximum_concurrency > 0 && p.maximum_concurrency <= ceiling
        })
        .ok_or_else(|| {
            ServiceError::rejected("invalid_admission", "Signed Call governance is required")
        })?;
    let mut policies = state
        .admission_policies
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(previous) = policies.get(&invocation.operation) {
        if policy.policy_revision < previous.policy_revision
            || (policy.policy_revision == previous.policy_revision && policy != previous)
        {
            return Err(ServiceError::retryable(
                "stale_governance",
                "Call governance has changed",
            ));
        }
    }
    policies.insert(invocation.operation.clone(), policy.clone());
    let exhausted =
        || ServiceError::retryable("capacity_exhausted", "No execution permit available");
    if ceiling as usize - budget.available_permits() >= policy.maximum_concurrency as usize {
        return Err(exhausted());
    }
    let total = state
        .total
        .clone()
        .try_acquire_owned()
        .map_err(|_| exhausted())?;
    let operation = budget
        .clone()
        .try_acquire_owned()
        .map_err(|_| exhausted())?;
    Ok((total, operation))
}

async fn invoke(
    State(state): State<Arc<HttpState>>,
    Json(invocation): Json<ServiceInvocation>,
) -> Response {
    let now = chrono::Utc::now().timestamp_millis();
    if *state.cancel.borrow() || state.connection.ensure_open().is_err() {
        return rejection(
            StatusCode::SERVICE_UNAVAILABLE,
            ServiceError::retryable("service_stopping", "Service is not accepting work"),
        );
    }
    if state.live_instance().as_ref() != invocation.target_instance.as_ref()
        || invocation.target_instance.is_none()
    {
        return rejection(
            StatusCode::CONFLICT,
            ServiceError::rejected(
                "stale_instance",
                "Invocation does not target the current live instance",
            ),
        );
    }
    if let Err(error) = state.registry.validate_invocation(&invocation, now) {
        return rejection(StatusCode::UNPROCESSABLE_ENTITY, error);
    }
    let InvocationRole::Call(call) = &invocation.context.invocation else {
        return rejection(
            StatusCode::UNPROCESSABLE_ENTITY,
            ServiceError::rejected("invalid_role", "Use the Event Dispatch adapter for events"),
        );
    };
    let Some(budget) = state.operations.get(&invocation.operation) else {
        return rejection(
            StatusCode::NOT_FOUND,
            ServiceError::rejected("operation_not_found", "Call operation unavailable"),
        );
    };
    let definition = state
        .registry
        .definition(&invocation.operation)
        .expect("validated definition");
    let ceiling = definition
        .call
        .as_ref()
        .expect("call binding")
        .maximum_concurrency;
    let (total, operation) = match acquire_call_permits(&state, &invocation, budget, ceiling) {
        Ok(permits) => permits,
        Err(error) => {
            let status = match error.code.as_str() {
                "invalid_admission" => StatusCode::UNPROCESSABLE_ENTITY,
                "stale_governance" => StatusCode::CONFLICT,
                _ => StatusCode::TOO_MANY_REQUESTS,
            };
            return rejection(status, error);
        }
    };
    let timeout = Duration::from_millis((invocation.context.deadline_ms - now) as u64).min(
        Duration::from_millis(definition.call.as_ref().expect("call binding").timeout_ms),
    );
    let mode = call.mode;
    let receipt = InvocationResponse::Accepted {
        call_id: call.call_id.clone(),
        attempt: call.attempt,
    };
    let completion = invocation.completion.clone();
    let call_id = call.call_id.clone();
    let attempt = call.attempt;
    let key = (invocation.operation.clone(), call_id.clone(), attempt);
    let (attempt_cancel, mut attempt_canceled) = watch::channel(false);
    {
        let mut attempts = state.attempts.lock().unwrap_or_else(|e| e.into_inner());
        if attempts.contains_key(&key) {
            return rejection(
                StatusCode::CONFLICT,
                ServiceError::retryable("attempt_running", "This attempt is already executing"),
            );
        }
        attempts.insert(key.clone(), attempt_cancel);
    }
    let attempt_guard = AttemptGuard {
        state: state.clone(),
        key,
    };
    state.active.fetch_add(1, Ordering::AcqRel);
    let guard = ActiveGuard(state.active.clone());
    let mut cancel = state.cancel.clone();
    let mut connection_closed = state.connection.subscribe_closed();
    let mut registration = state
        .enrollment
        .read()
        .ok()
        .and_then(|r| r.clone())
        .expect("live enrollment validated");
    let expected_instance = invocation
        .target_instance
        .clone()
        .expect("live instance validated");
    let run_state = state.clone();
    let renewable = completion.as_ref().is_some_and(|c| c.heartbeat.is_some());
    let execution = async move {
        let (_total, _operation, _guard, _attempt) = (total, operation, guard, attempt_guard);
        let progress = watch_progress(
            &run_state.connection,
            completion.as_ref(),
            &call_id,
            attempt,
            &expected_instance,
        );
        let outcome = tokio::select! {
            _ = canceled(&mut cancel) => return None,
            _ = canceled(&mut attempt_canceled) => return None,
            _ = progress => return None,
            _ = connection_closed.changed() => return None,
            _ = registration_stopped(&mut registration,&expected_instance,renewable)=>return None,
            result = tokio::time::timeout(timeout,std::panic::AssertUnwindSafe(run_state.registry.invoke(invocation)).catch_unwind()) => match result {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(_)) => ServiceOutcome::Failed {error:ServiceError::retryable("handler_panicked","Service handler failed before returning a result")},
                Err(_) => ServiceOutcome::Failed {error:ServiceError::retryable("deadline_exceeded","Service execution timed out; effects may already have committed")},
            }
        };
        // Bound output even when a permissive schema allows arbitrary JSON.
        let outcome =
            if serde_json::to_vec(&outcome).map_or(true, |b| b.len() > MAX_SERVICE_PAYLOAD_BYTES) {
                ServiceOutcome::Failed {
                    error: ServiceError::rejected(
                        "output_too_large",
                        "Service result exceeds the payload limit",
                    ),
                }
            } else {
                outcome
            };
        if let Some(completion) = completion {
            let result = ServiceCompletion {
                contract_version: SERVICE_CONTRACT_VERSION,
                call_id,
                attempt,
                outcome,
            };
            let report = report_result_managed(
                &run_state.connection,
                &completion,
                &expected_instance,
                &result,
            );
            let confirmed = tokio::select! { _=canceled(&mut cancel)=>false, _=canceled(&mut attempt_canceled)=>false, _=connection_closed.changed()=>false, _=registration_stopped(&mut registration,&expected_instance,renewable)=>false, confirmed=report=>confirmed };
            if !confirmed {
                run_state.unconfirmed.fetch_add(1, Ordering::Relaxed);
            }
            None
        } else {
            Some(outcome)
        }
    };
    if mode == CallMode::Async {
        // The task is bounded by execution deadline, cancellation, permits and
        // bounded reporting. No task survives as a durable local queue.
        tokio::spawn(execution);
        (StatusCode::ACCEPTED, Json(receipt)).into_response()
    } else {
        match execution.await {
            Some(outcome) => Json(InvocationResponse::Completed { outcome }).into_response(),
            None => rejection(
                StatusCode::SERVICE_UNAVAILABLE,
                ServiceError::retryable(
                    "service_stopping",
                    "Service stopped before returning its result",
                ),
            ),
        }
    }
}

const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);
async fn watch_progress(
    connection: &ServiceConnection,
    completion: Option<&CompletionTarget>,
    call_id: &str,
    attempt: u32,
    instance: &ServiceInstanceTarget,
) {
    let Some(completion) = completion else {
        return std::future::pending().await;
    };
    if let Some(policy) = &completion.heartbeat {
        watch_heartbeat(
            connection,
            completion,
            instance,
            call_id,
            attempt,
            policy,
            CallPhase::Running,
        )
        .await;
        return;
    }
    let progress = ServiceProgress {
        contract_version: SERVICE_CONTRACT_VERSION,
        call_id: call_id.into(),
        attempt,
    };
    loop {
        // Check immediately as well as periodically, so an admission/cancel race
        // converges without retaining cancellation tombstones in the provider.
        let request = match connection.service_request(
            reqwest::Method::POST,
            "progress",
            Some(&completion.token),
        ) {
            Ok(request) => request,
            Err(_) => return,
        };
        let response: Result<ProgressDisposition, ServiceAuthError> =
            crate::service_auth::response_json(request.json(&progress)).await;
        match response {
            Ok(ProgressDisposition::Invalidated) | Err(ServiceAuthError::Http(401 | 403)) => return,
            // Missing/temporarily unreachable progress never implies a business
            // retry. Fixed execution deadlines remain the recovery authority.
            _ => {}
        }
        tokio::time::sleep(PROGRESS_INTERVAL).await;
    }
}

async fn send_heartbeat(
    connection: &ServiceConnection,
    completion: &CompletionTarget,
    instance: &ServiceInstanceTarget,
    call: &str,
    attempt: u32,
    policy: &CallHeartbeatPolicy,
    phase: CallPhase,
) -> Result<HeartbeatDisposition, ServiceAuthError> {
    let input = ServiceHeartbeat {
        version: CALL_HEARTBEAT_VERSION,
        call_id: call.into(),
        attempt,
        epoch: policy.epoch.clone(),
        instance: instance.clone(),
        phase,
    };
    crate::service_auth::response_json(
        connection
            .service_request(reqwest::Method::POST, "heartbeats", Some(&completion.token))?
            .timeout(Duration::from_secs(3))
            .json(&input),
    )
    .await
}
async fn watch_heartbeat(
    connection: &ServiceConnection,
    completion: &CompletionTarget,
    instance: &ServiceInstanceTarget,
    call: &str,
    attempt: u32,
    policy: &CallHeartbeatPolicy,
    phase: CallPhase,
) {
    let hard = if phase == CallPhase::Running {
        policy.execution_deadline_ms
    } else {
        policy.delivery_deadline_ms
    };
    let mut safety = tokio::time::Instant::now() + Duration::from_millis(CALL_ACCEPTANCE_MS as u64);
    loop {
        let remaining = hard.saturating_sub(chrono::Utc::now().timestamp_millis());
        if remaining <= 0 {
            return;
        }
        let started = tokio::time::Instant::now();
        let limit = safety.min(started + Duration::from_millis(remaining as u64));
        let response = tokio::time::timeout_at(
            limit,
            send_heartbeat(
                connection, completion, instance, call, attempt, policy, phase,
            ),
        )
        .await;
        let retry_delay = if matches!(&response, Ok(Ok(HeartbeatDisposition::Renewed { .. }))) {
            policy.interval_ms
        } else {
            1000
        };
        match response {
            Ok(Ok(HeartbeatDisposition::Invalidated))
            | Ok(Err(ServiceAuthError::Http(401 | 403)))
            | Err(_) => return,
            Ok(Ok(HeartbeatDisposition::Renewed {
                store_now_ms,
                liveness_until_ms,
            })) => {
                let granted = liveness_until_ms.saturating_sub(store_now_ms);
                if granted <= 0 || granted > CALL_LIVENESS_MS {
                    return;
                }
                // Request start is conservative: never add network latency to
                // the lease the platform actually committed.
                safety = started + Duration::from_millis(granted as u64);
            }
            _ => {}
        }
        let next = (tokio::time::Instant::now() + Duration::from_millis(retry_delay)).min(safety);
        tokio::time::sleep_until(next).await;
        if tokio::time::Instant::now() >= safety {
            return;
        }
    }
}
async fn report_result_managed(
    connection: &ServiceConnection,
    completion: &CompletionTarget,
    instance: &ServiceInstanceTarget,
    result: &ServiceCompletion,
) -> bool {
    let Some(policy) = &completion.heartbeat else {
        return report_result(connection, &completion.token, result).await;
    };
    let remaining = policy
        .delivery_deadline_ms
        .saturating_sub(chrono::Utc::now().timestamp_millis())
        .max(0) as u64;
    let report = async {
        // Announce the phase before the first result attempt. Failure leaves a
        // bounded unknown outcome; no handler is re-executed by this SDK.
        match send_heartbeat(
            connection,
            completion,
            instance,
            &result.call_id,
            result.attempt,
            policy,
            CallPhase::Completing,
        )
        .await
        {
            Ok(HeartbeatDisposition::Invalidated) | Err(ServiceAuthError::Http(401 | 403)) => {
                return false
            }
            _ => {}
        }
        for retry in 0..8 {
            let request = match connection.service_request(
                reqwest::Method::POST,
                "completions",
                Some(&completion.token),
            ) {
                Ok(request) => request.timeout(Duration::from_secs(3)),
                Err(_) => return false,
            };
            let response: Result<CompletionDisposition, ServiceAuthError> =
                crate::service_auth::response_json(request.json(result)).await;
            match response {
                Ok(_) => return true,
                Err(ServiceAuthError::Http(code))
                    if (400..500).contains(&code) && code != 408 && code != 429 =>
                {
                    return false
                }
                _ => {}
            }
            if retry < 7 {
                tokio::time::sleep(Duration::from_millis((250u64 << retry).min(3000))).await;
            }
        }
        false
    };
    tokio::select! {
        result = report => result,
        _ = tokio::time::sleep(Duration::from_millis(remaining)) => false,
        _ = watch_heartbeat(connection, completion, instance, &result.call_id, result.attempt, policy, CallPhase::Completing) => false,
    }
}

async fn report_result(
    connection: &ServiceConnection,
    token: &str,
    result: &ServiceCompletion,
) -> bool {
    for attempt in 0..3 {
        let request =
            match connection.service_request(reqwest::Method::POST, "completions", Some(token)) {
                Ok(r) => r,
                Err(_) => return false,
            };
        let response: Result<CompletionDisposition, ServiceAuthError> =
            crate::service_auth::response_json(request.json(result)).await;
        match response {
            Ok(
                CompletionDisposition::Recorded
                | CompletionDisposition::Duplicate
                | CompletionDisposition::Invalidated,
            ) => return true,
            Err(ServiceAuthError::Http(code))
                if (400..500).contains(&code) && code != 408 && code != 429 =>
            {
                return false
            }
            _ => {}
        }
        if attempt < 2 {
            tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
        }
    }
    false
}

impl HttpState {
    fn live_instance(&self) -> Option<ServiceInstanceTarget> {
        let guard = self.enrollment.read().ok()?;
        let status = guard.as_ref()?.borrow();
        match &*status {
            ServiceEnrollmentStatus::Ready {
                node_id,
                generation,
                lease_expires_at_ms,
            } if *lease_expires_at_ms > chrono::Utc::now().timestamp_millis() => {
                Some(ServiceInstanceTarget {
                    node_id: node_id.clone(),
                    generation: generation.clone(),
                })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;

async fn registration_stopped(
    status: &mut watch::Receiver<ServiceEnrollmentStatus>,
    expected: &ServiceInstanceTarget,
    renewable: bool,
) {
    loop {
        let enrollment_status = status.borrow_and_update().clone();
        let deadline = match enrollment_status {
            ServiceEnrollmentStatus::Unavailable if renewable => {
                if status.changed().await.is_err() {
                    return;
                }
                continue;
            }
            ServiceEnrollmentStatus::Ready {
                node_id,
                generation,
                lease_expires_at_ms,
            } if node_id == expected.node_id && generation == expected.generation => {
                lease_expires_at_ms
            }
            _ => return,
        };
        let remaining = (deadline - chrono::Utc::now().timestamp_millis()).max(0) as u64;
        if renewable && remaining == 0 {
            if status.changed().await.is_err() {
                return;
            }
        } else {
            tokio::select! {_=tokio::time::sleep(Duration::from_millis(remaining)), if !renewable => return,
            r=status.changed()=>if r.is_err(){return;}}
        }
    }
}
