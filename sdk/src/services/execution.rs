//! Authenticated transport adapters share this execution and reporting owner.
//! No wire protocol, listener, retry executor or durable local queue lives here.
use super::*;
use crate::ServiceConnection;
use futures::FutureExt;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};
use tokio::sync::{watch, Semaphore};

#[derive(Debug, Clone)]
pub enum ServiceEnrollmentStatus {
    Ready {
        node_id: String,
        generation: String,
        lease_expires_at_ms: i64,
    },
    /// Discovery is unavailable; accepted calls retain their own finite authority.
    Unavailable,
    Stopped,
}

/// Transport errors are classified by the authenticated reporter. Unknown or
/// transient replies never grant business completion or rerun the handler.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CallReportError {
    AuthorityUnavailable,
    Rejected,
    Unavailable,
}

#[async_trait::async_trait]
pub(crate) trait CallReporter: Send + Sync {
    /// A native credential/physical owner can fence accepted execution even
    /// after its discovery listener has handed off. HTTP reporters retain the
    /// existing connection/registration checks instead.
    async fn authority_lost(&self) {
        std::future::pending::<()>().await;
    }
    async fn progress(
        &self,
        target: &CompletionTarget,
        progress: &ServiceProgress,
    ) -> Result<ProgressDisposition, CallReportError>;
    async fn heartbeat(
        &self,
        target: &CompletionTarget,
        heartbeat: &ServiceHeartbeat,
    ) -> Result<HeartbeatDisposition, CallReportError>;
    async fn complete(
        &self,
        target: &CompletionTarget,
        result: &ServiceCompletion,
        timeout: Option<Duration>,
    ) -> Result<CompletionDisposition, CallReportError>;
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RejectionKind {
    Invalid,
    Conflict,
    NotFound,
    Unavailable,
}
#[derive(Debug)]
pub(crate) enum CallExecutionError {
    Rejected {
        kind: RejectionKind,
        error: ServiceError,
    },
    OutcomeUnknown,
}
fn rejection(
    kind: RejectionKind,
    error: ServiceError,
) -> Result<InvocationResponse, CallExecutionError> {
    Err(CallExecutionError::Rejected { kind, error })
}

pub(crate) struct CallExecutionCore {
    pub(crate) registry: Arc<ServiceRegistry>,
    pub(crate) connection: ServiceConnection,
    pub(crate) maximum_in_flight: u32,
    admission_policies: Mutex<BTreeMap<OperationRef, ServiceCallAdmission>>,
    pub(crate) total: Arc<Semaphore>,
    operations: BTreeMap<OperationRef, Arc<Semaphore>>,
    pub(crate) cancel: watch::Receiver<bool>,
    pub(crate) enrollment: RwLock<Option<watch::Receiver<ServiceEnrollmentStatus>>>,
    pub(crate) active: Arc<AtomicUsize>,
    pub(crate) unconfirmed: Arc<AtomicU64>,
    pub(crate) attempts: Mutex<BTreeMap<(OperationRef, String, u32), watch::Sender<bool>>>,
    reporter: Arc<dyn CallReporter>,
    accepted_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    accepted_task_failed: AtomicBool,
    idle: tokio::sync::Notify,
}
impl CallExecutionCore {
    pub(crate) fn new(
        registry: Arc<ServiceRegistry>,
        connection: ServiceConnection,
        budget: crate::ServiceExecutionBudget,
        reporter: Arc<dyn CallReporter>,
    ) -> Result<(Arc<Self>, watch::Sender<bool>), ServiceError> {
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
        Ok((
            Arc::new(Self {
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
                reporter,
                accepted_tasks: Mutex::new(Vec::new()),
                accepted_task_failed: AtomicBool::new(false),
                idle: tokio::sync::Notify::new(),
            }),
            cancel,
        ))
    }

    /// Adapters must authenticate before entering this core. Unknown attempts
    /// retain no tombstones; durable progress remains owned by Workflow.
    pub(crate) fn cancel_attempt(&self, request: ServiceCancellation) -> bool {
        if request.contract_version != SERVICE_CONTRACT_VERSION
            || self.live_instance().as_ref() != Some(&request.target_instance)
        {
            return false;
        }
        if let Some(cancel) = self
            .attempts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(request.operation, request.call_id, request.attempt))
        {
            cancel.send_replace(true);
        }
        true
    }
}

pub(crate) struct ActiveGuard(pub(crate) Arc<AtomicUsize>);
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct CallActivityGuard {
    state: Arc<CallExecutionCore>,
}
impl Drop for CallActivityGuard {
    fn drop(&mut self) {
        // Decrement before notifying; a waiter must never observe the old count
        // and then miss the final transition to idle.
        self.state.active.fetch_sub(1, Ordering::AcqRel);
        self.state.idle.notify_waiters();
    }
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
    state: Arc<CallExecutionCore>,
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

/// Serialize policy observation and permit acquisition. Permit release does not
/// need this lock; reducing a quota drains existing work without canceling it.
pub(crate) fn acquire_call_permits(
    state: &CallExecutionCore,
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

impl CallExecutionCore {
    /// The authenticated adapter calls this once; the core never resubmits an invocation.
    pub(crate) async fn invoke(
        self: &Arc<Self>,
        invocation: ServiceInvocation,
    ) -> Result<InvocationResponse, CallExecutionError> {
        self.invoke_with_reporter(invocation, self.reporter.clone())
            .await
    }

    /// A native adapter pins one reporter per original attempt before admission.
    /// It never replaces another attempt's reporter or its shared execution owner.
    pub(crate) async fn invoke_with_reporter(
        self: &Arc<Self>,
        invocation: ServiceInvocation,
        reporter: Arc<dyn CallReporter>,
    ) -> Result<InvocationResponse, CallExecutionError> {
        let state = self;
        let now = chrono::Utc::now().timestamp_millis();
        if *state.cancel.borrow() || state.connection.ensure_open().is_err() {
            return rejection(
                RejectionKind::Unavailable,
                ServiceError::retryable("service_stopping", "Service is not accepting work"),
            );
        }
        let registration = state.live_registration();
        if registration.as_ref().map(|(instance, _)| instance)
            != invocation.target_instance.as_ref()
            || invocation.target_instance.is_none()
        {
            return rejection(
                RejectionKind::Conflict,
                ServiceError::rejected(
                    "stale_instance",
                    "Invocation does not target the current live instance",
                ),
            );
        }
        if let Err(error) = state.registry.validate_invocation(&invocation, now) {
            return rejection(RejectionKind::Invalid, error);
        }
        let InvocationRole::Call(call) = &invocation.context.invocation else {
            return rejection(
                RejectionKind::Invalid,
                ServiceError::rejected("invalid_role", "Use the Event Dispatch adapter for events"),
            );
        };
        let Some(budget) = state.operations.get(&invocation.operation) else {
            return rejection(
                RejectionKind::NotFound,
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
        let (total, operation) = match acquire_call_permits(state, &invocation, budget, ceiling) {
            Ok(permits) => permits,
            Err(error) => {
                let status = match error.code.as_str() {
                    "invalid_admission" => RejectionKind::Invalid,
                    "stale_governance" => RejectionKind::Conflict,
                    _ => RejectionKind::Unavailable,
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
                    RejectionKind::Conflict,
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
        let guard = CallActivityGuard {
            state: state.clone(),
        };
        let mut cancel = state.cancel.clone();
        let mut connection_closed = state.connection.subscribe_closed();
        // Capture the ready identity and its lifecycle watch under one read.
        let (expected_instance, mut registration) = registration.expect("live instance validated");
        let run_state = state.clone();
        let renewable = completion.as_ref().is_some_and(|c| c.heartbeat.is_some());
        let execution = async move {
            // Release the attempt mutex and both permits before publishing
            // idle. A drain waiter must not see zero while capacity is held.
            let _guard = guard;
            let (_total, _operation, _attempt) = (total, operation, attempt_guard);
            let progress = watch_progress(
                reporter.as_ref(),
                completion.as_ref(),
                &call_id,
                attempt,
                &expected_instance,
            );
            let outcome = tokio::select! {
                _ = reporter.authority_lost() => return None,
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
            let outcome = if serde_json::to_vec(&outcome)
                .map_or(true, |b| b.len() > MAX_SERVICE_PAYLOAD_BYTES)
            {
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
                    reporter.as_ref(),
                    &completion,
                    &expected_instance,
                    &result,
                );
                let confirmed = tokio::select! { _=reporter.authority_lost()=>false, _=canceled(&mut cancel)=>false, _=canceled(&mut attempt_canceled)=>false, _=connection_closed.changed()=>false, _=registration_stopped(&mut registration,&expected_instance,renewable)=>false, confirmed=report=>confirmed };
                if !confirmed {
                    run_state.unconfirmed.fetch_add(1, Ordering::Relaxed);
                }
                None
            } else {
                Some(outcome)
            }
        };
        #[cfg(feature = "service-zenoh")]
        let execution = crate::service_channel::trace::carry(execution);
        if mode == CallMode::Async {
            // The task is bounded by execution deadline, cancellation, permits and
            // bounded reporting. No task survives as a durable local queue.
            let task = tokio::spawn(async move {
                execution.await;
            });
            let mut tasks = state
                .accepted_tasks
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            tasks.retain_mut(|task| {
                if !task.is_finished() {
                    return true;
                }
                match (&mut *task).now_or_never() {
                    Some(Ok(())) => false,
                    Some(Err(_)) => {
                        state.accepted_task_failed.store(true, Ordering::Release);
                        false
                    }
                    None => true,
                }
            });
            tasks.push(task);
            Ok(receipt)
        } else {
            match execution.await {
                Some(outcome) => Ok(InvocationResponse::Completed { outcome }),
                // Execution may have committed effects before shutdown. Do not mark
                // this as a pre-acceptance rejection or encourage submission replay.
                None => Err(CallExecutionError::OutcomeUnknown),
            }
        }
    }

    /// Call only after withdrawing admission and joining invocation queries.
    /// Keep unjoined handles on timeout; cleanup must not be reported as success.
    #[cfg(feature = "service-call-zenoh")]
    pub(crate) async fn wait_idle(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }

    #[cfg(feature = "service-call-zenoh")]
    pub(crate) async fn join_accepted(&self) -> bool {
        let mut tasks = std::mem::take(
            &mut *self
                .accepted_tasks
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut clean = !self.accepted_task_failed.load(Ordering::Acquire);
        while let Some(mut task) = tasks.pop() {
            match tokio::time::timeout_at(deadline, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    self.accepted_task_failed.store(true, Ordering::Release);
                    clean = false;
                }
                Err(_) => {
                    tasks.push(task);
                    self.accepted_tasks
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .extend(tasks);
                    return false;
                }
            }
        }
        clean
    }
}
const PROGRESS_INTERVAL: Duration = Duration::from_secs(10);
async fn watch_progress(
    reporter: &dyn CallReporter,
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
            reporter,
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
        let response = reporter.progress(completion, &progress).await;
        match response {
            Ok(ProgressDisposition::Invalidated) | Err(CallReportError::AuthorityUnavailable) => {
                return
            }
            // Missing/temporarily unreachable progress never implies a business
            // retry. Fixed execution deadlines remain the recovery authority.
            _ => {}
        }
        tokio::time::sleep(PROGRESS_INTERVAL).await;
    }
}

async fn send_heartbeat(
    reporter: &dyn CallReporter,
    completion: &CompletionTarget,
    instance: &ServiceInstanceTarget,
    call: &str,
    attempt: u32,
    policy: &CallHeartbeatPolicy,
    phase: CallPhase,
) -> Result<HeartbeatDisposition, CallReportError> {
    let input = ServiceHeartbeat {
        version: CALL_HEARTBEAT_VERSION,
        call_id: call.into(),
        attempt,
        epoch: policy.epoch.clone(),
        instance: instance.clone(),
        phase,
    };
    tokio::time::timeout(
        Duration::from_secs(3),
        reporter.heartbeat(completion, &input),
    )
    .await
    .unwrap_or(Err(CallReportError::Unavailable))
}
async fn watch_heartbeat(
    reporter: &dyn CallReporter,
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
            send_heartbeat(reporter, completion, instance, call, attempt, policy, phase),
        )
        .await;
        let retry_delay = if matches!(&response, Ok(Ok(HeartbeatDisposition::Renewed { .. }))) {
            policy.interval_ms
        } else {
            1000
        };
        match response {
            Ok(Ok(HeartbeatDisposition::Invalidated))
            | Ok(Err(CallReportError::AuthorityUnavailable))
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
    reporter: &dyn CallReporter,
    completion: &CompletionTarget,
    instance: &ServiceInstanceTarget,
    result: &ServiceCompletion,
) -> bool {
    let Some(policy) = &completion.heartbeat else {
        return report_result(reporter, completion, result).await;
    };
    let remaining = policy
        .delivery_deadline_ms
        .saturating_sub(chrono::Utc::now().timestamp_millis())
        .max(0) as u64;
    let report = async {
        // Announce the phase before the first result attempt. Failure leaves a
        // bounded unknown outcome; no handler is re-executed by this SDK.
        match send_heartbeat(
            reporter,
            completion,
            instance,
            &result.call_id,
            result.attempt,
            policy,
            CallPhase::Completing,
        )
        .await
        {
            Ok(HeartbeatDisposition::Invalidated) | Err(CallReportError::AuthorityUnavailable) => {
                return false
            }
            _ => {}
        }
        for retry in 0..8 {
            let response = reporter
                .complete(completion, result, Some(Duration::from_secs(3)))
                .await;
            match response {
                Ok(_) => return true,
                Err(CallReportError::AuthorityUnavailable | CallReportError::Rejected) => {
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
        _ = watch_heartbeat(reporter, completion, instance, &result.call_id, result.attempt, policy, CallPhase::Completing) => false,
    }
}

async fn report_result(
    reporter: &dyn CallReporter,
    completion: &CompletionTarget,
    result: &ServiceCompletion,
) -> bool {
    for attempt in 0..3 {
        let response = reporter.complete(completion, result, None).await;
        match response {
            Ok(
                CompletionDisposition::Recorded
                | CompletionDisposition::Duplicate
                | CompletionDisposition::Invalidated,
            ) => return true,
            Err(CallReportError::AuthorityUnavailable | CallReportError::Rejected) => return false,
            _ => {}
        }
        if attempt < 2 {
            tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
        }
    }
    false
}

impl CallExecutionCore {
    pub(crate) fn live_instance(&self) -> Option<ServiceInstanceTarget> {
        self.live_registration().map(|(instance, _)| instance)
    }
    fn live_registration(
        &self,
    ) -> Option<(
        ServiceInstanceTarget,
        watch::Receiver<ServiceEnrollmentStatus>,
    )> {
        let guard = self.enrollment.read().ok()?;
        let registration = guard.as_ref()?;
        let status = registration.borrow();
        match &*status {
            ServiceEnrollmentStatus::Ready {
                node_id,
                generation,
                lease_expires_at_ms,
            } if *lease_expires_at_ms > chrono::Utc::now().timestamp_millis() => Some((
                ServiceInstanceTarget {
                    node_id: node_id.clone(),
                    generation: generation.clone(),
                },
                registration.clone(),
            )),
            _ => None,
        }
    }
}

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

#[cfg(test)]
#[path = "execution_tests.rs"]
pub(crate) mod tests;
