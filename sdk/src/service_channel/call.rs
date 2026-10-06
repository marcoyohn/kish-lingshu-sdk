//! Opt-in execution on an already registered, authenticated Call role.
use super::observation::{self, BusinessObservation, BusinessOutcome, Plane};
use super::ChannelSessionError;
use crate::services::execution::{
    CallExecutionCore, CallExecutionError, CallReportError, CallReporter,
};
use crate::{services::*, ServiceConnection, ServiceExecutionBudget};
use kish_lingshu_foundation_contract::{
    service_auth::{ChannelMessageSigner, ClientChannelIdentity},
    service_transport::{
        bootstrap::ChannelBootstrapResponse, CallReportRoute, ExactRouteKey, MessageKind,
        ProtocolVersion, RouteIdentity, ServiceEndpoint, TransportEnvelope,
    },
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::watch;

pub(super) type CallSlot = Arc<std::sync::Mutex<Option<Arc<NativeCallExecution>>>>;
pub(super) struct NativeCallExecution {
    #[cfg(test)]
    pub(super) received_lanes: [std::sync::atomic::AtomicUsize; 4],
    pub(super) core: Arc<CallExecutionCore>,
    stop: watch::Sender<bool>,
    enrollment: watch::Sender<ServiceEnrollmentStatus>,
    target: ServiceInstanceTarget,
    async_reports: Option<Arc<AsyncCallReports>>,
    handoff: AtomicBool,
    #[cfg(test)]
    faults: std::sync::Mutex<Option<Arc<faults::Faults>>>,
}
struct AsyncCallReports {
    session: zenoh::Session,
    authority: ChannelBootstrapResponse,
    signer: Arc<ChannelMessageSigner>,
    active: AtomicUsize,
    deadline: std::sync::Mutex<tokio::time::Instant>,
    physical: watch::Receiver<Option<super::ChannelCloseReason>>,
}
struct TrackedCallReporter {
    reporter: Arc<dyn CallReporter>,
    owner: Arc<AsyncCallReports>,
    business: BusinessObservation,
}
impl Drop for TrackedCallReporter {
    fn drop(&mut self) {
        self.owner.active.fetch_sub(1, Ordering::AcqRel);
    }
}
#[async_trait::async_trait]
impl CallReporter for TrackedCallReporter {
    async fn authority_lost(&self) {
        self.reporter.authority_lost().await;
    }
    async fn progress(
        &self,
        target: &CompletionTarget,
        progress: &ServiceProgress,
    ) -> Result<ProgressDisposition, CallReportError> {
        self.reporter.progress(target, progress).await
    }
    async fn heartbeat(
        &self,
        target: &CompletionTarget,
        heartbeat: &ServiceHeartbeat,
    ) -> Result<HeartbeatDisposition, CallReportError> {
        self.reporter.heartbeat(target, heartbeat).await
    }
    async fn complete(
        &self,
        target: &CompletionTarget,
        result: &ServiceCompletion,
        timeout: Option<Duration>,
    ) -> Result<CompletionDisposition, CallReportError> {
        self.business.finish(business_outcome(&result.outcome));
        self.reporter.complete(target, result, timeout).await
    }
}
struct NoAsyncReporter;
#[async_trait::async_trait]
impl CallReporter for NoAsyncReporter {
    async fn progress(
        &self,
        _: &CompletionTarget,
        _: &ServiceProgress,
    ) -> Result<ProgressDisposition, CallReportError> {
        Err(CallReportError::Rejected)
    }
    async fn heartbeat(
        &self,
        _: &CompletionTarget,
        _: &ServiceHeartbeat,
    ) -> Result<HeartbeatDisposition, CallReportError> {
        Err(CallReportError::Rejected)
    }
    async fn complete(
        &self,
        _: &CompletionTarget,
        _: &ServiceCompletion,
        _: Option<Duration>,
    ) -> Result<CompletionDisposition, CallReportError> {
        Err(CallReportError::Rejected)
    }
}
impl NativeCallExecution {
    #[cfg(test)]
    pub(super) fn test_binding(f: &crate::services::execution::tests::Fixture) -> Arc<Self> {
        Arc::new(Self {
            received_lanes: Default::default(),
            core: f.core.clone(),
            stop: f.stop.clone(),
            enrollment: f.registration.clone(),
            target: ServiceInstanceTarget {
                node_id: "node".into(),
                generation: "generation".into(),
            },
            async_reports: None,
            handoff: AtomicBool::new(false),
            faults: Default::default(),
        })
    }
    fn new(
        registry: Arc<ServiceRegistry>,
        connection: ServiceConnection,
        budget: ServiceExecutionBudget,
        target: ServiceInstanceTarget,
        expires: i64,
        async_reports: Option<Arc<AsyncCallReports>>,
    ) -> Result<Arc<Self>, ChannelSessionError> {
        let (core, stop) =
            CallExecutionCore::new(registry, connection, budget, Arc::new(NoAsyncReporter))
                .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let (enrollment, status) = watch::channel(ServiceEnrollmentStatus::Stopped);
        *core
            .enrollment
            .write()
            .map_err(|_| ChannelSessionError::InvalidConfig)? = Some(status);
        let binding = Arc::new(Self {
            #[cfg(test)]
            received_lanes: Default::default(),
            core,
            stop,
            enrollment,
            target,
            async_reports,
            handoff: AtomicBool::new(false),
            #[cfg(test)]
            faults: Default::default(),
        });
        binding.update(expires);
        Ok(binding)
    }
    pub(super) fn update(&self, expires: i64) {
        self.enrollment
            .send_replace(ServiceEnrollmentStatus::Ready {
                node_id: self.target.node_id.clone(),
                generation: self.target.generation.clone(),
                lease_expires_at_ms: expires,
            });
    }
    pub(super) fn stop(&self) {
        self.stop.send_replace(true);
        self.enrollment
            .send_replace(ServiceEnrollmentStatus::Stopped);
    }
    pub(super) fn withdraw(&self) {
        self.enrollment
            .send_replace(ServiceEnrollmentStatus::Unavailable);
    }
    pub(super) fn is_handoff(&self) -> bool {
        self.handoff.load(Ordering::Acquire)
    }
    pub(super) fn prepare_handoff(&self) {
        self.handoff.store(true, Ordering::Release);
    }
    pub(super) fn pending_reports(&self) -> bool {
        self.async_reports
            .as_ref()
            .is_some_and(|r| r.active.load(Ordering::Acquire) > 0)
    }
    pub(super) fn report_deadline(&self) -> tokio::time::Instant {
        self.async_reports
            .as_ref()
            .map_or_else(tokio::time::Instant::now, |r| {
                *r.deadline.lock().unwrap_or_else(|e| e.into_inner())
            })
    }
    pub(super) fn transferred(
        &self,
        pool: &super::ServiceChannelSessions,
    ) -> Result<Arc<Self>, ChannelSessionError> {
        if *self.stop.borrow() {
            return Err(ChannelSessionError::Closed);
        }
        Ok(Arc::new(Self {
            #[cfg(test)]
            received_lanes: Default::default(),
            core: self.core.clone(),
            stop: self.stop.clone(),
            enrollment: self.enrollment.clone(),
            target: self.target.clone(),
            async_reports: self
                .async_reports
                .as_ref()
                .map(|_| AsyncCallReports::for_pool(pool))
                .transpose()?,
            handoff: AtomicBool::new(false),
            #[cfg(test)]
            faults: Default::default(),
        }))
    }
    pub(super) async fn wait_idle(&self) {
        self.core.wait_idle().await;
    }
    pub(super) async fn join_accepted(&self) -> bool {
        self.core.join_accepted().await
    }
    pub(super) async fn reply(
        &self,
        endpoint: &ServiceEndpoint,
        target: &ExactRouteKey,
        bytes: &[u8],
        expires: i64,
        connection: &ServiceConnection,
        initial: &ChannelBootstrapResponse,
        signer: &ChannelMessageSigner,
    ) -> Result<Vec<u8>, ChannelSessionError> {
        connection
            .ensure_open()
            .map_err(|_| ChannelSessionError::Closed)?;
        let now = chrono::Utc::now().timestamp_millis();
        let envelope = TransportEnvelope::decode(bytes, target, &initial.application_id, now)
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let ServiceEndpoint::Zenoh {
            route,
            route_revision,
            lanes,
            ..
        } = endpoint
        else {
            return Err(ChannelSessionError::InvalidConfig);
        };
        let input: NativeCallRequest = serde_json::from_str(envelope.payload.get())
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        if input.route_revision != *route_revision
            || !lanes.contains(&input.lane)
            || route.invoke_key(&input.lane).as_ref() != Ok(target)
            || route.application_id != initial.application_id
            || route.instance_id.as_str() != initial.instance.instance_id
            || route.base_generation.as_str() != initial.instance.generation
            || route.role_generation.as_str() != self.target.generation
            || envelope.deadline_unix_ms > expires
            || envelope.deadline_unix_ms > now + NATIVE_SYNC_CALL_TIMEOUT_MS
        {
            return Err(ChannelSessionError::InvalidResponse);
        }
        if !matches!(
            (&input.action, envelope.kind),
            (
                NativeCallAction::Invoke { .. }
                    | NativeCallAction::Readiness { .. }
                    | NativeCallAction::AsyncReadiness { .. },
                MessageKind::InvokeCall
            ) | (NativeCallAction::Cancel { .. }, MessageKind::CancelCall)
        ) {
            return Err(ChannelSessionError::InvalidResponse);
        }
        connection
            .verify_channel_message(&initial.transport_trust, &envelope)
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        super::trace::scope(envelope.trace_parent.as_deref(), async {
            #[cfg(test)]
            if matches!(&input.action, NativeCallAction::Invoke { .. }) {
                self.received_lanes[usize::from(input.lane.lane.index())]
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            #[cfg(test)]
            let faults = self.faults.lock().unwrap().clone();
            #[cfg(test)]
            let measure_invoke = matches!(&input.action, NativeCallAction::Invoke { .. });
            #[cfg(test)]
            if matches!(&input.action, NativeCallAction::Invoke { .. }) {
                if let Some(faults) = &faults {
                    faults
                        .invocations
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if let NativeCallAction::Invoke { invocation } = &input.action {
                        faults.requests.lock().unwrap().push((**invocation).clone());
                    }
                }
            }
            let response = match input.action {
                NativeCallAction::Readiness { target_instance } => {
                    if self.core.live_instance().as_ref() == Some(&target_instance)
                        && !*self.stop.borrow()
                    {
                        NativeCallResponse::Ready
                    } else {
                        NativeCallResponse::Rejected {
                            error: ServiceError::rejected(
                                "stale_instance",
                                "Execution binding is not current",
                            ),
                        }
                    }
                }
                NativeCallAction::AsyncReadiness { target_instance } => {
                    if self.async_reports.is_some()
                        && self.core.live_instance().as_ref() == Some(&target_instance)
                        && !*self.stop.borrow()
                    {
                        NativeCallResponse::AsyncReady
                    } else {
                        NativeCallResponse::Rejected {
                            error: ServiceError::rejected(
                                "native_async_unavailable",
                                "Finite Async execution binding is not current",
                            ),
                        }
                    }
                }
                NativeCallAction::Invoke { invocation } => {
                    let business = BusinessObservation::new(Plane::Call);
                    let result = match &invocation.context.invocation {
                        InvocationRole::Call(call)
                            if call.mode == CallMode::Sync && invocation.completion.is_none() =>
                        {
                            if invocation.context.deadline_ms > envelope.deadline_unix_ms {
                                Err(ServiceError::rejected(
                                    "invalid_deadline",
                                    "Execution must fit the finite query deadline",
                                ))
                            } else {
                                Ok(None)
                            }
                        }
                        InvocationRole::Call(call) if call.mode == CallMode::Async => {
                            if let Some(reports) = &self.async_reports {
                                reports
                                    .reporter(
                                        &invocation,
                                        &self.target,
                                        connection,
                                        initial,
                                        business.clone(),
                                    )
                                    .map(Some)
                            } else {
                                Err(ServiceError::rejected(
                                    "native_async_unavailable",
                                    "Native asynchronous completion is not enabled",
                                ))
                            }
                        }
                        _ => Err(ServiceError::rejected(
                            "native_async_unavailable",
                            "Native asynchronous completion is not enabled",
                        )),
                    };
                    let response = match result {
                        Err(error) => NativeCallResponse::Rejected { error },
                        Ok(reporter) => {
                            let result = if let Some(reporter) = reporter {
                                #[cfg(test)]
                                let reporter = faults
                                    .as_ref()
                                    .map_or_else(|| reporter.clone(), |f| f.wrap(reporter.clone()));
                                self.core.invoke_with_reporter(*invocation, reporter).await
                            } else {
                                self.core.invoke(*invocation).await
                            };
                            match result {
                                Ok(InvocationResponse::Completed { outcome }) => {
                                    NativeCallResponse::Completed { outcome }
                                }
                                Ok(InvocationResponse::Accepted { call_id, attempt }) => {
                                    NativeCallResponse::Accepted { call_id, attempt }
                                }
                                Err(CallExecutionError::Rejected { error, .. }) => {
                                    NativeCallResponse::Rejected { error }
                                }
                                _ => NativeCallResponse::OutcomeUnknown,
                            }
                        }
                    };
                    match &response {
                        NativeCallResponse::Completed { outcome } => {
                            business.finish(business_outcome(outcome))
                        }
                        NativeCallResponse::Rejected { .. } => {
                            business.finish(BusinessOutcome::Rejected)
                        }
                        NativeCallResponse::OutcomeUnknown => business.finish(BusinessOutcome::Unknown),
                        // The original reporter owns accepted Async result observation.
                        _ => {}
                    }
                    response
                }
                NativeCallAction::Cancel { cancellation } => {
                    if self.core.cancel_attempt(cancellation) {
                        NativeCallResponse::Cancelled
                    } else {
                        NativeCallResponse::Rejected {
                            error: ServiceError::rejected(
                                "stale_instance",
                                "Cancellation does not target the live instance",
                            ),
                        }
                    }
                }
            };
            #[cfg(test)]
            if matches!(&response, NativeCallResponse::Rejected { error } if error.code == "capacity_exhausted")
            {
                if let Some(faults) = &faults {
                    faults.rejected.send_modify(|count| *count += 1);
                }
            }
            #[cfg(test)]
            if matches!(&response, NativeCallResponse::Accepted { .. }) {
                if let Some(faults) = &faults {
                    faults.before_accepted().await?;
                }
            }
            #[cfg(test)]
            let encoding_started = std::time::Instant::now();
            let now = chrono::Utc::now().timestamp_millis();
            let payload = serde_json::value::to_raw_value(&response)
                .map_err(|_| ChannelSessionError::InvalidResponse)?;
            let subject = ClientChannelIdentity {
                application_id: initial.application_id.clone(),
                instance_id: route.instance_id.clone(),
                base_generation: route.base_generation.clone(),
                certificate_identity: initial.certificate.certificate_identity.clone(),
            };
            let proof = signer
                .sign_message(
                    &subject,
                    envelope.kind,
                    target,
                    &envelope.request_id,
                    payload.get().as_bytes(),
                    now,
                    envelope.deadline_unix_ms,
                )
                .map_err(|_| ChannelSessionError::InvalidResponse)?;
            let bytes = TransportEnvelope {
                protocol_version: ProtocolVersion::V1,
                kind: envelope.kind,
                request_id: envelope.request_id,
                application_id: initial.application_id.clone(),
                target: target.clone(),
                deadline_unix_ms: envelope.deadline_unix_ms,
                proof,
                trace_parent: super::trace::current_trace_parent(),
                payload,
            }
            .encode(now)
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
            #[cfg(test)]
            if measure_invoke {
                metrics::histogram!("lingshu_sdk_call_response_encoding_seconds", "transport" => "zenoh").record(encoding_started.elapsed().as_secs_f64());
                metrics::histogram!("lingshu_sdk_call_response_encoded_bytes", "transport" => "zenoh").record(bytes.len() as f64);
            }
            observation::prepared_call_response(&response);
            Ok(bytes)
        }).await
    }
}
impl AsyncCallReports {
    fn for_pool(pool: &super::ServiceChannelSessions) -> Result<Arc<Self>, ChannelSessionError> {
        Ok(Arc::new(Self {
            session: pool
                .sessions
                .first()
                .ok_or(ChannelSessionError::InvalidConfig)?
                .clone(),
            authority: pool.identity.bootstrap_response().clone(),
            signer: Arc::new(
                ChannelMessageSigner::from_pkcs8(&pool.identity.credential.key.serialize_der())
                    .map_err(|_| ChannelSessionError::InvalidConfig)?,
            ),
            active: AtomicUsize::new(0),
            deadline: std::sync::Mutex::new(tokio::time::Instant::now()),
            physical: pool.closed.clone(),
        }))
    }
    fn reporter(
        self: &Arc<Self>,
        invocation: &ServiceInvocation,
        instance: &ServiceInstanceTarget,
        connection: &ServiceConnection,
        initial: &ChannelBootstrapResponse,
        business: BusinessObservation,
    ) -> Result<Arc<dyn CallReporter>, ServiceError> {
        let invalid = || {
            ServiceError::rejected(
                "invalid_native_report",
                "Native Async requires the original finite renewable report authority",
            )
        };
        let InvocationRole::Call(call) = &invocation.context.invocation else {
            return Err(invalid());
        };
        let completion = invocation.completion.as_ref().ok_or_else(invalid)?;
        let heartbeat = completion.heartbeat.as_ref().ok_or_else(invalid)?;
        if invocation.target_instance.as_ref() != Some(instance)
            || heartbeat.execution_deadline_ms != invocation.context.deadline_ms
            || self.authority.deployment != initial.deployment
            || self.authority.application_id != initial.application_id
            || self.authority.instance != initial.instance
            || self.authority.certificate.certificate_identity
                != initial.certificate.certificate_identity
            || self.authority.certificate.message_public_key
                != initial.certificate.message_public_key
            || self.authority.certificate.expires_unix_ms != initial.certificate.expires_unix_ms
        {
            return Err(invalid());
        }
        let target = NativeCallReportTarget {
            route: CallReportRoute {
                deployment: initial.deployment.clone(),
                application_id: initial.application_id.clone(),
                call_id: RouteIdentity::new(call.call_id.clone()).map_err(|_| invalid())?,
                attempt: call.attempt,
                epoch: RouteIdentity::new(heartbeat.epoch.clone()).map_err(|_| invalid())?,
            },
            instance: instance.clone(),
            heartbeat: heartbeat.clone(),
        };
        let reporter = super::NativeCallReportClient::new(
            self.session.clone(),
            connection.clone(),
            self.authority.clone(),
            self.signer.clone(),
            target,
            completion.clone(),
        )
        .map_err(|_| invalid())?
        .with_physical_authority(self.physical.clone());
        let deadline = reporter.delivery_deadline();
        let mut original = self.deadline.lock().map_err(|_| invalid())?;
        *original = (*original).max(deadline);
        self.active.fetch_add(1, Ordering::AcqRel);
        Ok(Arc::new(TrackedCallReporter {
            reporter: Arc::new(reporter),
            owner: self.clone(),
            business,
        }))
    }
}

fn business_outcome(outcome: &ServiceOutcome) -> BusinessOutcome {
    match outcome {
        ServiceOutcome::Succeeded { .. } => BusinessOutcome::Succeeded,
        ServiceOutcome::Failed { .. } => BusinessOutcome::Failed,
    }
}

impl Drop for NativeCallExecution {
    fn drop(&mut self) {
        if !self.is_handoff() {
            self.stop();
        }
    }
}

impl super::ServiceChannelSessions {
    /// Enable only synchronous calls after role proof. Declarations and Handler
    /// signatures are unchanged. Each role admits at most one execution binding.
    pub fn enable_sync_calls(
        &self,
        role: &mut super::RegisteredChannelRole,
        registry: Arc<ServiceRegistry>,
        budget: ServiceExecutionBudget,
    ) -> Result<(), ChannelSessionError> {
        self.enable_calls(role, registry, budget, false)
    }

    /// Explicit finite Async binding, also preserving synchronous calls. Each
    /// accepted attempt uses its signed original renewable completion authority.
    /// The platform must independently install the exact report grant/receiver.
    /// Role expiry withdraws admission; accepted renewable attempts retain their
    /// independent report authority. Explicit stop or pool/root closure cancels
    /// and joins them. This does not enable product Async dispatch.
    pub fn enable_async_calls(
        &self,
        role: &mut super::RegisteredChannelRole,
        registry: Arc<ServiceRegistry>,
        budget: ServiceExecutionBudget,
    ) -> Result<(), ChannelSessionError> {
        self.enable_calls(role, registry, budget, true)
    }

    fn enable_calls(
        &self,
        role: &mut super::RegisteredChannelRole,
        registry: Arc<ServiceRegistry>,
        budget: ServiceExecutionBudget,
        asynchronous: bool,
    ) -> Result<(), ChannelSessionError> {
        self.active_role_channel()?;
        let initial = self.identity.bootstrap_response();
        let ChannelRoleEnrollmentResponse::Call(response) = &role.response else {
            return Err(ChannelSessionError::InvalidConfig);
        };
        if !role.route_confirmed()
            || role.channel.application_id != initial.application_id
            || role.channel.certificate_identity != initial.certificate.certificate_identity
            || role.channel.instance_id.as_str() != initial.instance.instance_id
            || role.channel.base_generation.as_str() != initial.instance.generation
            || registry.manifest().application_id != initial.application_id.as_str()
            || role.call_contract.as_ref()
                != Some(&(registry.capabilities(), budget.maximum_in_flight()))
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let async_reports = if asynchronous {
            Some(AsyncCallReports::for_pool(self)?)
        } else {
            None
        };
        let mut slot = role
            .calls
            .lock()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        if slot.is_some() {
            return Err(ChannelSessionError::InvalidConfig);
        }
        *slot = Some(NativeCallExecution::new(
            registry,
            self.identity.connection.clone(),
            budget,
            ServiceInstanceTarget {
                node_id: response.session.node_id.clone(),
                generation: response.session.generation.clone(),
            },
            role.lease_window.borrow().expires_at_ms,
            async_reports,
        )?);
        Ok(())
    }
}

#[cfg(all(test, feature = "http-client"))]
#[path = "call_host_tests.rs"]
mod host_tests;
#[cfg(all(test, feature = "http-client", feature = "service-http"))]
#[path = "call_performance_host_tests.rs"]
mod performance_host_tests;
#[cfg(all(test, feature = "http-client"))]
#[path = "call_scale_host_tests.rs"]
mod scale_host_tests;
#[cfg(all(test, feature = "http-client"))]
#[path = "call_task_host_tests.rs"]
mod task_host_tests;
#[cfg(test)]
#[path = "call_tests.rs"]
pub(crate) mod tests;

#[cfg(test)]
#[path = "call_faults.rs"]
mod faults;
