//! One fixed renewable attempt's report exchange. This low-level client does
//! not grant a report route, enable Async invocation, or run a retry scheduler.
use super::observation::{self, Disposition, ExchangeObservation, Plane, Rejection};
use crate::{services::execution::*, services::*, ServiceConnection};
use kish_lingshu_foundation_contract::{
    service_auth::{ChannelMessageSigner, ClientChannelIdentity},
    service_transport::{
        bootstrap::ChannelBootstrapResponse, ExactRouteKey, MessageKind, ProtocolVersion,
        RouteIdentity, TransportEnvelope,
    },
};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use zenoh::{
    query::{ConsolidationMode, QueryTarget},
    Session,
};
type Error = NativeCallReportRejection;

/// SDK-side primitive for an already platform-authorized renewable attempt.
/// It pins the original capability, route, instance, CSR and hard deadlines.
/// Construction is not evidence that the platform has granted or bound a route.
pub struct NativeCallReportClient {
    session: Session,
    connection: ServiceConnection,
    authority: ChannelBootstrapResponse,
    signer: Arc<ChannelMessageSigner>,
    target: NativeCallReportTarget,
    completion: CompletionTarget,
    key: ExactRouteKey,
    execution_deadline: Instant,
    delivery_deadline: Instant,
    budget: Arc<tokio::sync::Semaphore>,
    physical: Option<tokio::sync::watch::Receiver<Option<super::ChannelCloseReason>>>,
}
impl NativeCallReportClient {
    pub(super) fn with_physical_authority(
        mut self,
        physical: tokio::sync::watch::Receiver<Option<super::ChannelCloseReason>>,
    ) -> Self {
        self.physical = Some(physical);
        self
    }
    pub(super) fn delivery_deadline(&self) -> Instant {
        self.delivery_deadline
    }
    pub fn new(
        session: Session,
        connection: ServiceConnection,
        authority: ChannelBootstrapResponse,
        signer: Arc<ChannelMessageSigner>,
        target: NativeCallReportTarget,
        completion: CompletionTarget,
    ) -> Result<Self, Error> {
        let now = chrono::Utc::now().timestamp_millis();
        let started = Instant::now();
        connection
            .ensure_open()
            .map_err(|_| Error::AuthorityUnavailable)?;
        target.validate(now).map_err(|_| Error::Rejected)?;
        let public_key: String = signer
            .public_key()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        if authority.application_id.as_str() != connection.application_id()
            || target.route.application_id != authority.application_id
            || target.route.deployment != authority.deployment
            || authority.certificate.message_public_key != public_key
            || completion.heartbeat.as_ref() != Some(&target.heartbeat)
            || completion.token.is_empty()
            || completion.token.len() > 16 * 1024
            || target.heartbeat.delivery_deadline_ms > authority.certificate.expires_unix_ms
        {
            return Err(Error::Rejected);
        }
        let execution_deadline = started
            + Duration::from_millis(
                target.heartbeat.execution_deadline_ms.saturating_sub(now) as u64
            );
        let delivery_deadline = started
            + Duration::from_millis(
                target.heartbeat.delivery_deadline_ms.saturating_sub(now) as u64
            );
        let key = target.route.key().map_err(|_| Error::Rejected)?;
        let budget = connection.call_report_budget();
        Ok(Self {
            session,
            connection,
            authority,
            signer,
            target,
            completion,
            key,
            execution_deadline,
            delivery_deadline,
            budget,
            physical: None,
        })
    }
    fn scope(
        &self,
        completion: &CompletionTarget,
        action: &NativeCallReportAction,
    ) -> Result<(MessageKind, i64, Instant), Error> {
        if completion != &self.completion {
            return Err(Error::Rejected);
        }
        let route = &self.target.route;
        match action {
            NativeCallReportAction::Heartbeat { heartbeat: h }
                if h.version == CALL_HEARTBEAT_VERSION
                    && h.call_id == route.call_id.as_str()
                    && h.attempt == route.attempt
                    && h.epoch == route.epoch.as_str()
                    && h.instance == self.target.instance =>
            {
                let (wall, mono) = if h.phase == CallPhase::Running {
                    (
                        self.target.heartbeat.execution_deadline_ms,
                        self.execution_deadline,
                    )
                } else {
                    (
                        self.target.heartbeat.delivery_deadline_ms,
                        self.delivery_deadline,
                    )
                };
                Ok((MessageKind::CallHeartbeat, wall, mono))
            }
            NativeCallReportAction::Complete { completion: c }
                if c.contract_version == SERVICE_CONTRACT_VERSION
                    && c.call_id == route.call_id.as_str()
                    && c.attempt == route.attempt =>
            {
                Ok((
                    MessageKind::CompleteCall,
                    self.target.heartbeat.delivery_deadline_ms,
                    self.delivery_deadline,
                ))
            }
            _ => Err(Error::Rejected),
        }
    }
    #[tracing::instrument(skip_all, name = "lingshu.sdk.native.report.send")]
    async fn exchange(
        &self,
        completion: &CompletionTarget,
        action: NativeCallReportAction,
        timeout: Option<Duration>,
    ) -> Result<NativeCallReportResponse, Error> {
        super::trace::scope(None, self.exchange_scoped(completion, action, timeout)).await
    }
    async fn exchange_scoped(
        &self,
        completion: &CompletionTarget,
        action: NativeCallReportAction,
        timeout: Option<Duration>,
    ) -> Result<NativeCallReportResponse, Error> {
        self.connection
            .ensure_open()
            .map_err(|_| Error::AuthorityUnavailable)?;
        let (kind, hard_wall, hard_mono) = self.scope(completion, &action)?;
        let plane = if kind == MessageKind::CallHeartbeat {
            Plane::Heartbeat
        } else {
            Plane::Completion
        };
        let _permit = self.budget.try_acquire().map_err(|_| {
            observation::rejected(plane, Rejection::CountExhausted);
            Error::Unavailable
        })?;
        let now = chrono::Utc::now().timestamp_millis();
        let remaining = hard_mono.saturating_duration_since(Instant::now());
        let duration = timeout
            .unwrap_or(Duration::from_secs(3))
            .min(Duration::from_secs(3))
            .min(remaining)
            .min(Duration::from_millis(
                hard_wall.saturating_sub(now).max(0) as u64
            ));
        if duration.as_millis() == 0 {
            return Err(Error::AuthorityUnavailable);
        }
        let deadline_ms = now.saturating_add(duration.as_millis() as i64);
        let deadline = Instant::now() + duration;
        let payload = serde_json::value::to_raw_value(&NativeCallReportRequest {
            instance: self.target.instance.clone(),
            token: completion.token.clone(),
            action,
        })
        .map_err(|_| Error::Rejected)?;
        if payload.get().len() > MAX_SERVICE_PAYLOAD_BYTES {
            return Err(Error::Rejected);
        }
        let subject = ClientChannelIdentity {
            application_id: self.authority.application_id.clone(),
            instance_id: RouteIdentity::new(self.authority.instance.instance_id.clone())
                .map_err(|_| Error::Rejected)?,
            base_generation: RouteIdentity::new(self.authority.instance.generation.clone())
                .map_err(|_| Error::Rejected)?,
            certificate_identity: self.authority.certificate.certificate_identity.clone(),
        };
        let request_id =
            RouteIdentity::new(uuid::Uuid::new_v4().to_string()).map_err(|_| Error::Rejected)?;
        let proof = self
            .signer
            .sign_message(
                &subject,
                kind,
                &self.key,
                &request_id,
                payload.get().as_bytes(),
                now,
                deadline_ms,
            )
            .map_err(|_| Error::Rejected)?;
        let request = TransportEnvelope {
            protocol_version: ProtocolVersion::V1,
            kind,
            request_id,
            application_id: subject.application_id,
            target: self.key.clone(),
            deadline_unix_ms: deadline_ms,
            proof,
            trace_parent: super::trace::current_trace_parent(),
            payload,
        };
        let bytes = request.encode(now).map_err(|_| Error::Rejected)?;
        let mut exchange = ExchangeObservation::new(plane);
        let query = async {
            let replies = self
                .session
                .get(self.key.as_str().to_owned())
                .target(QueryTarget::All)
                .consolidation(ConsolidationMode::None)
                .payload(bytes)
                .timeout(duration)
                .await
                .map_err(|_| Error::Unavailable)?;
            let reply = replies.recv_async().await.map_err(|_| Error::Unavailable)?;
            let result = reply.result();
            let sample = result.as_ref().map_err(|_| Error::Unavailable)?;
            if sample.key_expr().as_str() != self.key.as_str()
                || sample.payload().len() > MAX_NATIVE_CALL_REPORT_REPLY_BYTES
            {
                return Err(Error::Unavailable);
            }
            let response = TransportEnvelope::decode(
                &sample.payload().to_bytes(),
                &self.key,
                &request.application_id,
                chrono::Utc::now().timestamp_millis(),
            )
            .map_err(|_| Error::Unavailable)?;
            if response.kind != kind
                || response.request_id != request.request_id
                || response.deadline_unix_ms != deadline_ms
            {
                return Err(Error::Unavailable);
            }
            self.connection
                .verify_channel_message(&self.authority.transport_trust, &response)
                .map_err(|_| Error::Unavailable)?;
            let output: NativeCallReportResponse =
                serde_json::from_str(response.payload.get()).map_err(|_| Error::Unavailable)?;
            if replies.recv_async().await.is_ok() || Instant::now() >= deadline {
                return Err(Error::Unavailable);
            }
            match output {
                NativeCallReportResponse::Rejected { reason } => {
                    observation::disposition(
                        plane,
                        match reason {
                            Error::AuthorityUnavailable => Disposition::AuthorityUnavailable,
                            Error::Rejected => Disposition::Rejected,
                            Error::Unavailable => Disposition::Unavailable,
                        },
                    );
                    exchange.reply_verified();
                    Err(reason)
                }
                NativeCallReportResponse::Heartbeat { ref disposition }
                    if kind == MessageKind::CallHeartbeat =>
                {
                    observation::disposition(
                        plane,
                        match disposition {
                            HeartbeatDisposition::Renewed { .. } => Disposition::Renewed,
                            HeartbeatDisposition::Recovering => Disposition::Recovering,
                            HeartbeatDisposition::Invalidated => Disposition::Invalidated,
                        },
                    );
                    exchange.reply_verified();
                    Ok(output)
                }
                NativeCallReportResponse::Completed { disposition }
                    if kind == MessageKind::CompleteCall =>
                {
                    observation::disposition(
                        plane,
                        match disposition {
                            CompletionDisposition::Recorded => Disposition::Recorded,
                            CompletionDisposition::Duplicate => Disposition::Duplicate,
                            CompletionDisposition::Invalidated => Disposition::Invalidated,
                        },
                    );
                    exchange.reply_verified();
                    Ok(output)
                }
                _ => Err(Error::Unavailable),
            }
        };
        let mut closed = self.connection.subscribe_closed();
        tokio::select! {
            _ = closed.changed() => Err(Error::AuthorityUnavailable),
            result = tokio::time::timeout_at(deadline, query) => result.unwrap_or(Err(Error::Unavailable)),
        }
    }
    pub async fn heartbeat(
        &self,
        target: &CompletionTarget,
        heartbeat: &ServiceHeartbeat,
    ) -> Result<HeartbeatDisposition, Error> {
        match self
            .exchange(
                target,
                NativeCallReportAction::Heartbeat {
                    heartbeat: heartbeat.clone(),
                },
                None,
            )
            .await?
        {
            NativeCallReportResponse::Heartbeat { disposition } => Ok(disposition),
            _ => Err(Error::Unavailable),
        }
    }
    pub async fn complete(
        &self,
        target: &CompletionTarget,
        result: &ServiceCompletion,
        timeout: Option<Duration>,
    ) -> Result<CompletionDisposition, Error> {
        match self
            .exchange(
                target,
                NativeCallReportAction::Complete {
                    completion: result.clone(),
                },
                timeout,
            )
            .await?
        {
            NativeCallReportResponse::Completed { disposition } => Ok(disposition),
            _ => Err(Error::Unavailable),
        }
    }
}
fn report_error(e: Error) -> CallReportError {
    match e {
        Error::AuthorityUnavailable => CallReportError::AuthorityUnavailable,
        Error::Rejected => CallReportError::Rejected,
        Error::Unavailable => CallReportError::Unavailable,
    }
}
#[async_trait::async_trait]
impl CallReporter for NativeCallReportClient {
    async fn authority_lost(&self) {
        let physical = async {
            if let Some(mut closed) = self.physical.clone() {
                loop {
                    if closed.borrow_and_update().is_some() {
                        return;
                    }
                    if closed.changed().await.is_err() {
                        return;
                    }
                }
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! { _ = physical => {}, _ = tokio::time::sleep_until(self.delivery_deadline) => {} }
    }
    async fn progress(
        &self,
        _: &CompletionTarget,
        _: &ServiceProgress,
    ) -> Result<ProgressDisposition, CallReportError> {
        // Native reports require the renewable protocol. Never use HTTP progress.
        Err(CallReportError::Rejected)
    }
    async fn heartbeat(
        &self,
        target: &CompletionTarget,
        h: &ServiceHeartbeat,
    ) -> Result<HeartbeatDisposition, CallReportError> {
        self.heartbeat(target, h).await.map_err(report_error)
    }
    async fn complete(
        &self,
        target: &CompletionTarget,
        result: &ServiceCompletion,
        timeout: Option<Duration>,
    ) -> Result<CompletionDisposition, CallReportError> {
        self.complete(target, result, timeout)
            .await
            .map_err(report_error)
    }
}

#[cfg(test)]
#[path = "call_report_tests.rs"]
pub(crate) mod tests;
