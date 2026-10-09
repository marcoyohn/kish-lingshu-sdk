//! Native publication reuses the SDK's bounded in-call retry and journal owner.
use super::{ChannelCloseReason, ChannelSessionError, ServiceChannelSessions};
use crate::{
    binding::retry::{execute_with_retry, ReplaySafety, RetryFailure, RetryPolicy, RetryReason},
    event_dispatch::{EventDispatch, PublishEvent, PublishReceipt},
    ApplicationFailure, Error, MutationOptions, TransportFailure, TransportKind,
};
use kish_lingshu_event_dispatch_contract::{NativePublicationRequest, NativePublicationResponse};
use kish_lingshu_foundation_contract::{
    service_auth::{ChannelMessageSigner, ClientChannelIdentity},
    service_transport::{
        bootstrap::ChannelBootstrapResponse, MessageKind, ProtocolVersion, RouteIdentity,
        TransportEnvelope, MAX_BUSINESS_PAYLOAD_BYTES, MAX_ENVELOPE_OVERHEAD_BYTES,
    },
};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, time::Instant};

#[derive(Clone)]
pub(crate) struct NativePublicationSource {
    pub(crate) app_id: String,
    pub(crate) current: watch::Receiver<Option<Arc<NativePublicationClient>>>,
}
impl NativePublicationSource {
    pub(crate) async fn publish(
        &self,
        event: PublishEvent,
        options: MutationOptions,
        config: &crate::ClientConfig,
    ) -> Result<PublishReceipt, Error> {
        let client = self.current.borrow().clone().ok_or_else(|| {
            Error::Transport(TransportFailure {
                kind: TransportKind::Request,
                message: "native publication channel stopped".into(),
                request_id: Some(options.request().request_id().clone()),
                retryable: true,
            })
        })?;
        client.publish(event, options, config).await
    }
}
pub(crate) struct NativePublicationClient {
    session: zenoh::Session,
    initial: ChannelBootstrapResponse,
    signer: ChannelMessageSigner,
    connection: crate::ServiceConnection,
    closed: watch::Receiver<Option<ChannelCloseReason>>,
    authority: watch::Receiver<Instant>,
    inflight: Arc<tokio::sync::Semaphore>,
    #[cfg(test)]
    discard_acceptance: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    wire_attempts: std::sync::atomic::AtomicUsize,
}
impl ServiceChannelSessions {
    pub(crate) fn publication_client(
        &self,
    ) -> Result<Arc<NativePublicationClient>, ChannelSessionError> {
        self.active_role_channel()?;
        Ok(Arc::new(NativePublicationClient {
            session: self
                .sessions
                .first()
                .ok_or(ChannelSessionError::InvalidConfig)?
                .clone(),
            initial: self.identity.bootstrap_response().clone(),
            signer: ChannelMessageSigner::from_pkcs8(&self.identity.credential.key.serialize_der())
                .map_err(|_| ChannelSessionError::InvalidConfig)?,
            connection: self.identity.connection.clone(),
            closed: self.closed.clone(),
            authority: self.authority.clone(),
            inflight: self.publication_query_slots.clone(),
            #[cfg(test)]
            discard_acceptance: Default::default(),
            #[cfg(test)]
            wire_attempts: Default::default(),
        }))
    }
}
#[cfg(test)]
impl NativePublicationClient {
    pub(super) fn test_bootstrap(&self) -> &ChannelBootstrapResponse {
        &self.initial
    }
}
impl EventDispatch {
    /// Select native publication explicitly; metadata APIs retain their binding.
    /// The returned handle is pinned to this physical pool and expires with it.
    /// There is no HTTP fallback or background journal recovery.
    pub fn with_channel(
        mut self,
        channel: &ServiceChannelSessions,
    ) -> Result<Self, ChannelSessionError> {
        if self.inner.application_id.as_deref()
            != Some(
                channel
                    .identity
                    .bootstrap_response()
                    .application_id
                    .as_str(),
            )
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let (_, current) = watch::channel(Some(channel.publication_client()?));
        self.native_publication = Some(NativePublicationSource {
            app_id: channel
                .identity
                .bootstrap_response()
                .application_id
                .as_str()
                .into(),
            current,
        });
        Ok(self)
    }
}
impl EventDispatch {
    /// Follow only the supervisor's successfully finalized pool. A logical send
    /// retains its original pool; future calls/recovery use the adopted identity.
    pub fn with_managed_channel(
        mut self,
        channel: &super::ManagedRoleChannel,
    ) -> Result<Self, ChannelSessionError> {
        if self.inner.application_id.as_deref() != Some(channel.publication.app_id.as_str()) {
            return Err(ChannelSessionError::InvalidConfig);
        }
        self.native_publication = Some(channel.publication.clone());
        Ok(self)
    }
}
impl NativePublicationClient {
    fn available(&self) -> bool {
        self.connection.ensure_open().is_ok()
            && self.closed.borrow().is_none()
            && Instant::now() < *self.authority.borrow()
            && !self.session.is_closed()
    }
    pub(crate) async fn publish(
        &self,
        event: PublishEvent,
        options: MutationOptions,
        config: &crate::ClientConfig,
    ) -> Result<PublishReceipt, Error> {
        let input = NativePublicationRequest {
            event,
            idempotency_key: options.idempotency_key().to_string(),
            request_id: options.request().request_id().to_string(),
            correlation_id: options.request().correlation_id().to_string(),
            traceparent: options
                .request()
                .trace()
                .and_then(|t| t.traceparent.clone()),
            tracestate: options.request().trace().and_then(|t| t.tracestate.clone()),
        };
        let bytes: Arc<[u8]> = serde_json::to_vec(&input)
            .map_err(|_| Error::configuration("event", "native publication cannot be encoded"))?
            .into();
        if bytes.len() > MAX_BUSINESS_PAYLOAD_BYTES {
            return Err(Error::configuration(
                "event",
                "native publication exceeds protocol limit",
            ));
        }
        // One logical deadline across retries; each signed wire attempt gets a
        // new nonce while event bytes and scoped idempotency key remain fixed.
        let now = chrono::Utc::now();
        let deadline = options
            .request()
            .deadline()
            .map(|d| *d.as_datetime())
            .unwrap_or(
                now + chrono::Duration::from_std(config.timeout())
                    .unwrap_or(chrono::Duration::seconds(30)),
            );
        let request_options = options.request().clone().with_deadline(deadline);
        execute_with_retry(
            RetryPolicy::new(config.retry_limit()),
            ReplaySafety::IdempotentMutation,
            &request_options,
            bytes,
            |_, body| {
                let options = &options;
                async move {
                    self.send(body, options, deadline).await.map_err(|error| {
                        let reason = match &error {
                            Error::Application(failure) if failure.problem.retryable => {
                                RetryReason::HttpStatus {
                                    status: 503,
                                    retry_after: failure.retry_after,
                                }
                            }
                            Error::Transport(failure) if failure.retryable => {
                                RetryReason::Transport
                            }
                            _ => RetryReason::HttpStatus {
                                status: 400,
                                retry_after: None,
                            },
                        };
                        RetryFailure { error, reason }
                    })
                }
            },
        )
        .await
    }
    #[tracing::instrument(skip_all, name = "lingshu.sdk.native.publication.send")]
    async fn send(
        &self,
        body: Arc<[u8]>,
        options: &MutationOptions,
        logical_deadline: chrono::DateTime<chrono::Utc>,
    ) -> Result<PublishReceipt, Error> {
        super::trace::scope(
            options
                .request()
                .trace()
                .and_then(|trace| trace.traceparent.as_deref()),
            self.send_scoped(body, options, logical_deadline),
        )
        .await
    }
    async fn send_scoped(
        &self,
        body: Arc<[u8]>,
        options: &MutationOptions,
        logical_deadline: chrono::DateTime<chrono::Utc>,
    ) -> Result<PublishReceipt, Error> {
        let error = |retryable: bool| {
            Error::Transport(TransportFailure {
                kind: TransportKind::Request,
                message: "native Event publication unavailable or unconfirmed".into(),
                request_id: Some(options.request().request_id().clone()),
                retryable,
            })
        };
        if !self.available() {
            return Err(error(true));
        }
        let _permit = self.inflight.clone().try_acquire_owned().map_err(|_| {
            super::observation::rejected(
                super::observation::Plane::Publication,
                super::observation::Rejection::CountExhausted,
            );
            error(true)
        })?;
        let target = self
            .initial
            .control_route
            .publication_key()
            .map_err(|_| error(false))?;
        let now = chrono::Utc::now().timestamp_millis();
        let deadline_ms = (now + 10_000).min(logical_deadline.timestamp_millis());
        if deadline_ms <= now {
            return Err(error(true));
        }
        let deadline = (*self.authority.borrow())
            .min(Instant::now() + Duration::from_millis((deadline_ms - now) as u64));
        let request_id =
            RouteIdentity::new(uuid::Uuid::new_v4().to_string()).map_err(|_| error(false))?;
        let payload = serde_json::value::RawValue::from_string(
            String::from_utf8(body.to_vec()).map_err(|_| error(false))?,
        )
        .map_err(|_| error(false))?;
        let subject = ClientChannelIdentity {
            application_id: self.initial.application_id.clone(),
            instance_id: RouteIdentity::new(&self.initial.instance.instance_id)
                .map_err(|_| error(false))?,
            base_generation: RouteIdentity::new(&self.initial.instance.generation)
                .map_err(|_| error(false))?,
            certificate_identity: self.initial.certificate.certificate_identity.clone(),
        };
        let proof = self
            .signer
            .sign_message(
                &subject,
                MessageKind::PublishEvent,
                &target,
                &request_id,
                payload.get().as_bytes(),
                now,
                deadline_ms,
            )
            .map_err(|_| error(false))?;
        let bytes = TransportEnvelope {
            protocol_version: ProtocolVersion::V1,
            kind: MessageKind::PublishEvent,
            request_id: request_id.clone(),
            application_id: self.initial.application_id.clone(),
            target: target.clone(),
            deadline_unix_ms: deadline_ms,
            proof,
            trace_parent: super::trace::current_trace_parent().or_else(|| {
                options
                    .request()
                    .trace()
                    .and_then(|value| value.traceparent.as_deref())
                    .and_then(kish_lingshu_foundation_contract::trace::TraceParent::parse)
                    .map(|value| value.to_string())
            }),
            payload,
        }
        .encode(now)
        .map_err(|_| error(false))?;
        let mut exchange =
            super::observation::ExchangeObservation::new(super::observation::Plane::Publication);
        let operation = async {
            #[cfg(test)]
            self.wire_attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let replies = self
                .session
                .get(target.as_str().to_owned())
                .target(zenoh::query::QueryTarget::All)
                .consolidation(zenoh::query::ConsolidationMode::None)
                .timeout(deadline.saturating_duration_since(Instant::now()))
                .payload(bytes)
                .await
                .map_err(|_| error(true))?;
            let reply = replies.recv_async().await.map_err(|_| error(true))?;
            let result = reply.result();
            let sample = result.as_ref().map_err(|_| error(true))?;
            if sample.key_expr().as_str() != target.as_str()
                || sample.payload().len() > MAX_BUSINESS_PAYLOAD_BYTES + MAX_ENVELOPE_OVERHEAD_BYTES
            {
                return Err(error(true));
            }
            let envelope = TransportEnvelope::decode(
                &sample.payload().to_bytes(),
                &target,
                &self.initial.application_id,
                chrono::Utc::now().timestamp_millis(),
            )
            .map_err(|_| error(true))?;
            if envelope.kind != MessageKind::PublishEvent
                || envelope.request_id != request_id
                || envelope.deadline_unix_ms != deadline_ms
            {
                return Err(error(true));
            }
            self.connection
                .verify_channel_message(&self.initial.transport_trust, &envelope)
                .map_err(|_| error(true))?;
            if replies.recv_async().await.is_ok() || !self.available() {
                return Err(error(true));
            }
            match serde_json::from_str(envelope.payload.get()).map_err(|_| error(true))? {
                NativePublicationResponse::Accepted { receipt }
                    if receipt.mutation.request_id == *options.request().request_id() =>
                {
                    super::observation::disposition(
                        super::observation::Plane::Publication,
                        super::observation::Disposition::Accepted,
                    );
                    // Fixture-only loss after a fully verified center receipt;
                    // the next bounded send keeps the original Event/key.
                    #[cfg(test)]
                    if self
                        .discard_acceptance
                        .swap(false, std::sync::atomic::Ordering::SeqCst)
                    {
                        return Err(error(true));
                    }
                    exchange.reply_verified();
                    Ok(receipt)
                }
                NativePublicationResponse::Rejected {
                    code,
                    message,
                    retryable,
                    retry_after_ms,
                } => {
                    let code = kish_lingshu_runtime_contract::ProblemCode::new(code)
                        .map_err(|_| error(true))?;
                    let mut failure = ApplicationFailure::new(
                        kish_lingshu_runtime_contract::ApplicationProblem::new(
                            code,
                            message,
                            options.request().request_id().clone(),
                        )
                        .with_retryable(retryable),
                    );
                    failure.retry_after = retry_after_ms.map(Duration::from_millis);
                    super::observation::disposition(
                        super::observation::Plane::Publication,
                        if retryable {
                            super::observation::Disposition::RetryableFailure
                        } else {
                            super::observation::Disposition::PermanentFailure
                        },
                    );
                    exchange.reply_verified();
                    Err(Error::Application(failure))
                }
                _ => Err(error(true)),
            }
        };
        let mut physical = self.closed.clone();
        let mut logical = self.connection.subscribe_closed();
        tokio::select! {
            biased;
            _ = physical.changed() => Err(error(true)),
            _ = logical.changed() => Err(error(true)),
            result = tokio::time::timeout_at(deadline, operation) => result.map_err(|_| error(true))?,
        }
    }
}

#[cfg(test)]
impl NativePublicationSource {
    pub(crate) fn discard_next_acceptance(&self) {
        self.current
            .borrow()
            .as_ref()
            .unwrap()
            .discard_acceptance
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    pub(crate) fn wire_attempts(&self) -> usize {
        self.current
            .borrow()
            .as_ref()
            .unwrap()
            .wire_attempts
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}
