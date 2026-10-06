//! Finite, outbound-only physical sessions. No declarations, role renewal,
//! business replay or publication recovery run in this lifecycle owner.
use super::ServiceChannelIdentity;
use base64::{engine::general_purpose::STANDARD, Engine};
use kish_lingshu_foundation_contract::service_transport::{
    bootstrap::TlsEndpoint,
    channel::{ChannelAuthorization, ChannelRotationFinalization, MAX_CHANNEL_CONTROL_BYTES},
    MessageKind, ProtocolVersion, RouteIdentity, TransportEnvelope, MAX_CONTROL_PAYLOAD_BYTES,
    MAX_DATA_LANES, MAX_ENVELOPE_OVERHEAD_BYTES,
};
use std::{fmt, time::Duration};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
// Stock CloseBuilder has a ten-second internal timeout. Leave it time to
// finish rather than cancelling its cleanup with an earlier outer deadline.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(12);

// Cancellation or a panicking lifecycle task is not proof that native cleanup
// completed. Reserve that capacity until the logical connection is replaced.
struct SessionPermit(Option<tokio::sync::OwnedSemaphorePermit>);
impl SessionPermit {
    fn release(&mut self) {
        self.0.take();
    }
}
impl Drop for SessionPermit {
    fn drop(&mut self) {
        if let Some(permit) = self.0.take() {
            permit.forget();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChannelSessionError {
    #[error("invalid channel session configuration")]
    InvalidConfig,
    #[error("channel sessions require a Tokio multi-thread runtime")]
    UnsupportedRuntime,
    #[error("logical service connection channel capacity exhausted")]
    CapacityExceeded,
    #[error("channel authority expired")]
    AuthorityExpired,
    #[error("logical service connection closed")]
    Closed,
    #[error("channel connection failed")]
    Transport,
    #[error("invalid channel control response")]
    InvalidResponse,
    #[error("managed certificate rotation failed")]
    RotationFailed,
    #[error("channel cleanup failed")]
    CleanupFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelCloseReason {
    Explicit,
    ConnectionClosed,
    RotationFailed,
    AuthorityExpired,
    CleanupFailed,
}

/// One independent Session per lane; cloning a Session never increases this count.
#[derive(Debug, Clone, Copy)]
pub struct ChannelSessionConfig {
    lanes: usize,
    dedicated_control: bool,
}
impl Default for ChannelSessionConfig {
    fn default() -> Self {
        Self {
            lanes: 1,
            dedicated_control: false,
        }
    }
}
impl ChannelSessionConfig {
    pub fn new(lanes: usize) -> Result<Self, ChannelSessionError> {
        if !(1..=MAX_DATA_LANES).contains(&lanes) {
            return Err(ChannelSessionError::InvalidConfig);
        }
        Ok(Self {
            lanes,
            dedicated_control: false,
        })
    }
    /// Reserve a separate physical Session for control queries. It shares the
    /// same finite identity and four-Session connection cap; four data lanes
    /// therefore cannot also request a dedicated control Session.
    pub fn with_control_lane(mut self) -> Result<Self, ChannelSessionError> {
        if self.lanes >= MAX_DATA_LANES {
            return Err(ChannelSessionError::InvalidConfig);
        }
        self.dedicated_control = true;
        Ok(self)
    }
    pub fn has_control_lane(self) -> bool {
        self.dedicated_control
    }
    pub fn session_count(self) -> usize {
        self.lanes + usize::from(self.dedicated_control)
    }
    #[cfg(test)]
    pub(super) fn host_test() -> Self {
        let config = Self::new(
            std::env::var("LINGSHU_CHANNEL_TEST_LANES").map_or(1, |v| v.parse().unwrap()),
        )
        .unwrap();
        if std::env::var("LINGSHU_CHANNEL_TEST_CONTROL_LANE").as_deref() == Ok("true") {
            config.with_control_lane().unwrap()
        } else {
            config
        }
    }
    pub fn lanes(self) -> usize {
        self.lanes
    }
}

/// Prototype physical transport owner, not a ready business instance. Its
/// lifetime ends at the current finite authorization deadline. Only a scoped
/// signed control observation may update it; role and route binding are separate.
/// Call `close` to join cleanup; dropping also requests cleanup on the runtime.
pub struct ServiceChannelSessions {
    pub(super) identity: ServiceChannelIdentity,
    pub(super) sessions: Vec<zenoh::Session>,
    config: ChannelSessionConfig,
    stop: watch::Sender<bool>,
    pub(super) closed: watch::Receiver<Option<ChannelCloseReason>>,
    pub(super) task: Option<JoinHandle<Result<(), ChannelSessionError>>>,
    cleanup_failed: bool,
    pub(super) rotation_finalized: bool,
    pub(super) report_draining: bool,
    pub(super) authority: watch::Sender<Instant>,
    pub(super) connectivity: watch::Receiver<super::ChannelConnectivityStatus>,
    #[cfg(feature = "service-call-zenoh")]
    pub(super) call_query_slots: std::sync::Arc<tokio::sync::Semaphore>,
    #[cfg(feature = "event-consumer-zenoh")]
    pub(super) consumer_query_slots: std::sync::Arc<tokio::sync::Semaphore>,
    #[cfg(feature = "event-publication-zenoh")]
    pub(super) publication_query_slots: std::sync::Arc<tokio::sync::Semaphore>,
    pub(super) role_slots: std::sync::Arc<tokio::sync::Semaphore>,
    pub(super) role_control_slots: std::sync::Arc<tokio::sync::Semaphore>,
    pub(super) role_control_bytes: std::sync::Arc<tokio::sync::Semaphore>,
    pub(super) role_query_slots: std::sync::Arc<tokio::sync::Semaphore>,
    pub(super) role_query_bytes: std::sync::Arc<tokio::sync::Semaphore>,
    pub(super) role_keys: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    pub(super) declaration_observation: std::sync::Arc<super::observation::DeclarationAccounting>,
}
impl fmt::Debug for ServiceChannelSessions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceChannelSessions")
            .field("data_lanes", &self.lane_count())
            .field("control_lane", &self.config.dedicated_control)
            .field("close_reason", &*self.closed.borrow())
            .finish_non_exhaustive()
    }
}
impl ServiceChannelSessions {
    #[cfg(feature = "service-call-zenoh")]
    pub(super) fn retain_reports_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), ChannelSessionError> {
        self.active_role_channel()?;
        let now = chrono::Utc::now().timestamp_millis();
        let remaining = self
            .identity
            .bootstrap_response()
            .certificate
            .expires_unix_ms
            .saturating_sub(now)
            .max(0) as u64;
        let limit = Instant::now() + Duration::from_millis(remaining);
        self.report_draining = true;
        self.authority.send_replace(deadline.min(limit));
        Ok(())
    }
    pub(super) fn authorization_deadline(&self) -> Instant {
        *self.authority.borrow()
    }
    pub fn identity(&self) -> &ServiceChannelIdentity {
        &self.identity
    }
    pub fn lane_count(&self) -> usize {
        self.config.lanes
    }
    pub fn has_control_lane(&self) -> bool {
        self.config.dedicated_control
    }
    pub(super) fn session_config(&self) -> ChannelSessionConfig {
        self.config
    }
    pub(super) fn control_session(&self) -> Result<&zenoh::Session, ChannelSessionError> {
        let index = if self.config.dedicated_control {
            self.lane_count()
        } else {
            0
        };
        self.sessions
            .get(index)
            .filter(|s| !s.is_closed())
            .ok_or(ChannelSessionError::Transport)
    }
    /// Transport diagnostics only; these IDs never identify business instances.
    pub fn session_ids(&self) -> Vec<String> {
        self.sessions.iter().map(|s| s.zid().to_string()).collect()
    }
    /// Observe physical reachability only. No role or route-ready is implied.
    pub async fn connected_lanes(&self) -> usize {
        if self.identity.connection.ensure_open().is_err()
            || Instant::now() >= *self.authority.borrow()
            || self.closed.borrow().is_some()
        {
            return 0;
        }
        let mut connected = 0;
        for session in &self.sessions[..self.lane_count()] {
            if !session.is_closed() && session.info().routers_zid().await.next().is_some() {
                connected += 1;
            }
        }
        connected
    }
    pub fn subscribe_closed(&self) -> watch::Receiver<Option<ChannelCloseReason>> {
        self.closed.clone()
    }
    /// Coalesced physical evidence only. Neither link activity nor reconnect
    /// extends authority or confirms a business route.
    pub fn subscribe_connectivity(&self) -> watch::Receiver<super::ChannelConnectivityStatus> {
        self.connectivity.clone()
    }
    fn authorization_request(&self, now: i64) -> Result<TransportEnvelope, ChannelSessionError> {
        let target = self
            .identity
            .credential
            .response
            .control_route
            .key()
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let request_id = RouteIdentity::new(uuid::Uuid::new_v4().to_string())
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let payload = serde_json::value::RawValue::from_string("{}".into()).unwrap();
        let identity = kish_lingshu_foundation_contract::service_auth::ClientChannelIdentity {
            application_id: self.identity.credential.response.application_id.clone(),
            instance_id: RouteIdentity::new(
                self.identity
                    .credential
                    .response
                    .instance
                    .instance_id
                    .clone(),
            )
            .map_err(|_| ChannelSessionError::InvalidResponse)?,
            base_generation: RouteIdentity::new(
                self.identity
                    .credential
                    .response
                    .instance
                    .generation
                    .clone(),
            )
            .map_err(|_| ChannelSessionError::InvalidResponse)?,
            certificate_identity: self
                .identity
                .credential
                .response
                .certificate
                .certificate_identity
                .clone(),
        };
        let deadline = now + 5_000;
        let proof = self
            .identity
            .credential
            .signer
            .sign_message(
                &identity,
                MessageKind::ChannelAuthorization,
                &target,
                &request_id,
                payload.get().as_bytes(),
                now,
                deadline,
            )
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        Ok(TransportEnvelope {
            protocol_version: ProtocolVersion::V1,
            kind: MessageKind::ChannelAuthorization,
            request_id: request_id.clone(),
            application_id: identity.application_id.clone(),
            target: target.clone(),
            deadline_unix_ms: deadline,
            proof,
            trace_parent: super::trace::current_trace_parent(),
            payload,
        })
    }

    /// Observe the platform's currently authorized window. This neither renews
    /// the shared instance/roles nor rotates the certificate. No network retry
    /// or re-enrollment follows rejection; only a scoped signed ACK can update
    /// this physical owner's deadline, and an expired owner cannot be revived.
    pub async fn refresh_authorization(
        &mut self,
    ) -> Result<ChannelAuthorization, ChannelSessionError> {
        super::trace::scope(None, self.refresh_authorization_scoped()).await
    }

    async fn refresh_authorization_scoped(
        &mut self,
    ) -> Result<ChannelAuthorization, ChannelSessionError> {
        if self.report_draining {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        self.identity
            .connection
            .ensure_open()
            .map_err(|_| ChannelSessionError::Closed)?;
        if self.closed.borrow().is_some() || Instant::now() >= *self.authority.borrow() {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        if self.task.as_ref().is_none_or(|task| task.is_finished()) {
            return Err(ChannelSessionError::CleanupFailed);
        }
        let started = Instant::now();
        let now = chrono::Utc::now().timestamp_millis();
        let request = self.authorization_request(now)?;
        let target = request.target.clone();
        let request_id = request.request_id.clone();
        let deadline = request.deadline_unix_ms;
        let application = self.identity.credential.response.application_id.clone();
        let bytes = request
            .encode(now)
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let session = self.control_session()?;
        let mut observation =
            super::observation::ExchangeObservation::new(super::observation::Plane::Control);
        let operation = async {
            let replies = session
                .get(target.as_str().to_owned())
                .target(zenoh::query::QueryTarget::All)
                .consolidation(zenoh::query::ConsolidationMode::None)
                .payload(bytes)
                .timeout(Duration::from_secs(5))
                .await
                .map_err(|_| ChannelSessionError::Transport)?;
            let reply = replies
                .recv_async()
                .await
                .map_err(|_| ChannelSessionError::Transport)?;
            let reply_result = reply.result();
            let sample = reply_result
                .as_ref()
                .map_err(|_| ChannelSessionError::Transport)?;
            if sample.key_expr().as_str() != target.as_str()
                || sample.payload().len() > MAX_CHANNEL_CONTROL_BYTES
            {
                return Err(ChannelSessionError::InvalidResponse);
            }
            let bytes = sample.payload().to_bytes();
            let response = TransportEnvelope::decode(
                &bytes,
                &target,
                &application,
                chrono::Utc::now().timestamp_millis(),
            )
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
            if response.kind != MessageKind::ChannelAuthorization
                || response.request_id != request_id
                || response.deadline_unix_ms > deadline
            {
                return Err(ChannelSessionError::InvalidResponse);
            }
            self.identity
                .connection
                .verify_channel_message(
                    &self.identity.credential.response.transport_trust,
                    &response,
                )
                .map_err(|_| ChannelSessionError::InvalidResponse)?;
            #[cfg(test)]
            super::trace::assert_verified_reply_trace(&response);
            let authority: ChannelAuthorization = serde_json::from_str(response.payload.get())
                .map_err(|_| ChannelSessionError::InvalidResponse)?;
            authority
                .validate(
                    &self.identity.credential.response,
                    chrono::Utc::now().timestamp_millis(),
                )
                .map_err(|_| ChannelSessionError::InvalidResponse)?;
            Ok(authority)
        };
        let query_deadline = (started + Duration::from_secs(5)).min(*self.authority.borrow());
        let mut closed = self.identity.connection.subscribe_closed();
        let mut physical_closed = self.closed.clone();
        let authority = tokio::select! {
            _ = logical_closed(&mut closed) => return Err(ChannelSessionError::Closed),
            result = physical_closed.changed() => return Err(if result.is_err() { ChannelSessionError::CleanupFailed } else { ChannelSessionError::Closed }),
            result = tokio::time::timeout_at(query_deadline, operation) => result.map_err(|_| ChannelSessionError::Transport)??,
        };
        self.identity
            .connection
            .ensure_open()
            .map_err(|_| ChannelSessionError::Closed)?;
        if self.closed.borrow().is_some() || Instant::now() >= *self.authority.borrow() {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        if self.task.as_ref().is_none_or(|task| task.is_finished()) {
            return Err(ChannelSessionError::CleanupFailed);
        }
        let remaining = (authority.authorization_expires_unix_ms
            - chrono::Utc::now().timestamp_millis())
        .min(authority.authorization_expires_unix_ms - authority.authorization_issued_unix_ms);
        let remaining: u64 = remaining
            .try_into()
            .map_err(|_| ChannelSessionError::AuthorityExpired)?;
        let mut next = started + Duration::from_millis(remaining);
        if self.identity.rotation_predecessor().is_some() && !self.rotation_finalized {
            // Preparation alone cannot extend the original candidate budget.
            next = next.min(self.identity.authorization_deadline);
        }
        if next <= Instant::now() {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        self.authority.send_replace(next);
        observation.reply_verified();
        Ok(authority)
    }
    /// Promote this candidate only after the platform has accounted for every
    /// predecessor role and retired its exact control grant. Explicit fresh
    /// repeats recover a lost ACK; no automatic retry or domain re-enrollment.
    pub async fn finalize_certificate_rotation(
        &mut self,
    ) -> Result<ChannelRotationFinalization, ChannelSessionError> {
        let predecessor = self
            .identity
            .rotation_predecessor()
            .ok_or(ChannelSessionError::InvalidConfig)?
            .clone();
        let started = Instant::now();
        let response = self
            .role_control(MessageKind::FinalizeChannelRotation, &serde_json::json!({}))
            .await?;
        let receipt: ChannelRotationFinalization = serde_json::from_str(response.payload.get())
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        receipt
            .validate(
                self.identity.bootstrap_response(),
                &predecessor,
                chrono::Utc::now().timestamp_millis(),
            )
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        self.identity
            .connection
            .ensure_open()
            .map_err(|_| ChannelSessionError::Closed)?;
        if self.closed.borrow().is_some() || Instant::now() >= self.authorization_deadline() {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        if self.task.as_ref().is_none_or(|task| task.is_finished()) {
            return Err(ChannelSessionError::CleanupFailed);
        }
        let authority = &receipt.authorization;
        let remaining = (authority.authorization_expires_unix_ms
            - chrono::Utc::now().timestamp_millis())
        .min(authority.authorization_expires_unix_ms - authority.authorization_issued_unix_ms);
        let remaining: u64 = remaining
            .try_into()
            .map_err(|_| ChannelSessionError::AuthorityExpired)?;
        let next = started + Duration::from_millis(remaining);
        if next <= Instant::now() {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        // No await between validating this signed ACK and removing the local cap.
        self.rotation_finalized = true;
        self.authority.send_replace(next);
        Ok(receipt)
    }
    pub(super) fn request_close(&self) {
        self.stop.send_replace(true);
    }
    pub async fn close(&mut self) -> Result<(), ChannelSessionError> {
        self.stop.send_replace(true);
        if let Some(task) = self.task.as_mut() {
            // Keep the handle if the caller cancels this wait; a later close
            // must still join the cleanup rather than returning prematurely.
            let result = task.await;
            self.task.take();
            match result {
                Ok(result) => self.cleanup_failed = result.is_err(),
                Err(_) => {
                    // Record the terminal failure before awaiting fallback
                    // cleanup, so cancellation cannot hide the failed join.
                    self.cleanup_failed = true;
                    let _ = close_sessions(&self.sessions).await;
                }
            }
        }
        if self.cleanup_failed {
            Err(ChannelSessionError::CleanupFailed)
        } else {
            Ok(())
        }
    }
}
impl Drop for ServiceChannelSessions {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

pub(super) fn client_config(
    identity: &ServiceChannelIdentity,
    lane: usize,
) -> Result<zenoh::Config, ChannelSessionError> {
    let certificate = &identity.credential.response.certificate;
    // Official in-memory secret fields: no key file or caller-provided locator
    // suffix can weaken verification. Never serialize this object for logging.
    let value = serde_json::json!({
        "mode": "client",
        "listen": {"endpoints": []},
        "connect": {"endpoints": ordered_endpoints(&identity.credential.response.endpoints, lane), "timeout_ms": 0,
            "exit_on_failure": true,
            // Disable only the native initial retry loop; the SDK supplies one
            // five-second whole-pool deadline. Native reconnect still reads
            // this finite backoff and stops after its first successful Router.
            "retry": {"period_init_ms": 250, "period_max_ms": 5000, "period_increase_factor": 2.0}},
        "scouting": {"multicast": {"enabled": false}, "gossip": {"enabled": false}},
        "adminspace": {"enabled": false},
        "transport": {"unicast": {
            // One active Router plus a transient replacement. Endpoint lists
            // are failover choices, not permission for eight live transports.
            "max_sessions": 2, "max_links": 1, "accept_pending": 1,
            "open_timeout": 5000, "accept_timeout": 5000,
            "lowlatency": false, "qos": {"enabled": false}},
            "link": {"protocols": ["tls"],
            // Native framing/keys/receipt need room beyond the product envelope.
            "rx": {"buffer_size": 65535, "max_message_size": MAX_CONTROL_PAYLOAD_BYTES + MAX_ENVELOPE_OVERHEAD_BYTES + 32 * 1024},
            // Universal/no-QoS uses data. Sixteen lazy batches absorb bounded
            // bursts; keep the 250ms close and every other priority at two.
            "tx": {"batch_size": 65535, "queue": {
                "size": {"control": 2, "real_time": 2, "interactive_high": 2,
                    "interactive_low": 2, "data_high": 2, "data": 16, "data_low": 2, "background": 2},
                "allocation": {"mode": "lazy"},
                "congestion_control": {"block": {"wait_before_close": 250000}}
            }},
            "tls": {
                // Kernel receive storage is distinct from native RX batch pools.
                // Linux requires net.core.rmem_max >= 1MiB for this request.
                "so_rcvbuf": 1024 * 1024,
                "root_ca_certificate_base64": STANDARD.encode(&certificate.root_ca_pem),
                "connect_certificate_base64": STANDARD.encode(&certificate.certificate_pem),
                "connect_private_key_base64": STANDARD.encode(identity.credential.key.serialize_pem()),
                "enable_mtls": true, "verify_name_on_connect": true, "close_link_on_expiration": true
            }}}
    });
    zenoh::Config::from_json5(&value.to_string()).map_err(|_| ChannelSessionError::InvalidConfig)
}

fn ordered_endpoints(endpoints: &[TlsEndpoint], lane: usize) -> Vec<&str> {
    (0..endpoints.len())
        .map(|offset| endpoints[(lane + offset) % endpoints.len()].as_str())
        .collect()
}

async fn close_sessions(sessions: &[zenoh::Session]) -> Result<(), ChannelSessionError> {
    tokio::time::timeout(
        CLOSE_TIMEOUT,
        futures::future::join_all(sessions.iter().map(|s| async move { s.close().await })),
    )
    .await
    .map_err(|_| ChannelSessionError::CleanupFailed)?
    .into_iter()
    .try_for_each(|r| r.map_err(|_| ChannelSessionError::CleanupFailed))
}

async fn cleanup_pool(
    sessions: &[zenoh::Session],
    permit: &mut SessionPermit,
) -> Result<(), ChannelSessionError> {
    close_sessions(sessions).await?;
    permit.release();
    Ok(())
}

async fn logical_closed(receiver: &mut watch::Receiver<Option<crate::ServiceAuthError>>) {
    loop {
        if receiver.borrow().is_some() || receiver.changed().await.is_err() {
            return;
        }
    }
}

impl ServiceChannelIdentity {
    /// Establish independent outbound client Sessions using this identity's
    /// original logical connection and authorized endpoints. Native Zenoh owns
    /// reconnect within the finite window; this method never re-enrolls an
    /// instance, declares a route, or retries a business request.
    pub async fn open_sessions(
        self,
        config: ChannelSessionConfig,
    ) -> Result<ServiceChannelSessions, ChannelSessionError> {
        // Stock Zenoh panics inside a current-thread scheduler. Reject before
        // handing it any credential or creating a partial network runtime.
        if !tokio::runtime::Handle::try_current().is_ok_and(|handle| {
            handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
        }) {
            return Err(ChannelSessionError::UnsupportedRuntime);
        }
        self.connection
            .ensure_open()
            .map_err(|_| ChannelSessionError::Closed)?;
        if Instant::now() >= self.authorization_deadline {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        let permit = self
            .connection
            .channel_session_budget()
            .try_acquire_many_owned(config.session_count() as u32)
            .map_err(|_| {
                if self.connection.ensure_open().is_err() {
                    ChannelSessionError::Closed
                } else {
                    ChannelSessionError::CapacityExceeded
                }
            })?;
        let mut permit = SessionPermit(Some(permit));
        let mut root_closed = self.connection.subscribe_closed();
        let mut sessions = Vec::with_capacity(config.session_count());
        // Bound the whole pool, rather than granting each lane a fresh timeout.
        let deadline = (Instant::now() + CONNECT_TIMEOUT).min(self.authorization_deadline);
        for lane in 0..config.session_count() {
            let native_config = match client_config(&self, lane) {
                Ok(config) => config,
                Err(error) => {
                    if cleanup_pool(&sessions, &mut permit).await.is_err() {
                        return Err(ChannelSessionError::CleanupFailed);
                    }
                    return Err(error);
                }
            };
            let result = tokio::select! {
                _ = logical_closed(&mut root_closed) => Err(ChannelSessionError::Closed),
                _ = tokio::time::sleep_until(deadline) => {
                    Err(if Instant::now() >= self.authorization_deadline {
                        ChannelSessionError::AuthorityExpired
                    } else { ChannelSessionError::Transport })
                },
                result = zenoh::open(native_config) => result.map_err(|_| ChannelSessionError::Transport),
            };
            match result {
                Ok(session) => sessions.push(session),
                Err(error) => {
                    if cleanup_pool(&sessions, &mut permit).await.is_err() {
                        return Err(ChannelSessionError::CleanupFailed);
                    }
                    return Err(error);
                }
            }
        }
        // A shutdown/expiry racing a successful open must clean up the whole pool.
        if self.connection.ensure_open().is_err() || Instant::now() >= self.authorization_deadline {
            if cleanup_pool(&sessions, &mut permit).await.is_err() {
                return Err(ChannelSessionError::CleanupFailed);
            }
            return Err(if self.connection.ensure_open().is_err() {
                ChannelSessionError::Closed
            } else {
                ChannelSessionError::AuthorityExpired
            });
        }
        let (stop, mut stopped) = watch::channel(false);
        let (closed, close_status) = watch::channel(None);
        let active = sessions.clone();
        let (authority, mut authorization) = watch::channel(self.authorization_deadline);
        let mut connectivity = super::connectivity::ConnectivityTracker::new(
            super::connectivity::routers(&active).await,
            config.lanes,
        );
        let connectivity_status = connectivity.sender.subscribe();
        let task = tokio::spawn(async move {
            let mut observation = tokio::time::interval_at(
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
            );
            observation.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let reason = loop {
                let deadline = *authorization.borrow_and_update();
                tokio::select! {
                    biased;
                    _ = logical_closed(&mut root_closed) => break ChannelCloseReason::ConnectionClosed,
                    _ = tokio::time::sleep_until(deadline) => {
                        if Instant::now() >= *authorization.borrow() { break ChannelCloseReason::AuthorityExpired; }
                    },
                    _ = stopped.changed() => break ChannelCloseReason::Explicit,
                    result = authorization.changed() => {
                        if result.is_err() { break ChannelCloseReason::Explicit; }
                    },
                    _ = observation.tick() => connectivity.update(super::connectivity::routers(&active).await),
                }
            };
            connectivity.close();
            closed.send_replace(Some(reason));
            let result = cleanup_pool(&active, &mut permit).await;
            if result.is_err() {
                closed.send_replace(Some(ChannelCloseReason::CleanupFailed));
                // Unproven cleanup cannot release capacity to another pool.
            }
            let outcome = if result.is_err() {
                "cleanup_failed"
            } else {
                match reason {
                    ChannelCloseReason::Explicit => "explicit",
                    ChannelCloseReason::ConnectionClosed => "connection_closed",
                    ChannelCloseReason::RotationFailed => "rotation_failed",
                    ChannelCloseReason::AuthorityExpired => "authority_expired",
                    ChannelCloseReason::CleanupFailed => "cleanup_failed",
                }
            };
            metrics::counter!("lingshu_sdk_channel_terminations_total", "outcome" => outcome)
                .increment(1);
            result
        });
        Ok(ServiceChannelSessions {
            identity: self,
            sessions,
            config,
            stop,
            closed: close_status,
            task: Some(task),
            cleanup_failed: false,
            rotation_finalized: false,
            report_draining: false,
            authority,
            connectivity: connectivity_status,
            #[cfg(feature = "service-call-zenoh")]
            call_query_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(16)),
            #[cfg(feature = "event-consumer-zenoh")]
            consumer_query_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(16)),
            #[cfg(feature = "event-publication-zenoh")]
            publication_query_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(16)),
            role_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(1024)),
            role_control_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(8)),
            role_control_bytes: std::sync::Arc::new(tokio::sync::Semaphore::new(512 * 1024)),
            role_query_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
            role_query_bytes: std::sync::Arc::new(tokio::sync::Semaphore::new(8 * 1024 * 1024)),
            role_keys: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
            declaration_observation: super::observation::DeclarationAccounting::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn native_pool(id: &str) -> (crate::ServiceConnection, ServiceChannelSessions, Instant) {
        let connection = crate::ServiceConnection::connect(
            &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
            crate::ServiceCredential::new(
                std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap(),
                std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let identity = connection
            .bootstrap_channel(
                kish_lingshu_foundation_contract::ServiceInstanceRegistration {
                    instance_id: id.into(),
                    incarnation_id: format!("boot-{id}"),
                    generation: None,
                },
                None,
            )
            .await
            .unwrap();
        let initial = identity.authorization_deadline;
        let pool = identity
            .open_sessions(ChannelSessionConfig::default())
            .await
            .unwrap();
        (connection, pool, initial)
    }
    async fn raw_query(
        pool: &ServiceChannelSessions,
        envelope: &TransportEnvelope,
    ) -> Option<zenoh::query::Reply> {
        let replies = pool.sessions[0]
            .get(envelope.target.as_str().to_owned())
            .target(zenoh::query::QueryTarget::All)
            .consolidation(zenoh::query::ConsolidationMode::None)
            .payload(
                envelope
                    .encode(chrono::Utc::now().timestamp_millis())
                    .unwrap(),
            )
            .timeout(Duration::from_secs(5))
            .await
            .unwrap();
        // Native admission can silently drop a foreign request. Leave margin
        // after Zenoh's five-second timeout; absence is valid rejection evidence
        // for negative cases, while positive cases must receive a signed reply.
        tokio::time::timeout(Duration::from_secs(6), replies.recv_async())
            .await
            .ok()
            .and_then(Result::ok)
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires isolated native host; run zenss_channel_bootstrap_acceptance.py --sdk-test"]
    async fn native_dedicated_control_remains_independent_and_never_falls_back_to_data() {
        let (connection, mut original, _) = native_pool("dedicated-control").await;
        original.close().await.unwrap();
        let identity = connection
            .bootstrap_channel(
                kish_lingshu_foundation_contract::ServiceInstanceRegistration {
                    instance_id: "dedicated-control".into(),
                    incarnation_id: "boot-dedicated-control".into(),
                    generation: None,
                },
                None,
            )
            .await
            .unwrap();
        let mut pool = identity
            .open_sessions(
                ChannelSessionConfig::new(3)
                    .unwrap()
                    .with_control_lane()
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(pool.lane_count(), 3);
        assert!(pool.has_control_lane());
        assert_eq!(
            pool.session_ids()
                .into_iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            4
        );
        assert_eq!(connection.channel_session_budget().available_permits(), 0);
        assert_eq!(
            pool.control_session().unwrap().zid(),
            pool.sessions[3].zid()
        );
        pool.refresh_authorization().await.unwrap();
        // Lost data connectivity neither steals the control Session for a
        // business declaration nor prevents a signed physical observation.
        pool.sessions[0].close().await.unwrap();
        pool.refresh_authorization().await.unwrap();
        pool.sessions[3].close().await.unwrap();
        assert_eq!(
            pool.refresh_authorization().await.unwrap_err(),
            ChannelSessionError::Transport
        );
        assert!(!pool.sessions[1].is_closed());
        assert!(!pool.sessions[2].is_closed());
        pool.close().await.unwrap();
        assert_eq!(connection.channel_session_budget().available_permits(), 4);
        connection.shutdown().await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires isolated native host; run zenss_channel_bootstrap_acceptance.py --sdk-test"]
    async fn native_control_replies_are_signed_and_replay_or_forgery_is_rejected() {
        let (connection, mut pool, _) = native_pool("control-proof").await;
        let authority = pool.refresh_authorization().await.unwrap();
        authority
            .validate(
                pool.identity.bootstrap_response(),
                chrono::Utc::now().timestamp_millis(),
            )
            .unwrap();
        let request = pool
            .authorization_request(chrono::Utc::now().timestamp_millis())
            .unwrap();
        let reply = raw_query(&pool, &request)
            .await
            .expect("authorized control request must receive a reply");
        assert!(reply.result().is_ok());
        let result = reply.result();
        let bytes = result.as_ref().unwrap().payload().to_bytes();
        let response = TransportEnvelope::decode(
            &bytes,
            &request.target,
            &request.application_id,
            chrono::Utc::now().timestamp_millis(),
        )
        .unwrap();
        connection
            .verify_channel_message(
                &pool.identity.credential.response.transport_trust,
                &response,
            )
            .unwrap();
        assert!(connection
            .verify_channel_message(
                &pool.identity.credential.response.transport_trust,
                &response
            )
            .is_err());
        assert!(raw_query(&pool, &request)
            .await
            .is_none_or(|reply| reply.result().is_err()));
        let mut foreign = pool
            .authorization_request(chrono::Utc::now().timestamp_millis())
            .unwrap();
        foreign.application_id = RouteIdentity::new("other-app").unwrap();
        assert!(raw_query(&pool, &foreign)
            .await
            .is_none_or(|reply| reply.result().is_err()));
        let mut modified = pool
            .authorization_request(chrono::Utc::now().timestamp_millis())
            .unwrap();
        modified.payload =
            serde_json::value::RawValue::from_string("{\"roles\":[\"CALL\"]}".into()).unwrap();
        assert!(raw_query(&pool, &modified)
            .await
            .is_none_or(|reply| reply.result().is_err()));
        assert!(connection.subscribe_closed().borrow().is_none());
        pool.refresh_authorization().await.unwrap();
        pool.task.as_ref().unwrap().abort();
        while !pool.task.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            pool.refresh_authorization().await,
            Err(ChannelSessionError::CleanupFailed)
        ));
        assert_eq!(pool.close().await, Err(ChannelSessionError::CleanupFailed));
        assert_eq!(pool.close().await, Err(ChannelSessionError::CleanupFailed));
        connection.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "actual native admission negatives; run acceptance --security-test"]
    async fn native_gate_denies_wildcards_foreign_keys_declarations_and_claimed_router() {
        let (connection, mut pool, _) = native_pool("native-security").await;
        pool.refresh_authorization().await.unwrap();
        let original = pool.identity.bootstrap_response().clone();
        let control = original.control_route.key().unwrap();
        let request = pool
            .authorization_request(chrono::Utc::now().timestamp_millis())
            .unwrap()
            .encode(chrono::Utc::now().timestamp_millis())
            .unwrap();
        async fn denied(session: &zenoh::Session, key: &str, bytes: &[u8]) {
            let replies = session
                .get(key.to_owned())
                .target(zenoh::query::QueryTarget::All)
                .consolidation(zenoh::query::ConsolidationMode::None)
                .payload(bytes.to_vec())
                .timeout(Duration::from_millis(300))
                .await
                .unwrap();
            while let Ok(reply) = tokio::time::timeout(Duration::from_secs(2), replies.recv_async())
                .await
                .expect("negative query must finalize within its finite timeout")
            {
                assert!(
                    reply.result().is_err(),
                    "unauthorized query received a successful reply"
                );
            }
        }
        for key in [
            "ls/v1/646576/**",
            "ls/v1/646576/apps/6f746865722d617070/private/control",
            "ls/v1/6f746865722d6465706c6f796d656e74/platform/node/boot/control",
            "zenss/v1/dev/platform/private/control",
            "@/router/local/config",
        ] {
            denied(&pool.sessions[0], key, &request).await;
        }
        let rogue = zenoh::open(client_config(&pool.identity, 0).unwrap())
            .await
            .unwrap();
        let impersonation = rogue
            .declare_queryable(control.as_str().to_owned())
            .await
            .unwrap();
        let wildcard = rogue.declare_queryable("ls/v1/646576/**").await.unwrap();
        let subscriber = rogue.declare_subscriber("ls/v1/646576/**").await.unwrap();
        // A local declaration handle alone does not prove Router admission.
        // Actual signed queries through a different physical Session must still
        // reach only the product; neither unauthorized Queryable may see them.
        pool.refresh_authorization().await.unwrap();
        assert!(impersonation.try_recv().unwrap().is_none());
        assert!(wildcard.try_recv().unwrap().is_none());
        assert!(subscriber.try_recv().unwrap().is_none());
        impersonation.undeclare().await.unwrap();
        wildcard.undeclare().await.unwrap();
        subscriber.undeclare().await.unwrap();
        rogue.close().await.unwrap();
        for role in ["router", "peer"] {
            let mut config = client_config(&pool.identity, 0).unwrap();
            config.insert_json5("mode", &format!("\"{role}\"")).unwrap();
            match tokio::time::timeout(Duration::from_secs(5), zenoh::open(config)).await {
                Ok(Ok(rogue)) => {
                    let fresh = pool
                        .authorization_request(chrono::Utc::now().timestamp_millis())
                        .unwrap()
                        .encode(chrono::Utc::now().timestamp_millis())
                        .unwrap();
                    denied(&rogue, control.as_str(), &fresh).await;
                    rogue.close().await.unwrap();
                }
                Ok(Err(_)) => {}
                Err(_) => panic!("claimed Router/Peer must fail within the connection budget"),
            }
        }
        pool.refresh_authorization().await.unwrap();
        assert_eq!(
            pool.identity.bootstrap_response().instance,
            original.instance
        );
        assert_eq!(pool.connected_lanes().await, 1);
        assert!(connection.subscribe_closed().borrow().is_none());
        pool.close().await.unwrap();
        connection.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "real authority outage and root revocation; run acceptance --authority-expiry-test"]
    async fn native_root_revocation_and_unreadable_authority_keep_original_hard_deadlines() {
        use super::super::{ChannelSupervisorStatus, ManagedServiceChannel};
        let markers =
            std::path::PathBuf::from(std::env::var("LINGSHU_AUTHORITY_MARKER_DIR").unwrap());
        async fn marker(directory: &std::path::Path, name: &str) {
            tokio::time::timeout(Duration::from_secs(20), async {
                while !directory.join(name).exists() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
        }
        async fn expires(
            managed: &mut ManagedServiceChannel,
            deadline: Instant,
            sessions: &[zenoh::Session],
        ) {
            tokio::time::timeout_at(deadline + Duration::from_secs(5), async {
                loop {
                    match managed.status() {
                        ChannelSupervisorStatus::Active {
                            authorization_deadline,
                            observations,
                            ..
                        } => {
                            assert!(
                                authorization_deadline <= deadline,
                                "unreadable or revoked authority cannot extend the last ACK"
                            );
                            assert_eq!(observations, 0);
                        }
                        ChannelSupervisorStatus::Closed {
                            reason: ChannelCloseReason::AuthorityExpired,
                        } => break,
                        ChannelSupervisorStatus::Stopping {
                            reason: ChannelCloseReason::AuthorityExpired,
                        } => {}
                        other => panic!("unexpected finite authority termination: {other:?}"),
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("original authority deadline must close the physical pool");
            managed.close().await.unwrap();
            assert!(sessions.iter().all(zenoh::Session::is_closed));
        }
        let (unreadable_connection, mut unreadable, _) = native_pool("authority-unreadable").await;
        unreadable.refresh_authorization().await.unwrap();
        let deadline = unreadable.authorization_deadline();
        let observed = unreadable.sessions.clone();
        let mut managed = unreadable.manage().unwrap();
        std::fs::write(
            markers.join("unreadable-ready"),
            "original finite pool connected",
        )
        .unwrap();
        marker(&markers, "unreadable").await;
        expires(&mut managed, deadline, &observed).await;
        unreadable_connection.ensure_open().unwrap();
        assert_eq!(
            unreadable_connection
                .channel_session_budget()
                .available_permits(),
            4
        );
        std::fs::write(
            markers.join("unreadable-expired"),
            "original deadline; no root revocation inferred",
        )
        .unwrap();
        marker(&markers, "recovered").await;
        unreadable_connection.shutdown().await;

        let (first_connection, mut first, _) = native_pool("shared-root-first").await;
        let (second_connection, mut second, _) = native_pool("shared-root-second").await;
        first.refresh_authorization().await.unwrap();
        second.refresh_authorization().await.unwrap();
        assert_eq!(
            first
                .identity
                .bootstrap_response()
                .parent_credential_fingerprint,
            second
                .identity
                .bootstrap_response()
                .parent_credential_fingerprint
        );
        assert_ne!(
            first.identity.bootstrap_response().instance.instance_id,
            second.identity.bootstrap_response().instance.instance_id
        );
        let first_deadline = first.authorization_deadline();
        let second_deadline = second.authorization_deadline();
        let first_sessions = first.sessions.clone();
        let second_sessions = second.sessions.clone();
        let mut first = first.manage().unwrap();
        let mut second = second.manage().unwrap();
        std::fs::write(
            markers.join("shared-root-ready"),
            "two distinct instances share one root fingerprint",
        )
        .unwrap();
        marker(&markers, "root-revoked").await;
        tokio::join!(
            expires(&mut first, first_deadline, &first_sessions),
            expires(&mut second, second_deadline, &second_sessions)
        );
        for connection in [&first_connection, &second_connection] {
            connection.ensure_open().unwrap();
            assert_eq!(connection.channel_session_budget().available_permits(), 4);
        }
        std::fs::write(
            markers.join("authority-facts.json"),
            serde_json::to_vec(&serde_json::json!({
                "unreadable_authority_deadline_preserved":true, "shared_root_instances_expired":2,
                "physical_sessions_closed":true, "logical_connections_preserved":true,
                "all_session_permits_released":true, "successful_observation_extensions":0,
            }))
            .unwrap(),
        )
        .unwrap();
        first_connection.shutdown().await;
        second_connection.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires isolated native host and real deadlines; run zenss_channel_bootstrap_acceptance.py --sdk-test"]
    async fn native_signed_control_ack_updates_only_the_finite_physical_window() {
        let (connection, mut pool, initial) = native_pool("control-window").await;
        tokio::time::sleep(Duration::from_secs(11)).await;
        pool.refresh_authorization().await.unwrap();
        assert!(*pool.authority.borrow() > initial);
        tokio::time::sleep_until(initial + Duration::from_secs(1)).await;
        assert_eq!(pool.connected_lanes().await, 1);
        // No role was registered or renewed. The shared base has expired despite
        // the longer physical window; neither inspection nor keepalive restores it.
        assert!(pool.refresh_authorization().await.is_err());
        assert!(connection.subscribe_closed().borrow().is_none());
        let mut closed = pool.subscribe_closed();
        tokio::time::timeout(Duration::from_secs(30), async {
            while closed.borrow().is_none() {
                closed.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(*closed.borrow(), Some(ChannelCloseReason::AuthorityExpired));
        assert!(matches!(
            pool.refresh_authorization().await,
            Err(ChannelSessionError::AuthorityExpired)
        ));
        pool.close().await.unwrap();
        connection.shutdown().await;
    }
    #[tokio::test]
    async fn unproven_cleanup_never_releases_physical_capacity() {
        let budget = std::sync::Arc::new(tokio::sync::Semaphore::new(2));
        let permit = SessionPermit(Some(budget.clone().try_acquire_owned().unwrap()));
        let task = tokio::spawn(async move {
            let _permit = permit;
            panic!("simulated lifecycle failure");
        });
        assert!(task.await.unwrap_err().is_panic());
        assert_eq!(budget.available_permits(), 1);

        let mut permit = SessionPermit(Some(budget.clone().try_acquire_owned().unwrap()));
        assert_eq!(budget.available_permits(), 0);
        cleanup_pool(&[], &mut permit).await.unwrap();
        drop(permit);
        assert_eq!(budget.available_permits(), 1);
    }
    #[test]
    fn pool_size_is_explicit_and_bounded() {
        assert_eq!(ChannelSessionConfig::default().lanes(), 1);
        for size in [1, 2, 4] {
            assert_eq!(ChannelSessionConfig::new(size).unwrap().lanes(), size);
        }
        for data in [1, 2, 3] {
            let config = ChannelSessionConfig::new(data)
                .unwrap()
                .with_control_lane()
                .unwrap();
            assert_eq!(config.lanes(), data);
            assert_eq!(config.session_count(), data + 1);
            assert!(config.has_control_lane());
        }
        assert!(ChannelSessionConfig::new(4)
            .unwrap()
            .with_control_lane()
            .is_err());
        assert_eq!(ChannelSessionConfig::default().session_count(), 1);
        assert!(!ChannelSessionConfig::default().has_control_lane());
        for size in [0, 5, usize::MAX] {
            assert!(ChannelSessionConfig::new(size).is_err());
        }
    }
    #[tokio::test]
    async fn closure_observer_handles_already_closed_and_lost_authority() {
        let (sender, mut receiver) = watch::channel(Some(crate::ServiceAuthError::Closed));
        tokio::time::timeout(Duration::from_secs(1), logical_closed(&mut receiver))
            .await
            .unwrap();
        sender.send_replace(None);
        drop(sender);
        tokio::time::timeout(Duration::from_secs(1), logical_closed(&mut receiver))
            .await
            .unwrap();
    }
}

#[cfg(all(test, feature = "service-call-zenoh"))]
pub(crate) fn test_sync_pool(
    connection: crate::ServiceConnection,
    response: kish_lingshu_foundation_contract::service_transport::bootstrap::ChannelBootstrapResponse,
    key: rcgen::KeyPair,
    session: zenoh::Session,
) -> ServiceChannelSessions {
    test_sync_pool_lanes(connection, response, key, vec![session])
}

#[cfg(all(test, feature = "service-call-zenoh"))]
pub(crate) fn test_sync_pool_lanes(
    connection: crate::ServiceConnection,
    response: kish_lingshu_foundation_contract::service_transport::bootstrap::ChannelBootstrapResponse,
    key: rcgen::KeyPair,
    sessions: Vec<zenoh::Session>,
) -> ServiceChannelSessions {
    let lanes = sessions.len();
    let config = ChannelSessionConfig::new(lanes).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let signer = kish_lingshu_foundation_contract::service_auth::ChannelMessageSigner::from_pkcs8(
        &key.serialize_der(),
    )
    .unwrap();
    let identity = super::ServiceChannelIdentity {
        credential: super::PreparedIdentity {
            response,
            signer,
            key,
        },
        predecessor: None,
        connection: connection.clone(),
        authorization_deadline: deadline,
    };
    let (stop, mut stopped) = watch::channel(false);
    let (closed_tx, closed) = watch::channel(None);
    let (authority, _) = watch::channel(deadline);
    let connectivity = super::connectivity::ConnectivityTracker::new(
        vec![Some("test-router".into()); lanes],
        lanes,
    )
    .sender
    .subscribe();
    let owned = sessions.clone();
    let task = tokio::spawn(async move {
        let mut logical = connection.subscribe_closed();
        tokio::select! {_ = stopped.wait_for(|v|*v)=>{},_ = logical.changed()=>{}};
        close_sessions(&owned).await?;
        closed_tx.send_replace(Some(ChannelCloseReason::Explicit));
        Ok(())
    });
    ServiceChannelSessions {
        identity,
        sessions,
        config,
        stop,
        closed,
        task: Some(task),
        cleanup_failed: false,
        rotation_finalized: false,
        report_draining: false,
        authority,
        connectivity,
        call_query_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(16)),
        #[cfg(feature = "event-consumer-zenoh")]
        consumer_query_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(16)),
        #[cfg(feature = "event-publication-zenoh")]
        publication_query_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(16)),
        role_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(1024)),
        role_control_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(8)),
        role_control_bytes: std::sync::Arc::new(tokio::sync::Semaphore::new(512 * 1024)),
        role_query_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(64)),
        role_query_bytes: std::sync::Arc::new(tokio::sync::Semaphore::new(8 * 1024 * 1024)),
        role_keys: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        declaration_observation: super::observation::DeclarationAccounting::new(),
    }
}
