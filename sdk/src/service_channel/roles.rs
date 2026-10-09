//! Finite native registration and exact probe declarations. Business execution
//! and role renewal are separate; a confirmed route is not execution readiness.
use super::observation::{self, Plane, QueryReservation, Rejection};
use super::{ChannelSessionError, ServiceChannelSessions};
use kish_lingshu_foundation_contract::{
    service_auth::{ChannelMessageSigner, ClientChannelIdentity},
    service_transport::{
        channel::MAX_CHANNEL_CONTROL_BYTES,
        enrollment::{validate_route_binding, RequestedProviderEndpoint, RequestedServiceEndpoint},
        ExactRouteKey, MessageKind, ProtocolVersion, RouteIdentity, ServiceEndpoint,
        TransportEnvelope,
    },
};
use kish_lingshu_runtime_contract::service::{
    ChannelRoleAdoption, ChannelRoleAdoptionResponse, ChannelRoleDeregistration,
    ChannelRoleDeregistrationResponse, ChannelRoleEnrollment, ChannelRoleEnrollmentResponse,
    ChannelRoleRenewal, ChannelRoleRenewalResponse, ChannelRoleRenewalResult,
    ChannelRoleRouteConfirmation, MAX_CHANNEL_RENEWAL_ROLES,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::Instant,
};

#[cfg(all(test, feature = "service-call-zenoh", feature = "event-consumer-zenoh"))]
#[path = "roles_pressure_tests.rs"]
mod pressure_tests;

type RoleQuery = (
    ExactRouteKey,
    zenoh::query::Query,
    QueryReservation,
    Option<tokio::sync::OwnedSemaphorePermit>,
);

struct VerifiedRoleControlReply {
    envelope: TransportEnvelope,
    issued_at_unix_ms: i64,
}

fn reserved_control_kind(bytes: &[u8], limit: usize) -> bool {
    if bytes.len() > limit {
        return false;
    }
    #[derive(serde::Deserialize)]
    struct Header {
        kind: MessageKind,
    }
    matches!(
        serde_json::from_slice::<Header>(bytes),
        Ok(Header {
            kind: MessageKind::BindLane | MessageKind::CancelCall
        })
    )
}

/// Prefer bounded control work, but serve queued business after at most four
/// control requests. Cancellation of this future never consumes either queue.
async fn next_role_query<T>(
    control: &mut mpsc::Receiver<T>,
    business: &mut mpsc::Receiver<T>,
    burst: &mut usize,
) -> Option<T> {
    if *burst >= 4 {
        tokio::select! { biased;
            query = business.recv() => { *burst = 0; query },
            query = control.recv() => { *burst = 4; query },
        }
    } else {
        tokio::select! { biased;
            query = control.recv() => { *burst += 1; query },
            query = business.recv() => { *burst = 0; query },
        }
    }
}

/// Keep beside the physical pool. Drop withdraws local declarations; `close`
/// joins their cleanup. By default declarations accept authenticated BindLane
/// probes and an optional immutable Provider catalog. Execution requires a
/// separate opt-in enable_sync_calls or enable_async_calls binding.
pub struct RegisteredChannelRole {
    pub(super) response: ChannelRoleEnrollmentResponse,
    pub(super) logical_key: String,
    catalog: Option<Arc<super::catalog::CatalogSnapshot>>,
    endpoint: ServiceEndpoint,
    stop: watch::Sender<bool>,
    draining_handoff: Arc<AtomicBool>,
    task: Option<JoinHandle<Result<(), ChannelSessionError>>>,
    role_deadline: Instant,
    connectivity_gate: super::connectivity::RouteConnectivityGate,
    alive: watch::Receiver<bool>,
    cleanup_failed: bool,
    pub(super) channel: ClientChannelIdentity,
    remote_deregistered: bool,
    pub(super) lease_window: watch::Sender<RoleLeaseWindow>,
    #[cfg(feature = "service-call-zenoh")]
    pub(super) calls: super::call::CallSlot,
    #[cfg(feature = "event-consumer-zenoh")]
    pub(super) consumers: super::consumer::ConsumerSlot,
    #[cfg(feature = "event-consumer-zenoh")]
    pub(super) consumer_capacity: Option<u32>,
    #[cfg(feature = "service-call-zenoh")]
    pub(super) call_contract: Option<(
        Vec<kish_lingshu_runtime_contract::service::InstanceCapability>,
        u32,
    )>,
}
#[derive(Clone, Copy)]
pub(super) struct RoleLeaseWindow {
    pub(super) expires_at_ms: i64,
    deadline: Instant,
}
struct RoleDeclarations {
    stop: watch::Sender<bool>,
    draining_handoff: Arc<AtomicBool>,
    task: JoinHandle<Result<(), ChannelSessionError>>,
    alive: watch::Receiver<bool>,
    lease_window: watch::Sender<RoleLeaseWindow>,
    connectivity_gate: super::connectivity::RouteConnectivityGate,
    #[cfg(feature = "service-call-zenoh")]
    calls: super::call::CallSlot,
    #[cfg(feature = "event-consumer-zenoh")]
    consumers: super::consumer::ConsumerSlot,
}
impl RegisteredChannelRole {
    pub(super) fn begin_rotation_drain(&mut self) {
        self.withdraw_route_confirmation();
        self.draining_handoff.store(true, Ordering::Release);
        #[cfg(feature = "service-call-zenoh")]
        if let Some(binding) = self
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            binding.prepare_handoff();
        }
        self.stop.send_replace(true);
    }
    pub(super) fn request_stop(&self) {
        self.stop.send_replace(true);
        #[cfg(feature = "service-call-zenoh")]
        if let Some(binding) = self
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            binding.stop();
        }
    }
    pub(super) fn withdraw_route_confirmation(&mut self) {
        self.role_deadline = Instant::now();
        self.connectivity_gate.withdraw();
    }
    pub(super) fn lifecycle_status(&self, physical_deadline: Instant) -> super::ChannelRoleStatus {
        let authorization_deadline = self.lease_window.borrow().deadline.min(physical_deadline);
        let state = if self.cleanup_failed {
            super::RoleLifecycleState::CleanupFailed
        } else if *self.stop.borrow()
            || !*self.alive.borrow()
            || Instant::now() >= authorization_deadline
        {
            super::RoleLifecycleState::Stopped
        } else if self.task.as_ref().is_none_or(|task| task.is_finished()) {
            super::RoleLifecycleState::CleanupFailed
        } else {
            super::RoleLifecycleState::Active
        };
        let ServiceEndpoint::Zenoh { route, .. } = &self.endpoint else {
            unreachable!()
        };
        super::ChannelRoleStatus {
            role_generation: route.role_generation.as_str().into(),
            state,
            authorization_deadline,
            route_deadline: if self.connectivity_gate.ready() {
                self.role_deadline.min(authorization_deadline)
            } else {
                Instant::now().min(self.role_deadline)
            },
            last_renewal_status: None,
            last_error: None,
            remote_deregistered: self.remote_deregistered,
        }
    }
    pub fn registration(&self) -> &ChannelRoleEnrollmentResponse {
        &self.response
    }
    pub fn endpoint(&self) -> &ServiceEndpoint {
        &self.endpoint
    }
    /// Routing confirmation only; this does not advertise execution readiness.
    pub fn route_confirmed(&self) -> bool {
        !self.remote_deregistered
            && !*self.stop.borrow()
            && *self.alive.borrow()
            && self.connectivity_gate.ready()
            && Instant::now() < self.role_deadline.min(self.lease_window.borrow().deadline)
            && self.task.as_ref().is_some_and(|task| !task.is_finished())
    }
    pub async fn close(&mut self) -> Result<(), ChannelSessionError> {
        self.request_stop();
        let _ = self.close_declarations().await;
        #[cfg(feature = "service-call-zenoh")]
        {
            let binding = self.calls.lock().unwrap_or_else(|e| e.into_inner()).clone();
            if let Some(binding) = binding {
                self.cleanup_failed |= !binding.join_accepted().await;
            }
        }
        if self.cleanup_failed {
            Err(ChannelSessionError::CleanupFailed)
        } else {
            Ok(())
        }
    }
    pub(super) async fn close_declarations(&mut self) -> Result<(), ChannelSessionError> {
        self.stop.send_replace(true);
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task.take();
            self.cleanup_failed = !matches!(result, Ok(Ok(())));
        }
        if self.cleanup_failed {
            Err(ChannelSessionError::CleanupFailed)
        } else {
            Ok(())
        }
    }
}

// Reserve exact declarations across this pool before native I/O. Cancelled or
// failed cleanup retains the reservation until physical pool replacement.
struct DeclarationReservation {
    keys: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    names: Vec<String>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    observation: super::observation::DeclarationObservation,
}
impl DeclarationReservation {
    fn release(&mut self) {
        let mut keys = self.keys.lock().unwrap_or_else(|e| e.into_inner());
        for name in &self.names {
            keys.remove(name);
        }
        self.permit.take();
        self.observation.release();
    }
}
impl Drop for DeclarationReservation {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.take() {
            permit.forget();
        }
    }
}
impl Drop for RegisteredChannelRole {
    fn drop(&mut self) {
        self.request_stop();
    }
}

impl ServiceChannelSessions {
    /// Explicitly transfer a recorded predecessor's Call/Provider to this
    /// candidate. Stop/join old declarations before I/O, then validate the
    /// signed replacement and prove its new lanes. Never re-enroll the domain
    /// role or extend the candidate's original preparation window.
    ///
    /// Failure/cancellation can leave a completed remote transfer. There is no
    /// rollback or automatic retry; inspect the handle and explicitly clean up.
    /// A lost transfer ACK may be retried with the stopped predecessor handle.
    pub async fn adopt_role(
        &self,
        role: &mut RegisteredChannelRole,
    ) -> Result<(), ChannelSessionError> {
        self.active_role_channel()?;
        let initial = self.identity.bootstrap_response();
        let predecessor = self
            .identity
            .rotation_predecessor()
            .ok_or(ChannelSessionError::InvalidConfig)?;
        if role.channel.certificate_identity != *predecessor
            || role.channel.application_id != initial.application_id
            || role.channel.instance_id.as_str() != initial.instance.instance_id
            || role.channel.base_generation.as_str() != initial.instance.generation
            || role.remote_deregistered
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let old_deadline = role.lease_window.borrow().deadline;
        if old_deadline <= Instant::now() {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        let ServiceEndpoint::Zenoh { route, .. } = &role.endpoint else {
            return Err(ChannelSessionError::InvalidConfig);
        };
        // Adoption preserves the registered lanes. Refuse an undersized
        // candidate before withdrawing the predecessor or transferring authority.
        validate_role_session_lanes(&role.endpoint, self.lane_count())?;
        let generation = route.role_generation.as_str().to_owned();
        let previous_endpoint = role.endpoint.clone();
        role.withdraw_route_confirmation();
        #[cfg(feature = "service-call-zenoh")]
        let previous_calls = role
            .calls
            .lock()
            .map_err(|_| ChannelSessionError::InvalidConfig)?
            .clone();
        #[cfg(feature = "service-call-zenoh")]
        if let Some(binding) = &previous_calls {
            binding.prepare_handoff();
        }
        #[cfg(feature = "event-consumer-zenoh")]
        let previous_consumers = role
            .consumers
            .lock()
            .map_err(|_| ChannelSessionError::InvalidConfig)?
            .clone();
        role.close_declarations().await?;
        let started = Instant::now();
        let envelope = self
            .role_control(
                MessageKind::AdoptChannelRole,
                &ChannelRoleAdoption {
                    role_generation: generation.clone(),
                },
            )
            .await?;
        let response: ChannelRoleAdoptionResponse = serde_json::from_str(envelope.payload.get())
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let deadline = validate_adoption_response(
            &response,
            &previous_endpoint,
            predecessor,
            started,
            old_deadline.min(self.authorization_deadline()),
            chrono::Utc::now().timestamp_millis(),
        )?;
        // Persist received ownership before further awaits, so cancellation
        // cannot later send old-certificate deregistration for this handle.
        role.endpoint = response.endpoint;
        role.channel.certificate_identity = initial.certificate.certificate_identity.clone();
        let listener = self
            .install_role_declarations(
                &role.endpoint,
                &generation,
                response.authorization_expires_at_ms,
                deadline,
                role.catalog.clone(),
                matches!(&role.response, ChannelRoleEnrollmentResponse::Consumer(_)),
            )
            .await?;
        role.stop = listener.stop;
        role.draining_handoff = listener.draining_handoff;
        role.task = Some(listener.task);
        role.alive = listener.alive;
        role.lease_window = listener.lease_window;
        role.connectivity_gate = listener.connectivity_gate;
        #[cfg(feature = "event-consumer-zenoh")]
        {
            role.consumers = listener.consumers;
            *role
                .consumers
                .lock()
                .map_err(|_| ChannelSessionError::InvalidConfig)? = previous_consumers;
        }
        #[cfg(feature = "service-call-zenoh")]
        {
            role.calls = listener.calls;
            if let Some(binding) = previous_calls {
                let next = binding.transferred(self)?;
                next.update(response.authorization_expires_at_ms);
                *role
                    .calls
                    .lock()
                    .map_err(|_| ChannelSessionError::InvalidConfig)? = Some(next);
            }
        }
        role.cleanup_failed = false;
        if let Err(error) = self.confirm_role_route(role).await {
            role.close().await?;
            return Err(error);
        }
        Ok(())
    }
    pub(super) fn owns_role(&self, role: &RegisteredChannelRole) -> bool {
        let initial = self.identity.bootstrap_response();
        role.channel.application_id == initial.application_id
            && role.channel.instance_id.as_str() == initial.instance.instance_id
            && role.channel.base_generation.as_str() == initial.instance.generation
            && role.channel.certificate_identity == initial.certificate.certificate_identity
    }

    pub(super) fn active_role(
        &self,
        role: &RegisteredChannelRole,
    ) -> Result<(), ChannelSessionError> {
        if !self.owns_role(role) {
            return Err(ChannelSessionError::InvalidConfig);
        }
        self.active_role_channel()?;
        if role.remote_deregistered
            || *role.stop.borrow()
            || !*role.alive.borrow()
            || Instant::now() >= role.lease_window.borrow().deadline
        {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        if role.task.as_ref().is_none_or(|t| t.is_finished()) {
            return Err(ChannelSessionError::CleanupFailed);
        }
        Ok(())
    }

    /// One explicit bounded batch; no automatic retry, role recreation or
    /// route-ready extension. Observe physical authority separately after this
    /// succeeds, then explicitly re-prove routes whose leases remain valid.
    pub async fn renew_role_leases(
        &self,
        roles: &mut [&mut RegisteredChannelRole],
    ) -> Result<Vec<ChannelRoleRenewalResult>, ChannelSessionError> {
        if roles.is_empty() || roles.len() > MAX_CHANNEL_RENEWAL_ROLES {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut generations = Vec::new();
        for role in roles.iter() {
            self.active_role(role)?;
            let ServiceEndpoint::Zenoh { route, .. } = &role.endpoint else {
                return Err(ChannelSessionError::InvalidConfig);
            };
            let id = route.role_generation.as_str().to_owned();
            if !ids.insert(id.clone()) {
                return Err(ChannelSessionError::InvalidConfig);
            }
            generations.push(id);
        }
        let started = Instant::now();
        let response = self
            .role_control(
                MessageKind::RenewRoles,
                &ChannelRoleRenewal {
                    role_generations: generations.clone(),
                },
            )
            .await?;
        let response: ChannelRoleRenewalResponse = serde_json::from_str(response.payload.get())
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let now = chrono::Utc::now().timestamp_millis();
        let windows = validate_renewal_response(
            &response,
            &generations,
            started,
            now,
            self.authorization_deadline(),
            self.identity
                .bootstrap_response()
                .certificate
                .expires_unix_ms,
        )?;
        for ((role, result), window) in roles.iter_mut().zip(&response.roles).zip(windows) {
            if let Some(window) = window {
                role.lease_window.send_replace(window);
            } else if result.status != 503 {
                role.lease_window.send_replace(RoleLeaseWindow {
                    expires_at_ms: now,
                    deadline: Instant::now(),
                });
            }
        }
        Ok(response.roles)
    }

    /// Fresh signed probing of the same issued route. This never changes a
    /// generation, lane epoch or endpoint and cannot revive a stopped role.
    pub async fn confirm_role_route(
        &self,
        role: &mut RegisteredChannelRole,
    ) -> Result<(), ChannelSessionError> {
        self.active_role(role)?;
        let ServiceEndpoint::Zenoh {
            route,
            route_revision,
            ..
        } = &role.endpoint
        else {
            return Err(ChannelSessionError::InvalidConfig);
        };
        let request = ChannelRoleRouteConfirmation {
            role_generation: route.role_generation.as_str().into(),
            route_revision: *route_revision,
        };
        role.withdraw_route_confirmation();
        let connectivity_revision = role
            .connectivity_gate
            .revision()
            .ok_or(ChannelSessionError::Transport)?;
        let response = self.role_control(MessageKind::BindLane, &request).await?;
        let endpoint: ServiceEndpoint = serde_json::from_str(response.payload.get())
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        if endpoint != role.endpoint {
            return Err(ChannelSessionError::InvalidResponse);
        }
        self.active_role(role)?;
        if !role.connectivity_gate.confirm(connectivity_revision) {
            return Err(ChannelSessionError::Transport);
        }
        role.role_deadline = role
            .lease_window
            .borrow()
            .deadline
            .min(self.authorization_deadline());
        Ok(())
    }

    /// Withdraw the exact role on its original channel and join local cleanup.
    /// No automatic retry: after a lost ACK callers may explicitly retry within
    /// remaining authority. `RegisteredChannelRole::close` remains local-only.
    pub async fn deregister_role(
        &self,
        role: &mut RegisteredChannelRole,
    ) -> Result<(), ChannelSessionError> {
        if !self.owns_role(role) {
            return Err(ChannelSessionError::InvalidConfig);
        }
        // Even cancellation during remote I/O withdraws local declarations.
        // Keep the task handle so close can still prove cleanup afterwards.
        role.stop.send_replace(true);
        if !role.remote_deregistered {
            let ServiceEndpoint::Zenoh { route, .. } = &role.endpoint else {
                return Err(ChannelSessionError::InvalidConfig);
            };
            let generation = route.role_generation.as_str().to_owned();
            let result = self
                .role_control(
                    MessageKind::Deregister,
                    &ChannelRoleDeregistration {
                        role_generation: generation.clone(),
                    },
                )
                .await
                .and_then(|response| {
                    let response: ChannelRoleDeregistrationResponse =
                        serde_json::from_str(response.payload.get())
                            .map_err(|_| ChannelSessionError::InvalidResponse)?;
                    if response.role_generation != generation {
                        return Err(ChannelSessionError::InvalidResponse);
                    }
                    Ok(())
                });
            if let Err(error) = result {
                role.close().await?;
                return Err(error);
            }
            role.remote_deregistered = true;
        }
        role.close().await
    }

    /// Derive v2 registration from the same ServiceRegistry used by HTTP. The
    /// application does not declare Zenoh keys or rewrite business handlers.
    #[cfg(feature = "service-manifest")]
    pub async fn register_service_role(
        &self,
        node_id: impl Into<String>,
        maximum_in_flight: u32,
        registry: &crate::services::ServiceRegistry,
    ) -> Result<RegisteredChannelRole, ChannelSessionError> {
        if registry.manifest().application_id != self.identity.connection.application_id() {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let initial = self.identity.bootstrap_response();
        let instance = self
            .identity
            .connection
            .registration()
            .await
            .request(&initial.instance.instance_id);
        self.register_role(ChannelRoleEnrollment::Call(kish_lingshu_runtime_contract::service::ServiceEnrollmentV2 {
            enrollment_version: kish_lingshu_foundation_contract::service_transport::enrollment::EnrollmentVersion::V2,
            instance, node_id: node_id.into(), maximum_in_flight, capabilities: registry.capabilities(),
            endpoint: RequestedServiceEndpoint::Zenoh { protocol_version: ProtocolVersion::V1, lane_count: (self.lane_count() as u8).try_into().map_err(|_| ChannelSessionError::InvalidConfig)? },
        })).await
    }

    pub async fn register_provider_role(
        &self,
        catalog: &kish_lingshu_runtime_contract::provider::ProviderCatalog,
    ) -> Result<RegisteredChannelRole, ChannelSessionError> {
        if catalog.application_id != self.identity.connection.application_id() {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let snapshot = Arc::new(super::catalog::CatalogSnapshot::new(
            catalog,
            &self.identity.connection,
        )?);
        let digest = snapshot.digest.clone();
        let initial = self.identity.bootstrap_response();
        let instance = self
            .identity
            .connection
            .registration()
            .await
            .request(&initial.instance.instance_id);
        self.register_role_with_catalog(ChannelRoleEnrollment::Provider(kish_lingshu_runtime_contract::provider::ProviderEnrollmentV2 {
            enrollment_version: kish_lingshu_foundation_contract::service_transport::enrollment::EnrollmentVersion::V2,
            instance, provider_key: catalog.provider_key.clone(), release: catalog.release.clone(), catalog_digest: digest,
            endpoint: RequestedProviderEndpoint::Zenoh { protocol_version: ProtocolVersion::V1 },
        }), Some(snapshot)).await
    }

    pub(super) fn active_role_channel(&self) -> Result<(), ChannelSessionError> {
        if self.report_draining {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        self.identity
            .connection
            .ensure_open()
            .map_err(|_| ChannelSessionError::Closed)?;
        if self.closed.borrow().is_some() || Instant::now() >= self.authorization_deadline() {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        if !self.transport.is_running() {
            return Err(ChannelSessionError::CleanupFailed);
        }
        Ok(())
    }

    /// One bounded control query. A lost registration/binding response is not
    /// retried automatically and never causes HTTP fallback or generation reset.
    pub(super) async fn role_control<T: serde::Serialize>(
        &self,
        kind: MessageKind,
        value: &T,
    ) -> Result<TransportEnvelope, ChannelSessionError> {
        self.verified_role_control(kind, value)
            .await
            .map(|reply| reply.envelope)
    }

    async fn verified_role_control<T: serde::Serialize>(
        &self,
        kind: MessageKind,
        value: &T,
    ) -> Result<VerifiedRoleControlReply, ChannelSessionError> {
        super::trace::scope(None, self.role_control_scoped(kind, value)).await
    }

    async fn role_control_scoped<T: serde::Serialize>(
        &self,
        kind: MessageKind,
        value: &T,
    ) -> Result<VerifiedRoleControlReply, ChannelSessionError> {
        self.active_role_channel()?;
        let initial = self.identity.bootstrap_response();
        let target = initial
            .control_route
            .key()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let request_id = RouteIdentity::new(uuid::Uuid::new_v4().to_string())
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let now = chrono::Utc::now().timestamp_millis();
        let deadline = now + 10_000;
        let payload = serde_json::value::to_raw_value(value)
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let subject = ClientChannelIdentity {
            application_id: initial.application_id.clone(),
            instance_id: RouteIdentity::new(&initial.instance.instance_id)
                .map_err(|_| ChannelSessionError::InvalidConfig)?,
            base_generation: RouteIdentity::new(&initial.instance.generation)
                .map_err(|_| ChannelSessionError::InvalidConfig)?,
            certificate_identity: initial.certificate.certificate_identity.clone(),
        };
        let proof = self
            .identity
            .message_signer()
            .sign_message(
                &subject,
                kind,
                &target,
                &request_id,
                payload.get().as_bytes(),
                now,
                deadline,
            )
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let request = TransportEnvelope {
            protocol_version: ProtocolVersion::V1,
            kind,
            request_id: request_id.clone(),
            application_id: initial.application_id.clone(),
            target: target.clone(),
            deadline_unix_ms: deadline,
            proof,
            trace_parent: super::trace::current_trace_parent(),
            payload,
        }
        .encode(now)
        .map_err(|_| ChannelSessionError::InvalidConfig)?;
        if request.len() > MAX_CHANNEL_CONTROL_BYTES {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let session = self.control_session()?;
        let mut observation = observation::ExchangeObservation::new(Plane::Control);
        let operation = async {
            let replies = session
                .get(target.as_str().to_owned())
                .target(zenoh::query::QueryTarget::All)
                .consolidation(zenoh::query::ConsolidationMode::None)
                .timeout(Duration::from_secs(10))
                .payload(request)
                .await
                .map_err(|_| ChannelSessionError::Transport)?;
            let mut result = None;
            while let Ok(reply) = replies.recv_async().await {
                if result.is_some() {
                    return Err(ChannelSessionError::InvalidResponse);
                }
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
                    &initial.application_id,
                    chrono::Utc::now().timestamp_millis(),
                )
                .map_err(|_| ChannelSessionError::InvalidResponse)?;
                if response.kind != kind
                    || response.request_id != request_id
                    || response.deadline_unix_ms != deadline
                {
                    return Err(ChannelSessionError::InvalidResponse);
                }
                let claims = self
                    .identity
                    .connection
                    .verify_channel_message(&initial.transport_trust, &response)
                    .map_err(|_| ChannelSessionError::InvalidResponse)?;
                #[cfg(test)]
                super::trace::assert_verified_reply_trace(&response);
                result = Some(VerifiedRoleControlReply {
                    envelope: response,
                    issued_at_unix_ms: claims.issued_at_unix_ms,
                });
            }
            result.ok_or(ChannelSessionError::Transport)
        };
        let mut closed = self.closed.clone();
        let mut logical = self.identity.connection.subscribe_closed();
        let result = tokio::select! {
            _ = closed.changed() => Err(ChannelSessionError::Closed),
            _ = logical.changed() => Err(ChannelSessionError::Closed),
            result = tokio::time::timeout_at((Instant::now() + Duration::from_secs(10)).min(self.authorization_deadline()), operation) => result.map_err(|_| ChannelSessionError::Transport)?,
        }?;
        self.active_role_channel()?;
        observation.reply_verified();
        Ok(result)
    }

    /// Register an existing declaration and prove every server-issued lane.
    /// Attach its execution binding separately; optional role supervision owns
    /// renewal. Registration never invokes a business handler or replays work.
    pub async fn register_role(
        &self,
        request: ChannelRoleEnrollment,
    ) -> Result<RegisteredChannelRole, ChannelSessionError> {
        self.register_role_with_catalog(request, None).await
    }
    async fn register_role_with_catalog(
        &self,
        request: ChannelRoleEnrollment,
        catalog: Option<Arc<super::catalog::CatalogSnapshot>>,
    ) -> Result<RegisteredChannelRole, ChannelSessionError> {
        self.active_role_channel()?;
        request
            .validate()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let requested_lanes = match &request {
            ChannelRoleEnrollment::Call(r) => r.endpoint.lane_count(),
            ChannelRoleEnrollment::Consumer(r) => r.endpoint.lane_count(),
            ChannelRoleEnrollment::Provider(_) => 1,
        };
        if requested_lanes > self.lane_count() {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let initial = self.identity.bootstrap_response();
        let scope = request.instance();
        if scope.instance_id != initial.instance.instance_id
            || scope.generation.as_deref() != Some(&initial.instance.generation)
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let started = Instant::now();
        let verified = self
            .verified_role_control(MessageKind::Register, &request)
            .await?;
        let response: ChannelRoleEnrollmentResponse =
            serde_json::from_str(verified.envelope.payload.get())
                .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let (endpoint, generation, expires) = validate_registration_response(&request, &response)?;
        let ServiceEndpoint::Zenoh { route, lanes, .. } = &endpoint else {
            return Err(ChannelSessionError::InvalidResponse);
        };
        validate_route_binding(
            route,
            initial.deployment.as_str(),
            initial.application_id.as_str(),
            &initial.instance,
            &generation,
        )
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
        endpoint
            .validate()
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let now = chrono::Utc::now().timestamp_millis();
        if lanes
            .iter()
            .any(|lane| usize::from(lane.lane.index()) >= self.lane_count())
            || lanes.len() != requested_lanes
            || lanes
                .iter()
                .enumerate()
                .any(|(index, lane)| usize::from(lane.lane.index()) != index)
        {
            return Err(ChannelSessionError::InvalidResponse);
        }
        let deadline = initial_role_deadline(
            expires,
            verified.issued_at_unix_ms,
            now,
            started,
            self.authorization_deadline(),
        )?;
        let listener = self
            .install_role_declarations(
                &endpoint,
                &generation,
                expires,
                deadline,
                catalog.clone(),
                matches!(&request, ChannelRoleEnrollment::Consumer(_)),
            )
            .await?;
        let logical_key = match &request {
            ChannelRoleEnrollment::Provider(r) => format!("provider:{}", r.provider_key),
            ChannelRoleEnrollment::Call(r) => format!("call:{}", r.node_id),
            ChannelRoleEnrollment::Consumer(r) => format!("consumer:{}", r.group_key),
        };
        let mut handle = RegisteredChannelRole {
            response,
            logical_key,
            catalog,
            endpoint: endpoint.clone(),
            stop: listener.stop,
            draining_handoff: listener.draining_handoff,
            task: Some(listener.task),
            role_deadline: deadline,
            connectivity_gate: listener.connectivity_gate,
            alive: listener.alive,
            cleanup_failed: false,
            channel: ClientChannelIdentity {
                application_id: initial.application_id.clone(),
                instance_id: RouteIdentity::new(&initial.instance.instance_id)
                    .map_err(|_| ChannelSessionError::InvalidResponse)?,
                base_generation: RouteIdentity::new(&initial.instance.generation)
                    .map_err(|_| ChannelSessionError::InvalidResponse)?,
                certificate_identity: initial.certificate.certificate_identity.clone(),
            },
            remote_deregistered: false,
            lease_window: listener.lease_window,
            #[cfg(feature = "service-call-zenoh")]
            calls: listener.calls,
            #[cfg(feature = "event-consumer-zenoh")]
            consumers: listener.consumers,
            #[cfg(feature = "event-consumer-zenoh")]
            consumer_capacity: match &request {
                ChannelRoleEnrollment::Consumer(value) => Some(value.maximum_in_flight),
                _ => None,
            },
            #[cfg(feature = "service-call-zenoh")]
            call_contract: match &request {
                ChannelRoleEnrollment::Call(value) => {
                    Some((value.capabilities.clone(), value.maximum_in_flight))
                }
                _ => None,
            },
        };
        let accepted = self.confirm_role_route(&mut handle).await;
        if let Err(error) = accepted {
            handle.close().await?;
            return Err(error);
        }
        if !handle.route_confirmed() {
            handle.close().await?;
            return Err(ChannelSessionError::AuthorityExpired);
        }
        Ok(handle)
    }

    async fn install_role_declarations(
        &self,
        endpoint: &ServiceEndpoint,
        generation: &str,
        expires: i64,
        deadline: Instant,
        catalog: Option<Arc<super::catalog::CatalogSnapshot>>,
        consumer_presence: bool,
    ) -> Result<RoleDeclarations, ChannelSessionError> {
        validate_role_session_lanes(endpoint, self.lane_count())?;
        let ServiceEndpoint::Zenoh { route, lanes, .. } = endpoint else {
            return Err(ChannelSessionError::InvalidConfig);
        };
        let mut targets = lanes
            .iter()
            .map(|lane| {
                route
                    .invoke_key(lane)
                    .map(|key| (key, usize::from(lane.lane.index()), false))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        if let Some(snapshot) = &catalog {
            let lane = lanes.first().ok_or(ChannelSessionError::InvalidConfig)?;
            targets.push((
                route
                    .catalog_key(&snapshot.digest)
                    .map_err(|_| ChannelSessionError::InvalidConfig)?,
                usize::from(lane.lane.index()),
                true,
            ));
        }
        let names = targets
            .iter()
            .map(|(key, _, _)| key.as_str().to_owned())
            .collect::<Vec<_>>();
        let permit = self.role_slots.clone().try_acquire_owned().map_err(|_| {
            observation::rejected(Plane::Declaration, Rejection::CountExhausted);
            ChannelSessionError::CapacityExceeded
        })?;
        {
            let mut keys = self
                .role_keys
                .lock()
                .map_err(|_| ChannelSessionError::InvalidConfig)?;
            if names.iter().any(|key| keys.contains(key)) {
                observation::rejected(Plane::Declaration, Rejection::DuplicateDeclaration);
                return Err(ChannelSessionError::InvalidConfig);
            }
            keys.extend(names.iter().cloned());
        }
        let mut reservation = DeclarationReservation {
            keys: self.role_keys.clone(),
            observation: self.declaration_observation.reserve(names.len()),
            names,
            permit: Some(permit),
        };
        let (sender, mut receiver) = mpsc::channel::<RoleQuery>(16);
        let (control_sender, mut controls) = mpsc::channel::<RoleQuery>(4);
        let mut declarations = Vec::new();
        for (key, session_index, is_catalog) in targets {
            let session = self
                .sessions
                .get(session_index)
                .ok_or(ChannelSessionError::InvalidResponse)?;
            let catalog_budget = self.identity.connection.catalog_budgets().1;
            let sender = sender.clone();
            let control_sender = control_sender.clone();
            let target = key.clone();
            let budget = self.role_query_bytes.clone();
            let slots = self.role_query_slots.clone();
            let control_slots = self.role_control_slots.clone();
            let control_bytes = self.role_control_bytes.clone();
            let declaration = session
                .declare_queryable(key.as_str().to_owned())
                .callback(move |query| {
                    let limit = if (cfg!(feature = "service-call-zenoh") || cfg!(feature = "event-consumer-zenoh")) && !is_catalog {
                        kish_lingshu_foundation_contract::service_transport::MAX_BUSINESS_PAYLOAD_BYTES + kish_lingshu_foundation_contract::service_transport::MAX_ENVELOPE_OVERHEAD_BYTES
                    } else { MAX_CHANNEL_CONTROL_BYTES };
                    let Some(size) = kish_lingshu_foundation_contract::service_transport::budget::query_reservation_bytes(
                        query.payload().map(|p| p.len()), limit, query.key_expr().as_str().len(),
                        query.parameters().is_empty(), query.attachment().map_or(0, |p| p.len()),
                    ) else {
                        observation::rejected(if is_catalog { Plane::Catalog } else { Plane::RoleBusiness }, Rejection::Invalid);
                        return;
                    };
                    // Classification selects resource reservation only. The
                    // signed proof, exact role/lane and finite authority are
                    // still checked by the normal handler before any action.
                    let control = !is_catalog && query.payload().is_some_and(|p| {
                        reserved_control_kind(&p.to_bytes(), MAX_CHANNEL_CONTROL_BYTES)
                    });
                    let (slots, budget, sender) = if control {
                        (&control_slots, &control_bytes, &control_sender)
                    } else { (&slots, &budget, &sender) };
                    let plane = if control { Plane::RoleControl } else if is_catalog { Plane::Catalog } else { Plane::RoleBusiness };
                    let Some(reservation) = QueryReservation::acquire(plane, slots, budget, size) else { return; };
                    let catalog_permit = if is_catalog {
                        match catalog_budget.clone().try_acquire_owned() {
                            Ok(permit) => Some(permit),
                            Err(_) => {
                                observation::rejected(Plane::Catalog, Rejection::CatalogExhausted);
                                return;
                            },
                        }
                    } else { None };
                    observation::enqueue(sender, (target.clone(), query, reservation, catalog_permit), plane);
                })
                .await;
            match declaration {
                Ok(queryable) => declarations.push(queryable),
                Err(_) => {
                    let mut cleaned = true;
                    for queryable in declarations {
                        cleaned &= matches!(
                            tokio::time::timeout(Duration::from_secs(5), queryable.undeclare())
                                .await,
                            Ok(Ok(()))
                        );
                    }
                    if cleaned {
                        reservation.release();
                    }
                    return Err(if cleaned {
                        ChannelSessionError::Transport
                    } else {
                        ChannelSessionError::CleanupFailed
                    });
                }
            }
        }
        let presence = if consumer_presence {
            Some(
                self.sessions[0]
                    .liveliness()
                    .declare_token(
                        route
                            .consumer_presence_key(
                                &lanes[0],
                                &self.identity.bootstrap_response().control_route,
                            )
                            .map_err(|_| ChannelSessionError::InvalidConfig)?
                            .as_str()
                            .to_owned(),
                    )
                    .await
                    .map_err(|_| ChannelSessionError::Transport)?,
            )
        } else {
            None
        };
        drop(sender);
        drop(control_sender);
        let endpoint = endpoint.clone();
        let connectivity_gate =
            super::connectivity::RouteConnectivityGate::new(self.connectivity.clone(), lanes.len());
        let admission = connectivity_gate.clone();
        let connection = self.identity.connection.clone();
        let initial = self.identity.bootstrap_response().clone();
        let signer = Arc::new(
            ChannelMessageSigner::from_pkcs8(&self.identity.credential.key.serialize_der())
                .map_err(|_| ChannelSessionError::InvalidConfig)?,
        );
        let mut physical = self.closed.clone();
        let mut logical = connection.subscribe_closed();
        let mut authority = self.authority.clone();
        let generation = generation.to_owned();
        let (stop, mut stopped) = watch::channel(false);
        let draining_handoff = Arc::new(AtomicBool::new(false));
        #[cfg(feature = "event-consumer-zenoh")]
        let consumer_handoff = draining_handoff.clone();
        let (status, alive) = watch::channel(true);
        let (lease_window, mut role_window) = watch::channel(RoleLeaseWindow {
            expires_at_ms: expires,
            deadline,
        });
        #[cfg(feature = "service-call-zenoh")]
        let calls: super::call::CallSlot = Arc::new(std::sync::Mutex::new(None));
        #[cfg(feature = "service-call-zenoh")]
        let call_slot = calls.clone();
        #[cfg(feature = "service-call-zenoh")]
        let call_tasks_budget = self.call_query_slots.clone();
        #[cfg(feature = "event-consumer-zenoh")]
        let consumers: super::consumer::ConsumerSlot = Arc::new(std::sync::Mutex::new(None));
        #[cfg(feature = "event-consumer-zenoh")]
        let consumer_slot = consumers.clone();
        #[cfg(feature = "event-consumer-zenoh")]
        let consumer_tasks_budget = self.consumer_query_slots.clone();
        let task = tokio::spawn(async move {
            let mut call_tasks = tokio::task::JoinSet::<()>::new();
            let mut control_burst = 0;
            loop {
                if physical.borrow().is_some()
                    || connection.ensure_open().is_err()
                    || Instant::now() >= role_window.borrow().deadline.min(*authority.borrow())
                {
                    break;
                }
                #[cfg(feature = "service-call-zenoh")]
                if let Some(binding) = call_slot.lock().unwrap_or_else(|e| e.into_inner()).as_ref()
                {
                    binding.update(role_window.borrow().expires_at_ms);
                }
                let physical_deadline = role_window.borrow().deadline.min(*authority.borrow());
                let request = tokio::select! {
                    biased;
                    _ = stopped.wait_for(|v| *v) => break,
                    _ = physical.changed() => break,
                    _ = logical.changed() => break,
                    changed = authority.changed() => { if changed.is_err() { break; } continue; },
                    changed = role_window.changed() => { if changed.is_err() { break; } continue; },
                    _ = tokio::time::sleep_until(physical_deadline) => break,
                    _ = call_tasks.join_next(), if !call_tasks.is_empty() => continue,
                    query = next_role_query(&mut controls, &mut receiver, &mut control_burst) => query,
                };
                let Some((target, query, mut reservation, _catalog_slot)) = request else {
                    break;
                };
                reservation.start_processing();
                #[cfg(feature = "event-consumer-zenoh")]
                {
                    let bytes = query.payload().map(|p| p.to_bytes().into_owned());
                    let kind = bytes
                        .as_ref()
                        .and_then(|b| {
                            TransportEnvelope::decode(
                                b,
                                &target,
                                &initial.application_id,
                                chrono::Utc::now().timestamp_millis(),
                            )
                            .ok()
                        })
                        .map(|e| e.kind);
                    if kind == Some(MessageKind::InvokeEvent) {
                        if !admission.ready() {
                            continue;
                        }
                        let binding = consumer_slot
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .clone();
                        if let (Some(binding), Some(bytes), Ok(permit)) = (
                            binding,
                            bytes,
                            consumer_tasks_budget
                                .clone()
                                .try_acquire_owned()
                                .map_err(|error| {
                                    observation::rejected(
                                        Plane::Consumer,
                                        Rejection::TaskExhausted,
                                    );
                                    error
                                }),
                        ) {
                            let endpoint = endpoint.clone();
                            let connection = connection.clone();
                            let initial = initial.clone();
                            let signer = signer.clone();
                            let expires = role_window.borrow().expires_at_ms;
                            let deadline = role_window.borrow().deadline.min(*authority.borrow());
                            let mut physical = physical.clone();
                            let mut stopped = stopped.clone();
                            let handoff = consumer_handoff.clone();
                            let mut logical = logical.clone();
                            let mut exchange =
                                observation::InboundExchangeObservation::new(Plane::Consumer);
                            call_tasks.spawn(async move {
                                let (_reservation, _permit) = (reservation, permit);
                                tokio::select! {
                                    biased;
                                    _ = stop_unless_handoff(&mut stopped, &handoff) => {},
                                    _ = physical.changed() => {},
                                    _ = logical.changed() => {},
                                    _ = tokio::time::timeout_at(deadline, async {
                                        if let Ok(reply) = binding.reply(&endpoint, &target, &bytes, expires, &connection, &initial, &signer).await {
                                            if query.reply(target.as_str().to_owned(), reply).await.is_ok() {
                                                exchange.reply_submitted();
                                            }
                                        }
                                    }) => {},
                                }
                            });
                        }
                        continue;
                    }
                }
                #[cfg(feature = "service-call-zenoh")]
                {
                    let bytes = query.payload().map(|p| p.to_bytes().into_owned());
                    let kind = bytes
                        .as_ref()
                        .and_then(|b| {
                            TransportEnvelope::decode(
                                b,
                                &target,
                                &initial.application_id,
                                chrono::Utc::now().timestamp_millis(),
                            )
                            .ok()
                        })
                        .map(|e| e.kind);
                    if matches!(
                        kind,
                        Some(MessageKind::InvokeCall | MessageKind::CancelCall)
                    ) {
                        if kind == Some(MessageKind::InvokeCall) && !admission.ready() {
                            #[cfg(test)]
                            if std::env::var("LINGSHU_VERIFY_SDK_RESOURCES").as_deref()
                                == Ok("true")
                            {
                                eprintln!("native fixture Call dropped: route unconfirmed");
                            }
                            continue;
                        }
                        let binding = call_slot.lock().unwrap_or_else(|e| e.into_inner()).clone();
                        if let (Some(binding), Some(bytes)) = (binding, bytes) {
                            if kind == Some(MessageKind::CancelCall) {
                                // Reserved control path: a full execution-task budget must
                                // not block the cancellation that releases that budget.
                                let expires = role_window.borrow().expires_at_ms;
                                let mut exchange =
                                    observation::InboundExchangeObservation::new(Plane::Call);
                                if let Ok(reply) = binding
                                    .reply(
                                        &endpoint,
                                        &target,
                                        &bytes,
                                        expires,
                                        &connection,
                                        &initial,
                                        &signer,
                                    )
                                    .await
                                {
                                    if matches!(
                                        tokio::time::timeout(
                                            Duration::from_millis(250),
                                            query.reply(target.as_str().to_owned(), reply),
                                        )
                                        .await,
                                        Ok(Ok(_))
                                    ) {
                                        exchange.reply_submitted();
                                    }
                                }
                                continue;
                            }
                            let Ok(permit) = call_tasks_budget.clone().try_acquire_owned() else {
                                observation::rejected(Plane::Call, Rejection::TaskExhausted);
                                continue;
                            };
                            let endpoint = endpoint.clone();
                            let connection = connection.clone();
                            let initial = initial.clone();
                            let signer = signer.clone();
                            let expires = role_window.borrow().expires_at_ms;
                            let deadline = role_window
                                .borrow()
                                .deadline
                                .min(*authority.borrow())
                                .min(Instant::now() + Duration::from_secs(10));
                            let mut exchange =
                                observation::InboundExchangeObservation::new(Plane::Call);
                            call_tasks.spawn(async move {
                                let (_reservation, _permit) = (reservation, permit);
                                // Result loss is unknown; no local resubmission follows.
                                let reply = binding
                                    .reply(
                                        &endpoint,
                                        &target,
                                        &bytes,
                                        expires,
                                        &connection,
                                        &initial,
                                        &signer,
                                    )
                                    .await;
                                #[cfg(test)]
                                if std::env::var("LINGSHU_VERIFY_SDK_RESOURCES").as_deref()
                                    == Ok("true")
                                {
                                    if let Err(error) = &reply {
                                        eprintln!("native fixture Call reply rejected: {error:?}");
                                    }
                                }
                                if let Ok(reply) = reply {
                                    if matches!(
                                        tokio::time::timeout_at(
                                            deadline,
                                            query.reply(target.as_str().to_owned(), reply),
                                        )
                                        .await,
                                        Ok(Ok(_))
                                    ) {
                                        exchange.reply_submitted();
                                    }
                                }
                            });
                        }
                        continue;
                    }
                }
                let reply = (|| {
                    if connection.ensure_open().is_err()
                        || Instant::now() >= role_window.borrow().deadline.min(*authority.borrow())
                    {
                        return Err(ChannelSessionError::AuthorityExpired);
                    }
                    let bytes = query
                        .payload()
                        .ok_or(ChannelSessionError::InvalidResponse)?
                        .to_bytes();
                    let now = chrono::Utc::now().timestamp_millis();
                    if let Some(snapshot) = &catalog {
                        if route_catalog_target(&endpoint, snapshot, &target) {
                            if !admission.ready() {
                                return Err(ChannelSessionError::Transport);
                            }
                            return super::catalog::reply(
                                snapshot,
                                &endpoint,
                                &target,
                                &bytes,
                                role_window.borrow().expires_at_ms,
                                &connection,
                                &initial,
                                &signer,
                                now,
                            );
                        }
                    }
                    let request =
                        super::route_probe::validate_challenge(&endpoint, &target, &bytes, now)?;
                    let ServiceEndpoint::Zenoh { route, .. } = &endpoint else {
                        return Err(ChannelSessionError::InvalidConfig);
                    };
                    if route.role_generation.as_str() != generation
                        || request.deadline_unix_ms > role_window.borrow().expires_at_ms
                    {
                        return Err(ChannelSessionError::InvalidResponse);
                    }
                    let claims = connection
                        .verify_channel_message(&initial.transport_trust, &request)
                        .map_err(|_| ChannelSessionError::InvalidResponse)?;
                    super::route_probe::validate_challenge_time(&claims, now)?;
                    super::route_probe::sign_reply(
                        &ClientChannelIdentity {
                            application_id: initial.application_id.clone(),
                            instance_id: route.instance_id.clone(),
                            base_generation: route.base_generation.clone(),
                            certificate_identity: initial.certificate.certificate_identity.clone(),
                        },
                        &signer,
                        request,
                        now,
                    )
                })();
                if let Ok(reply) = reply {
                    let reply_deadline = role_window
                        .borrow()
                        .deadline
                        .min(*authority.borrow())
                        .min(Instant::now() + Duration::from_secs(5));
                    tokio::select! {
                        _ = stopped.wait_for(|v| *v) => break,
                        _ = physical.changed() => break,
                        _ = logical.changed() => break,
                        _ = tokio::time::timeout_at(reply_deadline, query.reply(target.as_str().to_owned(), reply)) => {},
                    }
                }
            }
            let mut cleaned = true;
            if let Some(presence) = presence {
                cleaned &= matches!(
                    tokio::time::timeout(Duration::from_secs(5), presence.undeclare()).await,
                    Ok(Ok(()))
                );
            }
            // Withdraw discovery before draining. Role expiry alone must not
            // cancel an accepted renewable attempt's independent report scope.
            #[cfg(feature = "service-call-zenoh")]
            let binding = call_slot.lock().unwrap_or_else(|e| e.into_inner()).clone();
            #[cfg(feature = "service-call-zenoh")]
            if let Some(binding) = &binding {
                binding.withdraw();
                if !binding.is_handoff()
                    && (*stopped.borrow()
                        || physical.borrow().is_some()
                        || connection.ensure_open().is_err()
                        || Instant::now() >= *authority.borrow())
                {
                    binding.stop();
                }
            }
            status.send_replace(false);
            receiver.close();
            controls.close();
            // Closed discovery may retain independently accepted reports. Drop
            // queued requests now so those old roles cannot hold another active
            // role's shared ingress/control reservations during report cleanup.
            drop(receiver);
            drop(controls);
            // No invocation can spawn another accepted task after query join.
            while call_tasks.join_next().await.is_some() {}
            for queryable in declarations {
                cleaned &= matches!(
                    tokio::time::timeout(Duration::from_secs(5), queryable.undeclare()).await,
                    Ok(Ok(()))
                );
            }
            #[cfg(feature = "service-call-zenoh")]
            if let Some(binding) = binding.filter(|b| !b.is_handoff()) {
                loop {
                    if *stopped.borrow()
                        || physical.borrow().is_some()
                        || connection.ensure_open().is_err()
                        || Instant::now() >= *authority.borrow()
                    {
                        binding.stop();
                        break;
                    }
                    let deadline = *authority.borrow();
                    tokio::select! {
                        _ = binding.wait_idle() => break,
                        _ = stopped.wait_for(|v| *v) => {},
                        _ = physical.changed() => {},
                        _ = logical.changed() => {},
                        changed = authority.changed() => { if changed.is_err() { binding.stop(); break; } },
                        _ = tokio::time::sleep_until(deadline) => {},
                    }
                }
                cleaned &= binding.join_accepted().await;
            }
            if cleaned {
                reservation.release();
                Ok(())
            } else {
                Err(ChannelSessionError::CleanupFailed)
            }
        });
        Ok(RoleDeclarations {
            stop,
            draining_handoff,
            task,
            alive,
            lease_window,
            connectivity_gate,
            #[cfg(feature = "service-call-zenoh")]
            calls,
            #[cfg(feature = "event-consumer-zenoh")]
            consumers,
        })
    }
}

#[cfg(feature = "event-consumer-zenoh")]
async fn stop_unless_handoff(stopped: &mut watch::Receiver<bool>, handoff: &AtomicBool) {
    let _ = stopped.wait_for(|v| *v).await;
    if handoff.load(Ordering::Acquire) {
        std::future::pending::<()>().await;
    }
}

fn validate_role_session_lanes(
    endpoint: &ServiceEndpoint,
    data_sessions: usize,
) -> Result<(), ChannelSessionError> {
    endpoint
        .validate()
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
    let ServiceEndpoint::Zenoh { lanes, .. } = endpoint else {
        return Err(ChannelSessionError::InvalidConfig);
    };
    if lanes
        .iter()
        .any(|lane| usize::from(lane.lane.index()) >= data_sessions)
    {
        return Err(ChannelSessionError::InvalidResponse);
    }
    Ok(())
}

/// The reply issuance time has already passed signature, binding and nonce
/// verification. Receipt latency and clock skew must not extend role authority.
fn initial_role_deadline(
    expires: i64,
    issued: i64,
    now: i64,
    started: Instant,
    physical_deadline: Instant,
) -> Result<Instant, ChannelSessionError> {
    use kish_lingshu_foundation_contract::service_transport::bootstrap::MAX_BOOTSTRAP_CLOCK_SKEW_MS;
    let duration = expires
        .checked_sub(issued)
        .ok_or(ChannelSessionError::InvalidResponse)?;
    if now < 0
        || issued < 0
        || issued > now.saturating_add(MAX_BOOTSTRAP_CLOCK_SKEW_MS)
        || expires <= now
        || !(1..=30_000).contains(&duration)
    {
        return Err(ChannelSessionError::InvalidResponse);
    }
    let remaining = duration.min(expires - now) as u64;
    let deadline = (started + Duration::from_millis(remaining)).min(physical_deadline);
    if deadline <= Instant::now() {
        return Err(ChannelSessionError::AuthorityExpired);
    }
    Ok(deadline)
}

fn validate_adoption_response(
    response: &ChannelRoleAdoptionResponse,
    previous: &ServiceEndpoint,
    predecessor: &RouteIdentity,
    started: Instant,
    cap: Instant,
    now: i64,
) -> Result<Instant, ChannelSessionError> {
    response
        .endpoint
        .validate()
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
    let (
        ServiceEndpoint::Zenoh {
            route: old_route,
            route_revision: old_revision,
            lanes: old_lanes,
            ..
        },
        ServiceEndpoint::Zenoh {
            route,
            route_revision,
            lanes,
            ..
        },
    ) = (previous, &response.endpoint)
    else {
        return Err(ChannelSessionError::InvalidResponse);
    };
    let duration = response
        .authorization_expires_at_ms
        .checked_sub(response.authorization_issued_at_ms)
        .ok_or(ChannelSessionError::InvalidResponse)?;
    if response.previous_certificate_identity != *predecessor
        || route != old_route
        || old_revision.checked_add(1) != Some(*route_revision)
        || lanes.len() != old_lanes.len()
        || lanes
            .iter()
            .zip(old_lanes)
            .any(|(new, old)| new.lane != old.lane || new.epoch == old.epoch)
        || !(1..=30_000).contains(&duration)
        || response.authorization_issued_at_ms < 0
        || response.authorization_issued_at_ms > now.saturating_add(5_000)
        || response.authorization_expires_at_ms <= now
    {
        return Err(ChannelSessionError::InvalidResponse);
    }
    let remaining = duration.min(response.authorization_expires_at_ms - now) as u64;
    let deadline = (started + Duration::from_millis(remaining)).min(cap);
    if deadline <= Instant::now() {
        return Err(ChannelSessionError::AuthorityExpired);
    }
    Ok(deadline)
}

fn validate_renewal_response(
    response: &ChannelRoleRenewalResponse,
    generations: &[String],
    started: Instant,
    now: i64,
    physical_deadline: Instant,
    certificate_expires_at_ms: i64,
) -> Result<Vec<Option<RoleLeaseWindow>>, ChannelSessionError> {
    if response.roles.len() != generations.len()
        || response.renewal_issued_at_ms < 0
        || response.renewal_issued_at_ms > now.saturating_add(kish_lingshu_foundation_contract::service_transport::bootstrap::MAX_BOOTSTRAP_CLOCK_SKEW_MS) {
        return Err(ChannelSessionError::InvalidResponse);
    }
    let mut windows = Vec::new();
    for (result, id) in response.roles.iter().zip(generations.iter()) {
        if &result.role_generation != id {
            return Err(ChannelSessionError::InvalidResponse);
        }
        match (
            result.status,
            result.lease_expires_at_ms,
            result.authorization_expires_at_ms,
        ) {
            (200, Some(lease), Some(expires))
                if expires > now
                    && expires <= lease
                    && lease <= response.renewal_issued_at_ms.saturating_add(30_000)
                    && expires <= certificate_expires_at_ms =>
            {
                let remaining = (expires - now).min(expires - response.renewal_issued_at_ms);
                let remaining: u64 = remaining
                    .try_into()
                    .map_err(|_| ChannelSessionError::InvalidResponse)?;
                let deadline = (started + Duration::from_millis(remaining)).min(physical_deadline);
                if deadline <= Instant::now() {
                    return Err(ChannelSessionError::AuthorityExpired);
                }
                windows.push(Some(RoleLeaseWindow {
                    expires_at_ms: expires,
                    deadline,
                }));
            }
            (403 | 409 | 503, None, None) => windows.push(None),
            _ => return Err(ChannelSessionError::InvalidResponse),
        }
    }
    Ok(windows)
}

fn validate_registration_response(
    request: &ChannelRoleEnrollment,
    response: &ChannelRoleEnrollmentResponse,
) -> Result<(ServiceEndpoint, String, i64), ChannelSessionError> {
    use kish_lingshu_foundation_contract::service_transport::enrollment::ProviderEndpoint;
    let result = match (request, response) {
        (ChannelRoleEnrollment::Call(request), ChannelRoleEnrollmentResponse::Call(response))
            if matches!(request.endpoint, RequestedServiceEndpoint::Zenoh { .. })
                && response.session.node_id == request.node_id
                && response.session.heartbeat_interval_ms == 10_000
                && !response.session.credential.is_empty()
                && response.session.instance.as_ref().is_some_and(|i| {
                    i.instance_id == request.instance.instance_id
                        && Some(&i.generation) == request.instance.generation.as_ref()
                }) =>
        {
            (
                response.endpoint.clone(),
                response.session.generation.clone(),
                response.session.lease_expires_at_ms,
            )
        }
        (
            ChannelRoleEnrollment::Provider(request),
            ChannelRoleEnrollmentResponse::Provider(response),
        ) if matches!(request.endpoint, RequestedProviderEndpoint::Zenoh { .. })
            && response.session.heartbeat_interval_ms == 10_000
            && !response.session.credential.is_empty()
            && response.session.instance.instance_id == request.instance.instance_id
            && Some(&response.session.instance.generation)
                == request.instance.generation.as_ref() =>
        {
            let ProviderEndpoint::Zenoh {
                protocol_version,
                route,
                route_revision,
                lane,
                catalog_digest,
            } = &response.endpoint
            else {
                return Err(ChannelSessionError::InvalidResponse);
            };
            if catalog_digest != &request.catalog_digest {
                return Err(ChannelSessionError::InvalidResponse);
            }
            (
                ServiceEndpoint::Zenoh {
                    protocol_version: *protocol_version,
                    route: route.clone(),
                    route_revision: *route_revision,
                    lanes: vec![lane.clone()],
                },
                response.session.generation.clone(),
                response.session.lease_expires_at_ms,
            )
        }
        (
            ChannelRoleEnrollment::Consumer(request),
            ChannelRoleEnrollmentResponse::Consumer(response),
        ) if matches!(request.endpoint, RequestedServiceEndpoint::Zenoh { .. })
            && response.session.group_key == request.group_key
            && response.session.group_id > 0
            && response.session.lease.node_id == request.node_id
            && response.session.lease.member_id > 0
            && response.session.lease.heartbeat_interval_seconds == 10
            && response.session.lease.lease_seconds == 30
            && response.session.lease.membership_generation > 0
            && !response.session.credential.is_empty()
            && response.session.instance.as_ref().is_some_and(|i| {
                i.instance_id == request.instance.instance_id
                    && Some(&i.generation) == request.instance.generation.as_ref()
            }) =>
        {
            let ServiceEndpoint::Zenoh { route, .. } = &response.endpoint else {
                return Err(ChannelSessionError::InvalidResponse);
            };
            (
                response.endpoint.clone(),
                route.role_generation.as_str().into(),
                response.session.lease.lease_expires_at.timestamp_millis(),
            )
        }
        _ => return Err(ChannelSessionError::InvalidResponse),
    };
    if result.1.is_empty() {
        return Err(ChannelSessionError::InvalidResponse);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn initial_role_lease_accepts_clock_offsets_without_extending_authority() {
        let started = Instant::now();
        let now = 100_000;
        let cap = started + Duration::from_secs(60);
        for offset in [-5_000, -2_271, 0, 2_271, 5_000] {
            let issued = now + offset;
            let expires = issued + 30_000;
            let deadline = initial_role_deadline(expires, issued, now, started, cap).unwrap();
            assert_eq!(
                deadline,
                started + Duration::from_millis((30_000 + offset).min(30_000) as u64),
                "offset={offset}"
            );
        }
        // A shorter lease remains short even when wall time suggests more life.
        assert_eq!(
            initial_role_deadline(now + 7_271, now + 2_271, now, started, cap).unwrap(),
            started + Duration::from_secs(5)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn initial_role_lease_rejects_invalid_signed_timing() {
        let started = Instant::now();
        let now = 100_000;
        let cap = started + Duration::from_secs(60);
        for (issued, expires) in [
            (now + 5_001, now + 35_001),
            (now, now + 30_001),
            // Fits the former local 31s check, but signed duration is excessive.
            (now - 2_271, now + 30_000),
            (now - 30_000, now),
            (now, now),
            (now + 2_271, now + 2_270),
            (-1, now + 1),
            (i64::MIN, i64::MAX),
        ] {
            assert_eq!(
                initial_role_deadline(expires, issued, now, started, cap),
                Err(ChannelSessionError::InvalidResponse),
                "issued={issued}, expires={expires}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn initial_role_lease_keeps_request_start_and_physical_deadlines() {
        let started = Instant::now();
        let now = 100_000;
        let cap = started + Duration::from_secs(60);
        tokio::time::advance(Duration::from_secs(3)).await;
        let deadline = initial_role_deadline(now + 32_271, now + 2_271, now, started, cap).unwrap();
        assert_eq!(deadline - Instant::now(), Duration::from_secs(27));
        let shorter_cap = started + Duration::from_secs(8);
        assert_eq!(
            initial_role_deadline(now + 32_271, now + 2_271, now, started, shorter_cap).unwrap(),
            shorter_cap
        );
        assert_eq!(
            initial_role_deadline(now + 32_271, now + 2_271, now, started, Instant::now()),
            Err(ChannelSessionError::AuthorityExpired)
        );
        tokio::time::advance(Duration::from_secs(28)).await;
        assert_eq!(
            initial_role_deadline(now + 32_271, now + 2_271, now, started, cap),
            Err(ChannelSessionError::AuthorityExpired)
        );
    }

    #[cfg(feature = "event-consumer-zenoh")]
    #[tokio::test]
    async fn rotation_stop_preserves_consumer_work_but_ordinary_stop_cancels() {
        let (stop, mut stopped) = watch::channel(false);
        let handoff = AtomicBool::new(true);
        let work = stop_unless_handoff(&mut stopped, &handoff);
        tokio::pin!(work);
        stop.send_replace(true);
        assert!(tokio::time::timeout(Duration::from_millis(1), &mut work)
            .await
            .is_err());
        let mut ordinary = stop.subscribe();
        tokio::time::timeout(
            Duration::from_secs(1),
            stop_unless_handoff(&mut ordinary, &AtomicBool::new(false)),
        )
        .await
        .unwrap();
    }

    #[test]
    fn proven_declaration_cleanup_releases_only_its_pool_contribution_once() {
        use super::super::observation::{tests::Capture, DeclarationAccounting};
        let capture = Capture::default();
        metrics::with_local_recorder(&capture, || {
            let make = |names: &[&str], observation: &Arc<DeclarationAccounting>| {
                let keys = Arc::new(std::sync::Mutex::new(
                    names.iter().map(|s| s.to_string()).collect(),
                ));
                let slots = Arc::new(tokio::sync::Semaphore::new(1));
                let reservation = DeclarationReservation {
                    keys: keys.clone(),
                    names: names.iter().map(|s| s.to_string()).collect(),
                    permit: Some(slots.clone().try_acquire_owned().unwrap()),
                    observation: observation.reserve(names.len()),
                };
                (reservation, slots, keys)
            };
            let old = DeclarationAccounting::new();
            let new = DeclarationAccounting::new();
            let (mut first, first_slots, first_keys) = make(&["old/invoke", "old/catalog"], &old);
            let (mut second, second_slots, _) = make(&["new/invoke"], &new);
            assert_eq!(
                capture.sum("lingshu_sdk_channel_reserved_roles", None, None),
                2.0
            );
            assert_eq!(
                capture.sum("lingshu_sdk_channel_reserved_declaration_keys", None, None),
                3.0
            );
            first.release();
            first.release();
            drop(first);
            drop(old);
            assert_eq!(first_slots.available_permits(), 1);
            assert!(first_keys.lock().unwrap().is_empty());
            assert_eq!(
                capture.sum("lingshu_sdk_channel_reserved_roles", None, None),
                1.0
            );
            assert_eq!(
                capture.sum("lingshu_sdk_channel_reserved_declaration_keys", None, None),
                1.0
            );
            second.release();
            drop(second);
            drop(new);
            assert_eq!(second_slots.available_permits(), 1);
        });
        assert_eq!(
            capture.sum("lingshu_sdk_channel_reserved_roles", None, None),
            0.0
        );
        assert_eq!(
            capture.sum("lingshu_sdk_channel_reserved_declaration_keys", None, None),
            0.0
        );
        assert_eq!(
            capture.sum(
                "lingshu_sdk_channel_declaration_reservations_retained_total",
                None,
                None
            ),
            0.0
        );
        capture.assert_bounded_labels();
    }

    #[test]
    fn unproven_cleanup_retains_namespace_capacity_and_observation_until_last_pool_owner() {
        use super::super::observation::{tests::Capture, DeclarationAccounting};
        let capture = Capture::default();
        metrics::with_local_recorder(&capture, || {
            let owner = DeclarationAccounting::new();
            let worker_owner = owner.clone();
            let names = vec!["private-route-key".to_owned()];
            let keys = Arc::new(std::sync::Mutex::new(names.iter().cloned().collect()));
            let slots = Arc::new(tokio::sync::Semaphore::new(1));
            let reservation = DeclarationReservation {
                keys: keys.clone(),
                names,
                permit: Some(slots.clone().try_acquire_owned().unwrap()),
                observation: owner.reserve(1),
            };
            drop(reservation);
            assert_eq!(slots.available_permits(), 0);
            assert!(keys.lock().unwrap().contains("private-route-key"));
            drop(owner);
            assert_eq!(
                capture.sum("lingshu_sdk_channel_reserved_roles", None, None),
                1.0
            );
            assert_eq!(
                capture.sum("lingshu_sdk_channel_reserved_declaration_keys", None, None),
                1.0
            );
            assert_eq!(
                capture.sum(
                    "lingshu_sdk_channel_declaration_reservations_retained_total",
                    None,
                    None
                ),
                1.0
            );
            drop(worker_owner);
        });
        assert_eq!(
            capture.sum("lingshu_sdk_channel_reserved_roles", None, None),
            0.0
        );
        assert_eq!(
            capture.sum("lingshu_sdk_channel_reserved_declaration_keys", None, None),
            0.0
        );
        capture.assert_bounded_labels();
    }

    #[tokio::test]
    async fn reserved_control_precedes_business_without_starving_it() {
        let (control_tx, mut control) = mpsc::channel(8);
        let (business_tx, mut business) = mpsc::channel(8);
        for i in 0..6 {
            control_tx.try_send(i).unwrap();
        }
        business_tx.try_send(100).unwrap();
        let mut burst = 0;
        for expected in [0, 1, 2, 3, 100, 4, 5] {
            assert_eq!(
                next_role_query(&mut control, &mut business, &mut burst).await,
                Some(expected)
            );
        }
        assert!(reserved_control_kind(br#"{"kind":"cancel_call"}"#, 32));
        assert!(reserved_control_kind(br#"{"kind":"bind_lane"}"#, 32));
        assert!(!reserved_control_kind(br#"{"kind":"invoke_call"}"#, 32));
        assert!(!reserved_control_kind(br#"{"kind":"cancel_call"}"#, 1));
        assert!(!reserved_control_kind(
            br#"{"kind":"cancel_call","kind":"bind_lane"}"#,
            128
        ));
    }
    use kish_lingshu_foundation_contract::service_transport::enrollment::EnrollmentVersion;
    use kish_lingshu_runtime_contract::provider::{
        ProviderCatalog, ProviderEnrollmentResponseV2, ProviderEnrollmentV2, ProviderSession,
    };

    #[test]
    fn adoption_receipts_preserve_domain_identity_and_bound_replacement_lanes() {
        use kish_lingshu_foundation_contract::service_transport::{
            DataLaneId, InstanceRoute, LaneIdentity,
        };
        let id = |s| RouteIdentity::new(s).unwrap();
        let old = ServiceEndpoint::Zenoh {
            protocol_version: ProtocolVersion::V1,
            route: InstanceRoute {
                deployment: id("dev"),
                application_id: id("app"),
                instance_id: id("sdk"),
                base_generation: id("base"),
                role_generation: id("role"),
            },
            route_revision: 1,
            lanes: vec![LaneIdentity {
                lane: DataLaneId::new(0).unwrap(),
                epoch: id("old"),
            }],
        };
        let mut endpoint = old.clone();
        assert!(validate_role_session_lanes(&old, 1).is_ok());
        assert!(validate_role_session_lanes(&old, 0).is_err());
        let mut two_lanes = old.clone();
        if let ServiceEndpoint::Zenoh { lanes, .. } = &mut two_lanes {
            lanes.push(LaneIdentity {
                lane: DataLaneId::new(1).unwrap(),
                epoch: id("second"),
            });
        }
        assert!(validate_role_session_lanes(&two_lanes, 2).is_ok());
        assert!(matches!(
            validate_role_session_lanes(&two_lanes, 1),
            Err(ChannelSessionError::InvalidResponse)
        ));
        if let ServiceEndpoint::Zenoh {
            route_revision,
            lanes,
            ..
        } = &mut endpoint
        {
            *route_revision = 2;
            lanes[0].epoch = id("new");
        }
        let response = ChannelRoleAdoptionResponse {
            previous_certificate_identity: id("old-certificate"),
            endpoint,
            authorization_issued_at_ms: 1000,
            authorization_expires_at_ms: 20_000,
        };
        let started = Instant::now();
        let cap = started + Duration::from_secs(2);
        assert_eq!(
            validate_adoption_response(&response, &old, &id("old-certificate"), started, cap, 1000)
                .unwrap(),
            cap
        );
        for changed in 0..8 {
            let mut bad = response.clone();
            match changed {
                0 => bad.previous_certificate_identity = id("foreign"),
                1 => {
                    if let ServiceEndpoint::Zenoh { route, .. } = &mut bad.endpoint {
                        route.role_generation = id("foreign");
                    }
                }
                2 => {
                    if let ServiceEndpoint::Zenoh { route_revision, .. } = &mut bad.endpoint {
                        *route_revision = 1;
                    }
                }
                3 => {
                    if let ServiceEndpoint::Zenoh { lanes, .. } = &mut bad.endpoint {
                        lanes[0].epoch = id("old");
                    }
                }
                4 => {
                    if let ServiceEndpoint::Zenoh { lanes, .. } = &mut bad.endpoint {
                        lanes[0].lane = DataLaneId::new(1).unwrap();
                    }
                }
                5 => bad.authorization_expires_at_ms = 31_001,
                6 => bad.authorization_issued_at_ms = 6001,
                _ => bad.authorization_expires_at_ms = 1000,
            }
            assert!(validate_adoption_response(
                &bad,
                &old,
                &id("old-certificate"),
                started,
                cap,
                1000
            )
            .is_err());
        }
        assert_eq!(
            validate_adoption_response(
                &response,
                &old,
                &id("old-certificate"),
                started,
                Instant::now(),
                1000
            ),
            Err(ChannelSessionError::AuthorityExpired)
        );
    }

    #[test]
    fn role_receipt_cannot_change_kind_digest_instance_or_heartbeat_contract() {
        let id = |s| RouteIdentity::new(s).unwrap();
        let instance = kish_lingshu_foundation_contract::ServiceInstanceIdentity {
            instance_id: "sdk".into(),
            generation: "base".into(),
        };
        let request = ChannelRoleEnrollment::Provider(ProviderEnrollmentV2 {
            enrollment_version: EnrollmentVersion::V2,
            instance: kish_lingshu_foundation_contract::ServiceInstanceRegistration {
                instance_id: "sdk".into(),
                incarnation_id: "boot".into(),
                generation: Some("base".into()),
            },
            provider_key: "provider".into(),
            release: "1".into(),
            catalog_digest: "a".repeat(64),
            endpoint: RequestedProviderEndpoint::Zenoh {
                protocol_version: ProtocolVersion::V1,
            },
        });
        let response = ProviderEnrollmentResponseV2 {
            enrollment_version: EnrollmentVersion::V2,
            session: ProviderSession { instance, generation: "role".into(), credential: "opaque".into(), lease_expires_at_ms: chrono::Utc::now().timestamp_millis()+30_000, heartbeat_interval_ms: 10_000 },
            endpoint: kish_lingshu_foundation_contract::service_transport::enrollment::ProviderEndpoint::Zenoh {
                protocol_version: ProtocolVersion::V1,
                route: kish_lingshu_foundation_contract::service_transport::InstanceRoute { deployment: id("dev"), application_id: id("app"), instance_id: id("sdk"), base_generation: id("base"), role_generation: id("role") },
                route_revision: 1, lane: kish_lingshu_foundation_contract::service_transport::LaneIdentity { lane: kish_lingshu_foundation_contract::service_transport::DataLaneId::new(0).unwrap(), epoch: id("epoch") }, catalog_digest: "a".repeat(64),
            },
        };
        assert!(validate_registration_response(
            &request,
            &ChannelRoleEnrollmentResponse::Provider(response.clone())
        )
        .is_ok());
        let mut changed = response.clone();
        changed.session.instance.generation = "foreign".into();
        assert!(validate_registration_response(
            &request,
            &ChannelRoleEnrollmentResponse::Provider(changed)
        )
        .is_err());
        let mut changed = response.clone();
        changed.session.heartbeat_interval_ms = 20_000;
        assert!(validate_registration_response(
            &request,
            &ChannelRoleEnrollmentResponse::Provider(changed)
        )
        .is_err());
        let mut changed = response.clone();
        if let kish_lingshu_foundation_contract::service_transport::enrollment::ProviderEndpoint::Zenoh { catalog_digest, .. } = &mut changed.endpoint { *catalog_digest = "b".repeat(64); }
        assert!(validate_registration_response(
            &request,
            &ChannelRoleEnrollmentResponse::Provider(changed)
        )
        .is_err());
        let wrong = ChannelRoleEnrollment::Consumer(
            kish_lingshu_event_dispatch_contract::ConsumerEnrollmentRequestV2 {
                enrollment_version: EnrollmentVersion::V2,
                instance: request.instance().clone(),
                group_key: "g".into(),
                node_id: "n".into(),
                maximum_in_flight: 1,
                endpoint: RequestedServiceEndpoint::Zenoh {
                    protocol_version: ProtocolVersion::V1,
                    lane_count: Default::default(),
                },
            },
        );
        assert!(validate_registration_response(
            &wrong,
            &ChannelRoleEnrollmentResponse::Provider(response)
        )
        .is_err());
    }

    #[test]
    fn renewal_receipts_reject_wrong_scope_results_and_unbounded_deadlines_atomically() {
        let ids = vec!["one".to_owned(), "two".to_owned()];
        let now = 1000;
        let started = Instant::now();
        let deadline = started + Duration::from_secs(30);
        let accepted = ChannelRoleRenewalResult {
            role_generation: "one".into(),
            status: 200,
            lease_expires_at_ms: Some(now + 30_000),
            authorization_expires_at_ms: Some(now + 20_000),
        };
        let rejected = ChannelRoleRenewalResult {
            role_generation: "two".into(),
            status: 503,
            lease_expires_at_ms: None,
            authorization_expires_at_ms: None,
        };
        let response = ChannelRoleRenewalResponse {
            renewal_issued_at_ms: now,
            roles: vec![accepted.clone(), rejected],
        };
        assert_eq!(
            validate_renewal_response(&response, &ids, started, now, deadline, now + 300_000)
                .unwrap()
                .len(),
            2
        );
        let mut skewed = response.clone();
        skewed.renewal_issued_at_ms = now + 500;
        skewed.roles[0].lease_expires_at_ms = Some(now + 30_500);
        assert!(
            validate_renewal_response(&skewed, &ids, started, now, deadline, now + 300_000).is_ok()
        );
        skewed.renewal_issued_at_ms = now + 5001;
        assert!(
            validate_renewal_response(&skewed, &ids, started, now, deadline, now + 300_000)
                .is_err()
        );
        let mut invalid = response.clone();
        invalid.roles.pop();
        assert!(
            validate_renewal_response(&invalid, &ids, started, now, deadline, now + 300_000)
                .is_err()
        );
        invalid = response.clone();
        invalid.roles[1].role_generation = "one".into();
        assert!(
            validate_renewal_response(&invalid, &ids, started, now, deadline, now + 300_000)
                .is_err()
        );
        for bad in [
            ChannelRoleRenewalResult {
                authorization_expires_at_ms: Some(now),
                ..accepted.clone()
            },
            ChannelRoleRenewalResult {
                authorization_expires_at_ms: Some(now + 31_000),
                ..accepted.clone()
            },
            ChannelRoleRenewalResult {
                lease_expires_at_ms: Some(now + 31_000),
                ..accepted.clone()
            },
            ChannelRoleRenewalResult {
                status: 503,
                ..accepted.clone()
            },
            ChannelRoleRenewalResult {
                status: 200,
                lease_expires_at_ms: None,
                ..accepted.clone()
            },
        ] {
            invalid = response.clone();
            invalid.roles[0] = bad;
            assert!(validate_renewal_response(
                &invalid,
                &ids,
                started,
                now,
                deadline,
                now + 300_000
            )
            .is_err());
        }
        assert!(
            validate_renewal_response(&response, &ids, started, now, deadline, now + 10_000)
                .is_err()
        );
        assert!(
            validate_renewal_response(&response, &ids, started, now, started, now + 300_000)
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "isolated native host; run zenss_channel_bootstrap_acceptance.py --role-test"]
    async fn native_role_renewal_preserves_identity_and_does_not_revive_omitted_roles() {
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let connection = crate::ServiceConnection::connect(
            &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
            crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
        let registration = kish_lingshu_foundation_contract::ServiceInstanceRegistration {
            instance_id: "native-renewal".into(),
            incarnation_id: "renewal-boot".into(),
            generation: None,
        };
        let identity = connection
            .bootstrap_test_channel(
                registration.clone(),
                Some(RouteIdentity::new("dev").unwrap()),
            )
            .await
            .unwrap();
        let base = identity.bootstrap_response().instance.clone();
        let mut pool = identity
            .open_sessions(super::super::ChannelSessionConfig::default())
            .await
            .unwrap();
        let mut catalog = ProviderCatalog {
            format_version: 1,
            application_id: app,
            provider_key: "native-renewed".into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![],
        };
        let mut role = pool.register_provider_role(&catalog).await.unwrap();
        catalog.provider_key = "native-omitted".into();
        let mut omitted = pool.register_provider_role(&catalog).await.unwrap();
        let original_endpoint = role.endpoint().clone();
        let ServiceEndpoint::Zenoh { route, .. } = &original_endpoint else {
            panic!("not native");
        };
        let generation = route.role_generation.as_str().to_owned();
        let ServiceEndpoint::Zenoh { route, .. } = omitted.endpoint() else {
            panic!("not native");
        };
        let omitted_generation = route.role_generation.as_str().to_owned();
        let original_route_deadline = role.role_deadline;
        let foreign = connection
            .bootstrap_test_channel(registration, Some(RouteIdentity::new("dev").unwrap()))
            .await
            .unwrap();
        let mut foreign = foreign
            .open_sessions(super::super::ChannelSessionConfig::default())
            .await
            .unwrap();
        assert!(matches!(
            foreign.renew_role_leases(&mut [&mut role]).await,
            Err(ChannelSessionError::InvalidConfig)
        ));
        let response = foreign
            .role_control(
                MessageKind::RenewRoles,
                &ChannelRoleRenewal {
                    role_generations: vec![generation.clone()],
                },
            )
            .await
            .unwrap();
        let response: ChannelRoleRenewalResponse =
            serde_json::from_str(response.payload.get()).unwrap();
        assert_eq!(response.roles[0].status, 403);
        assert!(role.route_confirmed());
        foreign.close().await.unwrap();
        let started = Instant::now();
        for tick in 1..=4 {
            tokio::time::sleep_until(started + Duration::from_secs(tick * 10)).await;
            pool.refresh_authorization().await.unwrap();
            let previous_route_deadline = role.role_deadline;
            let response = pool.renew_role_leases(&mut [&mut role]).await.unwrap();
            assert_eq!(response[0].status, 200);
            assert_eq!(role.role_deadline, previous_route_deadline);
            assert_eq!(role.endpoint(), &original_endpoint);
            // Renewing authority alone must not extend routing readiness.
            if tick == 4 {
                assert!(Instant::now() > original_route_deadline);
            }
            pool.confirm_role_route(&mut role).await.unwrap();
            assert!(role.route_confirmed());
        }
        assert!(!omitted.route_confirmed());
        let response = pool
            .role_control(
                MessageKind::RenewRoles,
                &ChannelRoleRenewal {
                    role_generations: vec![generation.clone(), omitted_generation.clone()],
                },
            )
            .await
            .unwrap();
        let response: ChannelRoleRenewalResponse =
            serde_json::from_str(response.payload.get()).unwrap();
        assert_eq!(response.roles[0].status, 200);
        assert_eq!(response.roles[1].status, 409);
        assert!(response.roles[1].authorization_expires_at_ms.is_none());
        assert!(pool
            .role_control(
                MessageKind::BindLane,
                &ChannelRoleRouteConfirmation {
                    role_generation: omitted_generation,
                    route_revision: 1
                }
            )
            .await
            .is_err());
        assert_eq!(pool.identity.bootstrap_response().instance, base);
        pool.deregister_role(&mut role).await.unwrap();
        let response = pool
            .role_control(
                MessageKind::RenewRoles,
                &ChannelRoleRenewal {
                    role_generations: vec![generation],
                },
            )
            .await
            .unwrap();
        let response: ChannelRoleRenewalResponse =
            serde_json::from_str(response.payload.get()).unwrap();
        assert_eq!(response.roles[0].status, 409);
        omitted.close().await.unwrap();
        assert_eq!(pool.role_slots.available_permits(), 1024);
        pool.close().await.unwrap();
        connection.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "isolated native host; run zenss_channel_bootstrap_acceptance.py --role-test"]
    async fn native_role_expiry_withdraws_declarations_without_resetting_logical_identity() {
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let connection = crate::ServiceConnection::connect(
            &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
            crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
        let identity = connection
            .bootstrap_test_channel(
                kish_lingshu_foundation_contract::ServiceInstanceRegistration {
                    instance_id: "native-role-expiry".into(),
                    incarnation_id: "expiry-boot".into(),
                    generation: None,
                },
                Some(RouteIdentity::new("dev").unwrap()),
            )
            .await
            .unwrap();
        let base = identity.bootstrap_response().instance.clone();
        let mut pool = identity
            .open_sessions(super::super::ChannelSessionConfig::default())
            .await
            .unwrap();
        let mut role = pool
            .register_provider_role(&ProviderCatalog {
                format_version: 1,
                application_id: app,
                provider_key: "expiry-source".into(),
                release: "1".into(),
                services: None,
                events: None,
                workflows: vec![],
            })
            .await
            .unwrap();
        assert!(role.route_confirmed());
        // Use the actual finite native grant, with no timer injection or fake
        // authority extension. Cleanup may finish after the deadline itself.
        tokio::time::sleep_until(role.role_deadline + Duration::from_millis(200)).await;
        assert!(!role.route_confirmed());
        tokio::time::timeout(Duration::from_secs(6), role.close())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pool.role_slots.available_permits(), 1024);
        assert!(pool.role_keys.lock().unwrap().is_empty());
        assert!(connection.subscribe_closed().borrow().is_none());
        assert_eq!(pool.identity.bootstrap_response().instance, base);
        pool.close().await.unwrap();
        connection.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "licensed Host resource component; run zenss_host_resource_acceptance.py"]
    async fn licensed_host_large_catalog_and_control_reclaim_over_repeated_rounds() {
        use kish_lingshu_runtime_contract::provider::{
            ProviderPlan, ProviderPreviewRequest, ProviderWorkflow, MAX_PROVIDER_CATALOG_BYTES,
        };
        use std::collections::BTreeMap;
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
        let token = std::env::var("LINGSHU_CHANNEL_TEST_PREVIEW_TOKEN").unwrap();
        let rounds: usize = std::env::var("LINGSHU_RESOURCE_ROUNDS")
            .unwrap()
            .parse()
            .unwrap();
        assert!((6..=20).contains(&rounds));
        let connection = crate::ServiceConnection::connect(
            &url,
            crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
        let identity = connection
            .bootstrap_test_channel(
                kish_lingshu_foundation_contract::ServiceInstanceRegistration {
                    instance_id: "native-resource-component".into(),
                    incarnation_id: "finite-pressure".into(),
                    generation: None,
                },
                Some(RouteIdentity::new("dev").unwrap()),
            )
            .await
            .unwrap();
        let mut pool = identity
            .open_sessions(super::super::ChannelSessionConfig::host_test())
            .await
            .unwrap();
        let catalog = ProviderCatalog {
            format_version: 1,
            application_id: app.clone(),
            provider_key: "native-resource-source".into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![ProviderWorkflow {
                key: "resource-workflow".into(),
                name: "Bounded catalog".into(),
                description: None,
                define_schema: BTreeMap::from([
                    ("type".into(), serde_json::json!("ReactFlow")),
                    (
                        "config".into(),
                        serde_json::json!({"reactflow": {
                            "nodes": [
                                {"id":"start","type":"startEvent","data":{"id":"start","type":"startEvent","name":"Start","trigger":{"type":"manual"}}},
                                {"id":"end","type":"endEvent","data":{"id":"end","type":"endEvent","name":"End"}}
                            ],
                            "edges": [{"id":"start-end","source":"start","target":"end"}]
                        }}),
                    ),
                    (
                        "resource_fixture_padding".into(),
                        serde_json::json!("x".repeat(MAX_PROVIDER_CATALOG_BYTES - 4096)),
                    ),
                ]),
            }],
        };
        let catalog_bytes = serde_json::to_vec(&catalog).unwrap().len();
        assert!(
            catalog_bytes > MAX_PROVIDER_CATALOG_BYTES - 8192
                && catalog_bytes <= MAX_PROVIDER_CATALOG_BYTES
        );
        let mut role = pool.register_provider_role(&catalog).await.unwrap();
        let ChannelRoleEnrollmentResponse::Provider(response) = role.registration() else {
            panic!("wrong role")
        };
        let preview = ProviderPreviewRequest {
            instance_id: response.session.instance.instance_id.clone(),
            generation: response.session.generation.clone(),
            catalog_digest: catalog.digest().unwrap(),
            environment_bindings: BTreeMap::new(),
            workflow_bindings: BTreeMap::new(),
        };
        let client = reqwest::Client::new();
        let markers =
            std::path::PathBuf::from(std::env::var("LINGSHU_RESOURCE_MARKER_DIR").unwrap());
        for round in 0..rounds {
            let started = Instant::now();
            let result: serde_json::Value = client
                .post(format!(
                    "{url}/api/admin/apps/{app}/providers/{}/preview",
                    catalog.provider_key
                ))
                .header("x-token", &token)
                .header("x-kish-app-id", &app)
                .json(&preview)
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap()
                .json()
                .await
                .unwrap();
            assert_eq!(result["status"], true, "{result:?}");
            let _: ProviderPlan = serde_json::from_value(result["data"].clone()).unwrap();
            let preview_ms = started.elapsed().as_millis();
            // Real licensed RX/storage pressure on every physical TLS Session.
            // Invalid authority is rejected before business admission. Large
            // native query parameters still exercise complete reassembly; a
            // payload-only limiter cannot substitute for this workload.
            let parameter_bytes = 20 * 1024 * 1024;
            let padding: Arc<str> = "x".repeat(parameter_bytes).into();
            let target = pool
                .identity
                .bootstrap_response()
                .control_route
                .key()
                .unwrap();
            let mut probes = tokio::task::JoinSet::new();
            for session in &pool.sessions {
                let session = session.clone();
                let padding = padding.clone();
                let target = target.clone();
                probes.spawn(async move {
                    let replies = session
                        .get(format!("{}?pad={padding}", target.as_str()))
                        .payload("pre-decode-resource-probe")
                        .timeout(Duration::from_secs(1))
                        .await
                        .unwrap();
                    while let Ok(reply) = replies.recv_async().await {
                        assert!(
                            reply.result().is_err(),
                            "unproved metadata probe became a business response"
                        );
                    }
                });
            }
            while let Some(result) = probes.join_next().await {
                result.unwrap();
            }
            let control = Instant::now();
            pool.renew_role_leases(&mut [&mut role]).await.unwrap();
            assert!(role.route_confirmed());
            let row = serde_json::json!({"round":round, "lanes":pool.lane_count(), "catalog_bytes":catalog_bytes, "rx_parameter_bytes_per_lane":parameter_bytes, "preview_ms":preview_ms, "control_ms":control.elapsed().as_millis()});
            std::fs::write(markers.join(format!("round-{round}.json")), row.to_string()).unwrap();
            eprintln!("HOST_PRESSURE_ROUND {row}");
            // This transport workload keeps the amount of durable governance
            // state fixed. The lab harness removes only this unused preview;
            // production imports and their recovery records are never touched.
            tokio::time::timeout(Duration::from_secs(5), async {
                while !markers.join(format!("reclaimed-{round}")).exists() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            // Separate successive workload rounds so isolated PID sampling can
            // observe warmed allocator storage; this delay is not a benchmark.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        pool.deregister_role(&mut role).await.unwrap();
        role.close().await.unwrap();
        assert_eq!(pool.role_slots.available_permits(), 1024);
        assert!(pool.role_keys.lock().unwrap().is_empty());
        pool.close().await.unwrap();
        connection.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "isolated native host; run zenss_channel_bootstrap_acceptance.py --role-test"]
    async fn native_provider_role_confirms_exact_route_and_rejects_duplicate_or_foreign_scope() {
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let connection = crate::ServiceConnection::connect(
            &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
            crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
        let registration = kish_lingshu_foundation_contract::ServiceInstanceRegistration {
            instance_id: "native-provider".into(),
            incarnation_id: "provider-boot".into(),
            generation: None,
        };
        let identity = connection
            .bootstrap_test_channel(
                registration.clone(),
                Some(RouteIdentity::new("dev").unwrap()),
            )
            .await
            .unwrap();
        let base = identity.bootstrap_response().instance.clone();
        let mut pool = identity
            .open_sessions(super::super::ChannelSessionConfig::default())
            .await
            .unwrap();
        let catalog = ProviderCatalog {
            format_version: 1,
            application_id: app.clone(),
            provider_key: "native-source".into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![],
        };
        let mut role = pool.register_provider_role(&catalog).await.unwrap();
        assert!(role.route_confirmed());
        let ChannelRoleEnrollmentResponse::Provider(response) = role.registration() else {
            panic!("wrong role")
        };
        assert_eq!(response.session.instance, base);
        let generation = response.session.generation.clone();
        let expires = response.session.lease_expires_at_ms;
        let foreign_identity = connection
            .bootstrap_test_channel(
                registration.clone(),
                Some(RouteIdentity::new("dev").unwrap()),
            )
            .await
            .unwrap();
        let mut foreign_pool = foreign_identity
            .open_sessions(super::super::ChannelSessionConfig::default())
            .await
            .unwrap();
        assert!(matches!(
            foreign_pool.deregister_role(&mut role).await,
            Err(ChannelSessionError::InvalidConfig)
        ));
        assert!(role.route_confirmed());
        // Even a correctly signed request from another certificate sharing the
        // same base cannot withdraw the original certificate's role.
        assert!(foreign_pool
            .role_control(
                MessageKind::Deregister,
                &ChannelRoleDeregistration {
                    role_generation: generation.clone(),
                }
            )
            .await
            .is_err());
        foreign_pool.close().await.unwrap();
        #[cfg(feature = "service-manifest")]
        let mut call = {
            let manifest: crate::services::ServiceManifest =
                serde_json::from_str(&std::env::var("LINGSHU_CHANNEL_TEST_MANIFEST").unwrap())
                    .unwrap();
            let mut builder = crate::services::ServiceRegistryBuilder::new(manifest).unwrap();
            let executions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let observed = executions.clone();
            builder
                .bind::<serde_json::Value, serde_json::Value, _, _>(
                    "native-fixture",
                    "echo",
                    "1",
                    move |_, value| {
                        observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        async move { Ok(value) }
                    },
                )
                .unwrap();
            let registry = builder.build().unwrap();
            let call = pool
                .register_service_role("native-call", 1, &registry)
                .await
                .unwrap();
            assert!(call.route_confirmed());
            assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert!(role.route_confirmed());
            let ChannelRoleEnrollmentResponse::Call(response) = call.registration() else {
                panic!("wrong role")
            };
            assert_eq!(response.session.instance.as_ref(), Some(&base));
            call
        };
        // Duplicate declaration is refused before local/native I/O.
        let mut unavailable_lane = role.endpoint().clone();
        if let ServiceEndpoint::Zenoh { lanes, .. } = &mut unavailable_lane {
            if pool.lane_count() < 4 {
                let template = lanes[0].clone();
                *lanes = (0..=pool.lane_count())
                    .map(|index| {
                        let mut lane = template.clone();
                        lane.lane =
                            kish_lingshu_foundation_contract::service_transport::DataLaneId::new(
                                index as u8,
                            )
                            .unwrap();
                        lane
                    })
                    .collect();
            }
        }
        if pool.lane_count() < 4 {
            let before = pool.role_slots.available_permits();
            assert!(matches!(
                pool.install_role_declarations(
                    &unavailable_lane,
                    &generation,
                    expires,
                    role.role_deadline,
                    None,
                    false
                )
                .await,
                Err(ChannelSessionError::InvalidResponse)
            ));
            assert_eq!(pool.role_slots.available_permits(), before);
            assert!(role.route_confirmed());
        }
        assert!(matches!(
            pool.install_role_declarations(
                role.endpoint(),
                &generation,
                expires,
                role.role_deadline,
                None,
                false
            )
            .await,
            Err(ChannelSessionError::InvalidConfig)
        ));
        let mut request = registration;
        request.generation = Some("foreign-base".into());
        assert!(matches!(
            pool.register_role(ChannelRoleEnrollment::Provider(ProviderEnrollmentV2 {
                enrollment_version: EnrollmentVersion::V2,
                instance: request,
                provider_key: "native-source".into(),
                release: "1".into(),
                catalog_digest: catalog.digest().unwrap(),
                endpoint: RequestedProviderEndpoint::Zenoh {
                    protocol_version: ProtocolVersion::V1
                },
            }))
            .await,
            Err(ChannelSessionError::InvalidConfig)
        ));
        pool.deregister_role(&mut role).await.unwrap();
        pool.deregister_role(&mut role).await.unwrap();
        assert!(role.remote_deregistered);
        #[cfg(feature = "service-manifest")]
        {
            // Fresh explicit repeat is idempotent while the sibling retains
            // common-base authority. A retired role can never become ready.
            pool.role_control(
                MessageKind::Deregister,
                &ChannelRoleDeregistration {
                    role_generation: generation.clone(),
                },
            )
            .await
            .unwrap();
            assert!(pool
                .role_control(
                    MessageKind::BindLane,
                    &ChannelRoleRouteConfirmation {
                        role_generation: generation,
                        route_revision: 1,
                    }
                )
                .await
                .is_err());
            assert!(call.route_confirmed());
        }
        #[cfg(feature = "service-manifest")]
        call.close().await.unwrap();
        assert!(!role.route_confirmed());
        assert_eq!(pool.role_slots.available_permits(), 1024);
        assert!(pool.role_keys.lock().unwrap().is_empty());
        assert!(connection.subscribe_closed().borrow().is_none());
        assert_eq!(pool.identity.bootstrap_response().instance, base);
        let mut sibling = catalog;
        sibling.provider_key = "native-sibling".into();
        let mut sibling = pool.register_provider_role(&sibling).await.unwrap();
        assert!(sibling.route_confirmed());
        // Closing the shared logical connection must withdraw every surviving
        // role declaration, even when its own finite lease has not expired.
        connection.shutdown().await;
        sibling.close().await.unwrap();
        assert!(!sibling.route_confirmed());
        assert_eq!(pool.role_slots.available_permits(), 1024);
        assert!(pool.role_keys.lock().unwrap().is_empty());
        pool.close().await.unwrap();
    }
}

fn route_catalog_target(
    endpoint: &ServiceEndpoint,
    snapshot: &super::catalog::CatalogSnapshot,
    target: &ExactRouteKey,
) -> bool {
    matches!(endpoint, ServiceEndpoint::Zenoh { route, .. } if route.catalog_key(&snapshot.digest).as_ref() == Ok(target))
}

#[cfg(all(test, feature = "service-call-zenoh"))]
#[path = "sync_role_tests.rs"]
mod sync_role_tests;
