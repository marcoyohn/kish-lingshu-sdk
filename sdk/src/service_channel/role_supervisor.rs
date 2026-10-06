//! One bounded serial owner for enrolled roles and explicit role mutations.
use super::{
    role_changes::{RoleCommand, RoleCommands, RoleMutation, MAX_ROLE_COMMANDS},
    supervisor::AuthorizationOwner,
    ChannelCloseReason, ChannelSessionError, ChannelSupervisorStatus, ManagedServiceChannel,
    RegisteredChannelRole, ServiceChannelSessions,
};
use kish_lingshu_foundation_contract::service_transport::RouteIdentity;
use kish_lingshu_runtime_contract::service::MAX_CHANNEL_RENEWAL_ROLES;
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

const MAX_MANAGED_ROLES: usize = 1024;
const CYCLE_BUDGET: Duration = Duration::from_secs(10);

/// Opt-in rotation interval; the same ten-second role scheduler drives it.
/// A candidate is never automatically retried after an uncertain transfer.
#[derive(Debug, Clone, Copy)]
pub struct ChannelCertificateRotationConfig {
    after: Duration,
}
impl Default for ChannelCertificateRotationConfig {
    fn default() -> Self {
        Self {
            after: Duration::from_secs(240),
        }
    }
}
impl ChannelCertificateRotationConfig {
    pub fn new(after: Duration) -> Result<Self, ChannelSessionError> {
        if !(Duration::from_secs(20)..=Duration::from_secs(240)).contains(&after) {
            return Err(ChannelSessionError::InvalidConfig);
        }
        Ok(Self { after })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertificateRotationPhase {
    Disabled,
    Scheduled,
    InProgress,
    Stopped,
    Failed,
}
/// Finite transport evidence, not business execution readiness. Session IDs
/// are diagnostic only and cannot identify an instance or authorize a route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertificateRotationStatus {
    pub phase: CertificateRotationPhase,
    pub certificate_identity: RouteIdentity,
    pub session_ids: Vec<String>,
    pub scheduled_at: Option<Instant>,
    pub completed: u64,
    pub last_error: Option<ChannelSessionError>,
}
fn rotation_due(
    after: Duration,
    now: Instant,
    certificate_remaining: Duration,
) -> Result<Instant, ChannelSessionError> {
    // Leave one full scheduler interval/cycle and bounded cleanup headroom.
    let available = certificate_remaining
        .checked_sub(Duration::from_secs(40))
        .ok_or(ChannelSessionError::AuthorityExpired)?;
    if available.is_zero() {
        return Err(ChannelSessionError::AuthorityExpired);
    }
    Ok(now + after.min(available))
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleLifecycleState {
    Active,
    /// Admission stopped or authority expired; consult terminal channel status
    /// to distinguish requested shutdown from proven native cleanup.
    Stopped,
    CleanupFailed,
}

/// Coalesced finite evidence, never business execution readiness. A retained
/// snapshot cannot extend authority: always check its deadlines at use time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRoleStatus {
    pub role_generation: String,
    pub state: RoleLifecycleState,
    pub authorization_deadline: Instant,
    pub route_deadline: Instant,
    pub last_renewal_status: Option<u16>,
    pub last_error: Option<ChannelSessionError>,
    pub remote_deregistered: bool,
}
impl ChannelRoleStatus {
    pub fn authorized(&self) -> bool {
        self.state == RoleLifecycleState::Active && Instant::now() < self.authorization_deadline
    }
    pub fn route_confirmed(&self) -> bool {
        self.authorized() && Instant::now() < self.route_deadline
    }
}

/// One serial supervisor owns physical observations, role renewal and fresh
/// probing. No second reconnect loop, Handler execution or registration replay.
/// Explicit additions/removals share its finite command queue and scheduler.
/// Close stops every declaration before one bounded remote drain pass. Remote
/// failures remain in role status; `close` proves local cleanup only.
pub struct ManagedRoleChannel {
    supervisor: ManagedServiceChannel,
    pub(super) commands: RoleCommands,
    pub(super) application_id: String,
    roles: watch::Receiver<Vec<ChannelRoleStatus>>,
    rotation: watch::Receiver<CertificateRotationStatus>,
    connectivity: watch::Receiver<super::ChannelConnectivityStatus>,
    #[cfg(feature = "event-publication-zenoh")]
    pub(crate) publication: super::publication::NativePublicationSource,
}
impl std::fmt::Debug for ManagedRoleChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedRoleChannel")
            .field("status", &self.status())
            .field("role_count", &self.roles.borrow().len())
            .finish_non_exhaustive()
    }
}
impl ManagedRoleChannel {
    pub fn status(&self) -> ChannelSupervisorStatus {
        self.supervisor.status()
    }
    pub fn subscribe_status(&self) -> watch::Receiver<ChannelSupervisorStatus> {
        self.supervisor.subscribe_status()
    }
    pub fn role_statuses(&self) -> Vec<ChannelRoleStatus> {
        let mut roles = self.roles.borrow().clone();
        if matches!(self.status(), ChannelSupervisorStatus::CleanupFailed) {
            for role in &mut roles {
                role.state = RoleLifecycleState::CleanupFailed;
            }
        }
        roles
    }
    /// Notifications coalesce. Expiry is also encoded in each snapshot; a slow
    /// control cycle can delay the expiry notification by its finite budget.
    pub fn subscribe_roles(&self) -> watch::Receiver<Vec<ChannelRoleStatus>> {
        self.roles.clone()
    }
    pub fn rotation_status(&self) -> CertificateRotationStatus {
        let mut status = self.rotation.borrow().clone();
        if matches!(self.status(), ChannelSupervisorStatus::CleanupFailed) {
            status.phase = CertificateRotationPhase::Failed;
            status.scheduled_at = None;
            status.last_error = Some(ChannelSessionError::CleanupFailed);
        }
        status
    }
    pub fn subscribe_rotation(&self) -> watch::Receiver<CertificateRotationStatus> {
        self.rotation.clone()
    }
    /// Follows the current pool across certificate handoffs. Link observations
    /// never replace the role-specific signed route confirmation.
    pub fn subscribe_connectivity(&self) -> watch::Receiver<super::ChannelConnectivityStatus> {
        self.connectivity.clone()
    }
    pub fn connectivity_status(&self) -> super::ChannelConnectivityStatus {
        self.connectivity.borrow().clone()
    }
    /// Cancellation retains the supervisor handle. Repeated calls retain any
    /// local cleanup failure. Inspect remote_deregistered separately.
    pub async fn close(&mut self) -> Result<(), ChannelSessionError> {
        self.supervisor.close().await
    }
}
impl ServiceChannelSessions {
    /// Opt into renewal of 1..1024 already enrolled roles on this exact pool.
    /// The authorization-only `manage()` retains its existing behavior.
    pub fn manage_roles(
        self,
        roles: Vec<RegisteredChannelRole>,
    ) -> Result<ManagedRoleChannel, ChannelSessionError> {
        self.manage_role_owner(roles, None)
    }
    /// Rotate the current Call/Provider/Consumer set on one serial owner. Pools
    /// with 1..2 Sessions overlap; larger pools drain and close before replacement
    /// within the existing four-Session logical budget. Partial failure is terminal;
    /// generations are never cleared, re-enrolled or automatically retried.
    pub fn manage_roles_with_rotation(
        self,
        roles: Vec<RegisteredChannelRole>,
        config: ChannelCertificateRotationConfig,
    ) -> Result<ManagedRoleChannel, ChannelSessionError> {
        self.manage_role_owner(roles, Some(config))
    }
    fn manage_role_owner(
        self,
        roles: Vec<RegisteredChannelRole>,
        rotation_config: Option<ChannelCertificateRotationConfig>,
    ) -> Result<ManagedRoleChannel, ChannelSessionError> {
        if !tokio::runtime::Handle::try_current().is_ok_and(|handle| {
            handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
        }) {
            return Err(ChannelSessionError::UnsupportedRuntime);
        }
        if roles.is_empty() || roles.len() > MAX_MANAGED_ROLES {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let mut ids = BTreeSet::new();
        let mut initial = Vec::with_capacity(roles.len());
        for role in &roles {
            self.active_role(role)?;
            let status = role.lifecycle_status(self.authorization_deadline());
            if !ids.insert(status.role_generation.clone()) {
                return Err(ChannelSessionError::InvalidConfig);
            }
            initial.push(status);
        }
        if rotation_config.is_some() {
            if self.identity.rotation_predecessor().is_some() && !self.rotation_finalized {
                return Err(ChannelSessionError::InvalidConfig);
            }
            if self.session_config().session_count() <= 2
                && self
                    .identity
                    .connection
                    .channel_session_budget()
                    .available_permits()
                    < self.session_config().session_count()
            {
                return Err(ChannelSessionError::CapacityExceeded);
            }
        }
        let scheduled_at = rotation_config
            .map(|config| pool_rotation_due(&self, config))
            .transpose()?;
        let (rotation_sender, rotation_receiver) = watch::channel(CertificateRotationStatus {
            phase: if rotation_config.is_some() {
                CertificateRotationPhase::Scheduled
            } else {
                CertificateRotationPhase::Disabled
            },
            certificate_identity: self
                .identity
                .bootstrap_response()
                .certificate
                .certificate_identity
                .clone(),
            session_ids: self.session_ids(),
            scheduled_at,
            completed: 0,
            last_error: None,
        });
        let (sender, receiver) = watch::channel(initial.clone());
        let pool_connectivity = self.subscribe_connectivity();
        let (connectivity_sender, connectivity_receiver) =
            watch::channel(pool_connectivity.borrow().clone());
        let application_id = self.identity.connection.application_id().to_owned();
        let (commands, mutations) = tokio::sync::mpsc::channel(MAX_ROLE_COMMANDS);
        #[cfg(feature = "event-publication-zenoh")]
        let (publication_sender, publication_receiver) =
            watch::channel(Some(self.publication_client()?));
        #[cfg(feature = "event-publication-zenoh")]
        let publication_app = self
            .identity
            .bootstrap_response()
            .application_id
            .as_str()
            .to_owned();
        Ok(ManagedRoleChannel {
            supervisor: ManagedServiceChannel::start(RoleOwner {
                pool: self,
                pool_connectivity,
                connectivity: connectivity_sender,
                #[cfg(feature = "event-publication-zenoh")]
                publication: publication_sender,
                roles,
                mutations,
                pending_command: None,
                report: initial,
                sender,
                cursor: 0,
                pending_cycle: BTreeSet::new(),
                rotation_config,
                rotation: rotation_sender,
                opening: None,
                candidate: None,
                retired: None,
                replacing_pool: Arc::new(AtomicBool::new(false)),
                #[cfg(feature = "service-call-zenoh")]
                retiring_calls: Vec::new(),
            }),
            commands: RoleCommands::new(commands),
            application_id,
            roles: receiver,
            rotation: rotation_receiver,
            connectivity: connectivity_receiver,
            #[cfg(feature = "event-publication-zenoh")]
            publication: super::publication::NativePublicationSource {
                app_id: publication_app,
                current: publication_receiver,
            },
        })
    }
}

#[path = "role_mutations.rs"]
mod mutations;

struct RoleOwner {
    pool: ServiceChannelSessions,
    pool_connectivity: watch::Receiver<super::ChannelConnectivityStatus>,
    connectivity: watch::Sender<super::ChannelConnectivityStatus>,
    #[cfg(feature = "event-publication-zenoh")]
    publication: watch::Sender<Option<std::sync::Arc<super::publication::NativePublicationClient>>>,
    roles: Vec<RegisteredChannelRole>,
    mutations: tokio::sync::mpsc::Receiver<RoleCommand>,
    pending_command: Option<RoleCommand>,
    report: Vec<ChannelRoleStatus>,
    sender: watch::Sender<Vec<ChannelRoleStatus>>,
    cursor: usize,
    pending_cycle: BTreeSet<usize>,
    rotation_config: Option<ChannelCertificateRotationConfig>,
    rotation: watch::Sender<CertificateRotationStatus>,
    opening: Option<tokio::task::JoinHandle<Result<ServiceChannelSessions, ChannelSessionError>>>,
    candidate: Option<ServiceChannelSessions>,
    retired: Option<ServiceChannelSessions>,
    replacing_pool: Arc<AtomicBool>,
    #[cfg(feature = "service-call-zenoh")]
    retiring_calls: Vec<std::sync::Arc<super::call::NativeCallExecution>>,
}
async fn join_candidate_open<T>(
    opening: &mut Option<tokio::task::JoinHandle<Result<T, ChannelSessionError>>>,
) -> Result<T, ChannelSessionError> {
    let result = opening
        .as_mut()
        .ok_or(ChannelSessionError::InvalidConfig)?
        .await;
    opening.take();
    result.map_err(|_| ChannelSessionError::CleanupFailed)?
}
fn pool_rotation_due(
    pool: &ServiceChannelSessions,
    config: ChannelCertificateRotationConfig,
) -> Result<Instant, ChannelSessionError> {
    let remaining = pool
        .identity
        .bootstrap_response()
        .certificate
        .expires_unix_ms
        - chrono::Utc::now().timestamp_millis();
    let remaining = u64::try_from(remaining).map_err(|_| ChannelSessionError::AuthorityExpired)?;
    rotation_due(
        config.after,
        Instant::now(),
        Duration::from_millis(remaining.min(300_000)),
    )
}
impl RoleOwner {
    async fn finish_opening(&mut self) -> Result<(), ChannelSessionError> {
        // Retain the opening task when the caller cancels this wait; stop must
        // still join it and own any returned pool before further cleanup I/O.
        self.candidate = Some(join_candidate_open(&mut self.opening).await?);
        Ok(())
    }
    async fn rotate(&mut self) -> Result<(), ChannelSessionError> {
        let identity = self.pool.prepare_certificate_rotation().await?;
        let config = self.pool.session_config();
        if config.session_count() > 2 {
            // Withdraw every role before waiting for any one declaration. Keep
            // original accepted executions/reports on the still-open transport.
            #[cfg(feature = "event-publication-zenoh")]
            self.publication.send_replace(None);
            for role in &mut self.roles {
                role.begin_rotation_drain();
            }
            self.publish();
            for role in &mut self.roles {
                role.close_declarations().await?;
                #[cfg(feature = "service-call-zenoh")]
                {
                    let binding = role
                        .calls
                        .lock()
                        .map_err(|_| ChannelSessionError::InvalidConfig)?
                        .clone();
                    if let Some(binding) = binding {
                        binding.wait_idle().await;
                        if !binding.join_accepted().await {
                            return Err(ChannelSessionError::CleanupFailed);
                        }
                    }
                }
            }
            self.replacing_pool.store(true, Ordering::Release);
            // close joins native cleanup and releases only proven capacity.
            self.pool.close().await?;
        }
        self.opening = Some(tokio::spawn(
            async move { identity.open_sessions(config).await },
        ));
        self.finish_opening().await?;
        #[cfg(feature = "service-call-zenoh")]
        {
            self.retiring_calls = self
                .roles
                .iter()
                .filter_map(|role| role.calls.lock().ok()?.clone())
                .collect();
        }
        for index in 0..self.roles.len() {
            self.roles[index].withdraw_route_confirmation();
            self.publish();
            self.candidate
                .as_ref()
                .unwrap()
                .adopt_role(&mut self.roles[index])
                .await?;
            self.publish();
        }
        self.candidate
            .as_mut()
            .unwrap()
            .finalize_certificate_rotation()
            .await?;
        let config = self.rotation_config.unwrap();
        let candidate = self.candidate.as_ref().unwrap();
        let scheduled_at = pool_rotation_due(candidate, config)?;
        let certificate_identity = candidate
            .identity
            .bootstrap_response()
            .certificate
            .certificate_identity
            .clone();
        let session_ids = candidate.session_ids();
        let completed = self
            .rotation
            .borrow()
            .completed
            .checked_add(1)
            .ok_or(ChannelSessionError::InvalidResponse)?;
        #[cfg(feature = "event-publication-zenoh")]
        let publication = self.candidate.as_ref().unwrap().publication_client()?;
        let old = std::mem::replace(&mut self.pool, self.candidate.take().unwrap());
        self.pool_connectivity = self.pool.subscribe_connectivity();
        #[cfg(feature = "event-publication-zenoh")]
        self.publication.send_replace(Some(publication));
        // Retain ownership before any fallible drain preparation. Cleanup must
        // still join both pools if the old window expired during finalization.
        self.retired = Some(old);
        #[cfg(feature = "service-call-zenoh")]
        if self.retiring_calls.iter().any(|b| b.pending_reports()) {
            let deadline = self
                .retiring_calls
                .iter()
                .filter(|b| b.pending_reports())
                .map(|b| b.report_deadline())
                .max()
                .unwrap();
            self.retired
                .as_mut()
                .unwrap()
                .retain_reports_until(deadline)?;
        }
        // No await after requesting old cleanup: the supervisor must first
        // observe this completed handoff and switch its closure/deadline watch.
        if !self.retired.as_ref().unwrap().report_draining {
            self.retired.as_ref().unwrap().request_close();
        }
        self.rotation.send_replace(CertificateRotationStatus {
            phase: CertificateRotationPhase::Scheduled,
            certificate_identity,
            session_ids,
            scheduled_at: Some(scheduled_at),
            completed,
            last_error: None,
        });
        self.publish();
        Ok(())
    }
    fn publish(&mut self) {
        let connectivity = self.pool_connectivity.borrow().clone();
        self.connectivity.send_if_modified(|current| {
            if *current == connectivity {
                return false;
            }
            *current = connectivity;
            true
        });
        for (role, status) in self.roles.iter().zip(&mut self.report) {
            let current = role.lifecycle_status(self.pool.authorization_deadline());
            status.state = current.state;
            status.authorization_deadline = current.authorization_deadline;
            status.route_deadline = current.route_deadline;
            status.remote_deregistered = current.remote_deregistered;
        }
        self.sender.send_replace(self.report.clone());
    }
    async fn renew_and_confirm(&mut self, plan: &[usize]) {
        let mut renewed = BTreeSet::new();
        // Renew all frames before spending budget on individual fresh proofs.
        // Each frame has <=64 entries and the entire cycle has one time budget.
        for batch in plan.chunks(MAX_CHANNEL_RENEWAL_ROLES) {
            let members: BTreeSet<_> = batch.iter().copied().collect();
            let mut positions = Vec::new();
            let mut handles = Vec::new();
            for (index, role) in self.roles.iter_mut().enumerate() {
                if members.contains(&index) {
                    positions.push(index);
                    handles.push(role);
                }
            }
            for &index in &positions {
                self.report[index].last_renewal_status = None;
                self.report[index].last_error = None;
            }
            match self.pool.renew_role_leases(&mut handles).await {
                Ok(results) => {
                    for (index, result) in positions.into_iter().zip(results) {
                        self.report[index].last_renewal_status = Some(result.status);
                        if result.status == 200 {
                            renewed.insert(index);
                        } else {
                            self.pending_cycle.remove(&index);
                            self.report[index].last_error = Some(if result.status == 503 {
                                ChannelSessionError::Transport
                            } else {
                                ChannelSessionError::AuthorityExpired
                            });
                        }
                    }
                }
                Err(error) => {
                    for index in positions {
                        self.pending_cycle.remove(&index);
                        self.report[index].last_error = Some(error);
                    }
                }
            }
            self.publish();
        }
        for &index in plan {
            if renewed.contains(&index) {
                self.roles[index].withdraw_route_confirmation();
                self.publish();
                self.report[index].last_error = self
                    .pool
                    .confirm_role_route(&mut self.roles[index])
                    .await
                    .err();
                self.pending_cycle.remove(&index);
                self.publish();
            }
        }
    }
}
// Rotate by one role per cycle, including after a timeout. Slow prefixes
// cannot monopolize the control budget. Expired roles are never re-enrolled.
fn renewal_plan(active: &[bool], cursor: &mut usize) -> Vec<usize> {
    if active.is_empty() {
        return Vec::new();
    }
    let start = *cursor % active.len();
    *cursor = (start + 1) % active.len();
    (0..active.len())
        .map(|offset| (start + offset) % active.len())
        .filter(|&index| active[index])
        .collect()
}
impl AuthorizationOwner for RoleOwner {
    fn deadline(&self) -> Instant {
        self.pool.authorization_deadline()
    }
    fn closed(&self) -> watch::Receiver<Option<ChannelCloseReason>> {
        self.pool.subscribe_closed()
    }
    fn replacing_pool(&self) -> Option<Arc<AtomicBool>> {
        Some(self.replacing_pool.clone())
    }
    fn notification_deadline(&self) -> Option<Instant> {
        let now = Instant::now();
        self.report
            .iter()
            .filter(|role| role.state == RoleLifecycleState::Active)
            .flat_map(|role| [role.authorization_deadline, role.route_deadline])
            .filter(|&deadline| deadline > now)
            .min()
    }
    fn notify(&mut self) {
        self.publish();
    }
    async fn changed(&mut self) {
        tokio::select! {
            biased;
            result = self.pool_connectivity.changed() => {
                if result.is_err() { std::future::pending::<()>().await; }
            },
            command = self.mutations.recv() => {
                match command {
                    Some(mut command) => {
                        command.start_processing();
                        self.pending_command = Some(command);
                    },
                    None => std::future::pending().await,
                }
            }
        }
    }
    async fn process_change(&mut self) -> Result<(), ChannelSessionError> {
        self.apply_pending_mutation().await;
        // A reconnect notification only withdraws stale local evidence. The
        // next existing bounded renewal cycle performs fresh signed probes;
        // no immediate control retry or enrollment/business replay is added.
        self.publish();
        Ok(())
    }
    async fn refresh(&mut self) -> Result<(), ChannelSessionError> {
        let deadline = (Instant::now() + CYCLE_BUDGET).min(self.pool.authorization_deadline());
        if let Some(retired) = self.retired.as_mut() {
            #[cfg(feature = "service-call-zenoh")]
            let pending = self.retiring_calls.iter().any(|b| b.pending_reports())
                && retired.closed.borrow().is_none()
                && Instant::now() < retired.authorization_deadline();
            #[cfg(not(feature = "service-call-zenoh"))]
            let pending = false;
            if !pending {
                match tokio::time::timeout_at(deadline, retired.close()).await {
                    Ok(Ok(())) => {
                        self.retired.take();
                        #[cfg(feature = "service-call-zenoh")]
                        self.retiring_calls.clear();
                    }
                    Ok(Err(_)) => return Err(ChannelSessionError::CleanupFailed),
                    Err(_) => return Err(ChannelSessionError::Transport),
                }
            }
        }
        // Failed physical observation leaves roles and their previous windows
        // intact. The outer owner enforces physical expiry and logical closure.
        if let Err(error) = self.pool.refresh_authorization().await {
            self.publish();
            return Err(error);
        }
        let active: Vec<_> = self
            .roles
            .iter()
            .map(|role| self.pool.active_role(role).is_ok())
            .collect();
        let plan = renewal_plan(&active, &mut self.cursor);
        self.pending_cycle = plan.iter().copied().collect();
        if tokio::time::timeout_at(deadline, self.renew_and_confirm(&plan))
            .await
            .is_err()
        {
            for &index in &self.pending_cycle {
                self.report[index].last_error = Some(ChannelSessionError::Transport);
            }
        }
        self.publish();
        #[cfg(feature = "service-call-zenoh")]
        if self.pool.session_config().session_count() > 2
            && self.roles.iter().any(|role| {
                role.calls
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .as_ref()
                    .is_some_and(|b| b.pending_reports())
            })
        {
            // Do not cut off an independently accepted result to free Sessions.
            // This does not extend the certificate or original report deadline.
            return Ok(());
        }
        if self.rotation_config.is_some()
            && self.retired.is_none()
            && self
                .rotation
                .borrow()
                .scheduled_at
                .is_some_and(|due| Instant::now() >= due)
        {
            self.rotation
                .send_modify(|status| status.phase = CertificateRotationPhase::InProgress);
            let result = if self
                .roles
                .iter()
                .all(|role| self.pool.active_role(role).is_ok() && role.route_confirmed())
            {
                tokio::time::timeout_at(deadline, self.rotate())
                    .await
                    .unwrap_or(Err(ChannelSessionError::Transport))
            } else {
                Err(ChannelSessionError::AuthorityExpired)
            };
            if let Err(error) = result {
                self.rotation.send_modify(|status| {
                    status.phase = CertificateRotationPhase::Failed;
                    status.scheduled_at = None;
                    status.last_error = Some(error);
                });
                return Err(ChannelSessionError::RotationFailed);
            }
        }
        // The physical ACK was valid; role failures are reported independently.
        Ok(())
    }
    async fn close(&mut self) -> Result<(), ChannelSessionError> {
        self.mutations.close();
        while let Ok(command) = self.mutations.try_recv() {
            let _ = command.response.send(Err(ChannelSessionError::Closed));
        }
        if let Some(command) = self.pending_command.take() {
            let _ = command.response.send(Err(ChannelSessionError::Closed));
        }
        #[cfg(feature = "event-publication-zenoh")]
        self.publication.send_replace(None);
        for (role, status) in self.roles.iter().zip(&mut self.report) {
            role.request_stop();
            status.last_error = Some(ChannelSessionError::Transport);
        }
        self.publish();
        let mut opening_failed = false;
        if self.opening.is_some() {
            match tokio::time::timeout(Duration::from_secs(18), self.finish_opening()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => opening_failed = error == ChannelSessionError::CleanupFailed,
                Err(_) => {
                    if let Some(task) = self.opening.take() {
                        task.abort();
                        let _ = task.await;
                    }
                    opening_failed = true;
                }
            }
        }
        let drain_deadline = Instant::now() + CYCLE_BUDGET;
        for index in 0..self.roles.len() {
            let pool = if self
                .candidate
                .as_ref()
                .is_some_and(|pool| pool.owns_role(&self.roles[index]))
            {
                self.candidate.as_ref().unwrap()
            } else {
                &self.pool
            };
            if !pool.owns_role(&self.roles[index])
                || pool.closed.borrow().is_some()
                || Instant::now() >= pool.authorization_deadline()
            {
                self.report[index].last_error = Some(ChannelSessionError::AuthorityExpired);
                continue;
            }
            self.report[index].last_error = Some(ChannelSessionError::Transport);
            match tokio::time::timeout_at(
                drain_deadline,
                pool.deregister_role(&mut self.roles[index]),
            )
            .await
            {
                Ok(result) => self.report[index].last_error = result.err(),
                Err(_) => break,
            }
            self.publish();
        }
        // Every role task already received stop, so this bounded fan-in waits
        // for concurrent native cleanup rather than serially starting it.
        let roles = async {
            let mut failed = false;
            for role in &mut self.roles {
                failed |= role.close().await.is_err();
            }
            failed
        };
        let candidate = async {
            if let Some(pool) = &mut self.candidate {
                pool.close().await
            } else {
                Ok(())
            }
        };
        let retired = async {
            if let Some(pool) = &mut self.retired {
                pool.close().await
            } else {
                Ok(())
            }
        };
        let (pool_result, role_failed, candidate_result, retired_result) =
            tokio::join!(self.pool.close(), roles, candidate, retired);
        self.publish();
        if self.rotation_config.is_some()
            && self.rotation.borrow().phase != CertificateRotationPhase::Failed
        {
            self.rotation.send_modify(|status| {
                status.phase = CertificateRotationPhase::Stopped;
                status.scheduled_at = None;
            });
        }
        if pool_result.is_err()
            || role_failed
            || candidate_result.is_err()
            || retired_result.is_err()
            || opening_failed
        {
            Err(ChannelSessionError::CleanupFailed)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service_channel::ChannelSessionConfig;
    #[tokio::test]
    async fn cancelling_candidate_open_wait_keeps_task_joinable_and_reports_panics() {
        let (result, completion) = tokio::sync::oneshot::channel();
        let mut opening = Some(tokio::spawn(async move {
            completion.await.map_err(|_| ChannelSessionError::Transport)
        }));
        assert!(
            tokio::time::timeout(Duration::from_millis(1), join_candidate_open(&mut opening))
                .await
                .is_err()
        );
        assert!(opening.as_ref().is_some_and(|task| !task.is_finished()));
        result.send(42).unwrap();
        assert_eq!(join_candidate_open(&mut opening).await.unwrap(), 42);
        assert!(opening.is_none());
        let mut opening = Some(tokio::spawn(async {
            panic!("simulated opening task failure");
            #[allow(unreachable_code)]
            Ok::<(), ChannelSessionError>(())
        }));
        assert_eq!(
            join_candidate_open(&mut opening).await,
            Err(ChannelSessionError::CleanupFailed)
        );
        assert!(opening.is_none());
    }
    #[test]
    fn rotation_schedule_reserves_certificate_headroom_without_extending_it() {
        let now = Instant::now();
        let default = ChannelCertificateRotationConfig::default();
        assert_eq!(
            rotation_due(default.after, now, Duration::from_secs(300)).unwrap(),
            now + Duration::from_secs(240)
        );
        assert_eq!(
            rotation_due(default.after, now, Duration::from_secs(75)).unwrap(),
            now + Duration::from_secs(35)
        );
        assert!(rotation_due(default.after, now, Duration::from_secs(40)).is_err());
        assert!(ChannelCertificateRotationConfig::new(Duration::from_secs(19)).is_err());
        assert!(ChannelCertificateRotationConfig::new(Duration::from_secs(241)).is_err());
        assert!(ChannelCertificateRotationConfig::new(Duration::from_secs(20)).is_ok());
    }

    #[cfg(feature = "service-manifest")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "isolated native host; run zenss_channel_bootstrap_acceptance.py --role-test"]
    async fn native_managed_rotation_hands_off_twice_with_four_session_budget_and_same_roles() {
        use kish_lingshu_foundation_contract::{
            service_transport::RouteIdentity, ServiceInstanceRegistration,
        };
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let connection = crate::ServiceConnection::connect(
            &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
            crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
        let identity = connection
            .bootstrap_channel(
                ServiceInstanceRegistration {
                    instance_id: "native-auto-rotation".into(),
                    incarnation_id: "auto-rotation-boot".into(),
                    generation: None,
                },
                Some(RouteIdentity::new("dev").unwrap()),
            )
            .await
            .unwrap();
        let base = identity.bootstrap_response().instance.clone();
        let old_certificate = identity
            .bootstrap_response()
            .certificate
            .certificate_identity
            .clone();
        let full = ChannelSessionConfig::host_test();
        let config = if full.session_count() > 2 {
            full
        } else if std::env::var("LINGSHU_CHANNEL_TEST_CONTROL_LANE").as_deref() == Ok("true") {
            ChannelSessionConfig::new(1)
                .unwrap()
                .with_control_lane()
                .unwrap()
        } else {
            ChannelSessionConfig::new(2).unwrap()
        };
        let pool = identity.open_sessions(config).await.unwrap();
        assert_eq!(pool.connected_lanes().await, config.lanes());
        assert_eq!(pool.session_ids().len(), config.session_count());
        let original_sessions = pool.session_ids();
        let catalog = |key: &str| kish_lingshu_runtime_contract::provider::ProviderCatalog {
            format_version: 1,
            application_id: app.clone(),
            provider_key: key.into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![],
        };
        let first = pool
            .register_provider_role(&catalog("auto-rotation-first"))
            .await
            .unwrap();
        let manifest: crate::services::ServiceManifest =
            serde_json::from_str(&std::env::var("LINGSHU_CHANNEL_TEST_MANIFEST").unwrap()).unwrap();
        let mut builder = crate::services::ServiceRegistryBuilder::new(manifest).unwrap();
        let executions = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
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
        let second = pool
            .register_service_role("native-auto-rotation-call", 1, &registry)
            .await
            .unwrap();
        use sha2::{Digest, Sha256};
        let digest = |value: &str| {
            Sha256::digest(serde_json::to_vec(value).unwrap())
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        };
        let role_index = format!("kish:services:registration:{{{}}}:roles", digest(&app));
        let fields = [
            "provider:auto-rotation-first:native-auto-rotation".to_owned(),
            format!("call:{}", digest("native-auto-rotation-call")),
        ];
        let read_role = |field: &str| {
            let output = std::process::Command::new("redis-cli")
                .args([
                    "-h",
                    "127.0.0.1",
                    "-p",
                    &std::env::var("LINGSHU_CHANNEL_TEST_REDIS_PORT").unwrap(),
                    "--raw",
                    "HGET",
                    &role_index,
                    field,
                ])
                .output()
                .unwrap();
            assert!(output.status.success());
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        };
        let original_pointers = fields.iter().map(|f| read_role(f)).collect::<Vec<_>>();
        assert!(original_pointers.iter().all(|p| !p.is_empty()));
        let original = [
            first.lifecycle_status(pool.authorization_deadline()),
            second.lifecycle_status(pool.authorization_deadline()),
        ];
        let mut managed = pool
            .manage_roles_with_rotation(
                vec![first, second],
                ChannelCertificateRotationConfig::new(Duration::from_secs(20)).unwrap(),
            )
            .unwrap();
        let mut rotation = managed.subscribe_rotation();
        let mut generations = BTreeSet::new();
        let mut certificates = BTreeSet::new();
        let mut dynamic = None;
        tokio::time::timeout(Duration::from_secs(65), async {
            loop {
                let snapshot = rotation.borrow_and_update().clone();
                assert_eq!(snapshot.last_error, None, "{snapshot:?}");
                assert!(matches!(
                    snapshot.phase,
                    CertificateRotationPhase::Scheduled | CertificateRotationPhase::InProgress
                ));
                assert!(connection.channel_session_budget().available_permits() <= 4);
                for role in managed.role_statuses() {
                    generations.insert(role.role_generation);
                }
                if snapshot.completed > 0 {
                    assert_ne!(snapshot.certificate_identity, old_certificate);
                    assert_eq!(snapshot.session_ids.len(), config.session_count());
                    assert!(snapshot
                        .session_ids
                        .iter()
                        .all(|id| !original_sessions.contains(id)));
                    certificates.insert(snapshot.certificate_identity.as_str().to_owned());
                }
                if snapshot.completed == 1 && dynamic.is_none() {
                    dynamic = Some(
                        managed
                            .enroll_provider(catalog("auto-rotation-dynamic"))
                            .await
                            .unwrap(),
                    );
                }
                if snapshot.completed >= 2 {
                    break;
                }
                rotation.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(certificates.len(), 2);
        assert_eq!(
            generations,
            original
                .iter()
                .map(|s| s.role_generation.clone())
                .chain(std::iter::once(
                    dynamic.as_ref().unwrap().role_generation.clone()
                ))
                .collect()
        );
        let dynamic_generation = dynamic.unwrap().role_generation;
        assert!(managed
            .role_statuses()
            .iter()
            .any(|s| s.role_generation == dynamic_generation && s.route_confirmed()));
        managed
            .deregister_role(RouteIdentity::new(dynamic_generation).unwrap())
            .await
            .unwrap();
        // The same supervisor must keep following the second new pool and
        // reclaim the last retired pool before its next regular observation.
        tokio::time::sleep(Duration::from_secs(11)).await;
        assert_eq!(managed.rotation_status().completed, 2);
        assert_eq!(
            connection.channel_session_budget().available_permits(),
            4 - config.session_count()
        );
        assert!(matches!(
            managed.status(),
            ChannelSupervisorStatus::Active {
                observations: 5..,
                last_error: None,
                ..
            }
        ));
        for (current, initial) in managed.role_statuses().iter().zip(&original) {
            assert_eq!(current.role_generation, initial.role_generation);
            assert!(current.route_confirmed(), "{current:?}");
            assert!(current.authorization_deadline > initial.authorization_deadline);
            assert_eq!(current.last_error, None);
        }
        assert_eq!(
            fields.iter().map(|f| read_role(f)).collect::<Vec<_>>(),
            original_pointers
        );
        assert_eq!(executions.load(std::sync::atomic::Ordering::SeqCst), 0);
        let _ = tokio::time::timeout(Duration::from_millis(1), managed.close()).await;
        managed.close().await.unwrap();
        managed.close().await.unwrap();
        assert_eq!(
            managed.rotation_status().phase,
            CertificateRotationPhase::Stopped
        );
        assert_eq!(managed.rotation_status().completed, 2);
        assert_eq!(connection.channel_session_budget().available_permits(), 4);
        assert!(managed
            .role_statuses()
            .iter()
            .all(|s| s.remote_deregistered && !s.route_confirmed()));
        assert_eq!(
            connection
                .registration()
                .await
                .request(&base.instance_id)
                .generation
                .as_deref(),
            Some(base.generation.as_str())
        );
        assert!(fields.iter().all(|f| read_role(f).is_empty()));
        connection.ensure_open().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "isolated native host; run zenss_channel_bootstrap_acceptance.py --role-test"]
    async fn native_managed_rotation_omitted_sibling_stops_and_drains_both_partial_pools() {
        use kish_lingshu_foundation_contract::ServiceInstanceRegistration;
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let connection = crate::ServiceConnection::connect(
            &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
            crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
        let identity = connection
            .bootstrap_channel(
                ServiceInstanceRegistration {
                    instance_id: "native-auto-rotation-fail".into(),
                    incarnation_id: "auto-rotation-fail-boot".into(),
                    generation: None,
                },
                None,
            )
            .await
            .unwrap();
        let base = identity.bootstrap_response().instance.clone();
        let pool = identity
            .open_sessions(if ChannelSessionConfig::host_test().session_count() > 2 {
                ChannelSessionConfig::host_test()
            } else {
                ChannelSessionConfig::default()
            })
            .await
            .unwrap();
        let catalog = |key: &str| kish_lingshu_runtime_contract::provider::ProviderCatalog {
            format_version: 1,
            application_id: app.clone(),
            provider_key: key.into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![],
        };
        let first = pool
            .register_provider_role(&catalog("auto-rotation-fail-first"))
            .await
            .unwrap();
        let mut omitted = pool
            .register_provider_role(&catalog("auto-rotation-fail-sibling"))
            .await
            .unwrap();
        let mut managed = pool
            .manage_roles_with_rotation(
                vec![first],
                ChannelCertificateRotationConfig::new(Duration::from_secs(20)).unwrap(),
            )
            .unwrap();
        let mut status = managed.subscribe_status();
        tokio::time::timeout(Duration::from_secs(29), async {
            loop {
                if matches!(
                    *status.borrow_and_update(),
                    ChannelSupervisorStatus::Closed {
                        reason: ChannelCloseReason::RotationFailed
                    }
                ) {
                    break;
                }
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        managed.close().await.unwrap();
        assert_eq!(
            managed.rotation_status().phase,
            CertificateRotationPhase::Failed
        );
        assert_eq!(managed.rotation_status().completed, 0);
        assert_eq!(
            managed.role_statuses()[0].state,
            RoleLifecycleState::Stopped
        );
        assert!(managed.role_statuses()[0].remote_deregistered);
        assert_eq!(connection.channel_session_budget().available_permits(), 4);
        // The omitted remote role was not adopted. Stopping its shared physical
        // pool also ends this local declaration; finite remote authority expires.
        assert!(!omitted.route_confirmed());
        omitted.close().await.unwrap();
        tokio::time::sleep(Duration::from_secs(11)).await;
        assert_eq!(managed.rotation_status().completed, 0);
        assert_eq!(
            connection
                .registration()
                .await
                .request(&base.instance_id)
                .generation
                .as_deref(),
            Some(base.generation.as_str())
        );
        connection.ensure_open().unwrap();
    }

    #[test]
    fn bounded_frames_rotate_across_capacity_and_never_include_expired_roles() {
        let mut active = vec![true; MAX_MANAGED_ROLES];
        active[65] = false;
        let mut cursor = 0;
        let mut first = BTreeSet::new();
        for _ in 0..MAX_MANAGED_ROLES {
            let plan = renewal_plan(&active, &mut cursor);
            assert_eq!(plan.len(), 1023);
            assert!(!plan.contains(&65));
            assert_eq!(plan.iter().copied().collect::<BTreeSet<_>>().len(), 1023);
            assert!(plan
                .chunks(MAX_CHANNEL_RENEWAL_ROLES)
                .all(|batch| batch.len() <= 64));
            first.insert(plan[0]);
        }
        assert_eq!(first.len(), 1023);
        assert_eq!(cursor, 0);
    }
    #[tokio::test(start_paused = true)]
    async fn retained_status_does_not_advertise_a_route_after_its_deadline() {
        let mut role = ChannelRoleStatus {
            role_generation: "role".into(),
            state: RoleLifecycleState::Active,
            authorization_deadline: Instant::now() + Duration::from_secs(30),
            route_deadline: Instant::now() + Duration::from_secs(10),
            last_renewal_status: Some(200),
            last_error: None,
            remote_deregistered: false,
        };
        assert!(role.route_confirmed());
        tokio::time::advance(Duration::from_secs(10)).await;
        assert!(role.authorized());
        assert!(!role.route_confirmed());
        role.route_deadline = Instant::now() + Duration::from_secs(30);
        tokio::time::advance(Duration::from_secs(20)).await;
        assert!(!role.authorized());
        assert!(!role.route_confirmed());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "isolated native host; run zenss_channel_bootstrap_acceptance.py --role-test"]
    async fn native_managed_roles_renew_reprove_and_drain_without_reenrollment() {
        use kish_lingshu_foundation_contract::{
            service_transport::RouteIdentity, ServiceInstanceRegistration,
        };
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let connection = crate::ServiceConnection::connect(
            &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
            crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
        let identity = connection
            .bootstrap_channel(
                ServiceInstanceRegistration {
                    instance_id: "native-managed".into(),
                    incarnation_id: "managed-boot".into(),
                    generation: None,
                },
                Some(RouteIdentity::new("dev").unwrap()),
            )
            .await
            .unwrap();
        let original_base = identity.bootstrap_response().instance.clone();
        let pool = identity
            .open_sessions(super::super::ChannelSessionConfig::default())
            .await
            .unwrap();
        let mut catalog = kish_lingshu_runtime_contract::provider::ProviderCatalog {
            format_version: 1,
            application_id: app,
            provider_key: "managed-first".into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![],
        };
        let first = pool.register_provider_role(&catalog).await.unwrap();
        catalog.provider_key = "managed-omitted".into();
        let mut omitted = pool.register_provider_role(&catalog).await.unwrap();
        let initial_first = first.lifecycle_status(pool.authorization_deadline());
        let mut managed = pool.manage_roles(vec![first]).unwrap();
        catalog.provider_key = "managed-second".into();
        let second = managed.enroll_provider(catalog.clone()).await.unwrap();
        assert!(second.route_confirmed());
        assert_eq!(
            managed.enroll_provider(catalog.clone()).await.unwrap_err(),
            ChannelSessionError::InvalidConfig
        );
        assert_eq!(managed.role_statuses().len(), 2);
        assert!(managed.role_statuses().iter().all(|s| s.route_confirmed()));
        let original = [initial_first, second.clone()];
        let notifications = managed.subscribe_roles();
        // Stay alive beyond initial thirty-second grants without explicit
        // renewal/reproof calls from the application.
        tokio::time::sleep(Duration::from_secs(41)).await;
        let statuses = managed.role_statuses();
        assert_eq!(statuses.len(), 2);
        assert!(matches!(
            managed.status(),
            ChannelSupervisorStatus::Active {
                observations: 3..,
                ..
            }
        ));
        for (role, initial) in statuses.iter().zip(original) {
            assert_eq!(role.role_generation, initial.role_generation);
            assert!(role.route_confirmed(), "{role:?}");
            assert!(role.authorization_deadline > initial.authorization_deadline);
            assert!(role.route_deadline > initial.route_deadline);
            assert_eq!(role.last_renewal_status, Some(200));
            assert_eq!(role.last_error, None);
        }
        assert!(!omitted.route_confirmed());
        omitted.close().await.unwrap();
        let removed = managed
            .deregister_role(RouteIdentity::new(second.role_generation.clone()).unwrap())
            .await
            .unwrap();
        assert!(removed.remote_deregistered);
        assert_eq!(removed.state, RoleLifecycleState::Stopped);
        assert_eq!(managed.role_statuses().len(), 1);
        assert!(managed.role_statuses()[0].route_confirmed());
        assert_eq!(
            managed
                .deregister_role(RouteIdentity::new(second.role_generation).unwrap())
                .await
                .unwrap_err(),
            ChannelSessionError::InvalidConfig
        );
        // Cancel the caller's close wait; ownership and remote/local cleanup
        // continue and remain joinable without a second drain pass.
        let _ = tokio::time::timeout(Duration::from_millis(1), managed.close()).await;
        managed.close().await.unwrap();
        managed.close().await.unwrap();
        assert_eq!(
            managed.status(),
            ChannelSupervisorStatus::Closed {
                reason: ChannelCloseReason::Explicit
            }
        );
        for role in managed.role_statuses() {
            assert_eq!(role.state, RoleLifecycleState::Stopped);
            assert!(role.remote_deregistered, "{role:?}");
            assert!(!role.route_confirmed());
            assert_eq!(role.last_error, None);
        }
        assert!(notifications
            .borrow()
            .iter()
            .all(|role| role.remote_deregistered));
        connection.ensure_open().unwrap();
        assert_eq!(
            connection
                .registration()
                .await
                .request(&original_base.instance_id)
                .generation
                .as_deref(),
            Some(original_base.generation.as_str())
        );
        connection.shutdown().await;
    }
}
