//! Reconstructible catalog readiness reconciliation, never business execution.
use super::role_changes::RoleMutation;
use super::{
    ChannelRoleStatus, ChannelSessionError, ChannelSupervisorStatus, ManagedRoleChannel,
    RoleLifecycleState,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

const CATALOG_RECHECK: Duration = Duration::from_secs(5);
const MAX_PLAN_BYTES: usize = 8 * 1024 * 1024;
const MAX_DESIRED_ROLES: usize = 1023; // Reserve the Provider in the existing 1024-role owner.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityActivationState {
    Pending,
    WaitingForCatalog,
    Active,
    Unavailable,
    Failed,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityActivationStatus {
    pub key: String,
    pub state: CapabilityActivationState,
    pub role_generation: Option<String>,
    pub ready_until: Option<tokio::time::Instant>,
    pub last_error: Option<ChannelSessionError>,
}
impl CapabilityActivationStatus {
    /// Observed readiness is finite; a retained snapshot never extends authority.
    pub fn ready(&self) -> bool {
        self.state == CapabilityActivationState::Active
            && self
                .ready_until
                .is_some_and(|deadline| tokio::time::Instant::now() < deadline)
    }
}
struct DesiredRole {
    mutation: RoleMutation,
    bytes: usize,
    status: CapabilityActivationStatus,
}

/// Build once from declarations, subscribe before `run`, and poll `run` alongside
/// application shutdown. Dropping the future stops reconciliation; the managed
/// channel still owns cleanup of any accepted or partially accepted role.
#[derive(Default)]
pub struct CatalogActivationPlan {
    desired: Vec<DesiredRole>,
}
impl CatalogActivationPlan {
    pub fn new() -> Self {
        Self::default()
    }
    fn add(
        &mut self,
        key: String,
        mutation: RoleMutation,
        bytes: usize,
    ) -> Result<(), ChannelSessionError> {
        if bytes > MAX_PLAN_BYTES
            || self
                .desired
                .iter()
                .map(|d| d.bytes)
                .sum::<usize>()
                .saturating_add(bytes)
                > MAX_PLAN_BYTES
        {
            return Err(ChannelSessionError::CapacityExceeded);
        }
        if self.desired.len() >= MAX_DESIRED_ROLES {
            return Err(ChannelSessionError::CapacityExceeded);
        }
        if self.desired.iter().any(|d| d.status.key == key) {
            return Err(ChannelSessionError::InvalidConfig);
        }
        self.desired.push(DesiredRole {
            mutation,
            bytes,
            status: CapabilityActivationStatus {
                key,
                state: CapabilityActivationState::Pending,
                role_generation: None,
                ready_until: None,
                last_error: None,
            },
        });
        Ok(())
    }
    #[cfg(feature = "event-consumer-zenoh")]
    pub fn add_consumer(
        &mut self,
        group_key: &str,
        node_id: &str,
        maximum_in_flight: u32,
        registry: Arc<crate::event_dispatch::ConsumerRegistry>,
        budget: crate::ServiceExecutionBudget,
    ) -> Result<(), ChannelSessionError> {
        if !registry.supports_group(group_key)
            || maximum_in_flight == 0
            || kish_lingshu_foundation_contract::service_transport::RouteIdentity::new(node_id)
                .is_err()
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        self.add(
            format!("consumer:{group_key}"),
            RoleMutation::Consumer {
                group_key: group_key.into(),
                node_id: node_id.into(),
                maximum_in_flight,
                registry,
                budget,
            },
            group_key.len() + node_id.len() + 256,
        )
    }
    /// Each immutable operation has its own stable Call role, so newly added
    /// unpublished operations do not block existing published ones.
    #[cfg(feature = "service-call-zenoh")]
    pub fn add_services(
        &mut self,
        node_id: &str,
        registry: Arc<crate::services::ServiceRegistry>,
        budget: crate::ServiceExecutionBudget,
        asynchronous: bool,
    ) -> Result<(), ChannelSessionError> {
        use kish_lingshu_runtime_contract::service::canonical_digest;
        if kish_lingshu_foundation_contract::service_transport::RouteIdentity::new(node_id).is_err()
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let mut additions = Self::new();
        for capability in registry.capabilities() {
            let target = &capability.operation;
            let projected = Arc::new(
                registry
                    .select_operation(target)
                    .map_err(|_| ChannelSessionError::InvalidConfig)?,
            );
            let derived_node = format!(
                "operation-{}",
                canonical_digest(&(node_id, target))
                    .map_err(|_| ChannelSessionError::InvalidConfig)?
            );
            let bytes = serde_json::to_vec(projected.manifest())
                .map_err(|_| ChannelSessionError::InvalidConfig)?
                .len()
                + derived_node.len();
            additions.add(
                format!(
                    "call:{}/{}/{}:{}",
                    target.service_key,
                    target.operation_key,
                    target.version,
                    target.contract_digest
                ),
                RoleMutation::Call {
                    registration: None,
                    node_id: derived_node,
                    registry: projected,
                    budget: budget.clone(),
                    asynchronous,
                },
                bytes,
            )?;
        }
        if self
            .desired
            .iter()
            .chain(&additions.desired)
            .map(|d| d.bytes)
            .sum::<usize>()
            > MAX_PLAN_BYTES
        {
            return Err(ChannelSessionError::CapacityExceeded);
        }
        if self.desired.len() + additions.desired.len() > MAX_DESIRED_ROLES {
            return Err(ChannelSessionError::CapacityExceeded);
        }
        if additions.desired.iter().any(|new| {
            self.desired
                .iter()
                .any(|old| old.status.key == new.status.key)
        }) {
            return Err(ChannelSessionError::InvalidConfig);
        }
        self.desired.extend(additions.desired);
        Ok(())
    }
    pub fn prepare(
        self,
    ) -> (
        CatalogActivation,
        watch::Receiver<Vec<CapabilityActivationStatus>>,
    ) {
        let (status, receiver) =
            watch::channel(self.desired.iter().map(|d| d.status.clone()).collect());
        (
            CatalogActivation {
                desired: self.desired,
                status,
            },
            receiver,
        )
    }
}

pub struct CatalogActivation {
    desired: Vec<DesiredRole>,
    status: watch::Sender<Vec<CapabilityActivationStatus>>,
}
fn apply_result(
    status: &mut CapabilityActivationStatus,
    result: Result<ChannelRoleStatus, ChannelSessionError>,
) {
    match result {
        Ok(role) => {
            status.state = if role.route_confirmed() {
                CapabilityActivationState::Active
            } else {
                CapabilityActivationState::Unavailable
            };
            status.ready_until = Some(role.authorization_deadline.min(role.route_deadline));
            status.role_generation = Some(role.role_generation);
            status.last_error = role.last_error;
        }
        Err(ChannelSessionError::CatalogNotReady) => {
            status.state = CapabilityActivationState::WaitingForCatalog;
            status.last_error = Some(ChannelSessionError::CatalogNotReady);
        }
        Err(error) => {
            status.state = CapabilityActivationState::Failed;
            status.last_error = Some(error);
        }
    }
}
impl CatalogActivation {
    fn publish(&self) {
        self.status.send_if_modified(|current| {
            let next: Vec<_> = self.desired.iter().map(|d| d.status.clone()).collect();
            if *current == next {
                false
            } else {
                *current = next;
                true
            }
        });
    }
    fn observe(&mut self, roles: &[ChannelRoleStatus]) {
        for desired in &mut self.desired {
            if let Some(generation) = &desired.status.role_generation {
                let role = roles
                    .iter()
                    .find(|role| &role.role_generation == generation);
                desired.status.state = if role.is_some_and(ChannelRoleStatus::route_confirmed) {
                    CapabilityActivationState::Active
                } else {
                    CapabilityActivationState::Unavailable
                };
                desired.status.ready_until =
                    role.map(|role| role.authorization_deadline.min(role.route_deadline));
                desired.status.last_error = role.and_then(|role| role.last_error);
                // Never automatically re-enroll an expired/revoked generation.
                if role.is_some_and(|r| r.state == RoleLifecycleState::CleanupFailed) {
                    desired.status.state = CapabilityActivationState::Failed;
                }
            }
        }
        self.publish();
    }
    async fn attempt_pending<F, Fut>(&mut self, mut enroll: F)
    where
        F: FnMut(RoleMutation, usize) -> Fut,
        Fut: std::future::Future<Output = Result<ChannelRoleStatus, ChannelSessionError>>,
    {
        for index in 0..self.desired.len() {
            if matches!(
                self.desired[index].status.state,
                CapabilityActivationState::Pending | CapabilityActivationState::WaitingForCatalog
            ) {
                let desired = &self.desired[index];
                let result = enroll(desired.mutation.clone(), desired.bytes).await;
                apply_result(&mut self.desired[index].status, result);
                self.publish();
            }
        }
    }
    pub async fn run(mut self, channel: &ManagedRoleChannel) -> Result<(), ChannelSessionError> {
        // Validate all identities before any I/O, including mixed-application plans.
        for desired in &self.desired {
            let application: &str = match &desired.mutation {
                #[cfg(feature = "event-consumer-zenoh")]
                RoleMutation::Consumer { registry, .. } => registry.app_id(),
                #[cfg(feature = "service-call-zenoh")]
                RoleMutation::Call { registry, .. } => &registry.manifest().application_id,
                _ => return Err(ChannelSessionError::InvalidConfig),
            };
            if application != channel.application_id {
                return Err(ChannelSessionError::InvalidConfig);
            }
        }
        let mut roles = channel.subscribe_roles();
        let mut authority = channel.subscribe_status();
        loop {
            if !matches!(channel.status(), ChannelSupervisorStatus::Active { .. }) {
                return Err(ChannelSessionError::Closed);
            }
            // The existing role command owner serializes registration and renewal.
            // A pending catalog never prevents the next independent capability.
            self.attempt_pending(|mutation, bytes| channel.commands.send(mutation, bytes))
                .await;
            let delay = tokio::time::sleep(CATALOG_RECHECK);
            tokio::pin!(delay);
            loop {
                self.observe(&channel.role_statuses());
                tokio::select! {
                    _ = &mut delay => break,
                    changed = roles.changed() => { changed.map_err(|_| ChannelSessionError::Closed)?; },
                    changed = authority.changed() => {
                        changed.map_err(|_| ChannelSessionError::Closed)?;
                        if !matches!(channel.status(), ChannelSupervisorStatus::Active { .. }) { return Err(ChannelSessionError::Closed); }
                    },
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kish_lingshu_foundation_contract::service_transport::RouteIdentity;
    fn role(generation: &str) -> ChannelRoleStatus {
        ChannelRoleStatus {
            call_registration: None,
            consumer_registration: None,
            role_generation: generation.into(),
            state: RoleLifecycleState::Active,
            authorization_deadline: tokio::time::Instant::now() + Duration::from_secs(30),
            route_deadline: tokio::time::Instant::now() + Duration::from_secs(20),
            last_renewal_status: None,
            last_error: None,
            remote_deregistered: false,
        }
    }
    fn plan(keys: &[&str]) -> CatalogActivation {
        let mut plan = CatalogActivationPlan::new();
        for key in keys {
            plan.add(
                (*key).into(),
                RoleMutation::Remove(RouteIdentity::new(*key).unwrap()),
                1,
            )
            .unwrap();
        }
        plan.prepare().0
    }
    #[tokio::test]
    async fn missing_new_group_does_not_block_siblings_and_only_missing_is_retried() {
        let mut plan = plan(&["old", "new", "other"]);
        let mut seen = Vec::new();
        plan.attempt_pending(|mutation, _| {
            let RoleMutation::Remove(key) = mutation else {
                unreachable!()
            };
            seen.push(key.as_str().to_string());
            std::future::ready(if key.as_str() == "new" {
                Err(ChannelSessionError::CatalogNotReady)
            } else {
                Ok(role(key.as_str()))
            })
        })
        .await;
        assert_eq!(seen, ["old", "new", "other"]);
        assert_eq!(
            plan.desired[0].status.state,
            CapabilityActivationState::Active
        );
        assert_eq!(
            plan.desired[1].status.state,
            CapabilityActivationState::WaitingForCatalog
        );
        seen.clear();
        plan.attempt_pending(|mutation, _| {
            let RoleMutation::Remove(key) = mutation else {
                unreachable!()
            };
            seen.push(key.as_str().to_string());
            std::future::ready(Ok(role(key.as_str())))
        })
        .await;
        assert_eq!(seen, ["new"]);
        assert!(plan
            .desired
            .iter()
            .all(|d| d.status.state == CapabilityActivationState::Active));
    }
    #[tokio::test]
    async fn uncertain_rejected_and_revoked_roles_are_not_replayed() {
        for error in [
            ChannelSessionError::Transport,
            ChannelSessionError::InvalidResponse,
            ChannelSessionError::ControlRejected,
            ChannelSessionError::AuthorityExpired,
            ChannelSessionError::CapacityExceeded,
        ] {
            let mut plan = plan(&["broken", "good"]);
            plan.attempt_pending(|mutation, _| {
                let RoleMutation::Remove(key) = mutation else {
                    unreachable!()
                };
                std::future::ready(if key.as_str() == "broken" {
                    Err(error)
                } else {
                    Ok(role("good"))
                })
            })
            .await;
            let mut revoked = role("good");
            revoked.state = RoleLifecycleState::Stopped;
            plan.observe(&[revoked]);
            assert_eq!(
                plan.desired[0].status.state,
                CapabilityActivationState::Failed
            );
            assert_eq!(
                plan.desired[1].status.state,
                CapabilityActivationState::Unavailable
            );
            plan.attempt_pending(|_, _| {
                panic!("unknown or revoked role must not replay");
                #[allow(unreachable_code)]
                std::future::ready(Ok(role("bad")))
            })
            .await;
        }
    }
    #[tokio::test]
    async fn cancelled_attempt_cannot_run_a_second_enrollment() {
        let mut plan = plan(&["first", "second"]);
        let mut calls = 0;
        {
            let pending = plan.attempt_pending(|_, _| {
                calls += 1;
                std::future::pending()
            });
            tokio::pin!(pending);
            tokio::select! { biased; _ = &mut pending => panic!("must remain pending"), _ = std::future::ready(()) => {} }
        }
        assert_eq!(calls, 1);
        // Dropping the plan's enclosing run future cancels only reconciliation;
        // the existing RoleOwner retains accepted cleanup ownership.
    }
    #[tokio::test(start_paused = true)]
    async fn retained_capability_snapshot_cannot_extend_role_authority() {
        let mut plan = plan(&["good"]);
        plan.attempt_pending(|_, _| std::future::ready(Ok(role("good"))))
            .await;
        let retained = plan.desired[0].status.clone();
        assert!(retained.ready());
        tokio::time::advance(Duration::from_secs(21)).await;
        assert!(!retained.ready());
    }
    #[test]
    fn plan_bounds_and_duplicate_keys_reject_before_io() {
        let mut plan = CatalogActivationPlan::new();
        let mutation = RoleMutation::Remove(RouteIdentity::new("group").unwrap());
        plan.add("group".into(), mutation.clone(), MAX_PLAN_BYTES)
            .unwrap();
        assert_eq!(
            plan.add("another".into(), mutation.clone(), 1),
            Err(ChannelSessionError::CapacityExceeded)
        );
        let mut plan = CatalogActivationPlan::new();
        plan.add("group".into(), mutation.clone(), 1).unwrap();
        assert_eq!(
            plan.add("group".into(), mutation, 1),
            Err(ChannelSessionError::InvalidConfig)
        );
    }
}

#[cfg(all(test, feature = "service-call-zenoh", feature = "event-consumer-zenoh"))]
#[path = "activation_wire_tests.rs"]
mod wire_tests;
