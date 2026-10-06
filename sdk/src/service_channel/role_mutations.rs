//! Mutation work runs on RoleOwner; partial handles remain in its cleanup set.
use super::{ChannelRoleStatus, ChannelSessionError, RoleMutation, RoleOwner, MAX_MANAGED_ROLES};
use tokio::time::Instant;

impl RoleOwner {
    pub(super) async fn apply_pending_mutation(&mut self) {
        let Some(command) = self.pending_command.take() else {
            return;
        };
        if command.response.is_closed() {
            return;
        }
        let deadline = command.deadline.min(self.pool.authorization_deadline());
        let result = if Instant::now() >= deadline {
            Err(ChannelSessionError::AuthorityExpired)
        } else {
            tokio::time::timeout_at(deadline, self.apply_mutation(command.mutation))
                .await
                .unwrap_or(Err(ChannelSessionError::Transport))
        };
        self.publish();
        let _ = command.response.send(result);
    }
    async fn apply_mutation(
        &mut self,
        mutation: RoleMutation,
    ) -> Result<ChannelRoleStatus, ChannelSessionError> {
        if let RoleMutation::Remove(generation) = mutation {
            let index = self
                .report
                .iter()
                .position(|role| role.role_generation == generation.as_str())
                .ok_or(ChannelSessionError::InvalidConfig)?;
            // Retain the handle until both remote deregistration and native
            // cleanup complete. Cancellation leaves it stopped and owned.
            self.roles[index].request_stop();
            self.publish();
            let expired = Instant::now() >= self.report[index].authorization_deadline;
            if let Err(error) = self.pool.deregister_role(&mut self.roles[index]).await {
                if !expired {
                    return Err(error);
                }
                // Expired authority may no longer accept deregistration. Join
                // local cleanup and retain remote_deregistered=false evidence;
                // never claim an ACK or reset this generation automatically.
                self.roles[index].close().await?;
            }
            self.publish();
            let status = self.report[index].clone();
            self.roles.remove(index);
            self.report.remove(index);
            self.pending_cycle.clear();
            return Ok(status);
        }
        if self.roles.len() >= MAX_MANAGED_ROLES {
            return Err(ChannelSessionError::CapacityExceeded);
        }
        let logical_key = match &mutation {
            RoleMutation::Provider(catalog) => format!("provider:{}", catalog.provider_key),
            #[cfg(feature = "service-call-zenoh")]
            RoleMutation::Call { node_id, .. } => format!("call:{node_id}"),
            #[cfg(feature = "event-consumer-zenoh")]
            RoleMutation::Consumer { group_key, .. } => format!("consumer:{group_key}"),
            RoleMutation::Remove(_) => unreachable!(),
        };
        // Reject duplicates before native I/O, including retained expired roles.
        // An application must explicitly remove its old generation first.
        if self
            .roles
            .iter()
            .any(|role| role.logical_key == logical_key)
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let mut role = match &mutation {
            RoleMutation::Provider(catalog) => self.pool.register_provider_role(catalog).await?,
            #[cfg(feature = "service-call-zenoh")]
            RoleMutation::Call {
                node_id,
                registry,
                budget,
                ..
            } => {
                self.pool
                    .register_service_role(node_id, budget.maximum_in_flight(), registry)
                    .await?
            }
            #[cfg(feature = "event-consumer-zenoh")]
            RoleMutation::Consumer {
                group_key,
                node_id,
                maximum_in_flight,
                ..
            } => {
                self.pool
                    .register_consumer_role(group_key, node_id, *maximum_in_flight)
                    .await?
            }
            RoleMutation::Remove(_) => unreachable!(),
        };
        // Binding is synchronous. Record before any further I/O so an expired
        // or invalid execution binding cannot lose the registered cleanup owner.
        let binding = match mutation {
            RoleMutation::Provider(_) => Ok(()),
            #[cfg(feature = "service-call-zenoh")]
            RoleMutation::Call {
                registry,
                budget,
                asynchronous,
                ..
            } => {
                if asynchronous {
                    self.pool.enable_async_calls(&mut role, registry, budget)
                } else {
                    self.pool.enable_sync_calls(&mut role, registry, budget)
                }
            }
            #[cfg(feature = "event-consumer-zenoh")]
            RoleMutation::Consumer {
                registry, budget, ..
            } => self.pool.enable_sync_consumers(&mut role, registry, budget),
            RoleMutation::Remove(_) => unreachable!(),
        };
        if binding.is_err() {
            role.request_stop();
        }
        let mut status = role.lifecycle_status(self.pool.authorization_deadline());
        status.last_error = binding.as_ref().err().copied();
        if self
            .report
            .iter()
            .any(|old| old.role_generation == status.role_generation)
        {
            role.request_stop();
            self.report.push(status);
            self.roles.push(role);
            return Err(ChannelSessionError::InvalidResponse);
        }
        self.report.push(status.clone());
        self.roles.push(role);
        binding?;
        Ok(status)
    }
}
