//! Signed, group-targeted synchronous delivery. Dispatch owns all redelivery.
use super::observation::{self, BusinessObservation, BusinessOutcome, Plane};
use super::{ChannelSessionError, ServiceChannelSessions};
use crate::{
    event_dispatch::{ConsumerError, ConsumerRegistry},
    ServiceExecutionBudget,
};
use kish_lingshu_event_dispatch_contract::{
    NativeConsumerRequest, NativeConsumerResponse, NATIVE_CONSUMER_TIMEOUT_MS,
};
use kish_lingshu_foundation_contract::{
    service_auth::{ChannelMessageSigner, ClientChannelIdentity},
    service_transport::{
        bootstrap::ChannelBootstrapResponse, ExactRouteKey, MessageKind, ProtocolVersion,
        ServiceEndpoint, TransportEnvelope, MAX_BUSINESS_PAYLOAD_BYTES,
        MAX_ENVELOPE_OVERHEAD_BYTES,
    },
};
use kish_lingshu_runtime_contract::service::ChannelRoleEnrollmentResponse;
use std::sync::{Arc, Mutex};

pub(super) type ConsumerSlot = Arc<Mutex<Option<Arc<NativeConsumerExecution>>>>;
pub(super) struct NativeConsumerExecution {
    #[cfg(test)]
    received_lanes: [std::sync::atomic::AtomicUsize; 4],
    registry: Arc<ConsumerRegistry>,
    total: ServiceExecutionBudget,
    capacity: tokio::sync::Semaphore,
    group_key: String,
    group_id: u64,
    member_id: u64,
    membership_generation: u64,
}
impl NativeConsumerExecution {
    #[cfg(test)]
    pub(super) fn test_binding(
        registry: Arc<ConsumerRegistry>,
        total: ServiceExecutionBudget,
        capacity: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            received_lanes: Default::default(),
            registry,
            total,
            capacity: tokio::sync::Semaphore::new(capacity),
            group_key: "chosen".into(),
            group_id: 4,
            member_id: 11,
            membership_generation: 12,
        })
    }
    pub(super) async fn reply(
        &self,
        endpoint: &ServiceEndpoint,
        target: &ExactRouteKey,
        bytes: &[u8],
        expires: i64,
        connection: &crate::ServiceConnection,
        initial: &ChannelBootstrapResponse,
        signer: &ChannelMessageSigner,
    ) -> Result<Vec<u8>, ChannelSessionError> {
        let now = chrono::Utc::now().timestamp_millis();
        if bytes.len() > MAX_BUSINESS_PAYLOAD_BYTES + MAX_ENVELOPE_OVERHEAD_BYTES {
            return Err(ChannelSessionError::InvalidResponse);
        }
        let request = TransportEnvelope::decode(bytes, target, &initial.application_id, now)
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        connection
            .verify_channel_message(&initial.transport_trust, &request)
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let input: NativeConsumerRequest = serde_json::from_str(request.payload.get())
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
        if request.kind != MessageKind::InvokeEvent
            || request.deadline_unix_ms > expires
            || request.deadline_unix_ms > now + NATIVE_CONSUMER_TIMEOUT_MS
            || input.route_revision != *route_revision
            || !lanes.contains(&input.lane)
            || route.invoke_key(&input.lane).ok().as_ref() != Some(target)
            || input.member_id != self.member_id
            || input.membership_generation != self.membership_generation
            || input.invocation.consumption.group_id != self.group_id
            || input.invocation.consumption.group_key.as_deref() != Some(self.group_key.as_str())
            || input
                .invocation
                .consumption
                .invocation_deadline
                .timestamp_millis()
                != request.deadline_unix_ms
        {
            return Err(ChannelSessionError::InvalidResponse);
        }
        super::trace::scope(request.trace_parent.as_deref(), async {
            #[cfg(test)]
            self.received_lanes[usize::from(input.lane.lane.index())]
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // The logical connection owns the common signed-message replay cache.
            let business = BusinessObservation::new(Plane::Consumer);
            let response = match (self.total.acquire(), self.capacity.try_acquire()) {
                (Ok(_total), Ok(_role)) => {
                    match crate::event_dispatch::consumer_execution::execute_sync(
                        &self.registry,
                        input.invocation,
                    )
                    .await
                    {
                        Ok(result) => NativeConsumerResponse::Completed { result },
                        Err(crate::event_dispatch::consumer_execution::ConsumerExecutionError::Handler(error)) => outcome(error),
                        Err(crate::event_dispatch::consumer_execution::ConsumerExecutionError::DeadlineExceeded) => NativeConsumerResponse::TimedOut,
                    }
                }
                _ => NativeConsumerResponse::Throttled {
                    code: "capacity_exhausted".into(),
                    retry_after_ms: None,
                },
            };
            let mut response_outcome = observation::consumer_outcome(&response);
            business.finish(response_outcome);
            let mut payload = serde_json::value::to_raw_value(&response)
                .map_err(|_| ChannelSessionError::InvalidResponse)?;
            if payload.get().len() > MAX_BUSINESS_PAYLOAD_BYTES {
                response_outcome = BusinessOutcome::PermanentFailure;
                payload = serde_json::value::to_raw_value(&NativeConsumerResponse::PermanentFailure {
                    code: "consumer_result_too_large".into(),
                    message: "Consumer result exceeds native protocol limit".into(),
                })
                .map_err(|_| ChannelSessionError::InvalidResponse)?;
            }
            let now = chrono::Utc::now().timestamp_millis();
            let subject = ClientChannelIdentity {
                application_id: initial.application_id.clone(),
                instance_id: route.instance_id.clone(),
                base_generation: route.base_generation.clone(),
                certificate_identity: initial.certificate.certificate_identity.clone(),
            };
            let proof = signer
                .sign_message(
                    &subject,
                    request.kind,
                    target,
                    &request.request_id,
                    payload.get().as_bytes(),
                    now,
                    request.deadline_unix_ms,
                )
                .map_err(|_| ChannelSessionError::InvalidResponse)?;
            let result = TransportEnvelope {
                protocol_version: ProtocolVersion::V1,
                kind: request.kind,
                request_id: request.request_id,
                application_id: request.application_id,
                target: target.clone(),
                deadline_unix_ms: request.deadline_unix_ms,
                proof,
                trace_parent: super::trace::current_trace_parent(),
                payload,
            }
            .encode(now)
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
            if result.len() > MAX_BUSINESS_PAYLOAD_BYTES + MAX_ENVELOPE_OVERHEAD_BYTES {
                return Err(ChannelSessionError::InvalidResponse);
            }
            observation::prepared_consumer_response(response_outcome);
            Ok(result)
        }).await
    }
}
fn outcome(error: ConsumerError) -> NativeConsumerResponse {
    let milliseconds = |duration: Option<std::time::Duration>| {
        duration.map(|d| d.as_millis().min(u64::MAX as u128) as u64)
    };
    match error {
        ConsumerError::Retryable {
            code,
            message,
            retry_after,
            ..
        } => NativeConsumerResponse::RetryableFailure {
            code,
            message,
            retry_after_ms: milliseconds(retry_after),
        },
        ConsumerError::Permanent { code, message, .. } => {
            NativeConsumerResponse::PermanentFailure { code, message }
        }
        ConsumerError::Throttled {
            code, retry_after, ..
        } => NativeConsumerResponse::Throttled {
            code,
            retry_after_ms: milliseconds(retry_after),
        },
    }
}
impl ServiceChannelSessions {
    /// Bind the existing typed registry and the same total budget used by Calls.
    /// Async is deliberately absent; this never spawns accepted business work.
    pub fn enable_sync_consumers(
        &self,
        role: &mut super::RegisteredChannelRole,
        registry: Arc<ConsumerRegistry>,
        budget: ServiceExecutionBudget,
    ) -> Result<(), ChannelSessionError> {
        self.active_role_channel()?;
        let initial = self.identity.bootstrap_response();
        let ChannelRoleEnrollmentResponse::Consumer(response) = &role.response else {
            return Err(ChannelSessionError::InvalidConfig);
        };
        if !role.route_confirmed()
            || !self.owns_role(role)
            || registry.app_id() != initial.application_id.as_str()
            || !registry.supports_group(&response.session.group_key)
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let mut slot = role
            .consumers
            .lock()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        if slot.is_some() {
            return Err(ChannelSessionError::InvalidConfig);
        }
        *slot = Some(Arc::new(NativeConsumerExecution {
            #[cfg(test)]
            received_lanes: Default::default(),
            registry,
            total: budget,
            capacity: tokio::sync::Semaphore::new(
                role.consumer_capacity
                    .ok_or(ChannelSessionError::InvalidConfig)? as usize,
            ),
            group_key: response.session.group_key.clone(),
            group_id: response.session.group_id,
            member_id: response.session.lease.member_id,
            membership_generation: response.session.lease.membership_generation,
        }));
        Ok(())
    }
    pub async fn register_consumer_role(
        &self,
        group_key: impl Into<String>,
        node_id: impl Into<String>,
        maximum_in_flight: u32,
    ) -> Result<super::RegisteredChannelRole, ChannelSessionError> {
        let initial = self.identity.bootstrap_response();
        let instance = self
            .identity
            .connection
            .registration()
            .await
            .request(&initial.instance.instance_id);
        self.register_role(kish_lingshu_runtime_contract::service::ChannelRoleEnrollment::Consumer(
            kish_lingshu_event_dispatch_contract::ConsumerEnrollmentRequestV2 {
                enrollment_version: kish_lingshu_foundation_contract::service_transport::enrollment::EnrollmentVersion::V2,
                instance, group_key: group_key.into(), node_id: node_id.into(), maximum_in_flight,
                endpoint: kish_lingshu_foundation_contract::service_transport::enrollment::RequestedServiceEndpoint::Zenoh { protocol_version: ProtocolVersion::V1, lane_count: (self.lane_count() as u8).try_into().map_err(|_| ChannelSessionError::InvalidConfig)? },
            }
        )).await
    }
}
#[cfg(all(test, feature = "event-publication-zenoh"))]
#[path = "consumer_host_tests.rs"]
mod host_tests;

#[cfg(all(test, feature = "service-call-zenoh"))]
#[path = "consumer_tests.rs"]
pub(crate) mod tests;
