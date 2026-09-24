//! Compatibility bridge into the existing Event consumer registry. Event
//! transport and group routing remain owned by Event Dispatch.
use super::*;
use crate::event_dispatch::{ConsumerError, ConsumerRegistry, ConsumerSelector, EventConsumer};
use futures::FutureExt;

#[derive(Clone)]
pub(super) struct EventAdmission {
    pub cancel: tokio::sync::watch::Receiver<bool>,
    pub connection: crate::ServiceConnection,
    pub active: Arc<std::sync::atomic::AtomicUsize>,
    pub memberships: Arc<
        std::sync::RwLock<
            BTreeMap<
                String,
                tokio::sync::watch::Receiver<crate::event_dispatch::EnrolledConsumerNodeStatus>,
            >,
        >,
    >,
}
impl EventAdmission {
    pub(super) fn has_live_membership(&self) -> bool {
        self.memberships.read().is_ok_and(|memberships| {
            memberships.values().any(|status| {
                let status = status.borrow();
                !status.is_terminal() && status.lease().lease_expires_at > chrono::Utc::now()
            })
        })
    }
    fn membership(
        &self,
        group: &str,
    ) -> Result<
        tokio::sync::watch::Receiver<crate::event_dispatch::EnrolledConsumerNodeStatus>,
        ConsumerError,
    > {
        if *self.cancel.borrow() || self.connection.ensure_open().is_err() {
            return Err(ConsumerError::retryable(
                "service_stopping",
                "Service stopped",
            ));
        }
        let status = self
            .memberships
            .read()
            .ok()
            .and_then(|m| m.get(group).cloned())
            .ok_or_else(|| {
                ConsumerError::retryable("event_not_ready", "Event membership is not registered")
            })?;
        if status.borrow().is_terminal()
            || status.borrow().lease().lease_expires_at <= chrono::Utc::now()
        {
            return Err(ConsumerError::retryable(
                "event_not_ready",
                "Event membership expired",
            ));
        }
        Ok(status)
    }
}
async fn membership_stopped(
    mut status: tokio::sync::watch::Receiver<crate::event_dispatch::EnrolledConsumerNodeStatus>,
) {
    loop {
        let value = status.borrow_and_update().clone();
        if value.is_terminal() {
            return;
        }
        let remaining = (value.lease().lease_expires_at - chrono::Utc::now())
            .to_std()
            .unwrap_or_default();
        tokio::select! {_=tokio::time::sleep(remaining)=>return,r=status.changed()=>if r.is_err(){return;}}
    }
}

struct ServiceEventConsumer {
    admission: EventAdmission,
    registry: Arc<ServiceRegistry>,
    reference: OperationRef,
    binding: EventBinding,
    total: Arc<tokio::sync::Semaphore>,
    operation: Arc<tokio::sync::Semaphore>,
}
#[async_trait::async_trait]
impl EventConsumer for ServiceEventConsumer {
    type Event = Value;
    type Output = Value;
    fn selector(&self) -> ConsumerSelector {
        ConsumerSelector::new(&self.binding.topic, &self.binding.event_type)
            .expect("validated route")
            .with_consumer_group(&self.binding.consumer_group)
            .expect("validated group")
    }
    async fn consume(
        &self,
        event: crate::event_dispatch::EventContext,
        input: Value,
    ) -> Result<Value, ConsumerError> {
        let membership = self.admission.membership(&self.binding.consumer_group)?;
        let mut cancel = self.admission.cancel.clone();
        let mut closed = self.admission.connection.subscribe_closed();
        let _total = self.total.clone().try_acquire_owned().map_err(|_| {
            ConsumerError::throttled(
                "capacity_exhausted",
                "Service instance capacity exhausted",
                None,
            )
        })?;
        let _operation = self.operation.clone().try_acquire_owned().map_err(|_| {
            ConsumerError::throttled(
                "capacity_exhausted",
                "Service operation capacity exhausted",
                None,
            )
        })?;
        self.admission
            .active
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let _active = super::http::ActiveGuard(self.admission.active.clone());
        let invocation = ServiceInvocation {
            admission: None,
            target_instance: None,
            contract_version: SERVICE_CONTRACT_VERSION,
            operation: self.reference.clone(),
            context: ServiceContext {
                application_id: event.app_id().into(),
                idempotency_key: event.idempotency_key().into(),
                deadline_ms: event.invocation_deadline().timestamp_millis(),
                trace_id: event.traceparent().map(str::to_owned),
                invocation: InvocationRole::Event(EventContext {
                    event_id: event.event_id().to_string(),
                    source: event.source().into(),
                    occurred_at_ms: event.occurred_at().timestamp_millis(),
                    binding: self.binding.clone(),
                }),
            },
            input,
            completion: None,
        };
        let deadline = std::time::Duration::from_millis(
            (invocation.context.deadline_ms - chrono::Utc::now().timestamp_millis()).max(0) as u64,
        );
        let outcome = tokio::select! {
            _=cancel.changed()=>return Err(ConsumerError::retryable("service_stopping","Service stopped")),
            _=closed.changed()=>return Err(ConsumerError::retryable("service_stopping","Service connection closed")),
            _=membership_stopped(membership)=>return Err(ConsumerError::retryable("event_not_ready","Event membership ended")),
            result=tokio::time::timeout(deadline,std::panic::AssertUnwindSafe(self.registry.invoke(invocation)).catch_unwind())=>result.map_err(|_|ConsumerError::retryable("deadline_exceeded","Event handler timed out"))?.map_err(|_|ConsumerError::retryable("handler_panicked","Event handler failed"))?,
        };
        match outcome {
            ServiceOutcome::Succeeded { result } => Ok(result),
            ServiceOutcome::Failed { error } => Err(if error.retryable {
                ConsumerError::Retryable {
                    code: error.code,
                    message: error.message,
                    retry_after: None,
                    details: error.details,
                }
            } else {
                ConsumerError::Permanent {
                    code: error.code,
                    message: error.message,
                    details: error.details,
                }
            }),
        }
    }
}
impl ServiceRegistry {
    /// The same bound method can be installed as an Event consumer without
    /// manufacturing Event fields for Call invocations.
    pub(super) fn event_registry(
        self: &Arc<Self>,
        admission: EventAdmission,
        total: Arc<tokio::sync::Semaphore>,
        operations: &BTreeMap<OperationRef, Arc<tokio::sync::Semaphore>>,
    ) -> Result<ConsumerRegistry, ServiceError> {
        let mut consumers = ConsumerRegistry::new(&self.manifest.application_id)
            .map_err(|e| ServiceError::rejected("invalid_application", e.to_string()))?;
        for operation in self.operations.values() {
            for binding in &operation.definition.events {
                consumers
                    .register(ServiceEventConsumer {
                        admission: admission.clone(),
                        registry: self.clone(),
                        reference: operation.reference.clone(),
                        binding: binding.clone(),
                        total: total.clone(),
                        operation: operations[&operation.reference].clone(),
                    })
                    .map_err(|e| {
                        ServiceError::rejected("duplicate_event_handler", e.to_string())
                    })?;
            }
        }
        Ok(consumers)
    }
}
