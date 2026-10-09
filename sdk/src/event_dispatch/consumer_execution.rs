//! One bounded business attempt shared by HTTP and native delivery adapters.
use super::{ConsumerError, ConsumerRegistry, EventContext};
use chrono::Utc;
use kish_lingshu_event_dispatch_contract::{
    DeliveryMode, InvocationV1, INVOCATION_CONTRACT_VERSION,
};
use serde_json::Value;

pub(crate) enum ConsumerExecutionError {
    Handler(ConsumerError),
    DeadlineExceeded,
}

impl From<ConsumerError> for ConsumerExecutionError {
    fn from(error: ConsumerError) -> Self {
        Self::Handler(error)
    }
}

pub(crate) async fn execute_sync(
    registry: &ConsumerRegistry,
    invocation: InvocationV1,
) -> Result<Value, ConsumerExecutionError> {
    if invocation.contract_version != INVOCATION_CONTRACT_VERSION
        || invocation.consumption.mode != DeliveryMode::Sync
        || invocation.completion.is_some()
        || invocation.event.app_id != registry.app_id()
        || invocation.event.event_id == 0
        || invocation.consumption.consumption_id == 0
        || invocation.consumption.subscription_id == 0
        || invocation.consumption.group_id == 0
        || invocation.consumption.invocation_id == 0
        || invocation.consumption.attempt_generation == 0
        || invocation.consumption.idempotency_key.trim().is_empty()
    {
        return Err(ConsumerError::permanent(
            "invalid_invocation",
            "invalid synchronous Event invocation",
        )
        .into());
    }
    let consumer = registry.find(&invocation).ok_or_else(|| {
        ConsumerError::permanent(
            "event_consumer_not_registered",
            "no Event consumer is registered for the Event selector",
        )
    })?;
    let timeout = (invocation.consumption.invocation_deadline - Utc::now())
        .to_std()
        .ok()
        .filter(|timeout| !timeout.is_zero())
        .ok_or(ConsumerExecutionError::DeadlineExceeded)?;
    let context = EventContext::from_invocation(&invocation);
    tokio::time::timeout(timeout, consumer.consume(context, invocation.event.payload))
        .await
        .map_err(|_| ConsumerExecutionError::DeadlineExceeded)?
        .map_err(ConsumerExecutionError::Handler)
}
