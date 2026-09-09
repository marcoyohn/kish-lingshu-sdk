use std::{
    any::{type_name, Any, TypeId},
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use kish_lingshu_event_dispatch_contract::{
    ConsumerDefinition, ConsumerPolicy, DeliveryMode, DispatchPolicy, EventSelector, ExecutionMode,
    InvocationV1, OrderingScope, PausePolicy, RateLimitPolicy, RetryPolicy,
    ScheduleEventMetadataV1, ThrottlePolicy, TimeoutPolicy,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use super::SourceCatalogError;

/// Exact Topic/Event-type route used by the invocation registry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConsumerSelector {
    topic: String,
    event_type: String,
}

impl ConsumerSelector {
    pub fn new(
        topic: impl Into<String>,
        event_type: impl Into<String>,
    ) -> Result<Self, ConsumerRegistryError> {
        let selector = Self {
            topic: topic.into(),
            event_type: event_type.into(),
        };
        validate_selector("topic", &selector.topic)?;
        validate_selector("event_type", &selector.event_type)?;
        Ok(selector)
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn event_type(&self) -> &str {
        &self.event_type
    }
}

#[doc(hidden)]
pub type EventDispatchHandlerFuture =
    Pin<Box<dyn Future<Output = Result<Value, ConsumerError>> + Send + 'static>>;

#[doc(hidden)]
pub type EventDispatchHandlerInvokeFn =
    fn(Arc<dyn Any + Send + Sync>, EventContext, Value) -> EventDispatchHandlerFuture;

/// One linked descriptor drives both manifest export and invocation routing.
#[doc(hidden)]
pub struct EventDispatchHandlerDescriptor {
    pub consumer_key: &'static str,
    pub group_key: &'static str,
    pub event_key: &'static str,
    pub topic: &'static str,
    pub event_type: &'static str,
    pub maximum_concurrency: u32,
    pub handler_type_id: fn() -> TypeId,
    pub handler_type_name: fn() -> &'static str,
    pub diagnostic_name: &'static str,
    pub invoke: EventDispatchHandlerInvokeFn,
}

inventory::collect!(EventDispatchHandlerDescriptor);

/// Business-useful invocation facts. Queue and callback internals stay hidden.
#[derive(Debug, Clone)]
pub struct EventContext {
    app_id: String,
    event_id: u64,
    source: String,
    subject: Option<String>,
    occurred_at: DateTime<Utc>,
    published_at: DateTime<Utc>,
    not_before: DateTime<Utc>,
    partition_key: Option<String>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    headers: BTreeMap<String, Value>,
    schedule: Option<ScheduleEventMetadataV1>,
    idempotency_key: String,
    attempt_generation: u64,
    invocation_deadline: DateTime<Utc>,
    traceparent: Option<String>,
    tracestate: Option<String>,
}

impl EventContext {
    pub fn app_id(&self) -> &str {
        &self.app_id
    }

    pub fn event_id(&self) -> u64 {
        self.event_id
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }

    pub fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    pub fn published_at(&self) -> DateTime<Utc> {
        self.published_at
    }

    pub fn not_before(&self) -> DateTime<Utc> {
        self.not_before
    }

    pub fn partition_key(&self) -> Option<&str> {
        self.partition_key.as_deref()
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    pub fn causation_id(&self) -> Option<&str> {
        self.causation_id.as_deref()
    }

    pub fn headers(&self) -> &BTreeMap<String, Value> {
        &self.headers
    }

    pub fn schedule(&self) -> Option<&ScheduleEventMetadataV1> {
        self.schedule.as_ref()
    }

    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    pub fn attempt_generation(&self) -> u64 {
        self.attempt_generation
    }

    pub fn invocation_deadline(&self) -> DateTime<Utc> {
        self.invocation_deadline
    }

    pub fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }

    pub(super) fn from_invocation(invocation: &InvocationV1) -> Self {
        Self {
            app_id: invocation.event.app_id.clone(),
            event_id: invocation.event.event_id,
            source: invocation.event.source.clone(),
            subject: invocation.event.subject.clone(),
            occurred_at: invocation.event.occurred_at,
            published_at: invocation.event.published_at,
            not_before: invocation.event.not_before,
            partition_key: invocation.event.partition_key.clone(),
            correlation_id: invocation.event.correlation_id.clone(),
            causation_id: invocation.event.causation_id.clone(),
            headers: invocation.event.headers.clone(),
            schedule: invocation.event.schedule.clone(),
            idempotency_key: invocation.consumption.idempotency_key.clone(),
            attempt_generation: invocation.consumption.attempt_generation,
            invocation_deadline: invocation.consumption.invocation_deadline,
            traceparent: invocation.trace.traceparent.clone(),
            tracestate: invocation.trace.tracestate.clone(),
        }
    }
}

#[async_trait]
pub trait EventConsumer: Send + Sync + 'static {
    type Event: DeserializeOwned + Send + 'static;
    type Output: Serialize + Send + 'static;

    fn selector(&self) -> ConsumerSelector;

    async fn consume(
        &self,
        context: EventContext,
        event: Self::Event,
    ) -> Result<Self::Output, ConsumerError>;
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ConsumerError {
    #[error("{code}: {message}")]
    Retryable {
        code: String,
        message: String,
        retry_after: Option<Duration>,
        details: Option<Value>,
    },
    #[error("{code}: {message}")]
    Permanent {
        code: String,
        message: String,
        details: Option<Value>,
    },
    #[error("{code}: {message}")]
    Throttled {
        code: String,
        message: String,
        retry_after: Option<Duration>,
        details: Option<Value>,
    },
}

impl ConsumerError {
    pub fn retryable(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Retryable {
            code: code.into(),
            message: message.into(),
            retry_after: None,
            details: None,
        }
    }

    pub fn retryable_after(
        code: impl Into<String>,
        message: impl Into<String>,
        retry_after: Duration,
    ) -> Self {
        Self::Retryable {
            code: code.into(),
            message: message.into(),
            retry_after: Some(retry_after),
            details: None,
        }
    }

    pub fn permanent(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Permanent {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }

    pub fn throttled(
        code: impl Into<String>,
        message: impl Into<String>,
        retry_after: Option<Duration>,
    ) -> Self {
        Self::Throttled {
            code: code.into(),
            message: message.into(),
            retry_after,
            details: None,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConsumerRegistryError {
    #[error("application id must be non-empty")]
    EmptyApplicationId,
    #[error("Event consumer {field} must be non-empty, bounded ASCII without whitespace")]
    InvalidSelector { field: &'static str },
    #[error("Event consumer is already registered for {topic}/{event_type}")]
    DuplicateConsumer { topic: String, event_type: String },
    #[error("Event Dispatch Handler type {handler_type} is already bound")]
    DuplicateBinding { handler_type: String },
    #[error("annotated Event Dispatch Handler type {handler_type} has no bound instance")]
    MissingBinding { handler_type: String },
    #[error("bound Event Dispatch Handler type {handler_type} has no linked annotations")]
    UnusedBinding { handler_type: String },
}

pub struct ConsumerRegistry {
    app_id: String,
    consumers: HashMap<ConsumerSelector, Arc<dyn ErasedConsumer>>,
}

impl ConsumerRegistry {
    pub fn new(app_id: impl Into<String>) -> Result<Self, ConsumerRegistryError> {
        let app_id = app_id.into();
        if app_id.trim().is_empty() {
            return Err(ConsumerRegistryError::EmptyApplicationId);
        }
        Ok(Self {
            app_id,
            consumers: HashMap::new(),
        })
    }

    pub fn builder(
        app_id: impl Into<String>,
    ) -> Result<ConsumerRegistryBuilder, ConsumerRegistryError> {
        Ok(ConsumerRegistryBuilder {
            registry: Self::new(app_id)?,
            bindings: HashMap::new(),
        })
    }

    pub fn register<C: EventConsumer>(
        &mut self,
        consumer: C,
    ) -> Result<&mut Self, ConsumerRegistryError> {
        let selector = consumer.selector();
        validate_selector("topic", selector.topic())?;
        validate_selector("event_type", selector.event_type())?;
        if self.consumers.contains_key(&selector) {
            return Err(ConsumerRegistryError::DuplicateConsumer {
                topic: selector.topic,
                event_type: selector.event_type,
            });
        }
        self.consumers
            .insert(selector, Arc::new(RegisteredConsumer(consumer)));
        Ok(self)
    }

    pub fn app_id(&self) -> &str {
        &self.app_id
    }

    pub(super) fn find(&self, invocation: &InvocationV1) -> Option<Arc<dyn ErasedConsumer>> {
        self.consumers
            .get(&ConsumerSelector {
                topic: invocation.event.topic.clone(),
                event_type: invocation.event.event_type.clone(),
            })
            .cloned()
    }
}

struct BoundEventDispatchHandler {
    type_name: &'static str,
    instance: Arc<dyn Any + Send + Sync>,
}

pub struct ConsumerRegistryBuilder {
    registry: ConsumerRegistry,
    bindings: HashMap<TypeId, BoundEventDispatchHandler>,
}

impl ConsumerRegistryBuilder {
    pub fn bind<T>(&mut self, handler: Arc<T>) -> Result<&mut Self, ConsumerRegistryError>
    where
        T: Any + Send + Sync + 'static,
    {
        let handler_type_id = TypeId::of::<T>();
        let handler_type = type_name::<T>();
        if self.bindings.contains_key(&handler_type_id) {
            return Err(ConsumerRegistryError::DuplicateBinding {
                handler_type: handler_type.to_string(),
            });
        }
        let instance: Arc<dyn Any + Send + Sync> = handler;
        self.bindings.insert(
            handler_type_id,
            BoundEventDispatchHandler {
                type_name: handler_type,
                instance,
            },
        );
        Ok(self)
    }

    pub fn build(self) -> Result<ConsumerRegistry, ConsumerRegistryError> {
        self.build_inner(false)
    }

    pub fn build_bound_handlers(self) -> Result<ConsumerRegistry, ConsumerRegistryError> {
        self.build_inner(true)
    }

    fn build_inner(
        mut self,
        bound_handlers_only: bool,
    ) -> Result<ConsumerRegistry, ConsumerRegistryError> {
        let mut descriptors = inventory::iter::<EventDispatchHandlerDescriptor>
            .into_iter()
            .collect::<Vec<_>>();
        if bound_handlers_only {
            descriptors
                .retain(|descriptor| self.bindings.contains_key(&(descriptor.handler_type_id)()));
        }
        descriptors.sort_by_key(|descriptor| {
            (
                descriptor.topic,
                descriptor.event_type,
                descriptor.diagnostic_name,
            )
        });
        let mut described_types = HashSet::new();

        for descriptor in descriptors {
            validate_selector("topic", descriptor.topic)?;
            validate_selector("event_type", descriptor.event_type)?;
            let handler_type_id = (descriptor.handler_type_id)();
            described_types.insert(handler_type_id);
            let binding = self.bindings.get(&handler_type_id).ok_or_else(|| {
                ConsumerRegistryError::MissingBinding {
                    handler_type: (descriptor.handler_type_name)().to_string(),
                }
            })?;
            let selector = ConsumerSelector::new(descriptor.topic, descriptor.event_type)?;
            if self.registry.consumers.contains_key(&selector) {
                return Err(ConsumerRegistryError::DuplicateConsumer {
                    topic: selector.topic,
                    event_type: selector.event_type,
                });
            }
            self.registry.consumers.insert(
                selector,
                Arc::new(LinkedConsumer {
                    instance: binding.instance.clone(),
                    invoke: descriptor.invoke,
                }),
            );
        }

        let mut unused_bindings = self
            .bindings
            .iter()
            .filter_map(|(handler_type_id, binding)| {
                (!described_types.contains(handler_type_id)).then_some(binding.type_name)
            })
            .collect::<Vec<_>>();
        unused_bindings.sort_unstable();
        if let Some(handler_type) = unused_bindings.first() {
            return Err(ConsumerRegistryError::UnusedBinding {
                handler_type: (*handler_type).to_string(),
            });
        }

        Ok(self.registry)
    }
}

#[async_trait]
pub(super) trait ErasedConsumer: Send + Sync {
    async fn consume(&self, context: EventContext, payload: Value) -> Result<Value, ConsumerError>;
}

struct RegisteredConsumer<C>(C);

#[async_trait]
impl<C: EventConsumer> ErasedConsumer for RegisteredConsumer<C> {
    async fn consume(&self, context: EventContext, payload: Value) -> Result<Value, ConsumerError> {
        let event = serde_json::from_value(payload).map_err(|error| {
            ConsumerError::permanent(
                "invalid_event_payload",
                format!("invalid Event payload: {error}"),
            )
        })?;
        let output = self.0.consume(context, event).await?;
        serde_json::to_value(output).map_err(|error| {
            ConsumerError::retryable(
                "event_result_serialization_failed",
                format!("failed to serialize Event result: {error}"),
            )
        })
    }
}

struct LinkedConsumer {
    instance: Arc<dyn Any + Send + Sync>,
    invoke: EventDispatchHandlerInvokeFn,
}

#[async_trait]
impl ErasedConsumer for LinkedConsumer {
    async fn consume(&self, context: EventContext, payload: Value) -> Result<Value, ConsumerError> {
        (self.invoke)(self.instance.clone(), context, payload).await
    }
}

pub(super) fn linked_consumer_definitions() -> Result<Vec<ConsumerDefinition>, SourceCatalogError> {
    Ok(inventory::iter::<EventDispatchHandlerDescriptor>
        .into_iter()
        .map(|descriptor| {
            default_consumer_definition(
                descriptor.consumer_key,
                descriptor.group_key,
                descriptor.maximum_concurrency,
                EventSelector {
                    event_key: descriptor.event_key.to_string(),
                    topic: descriptor.topic.to_string(),
                    event_type: descriptor.event_type.to_string(),
                },
            )
        })
        .collect())
}

pub(super) fn default_consumer_definition(
    key: impl Into<String>,
    group_key: impl Into<String>,
    maximum_concurrency: u32,
    selector: EventSelector,
) -> ConsumerDefinition {
    ConsumerDefinition {
        key: key.into(),
        group_key: group_key.into(),
        selectors: vec![selector],
        delivery_mode: DeliveryMode::Sync,
        maximum_concurrency,
        policy: ConsumerPolicy {
            rate_limit: None::<RateLimitPolicy>,
            retry: RetryPolicy {
                maximum_failure_attempts: 5,
                initial_delay_milliseconds: 5_000,
                maximum_delay_milliseconds: 300_000,
                multiplier: 2.0,
                jitter_ratio: 0.2,
            },
            throttle: ThrottlePolicy {
                minimum_cooldown_milliseconds: 1_000,
                maximum_cooldown_milliseconds: 300_000,
                maximum_throttle_duration_milliseconds: 3_600_000,
                half_open_probe_limit: 1,
            },
            timeout: TimeoutPolicy {
                invocation_milliseconds: 10_000,
                completion_milliseconds: 10_000,
                maximum_completion_milliseconds: 86_400_000,
            },
            dispatch: DispatchPolicy {
                ordering_scope: OrderingScope::Partition,
                mode: ExecutionMode::Serial,
            },
            pause: PausePolicy::Retain,
        },
    }
}

fn validate_selector(field: &'static str, value: &str) -> Result<(), ConsumerRegistryError> {
    if value.is_empty()
        || value.len() > 160
        || !value.is_ascii()
        || value.chars().any(char::is_whitespace)
    {
        return Err(ConsumerRegistryError::InvalidSelector { field });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopConsumer;

    #[async_trait]
    impl EventConsumer for NoopConsumer {
        type Event = ();
        type Output = ();

        fn selector(&self) -> ConsumerSelector {
            ConsumerSelector::new("system.events", "system.noop.requested").unwrap()
        }

        async fn consume(
            &self,
            _context: EventContext,
            _event: Self::Event,
        ) -> Result<Self::Output, ConsumerError> {
            Ok(())
        }
    }

    #[test]
    fn registry_rejects_duplicate_event_selector() {
        let mut registry = ConsumerRegistry::new("application-1").unwrap();
        registry.register(NoopConsumer).unwrap();
        assert!(matches!(
            registry.register(NoopConsumer),
            Err(ConsumerRegistryError::DuplicateConsumer { .. })
        ));
    }
}
