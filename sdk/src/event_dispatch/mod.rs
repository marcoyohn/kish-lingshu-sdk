//! Event publication, source declarations, manifest export, and consumption.

use std::sync::Arc;

use crate::{
    client::ClientInner, Error, MutationOptions, ProtocolDirection, ProtocolError, RequestOptions,
};

#[cfg(feature = "event-consumer")]
mod consumer;
#[cfg(feature = "event-consumer-http")]
mod consumer_http;
#[cfg(feature = "event-consumer-http")]
mod consumer_node;
#[cfg(feature = "event-manifest")]
mod declaration;
mod reliable;

#[cfg(feature = "event-consumer")]
pub use consumer::{
    ConsumerError, ConsumerRegistry, ConsumerRegistryBuilder, ConsumerRegistryError,
    ConsumerSelector, EventConsumer, EventContext,
};
#[cfg(feature = "event-consumer-http")]
pub use consumer_http::{ConsumerHttpAdapter, ConsumerHttpConfig};
#[cfg(feature = "event-consumer-http")]
pub use consumer_node::{
    ConsumerNode, ConsumerNodeConfig, ConsumerNodeConfigError, ConsumerNodeError,
    ConsumerNodeFailure, ConsumerNodeStatus,
};
#[cfg(feature = "event-manifest")]
pub use declaration::{SourceCatalog, SourceCatalogError};
pub use reliable::{
    DurableEventPublication, EventPublicationFailure, EventPublicationFailureKind,
    EventPublicationJournal, EventPublicationJournalError, EventPublicationJournalState,
    EventPublicationOutcome, EventPublicationPolicyCatalog, EventPublicationPolicyError,
    EventPublicationRecoveryResult, EventPublicationReliability, EventPublicationRoute,
    PendingEventPublication, ReliableEventPublisher, ReliablePublicationConfig,
    ReliablePublicationConfigError, ReliablePublicationError, TransactionalEventPublisher,
};

pub use kish_lingshu_event_dispatch_contract::{
    ConsumerDefinition, ConsumerPolicy, ConsumptionOrder, DeliveryMode, DeliveryTime,
    DigestAlgorithm, DispatchPolicy, DynamicEvent, EnvironmentBindingRequirement, EventDefinition,
    EventDispatchManifestV1, EventId, EventPayload, EventRecord, EventRoute, EventSelector,
    EventVisibility, ExecutionMode, IntervalBasis, JobDefinition, JobEventTemplate, JobTrigger,
    ManifestDigest, ManifestError, MisfirePolicy, OrderingScope, OverlapPolicy, PausePolicy,
    ProducerMetadata, PublishEvent, PublishEventBuildError, PublishReceipt, RateLimitPolicy,
    RetryPolicy, ThrottlePolicy, TimeoutPolicy, TopicDefaults,
};

#[cfg(feature = "event-manifest")]
#[doc(hidden)]
pub mod __private {
    #[cfg(feature = "event-consumer")]
    pub use super::consumer::{
        EventDispatchHandlerDescriptor, EventDispatchHandlerFuture, EventDispatchHandlerInvokeFn,
    };
    pub use super::declaration::{EventDefinitionDescriptor, JobDefinitionDescriptor};
    pub use chrono;
    pub use inventory;
    pub use schemars;
    pub use serde_json;
}

#[cfg(feature = "event-consumer")]
pub use kish_lingshu_sdk_macros::{event_consumer, event_dispatch};
#[cfg(feature = "event-manifest")]
pub use kish_lingshu_sdk_macros::{event_job, EventPayload};

#[derive(Clone)]
pub struct EventDispatch {
    pub(crate) inner: Arc<ClientInner>,
}

impl EventDispatch {
    pub(crate) fn new(inner: Arc<ClientInner>) -> Self {
        Self { inner }
    }

    /// Publishes an Event and returns only after Kish Lingshu confirms custody.
    ///
    /// This direct call uses bounded in-call retry but does not make the Event
    /// atomic with a producer business transaction. Use [`Self::reliable_publisher`]
    /// and a durable journal when that crash window must be closed.
    pub async fn publish(
        &self,
        event: PublishEvent,
        options: MutationOptions,
    ) -> Result<PublishReceipt, Error> {
        event
            .validate()
            .map_err(|error| Error::configuration("event", error.to_string()))?;
        self.inner.binding.publish_event(event, options).await
    }

    async fn publish_best_effort(
        &self,
        event: PublishEvent,
        options: MutationOptions,
    ) -> Result<PublishReceipt, Error> {
        self.publish(event, options.without_retry()).await
    }

    /// Builds and publishes an Event whose route is declared by its payload type.
    pub async fn publish_typed<T: EventPayload>(
        &self,
        source: impl Into<String>,
        payload: &T,
        options: MutationOptions,
    ) -> Result<PublishReceipt, Error> {
        let event = PublishEvent::typed(source, payload).map_err(|error| match error {
            PublishEventBuildError::Payload(error) => Error::Protocol(ProtocolError {
                direction: ProtocolDirection::EncodeRequest,
                message: error.to_string(),
                request_id: Some(options.request().request_id().clone()),
            }),
            other => Error::configuration("event", other.to_string()),
        })?;
        self.publish(event, options).await
    }

    /// Publishes a validated dynamic JSON Event.
    pub async fn publish_dynamic(
        &self,
        source: impl Into<String>,
        event: DynamicEvent,
        options: MutationOptions,
    ) -> Result<PublishReceipt, Error> {
        self.publish(PublishEvent::dynamic(source, event), options)
            .await
    }

    /// Looks up one Event within the authenticated Application scope.
    pub async fn get(
        &self,
        event_id: EventId,
        options: RequestOptions,
    ) -> Result<Option<EventRecord>, Error> {
        self.inner.binding.get_event(event_id, options).await
    }
}
