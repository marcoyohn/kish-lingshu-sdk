use async_trait::async_trait;
use kish_lingshu_event_dispatch_contract::{EventId, EventRecord, PublishEvent, PublishReceipt};

use crate::{ApplicationResult, IdempotencyKey, RequestContext};

/// Application-scoped outbound Event publication and receipt lookup.
#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish(
        &self,
        context: RequestContext,
        event: PublishEvent,
        idempotency_key: IdempotencyKey,
    ) -> ApplicationResult<PublishReceipt>;

    async fn get(
        &self,
        context: RequestContext,
        event_id: EventId,
    ) -> ApplicationResult<Option<EventRecord>>;
}
