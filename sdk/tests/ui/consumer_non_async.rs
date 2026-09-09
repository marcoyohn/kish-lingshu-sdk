use kish_lingshu_sdk::{
    event_dispatch::{ConsumerError, EventContext},
    event_dispatch, EventPayload,
};
use schemars::JsonSchema;
use serde::Serialize;

#[derive(EventPayload, JsonSchema, Serialize)]
#[event(
    key = "orders.created",
    topic = "orders",
    event_type = "order.created",
    schema_version = "1"
)]
struct OrderCreated;

struct Handlers;

#[event_dispatch(group = "order-workers")]
impl Handlers {
    #[event_consumer(key = "orders.on-created", event = OrderCreated)]
    fn on_created(
        &self,
        _context: EventContext,
        _event: OrderCreated,
    ) -> Result<(), ConsumerError> {
        Ok(())
    }
}

fn main() {}
