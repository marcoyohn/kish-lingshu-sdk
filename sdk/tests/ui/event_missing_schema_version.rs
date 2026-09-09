use kish_lingshu_sdk::EventPayload;

#[derive(EventPayload)]
#[event(
    key = "orders.created",
    topic = "orders",
    event_type = "order.created"
)]
struct OrderCreated;

fn main() {}
