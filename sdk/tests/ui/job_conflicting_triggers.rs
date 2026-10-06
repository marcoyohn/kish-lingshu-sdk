use kish_lingshu_sdk::{event_job, EventPayload};
use schemars::JsonSchema;
use serde::Serialize;

#[derive(EventPayload, JsonSchema, Serialize)]
#[event(
    key = "orders.expire-requested",
    topic = "orders",
    event_type = "order.expire.requested",
    schema_version = "1"
)]
struct ExpireOrders;

#[event_job(
    key = "orders.expire",
    event = ExpireOrders,
    cron = "0 0 * * * *",
    once = "2030-01-01T00:00:00Z"
)]
fn expire_orders() -> ExpireOrders {
    ExpireOrders
}

fn main() {}
