#![cfg(feature = "event-consumer-http")]

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::Utc;
use kish_lingshu_event_dispatch_contract::{
    DeliveryMode, EventEnvelopeV1, InvocationConsumptionV1, InvocationTraceV1, InvocationV1,
    OverlapPolicy, ScheduleEventMetadataV1, IDEMPOTENCY_HEADER, INVOCATION_CONTRACT_VERSION,
    INVOCATION_ID_HEADER,
};
use kish_lingshu_sdk::{
    event_dispatch,
    event_dispatch::{
        ConsumerError, ConsumerHttpAdapter, ConsumerRegistry, EventContext, EventPayload,
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Deserialize, EventPayload, JsonSchema, Serialize)]
#[event(
    key = "orders.expire-requested",
    topic = "orders",
    event_type = "order.expire.requested",
    schema_version = "1",
    topic_name = "Orders"
)]
struct ExpireOrders {
    maximum_age_days: u32,
}

#[derive(Default)]
struct ScheduledHandlers {
    calls: AtomicUsize,
}

#[event_dispatch(group = "order-workers", maximum_concurrency = 8)]
impl ScheduledHandlers {
    #[event_consumer(key = "orders.on-expire-requested", event = ExpireOrders)]
    async fn expire(
        &self,
        context: EventContext,
        event: ExpireOrders,
    ) -> Result<Value, ConsumerError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let schedule = context.schedule().expect("scheduled Event metadata");
        Ok(json!({
            "schedule_key": schedule.schedule_key,
            "maximum_age_days": event.maximum_age_days
        }))
    }
}

fn invocation(mode: DeliveryMode, deadline_offset_seconds: i64) -> InvocationV1 {
    let now = Utc::now();
    InvocationV1 {
        contract_version: INVOCATION_CONTRACT_VERSION.to_string(),
        event: EventEnvelopeV1 {
            event_id: 1001,
            app_id: "orders-app".to_string(),
            topic: ExpireOrders::TOPIC.to_string(),
            event_type: ExpireOrders::EVENT_TYPE.to_string(),
            schema_version: "2".to_string(),
            source: "orders.expire".to_string(),
            subject: None,
            occurred_at: now,
            published_at: now,
            not_before: now,
            partition_key: None,
            correlation_id: Some("correlation-1".to_string()),
            causation_id: None,
            headers: Default::default(),
            payload: json!({"maximum_age_days": 30}),
            schedule: Some(ScheduleEventMetadataV1 {
                schedule_id: 9001,
                scheduled_at: now,
                schedule_key: "orders.expire".to_string(),
                overlap_policy: OverlapPolicy::Serialize,
            }),
        },
        consumption: InvocationConsumptionV1 {
            consumption_id: 2001,
            subscription_id: 3001,
            group_id: 4001,
            subscription_epoch: 1,
            queue_epoch: 1,
            queue_id: 0,
            queue_offset: 1,
            invocation_id: 5001,
            attempt_generation: 1,
            mode,
            idempotency_key: "consumption/orders/1001".to_string(),
            invocation_deadline: now + chrono::Duration::seconds(deadline_offset_seconds),
        },
        completion: None,
        trace: InvocationTraceV1::default(),
    }
}

async fn start_server(
    handlers: Arc<ScheduledHandlers>,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut builder = ConsumerRegistry::builder("orders-app").unwrap();
    builder.bind(handlers).unwrap();
    let app = ConsumerHttpAdapter::new(Arc::new(builder.build().unwrap())).router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (address, task)
}

#[tokio::test]
async fn scheduled_events_use_normal_exact_route_and_expose_schedule_context() {
    let handlers = Arc::new(ScheduledHandlers::default());
    let (address, server) = start_server(handlers.clone()).await;
    let response = reqwest::Client::new()
        .post(format!("http://{address}/"))
        .header(IDEMPOTENCY_HEADER, "consumption/orders/1001")
        .header(INVOCATION_ID_HEADER, "5001")
        .json(&invocation(DeliveryMode::Sync, 10))
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({"result": {"schedule_key": "orders.expire", "maximum_age_days": 30}})
    );
    assert_eq!(handlers.calls.load(Ordering::Relaxed), 1);
    server.abort();
}

#[tokio::test]
async fn invalid_identity_deadline_and_async_mode_are_rejected_before_handler_execution() {
    let handlers = Arc::new(ScheduledHandlers::default());
    let (address, server) = start_server(handlers.clone()).await;
    let client = reqwest::Client::new();

    let mismatch = client
        .post(format!("http://{address}/"))
        .header(IDEMPOTENCY_HEADER, "different")
        .header(INVOCATION_ID_HEADER, "5001")
        .json(&invocation(DeliveryMode::Sync, 10))
        .send()
        .await
        .unwrap();
    assert_eq!(mismatch.status(), reqwest::StatusCode::BAD_REQUEST);

    let expired = client
        .post(format!("http://{address}/"))
        .header(IDEMPOTENCY_HEADER, "consumption/orders/1001")
        .header(INVOCATION_ID_HEADER, "5001")
        .json(&invocation(DeliveryMode::Sync, -1))
        .send()
        .await
        .unwrap();
    assert_eq!(expired.status(), reqwest::StatusCode::REQUEST_TIMEOUT);

    let asynchronous = client
        .post(format!("http://{address}/"))
        .header(IDEMPOTENCY_HEADER, "consumption/orders/1001")
        .header(INVOCATION_ID_HEADER, "5001")
        .json(&invocation(DeliveryMode::Async, 10))
        .send()
        .await
        .unwrap();
    assert_eq!(
        asynchronous.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(handlers.calls.load(Ordering::Relaxed), 0);
    server.abort();
}
