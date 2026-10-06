#![cfg(feature = "event-consumer-http")]

use axum::{body::Body, http::Request};
use kish_lingshu_sdk::{
    event_dispatch,
    event_dispatch::{
        ConsumerError, ConsumerHttpAdapter, ConsumerRegistry, EventContext, SourceCatalog,
    },
    EventPayload,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tower::ServiceExt;

#[derive(Deserialize, Serialize, JsonSchema, EventPayload)]
#[event(
    key = "grouped.changed",
    topic = "grouped",
    event_type = "grouped.changed",
    schema_version = "1",
    topic_name = "Grouped"
)]
struct Changed {
    value: u32,
}

#[derive(Default)]
struct Handlers {
    a: AtomicUsize,
    b: AtomicUsize,
}

#[event_dispatch(maximum_concurrency = 4)]
impl Handlers {
    #[event_consumer(consumer_group="projection-a", event=Changed)]
    async fn a(&self, _: EventContext, _: Changed) -> Result<(), ConsumerError> {
        self.a.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    #[event_consumer(consumer_group="projection-b", event=Changed)]
    async fn b(&self, _: EventContext, _: Changed) -> Result<(), ConsumerError> {
        self.b.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn body(group: Option<&str>) -> serde_json::Value {
    let mut value: serde_json::Value = serde_json::from_str(include_str!(
        "../../contracts/event-dispatch/tests/fixtures/event_invocation.json"
    ))
    .unwrap();
    value["event"]["topic"] = "grouped".into();
    value["event"]["event_type"] = "grouped.changed".into();
    value["event"]["payload"] = serde_json::json!({"value": 1});
    value["consumption"]["invocation_deadline"] = (chrono::Utc::now()
        + chrono::Duration::seconds(30))
    .to_rfc3339()
    .into();
    if let Some(group) = group {
        value["consumption"]["group_key"] = group.into();
    }
    value
}

#[tokio::test]
async fn authoritative_group_selects_only_one_handler_and_missing_or_wrong_group_fails_closed() {
    let handlers = Arc::new(Handlers::default());
    let mut builder = ConsumerRegistry::builder("orders-app").unwrap();
    builder.bind(handlers.clone()).unwrap();
    let router = ConsumerHttpAdapter::new(Arc::new(builder.build().unwrap())).router();
    for (group, success) in [
        (Some("projection-a"), true),
        (Some("projection-b"), true),
        (Some("unknown"), false),
        (None, false),
    ] {
        let value = body(group);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/")
                    .header("content-type", "application/json")
                    .header("Idempotency-Key", "consumption/4101/2/1/3/9")
                    .header("X-Event-Invocation-Id", "7101")
                    .body(Body::from(serde_json::to_vec(&value).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status().is_success(),
            success,
            "group={group:?}, status={}",
            response.status()
        );
    }
    assert_eq!(handlers.a.load(Ordering::SeqCst), 1);
    assert_eq!(handlers.b.load(Ordering::SeqCst), 1);
}

#[test]
fn manifest_generates_stable_distinct_keys_without_handwritten_keys() {
    let a = SourceCatalog::collect().unwrap();
    let b = SourceCatalog::collect().unwrap();
    assert_eq!(a.consumers(), b.consumers());
    assert_eq!(a.consumers().len(), 2);
    assert_ne!(a.consumers()[0].key, a.consumers()[1].key);
    assert_eq!(a.consumers()[0].selectors, a.consumers()[1].selectors);
}
