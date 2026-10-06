use std::time::Duration;

use kish_lingshu_runtime_contract::{
    test_support::ContractProductRuntimeFixture, InvocationSource, MutationDisposition,
    PrincipalKind, TrustedContextFactory,
};
use kish_lingshu_sdk::{
    event_dispatch::{
        DeliveryTime, DynamicEvent, EventPayload, EventRoute, EventVisibility, PublishEvent,
    },
    ClientBuilder, ClientConfig, Error, MutationOptions, RequestOptions, ServiceCredential,
};
use serde::Serialize;
use serde_json::json;

#[derive(Serialize)]
struct OrderCreated {
    order_id: u64,
}

impl EventPayload for OrderCreated {
    const TOPIC: &'static str = "orders";
    const EVENT_TYPE: &'static str = "order.created";
    const SCHEMA_VERSION: &'static str = "1";
}

fn service_client(
    fixture: &ContractProductRuntimeFixture,
) -> kish_lingshu_sdk::Client<kish_lingshu_sdk::ServicePrincipal> {
    ClientBuilder::new(ClientConfig::in_process())
        .service_credential(ServiceCredential::new("orders-app", "service-secret").unwrap())
        .bind_runtime(
            fixture.facade.clone(),
            TrustedContextFactory::new(
                "orders-service",
                PrincipalKind::Service,
                Some("orders-app".to_string()),
                InvocationSource::EmbeddedSdk,
            )
            .unwrap(),
        )
        .unwrap()
}

#[tokio::test]
async fn public_event_dispatch_publishes_replays_conflicts_and_looks_up_events() {
    let fixture = ContractProductRuntimeFixture::default();
    let events = service_client(&fixture).event_dispatch();
    let mut event = PublishEvent::typed("checkout", &OrderCreated { order_id: 42 }).unwrap();
    event.subject = Some("order/42".to_string());
    event.partition_key = Some("customer/7".to_string());
    event.delivery = DeliveryTime::after(Duration::from_secs(5));

    let accepted = events
        .publish(
            event.clone(),
            MutationOptions::new("orders/42/created").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.visibility, EventVisibility::Delayed);
    assert_eq!(accepted.mutation.disposition, MutationDisposition::Accepted);

    let duplicate = events
        .publish(
            event.clone(),
            MutationOptions::new("orders/42/created").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(duplicate.event_id, accepted.event_id);
    assert_eq!(
        duplicate.mutation.disposition,
        MutationDisposition::Duplicate
    );

    let observed = events
        .get(accepted.event_id, RequestOptions::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.topic, OrderCreated::TOPIC);
    assert_eq!(observed.event_type, OrderCreated::EVENT_TYPE);
    assert_eq!(observed.payload, json!({"order_id": 42}));

    event.payload = json!({"order_id": 43});
    let conflict = events
        .publish(event, MutationOptions::new("orders/42/created").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        Error::Application(failure) if failure.problem.code.as_str() == "conflict"
    ));
    assert_eq!(fixture.event_publisher.published_count(), 1);
}

#[tokio::test]
async fn typed_and_dynamic_convenience_methods_use_the_same_publication_contract() {
    let fixture = ContractProductRuntimeFixture::default();
    let events = service_client(&fixture).event_dispatch();

    let typed = events
        .publish_typed(
            "checkout",
            &OrderCreated { order_id: 42 },
            MutationOptions::new("orders/42/typed").unwrap(),
        )
        .await
        .unwrap();
    let dynamic = events
        .publish_dynamic(
            "checkout",
            DynamicEvent::new(
                EventRoute::new("orders", "order.created", "1").unwrap(),
                json!({"order_id": 43}),
            )
            .unwrap(),
            MutationOptions::new("orders/43/dynamic").unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(typed.event_id, dynamic.event_id);
    assert_eq!(fixture.event_publisher.published_count(), 2);
}

#[tokio::test]
async fn invalid_publication_is_rejected_before_runtime_io() {
    let fixture = ContractProductRuntimeFixture::default();
    let events = service_client(&fixture).event_dispatch();
    let event = PublishEvent {
        topic: "orders with spaces".to_string(),
        event_type: "order.created".to_string(),
        schema_version: "1".to_string(),
        payload: json!({"order_id": 42}),
        source: "checkout".to_string(),
        occurred_at: None,
        subject: None,
        partition_key: None,
        correlation_id: None,
        causation_id: None,
        headers: Default::default(),
        delivery: DeliveryTime::Immediate,
    };

    let error = events
        .publish(event, MutationOptions::new("orders/42/invalid").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Configuration(_)));
    assert_eq!(fixture.event_publisher.published_count(), 0);
}
