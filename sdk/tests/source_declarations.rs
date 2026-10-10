#![cfg(feature = "event-consumer")]

use std::sync::Arc;

use kish_lingshu_sdk::{
    event_dispatch,
    event_dispatch::{
        ConsumerError, ConsumerRegistry, EventContext, EventPayload as EventPayloadContract,
        JobTrigger, ProducerMetadata, SourceCatalog,
    },
    event_job, EventPayload,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, EventPayload, JsonSchema, Serialize)]
#[event(
    key = "orders.created",
    topic = "orders",
    event_type = "order.created",
    schema_version = "1",
    description = "An order was accepted",
    topic_name = "Orders",
    topic_description = "Order lifecycle Events",
    partition_count = 8,
    consumption_order = "partition_ordered"
)]
struct OrderCreated {
    order_id: u64,
}

#[derive(Clone, Debug, Deserialize, EventPayload, JsonSchema, Serialize)]
#[event(
    key = "orders.expire-requested",
    topic = "orders",
    event_type = "order.expire.requested",
    schema_version = "1",
    topic_name = "Orders",
    topic_description = "Order lifecycle Events",
    partition_count = 8,
    consumption_order = "partition_ordered"
)]
struct ExpireOrders {
    maximum_age_days: u32,
}

struct OrderHandlers;

#[event_dispatch(group = "order-workers", maximum_concurrency = 32)]
impl OrderHandlers {
    #[event_consumer(key = "orders.on-created", event = OrderCreated)]
    async fn on_created(
        &self,
        _context: EventContext,
        event: OrderCreated,
    ) -> Result<u64, ConsumerError> {
        Ok(event.order_id)
    }
}

#[event_job(
    key = "orders.expire",
    event = ExpireOrders,
    cron = "0 */5 * * * *",
    timezone = "UTC",
    misfire = "fire_once",
    overlap = "serialize"
)]
fn expire_orders() -> ExpireOrders {
    ExpireOrders {
        maximum_age_days: 30,
    }
}

#[test]
fn linked_source_definitions_export_without_client_or_handler_construction() {
    let catalog = SourceCatalog::collect().unwrap();
    assert_eq!(catalog.events().len(), 2);
    assert_eq!(catalog.consumers().len(), 1);
    assert_eq!(catalog.jobs().len(), 1);

    let created = catalog
        .events()
        .iter()
        .find(|event| event.key == OrderCreated::DEFINITION_KEY)
        .unwrap();
    assert_eq!(created.topic, OrderCreated::TOPIC);
    assert_eq!(created.event_type, OrderCreated::EVENT_TYPE);
    assert_eq!(created.topic_defaults.partition_count, 8);
    assert!(created.payload_schema.is_object());

    let consumer = &catalog.consumers()[0];
    assert_eq!(consumer.key, "orders.on-created");
    assert_eq!(consumer.group_key, "order-workers");
    assert_eq!(consumer.maximum_concurrency, 32);
    assert_eq!(
        consumer.selectors[0].event_key,
        OrderCreated::DEFINITION_KEY
    );
    assert_eq!(consumer.selectors[0].topic, OrderCreated::TOPIC);

    let job = &catalog.jobs()[0];
    assert_eq!(job.key, "orders.expire");
    assert_eq!(job.event.event_key, ExpireOrders::DEFINITION_KEY);
    assert!(matches!(job.trigger, JobTrigger::Cron { .. }));
}

#[test]
fn manifest_export_is_byte_stable_and_contains_no_environment_identity() {
    let catalog = SourceCatalog::collect().unwrap();
    let producer = ProducerMetadata {
        package_name: "orders-application".to_string(),
        package_version: "1.2.3".to_string(),
    };
    let first = catalog.export_json(producer.clone()).unwrap();
    let second = SourceCatalog::collect()
        .unwrap()
        .export_json(producer)
        .unwrap();
    assert_eq!(first, second);

    let json = String::from_utf8(first).unwrap();
    assert!(json.contains("orders.on-created"));
    assert!(json.contains("orders.expire"));
    for forbidden in [
        "app_id",
        "credential",
        "callback_token",
        "invocation_url",
        "node_id",
        "lease",
        "generated_at",
    ] {
        assert!(!json.contains(forbidden), "manifest leaked {forbidden}");
    }
}

#[test]
fn the_exported_consumer_descriptor_builds_the_invocation_registry() {
    let mut builder = ConsumerRegistry::builder("orders-app").unwrap();
    builder.bind(Arc::new(OrderHandlers)).unwrap();
    let registry = builder.build().unwrap();
    assert_eq!(registry.app_id(), "orders-app");
}

#[test]
fn registration_catalog_contains_only_bound_handlers_and_preserves_source_identity() {
    use kish_lingshu_runtime_contract::provider::ProviderCatalog;
    let source = SourceCatalog::collect().unwrap();
    let manifest = source
        .manifest(ProducerMetadata {
            package_name: "orders-package".into(),
            package_version: "1.0.0".into(),
        })
        .unwrap();
    let catalog = ProviderCatalog {
        format_version: 1,
        application_id: "orders-app".into(),
        provider_key: "orders-provider".into(),
        release: "1.0.0".into(),
        services: None,
        events: Some(manifest),
        workflows: vec![],
    };
    let empty = ConsumerRegistry::new("orders-app").unwrap();
    assert!(empty
        .registration_declarations(&catalog)
        .unwrap()
        .is_empty());
    let mut builder = ConsumerRegistry::builder("orders-app").unwrap();
    builder.bind(Arc::new(OrderHandlers)).unwrap();
    let registry = builder.build().unwrap();
    let declared = registry.registration_declarations(&catalog).unwrap();
    assert_eq!(declared.len(), 1);
    assert_eq!(declared[0].provider_key, "orders-provider");
    assert_eq!(declared[0].producer.package_name, "orders-package");
    assert_eq!(declared[0].consumer.key, "orders.on-created");
    assert_eq!(declared[0].events.len(), 1);
    assert_eq!(declared[0].events[0].key, "orders.created");
    let mut foreign = catalog.clone();
    foreign.application_id = "other".into();
    assert!(registry.registration_declarations(&foreign).is_err());
    let mut missing = catalog.clone();
    missing.events = None;
    assert!(registry.registration_declarations(&missing).is_err());
}
