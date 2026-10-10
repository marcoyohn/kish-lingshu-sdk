use super::*;
use crate::*;

fn declaration() -> ConsumerDeclaration {
    serde_json::from_value(serde_json::json!({
        "provider_key":"orders", "producer":{"package_name":"orders-app","package_version":"1.0.0"},
        "consumer":{"key":"orders.created.consumer","group_key":"order-workers",
            "selectors":[{"event_key":"orders.created","topic":"orders","event_type":"created"}],
            "delivery_mode":"sync", "maximum_concurrency":8,
            "policy":{"rate_limit":null,
                "retry":{"maximum_failure_attempts":3,"initial_delay_milliseconds":100,"maximum_delay_milliseconds":1000,"multiplier":2.0,"jitter_ratio":0.1},
                "throttle":{"minimum_cooldown_milliseconds":10,"maximum_cooldown_milliseconds":1000,"maximum_throttle_duration_milliseconds":10000,"half_open_probe_limit":1},
                "timeout":{"invocation_milliseconds":1000,"completion_milliseconds":1000,"maximum_completion_milliseconds":10000},
                "dispatch":{"ordering_scope":"partition","mode":"serial"},"pause":"retain"}},
        "events":[{"key":"orders.created","topic":"orders","event_type":"created","schema_version":"1",
            "payload_schema":{"type":"object"},"topic_defaults":{"name":"Orders","partition_count":8,"consumption_order":"partition_ordered"}}]
    })).unwrap()
}

#[test]
fn registration_requires_an_exact_bounded_consumption_catalog() {
    let original = declaration();
    original.validate().unwrap();
    let mut bad = original.clone();
    bad.consumer.selectors[0].event_key = "unknown".into();
    assert_eq!(
        bad.validate(),
        Err(ConsumerRegistrationError::InvalidDeclaration)
    );
    let mut bad = original.clone();
    bad.consumer
        .selectors
        .push(bad.consumer.selectors[0].clone());
    assert_eq!(
        bad.validate(),
        Err(ConsumerRegistrationError::InvalidDeclaration)
    );
    let mut bad = original.clone();
    bad.events[0].topic = "*".into();
    assert!(bad.validate().is_err());
    let mut bad = original.clone();
    bad.events[0].description = Some("x".repeat(MAX_CONSUMER_DECLARATION_BYTES));
    assert_eq!(
        bad.validate(),
        Err(ConsumerRegistrationError::CapacityExceeded)
    );
    let mut bad = original.clone();
    bad.consumer.delivery_mode = DeliveryMode::Async;
    assert!(bad.validate().is_err());
    let mut bad = original;
    bad.events.push(bad.events[0].clone());
    assert!(bad.validate().is_err());
}

#[test]
fn digest_binds_provider_and_contract_without_order_dependence() {
    let mut first = declaration();
    let mut second_event = first.events[0].clone();
    second_event.key = "orders.cancelled".into();
    second_event.event_type = "cancelled".into();
    first.consumer.selectors.push(EventSelector {
        event_key: second_event.key.clone(),
        topic: second_event.topic.clone(),
        event_type: second_event.event_type.clone(),
    });
    first.events.push(second_event);
    let mut second = first.clone();
    second.events.reverse();
    second.consumer.selectors.reverse();
    assert_eq!(first.digest().unwrap(), second.digest().unwrap());
    second.provider_key = "different-owner".into();
    assert_ne!(first.digest().unwrap(), second.digest().unwrap());
    second = first.clone();
    second.events[0].schema_version = "2".into();
    assert_ne!(first.digest().unwrap(), second.digest().unwrap());
}

#[test]
fn wire_cannot_select_application_group_id_or_unknown_protocol() {
    let request = ConsumerDeclarationUpdate {
        protocol: ConsumerRegistrationProtocol::V1,
        registration_key: "bound-orders".into(),
        operation_id: "request-1".into(),
        expected_revision: None,
        declaration: declaration(),
    };
    request.validate().unwrap();
    let value = serde_json::to_value(&request).unwrap();
    for field in ["application_id", "group_id", "connection_epoch"] {
        let mut invalid = value.clone();
        invalid[field] = serde_json::json!("client-selected");
        assert!(serde_json::from_value::<ConsumerDeclarationUpdate>(invalid).is_err());
    }
    let mut invalid = value;
    invalid["protocol"] = serde_json::json!("consumer-declarations/999");
    assert!(serde_json::from_value::<ConsumerDeclarationUpdate>(invalid).is_err());
}

#[test]
fn native_control_negotiates_and_rejects_untrusted_identity_fields() {
    let request = ConsumerRegistrationControl {
        consumer_registration: ConsumerRegistrationProtocol::V1,
        command: ConsumerRegistrationCommand::Declare {
            previous_connection_epoch: None,
            update: ConsumerDeclarationUpdate {
                protocol: ConsumerRegistrationProtocol::V1,
                registration_key: "orders".into(),
                operation_id: "create-1".into(),
                expected_revision: None,
                declaration: declaration(),
            },
        },
    };
    request.validate().unwrap();
    let json = serde_json::to_value(&request).unwrap();
    for field in [
        "application_id",
        "connection_epoch",
        "group_id",
        "owner_boot",
    ] {
        let mut forged = json.clone();
        forged[field] = serde_json::json!("chosen-by-client");
        assert!(serde_json::from_value::<ConsumerRegistrationControl>(forged).is_err());
    }
    let mut unsupported = json;
    unsupported["consumer_registration"] = serde_json::json!("consumer-declarations/99");
    assert!(serde_json::from_value::<ConsumerRegistrationControl>(unsupported).is_err());
}

#[test]
fn metadata_can_reuse_route_but_connection_or_reactivation_cannot() {
    let old = ConsumerActivationVersion {
        owner_boot: "owner".into(),
        connection_epoch: "connection".into(),
        registration_key: "orders".into(),
        declaration_revision: 2,
        catalog_revision: 3,
        activation_revision: 4,
    };
    let mut metadata = old.clone();
    metadata.declaration_revision += 1;
    assert!(metadata.preserves_route_from(&old));
    assert!(!old.preserves_route_from(&metadata));
    for field in 0..5 {
        let mut changed = metadata.clone();
        match field {
            0 => changed.owner_boot = "replacement".into(),
            1 => changed.connection_epoch = "new".into(),
            2 => changed.registration_key = "different".into(),
            3 => changed.catalog_revision += 1,
            _ => changed.activation_revision += 1,
        }
        assert!(!changed.preserves_route_from(&old));
    }
}
