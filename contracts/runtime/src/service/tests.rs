use super::*;
use serde_json::json;

fn operation() -> OperationDefinition {
    OperationDefinition {
        user_task_completion: None,
        operation_key: "generate".into(),
        version: "v1".into(),
        description: String::new(),
        action: "report.generate".into(),
        idempotent: true,
        input_schema: json!({"type":"object"}),
        output_schema: json!({"type":"string"}),
        error_schema: json!({}),
        call: Some(CallBinding {
            modes: [CallMode::Sync, CallMode::Async].into_iter().collect(),
            maximum_concurrency: 2,
            timeout_ms: 30_000,
        }),
        events: BTreeSet::new(),
    }
}
#[test]
fn digest_is_independent_of_object_key_order_but_covers_contract() {
    let mut a = operation();
    let mut b = a.clone();
    a.input_schema =
        serde_json::from_str(r#"{"type":"object","properties":{"b":{},"a":{}}}"#).unwrap();
    b.input_schema =
        serde_json::from_str(r#"{"properties":{"a":{},"b":{}},"type":"object"}"#).unwrap();
    assert_eq!(
        a.reference("report").unwrap(),
        b.reference("report").unwrap()
    );
    b.action = "report.admin".into();
    assert_ne!(
        a.reference("report").unwrap().contract_digest,
        b.reference("report").unwrap().contract_digest
    );
}
#[test]
fn all_three_roles_are_valid_and_duplicate_event_routes_are_rejected() {
    let call = operation();
    let mut event = operation();
    event.call = None;
    event.events.insert(EventBinding {
        topic: "reports".into(),
        event_type: "requested".into(),
        consumer_group: "generator".into(),
    });
    let mut both = event.clone();
    both.call = call.call.clone();
    for operation in [call, event.clone(), both] {
        assert!(operation.validate().is_ok());
    }
    let mut second = event.clone();
    second.operation_key = "other".into();
    let manifest = ServiceManifest {
        contract_version: 1,
        application_id: "app".into(),
        services: vec![ServiceDefinition {
            service_key: "report".into(),
            description: String::new(),
            operations: vec![event, second],
        }],
    };
    assert_eq!(
        manifest.validate().unwrap_err().code,
        "duplicate_event_route"
    );
}
#[test]
fn invocation_validates_application_mode_and_expiry_without_event_fields() {
    let mut invocation = ServiceInvocation {
        admission: None,
        target_instance: None,
        contract_version: 1,
        operation: operation().reference("report").unwrap(),
        context: ServiceContext {
            application_id: "app".into(),
            idempotency_key: "business-42".into(),
            deadline_ms: 1000,
            trace_id: None,
            invocation: InvocationRole::Call(CallContext {
                user_task_completion: None,
                call_id: "call-1".into(),
                attempt: 1,
                caller: "workflow".into(),
                mode: CallMode::Async,
                workflow: None,
            }),
        },
        input: json!({}),
        completion: Some(CompletionTarget {
            heartbeat: None,
            token: "secret".into(),
        }),
    };
    assert!(invocation.validate("app", 0).is_ok());
    assert!(invocation.validate("other", 0).is_err());
    assert!(invocation.validate("app", 1000).is_err());
    assert!(!format!("{:?}", invocation).contains("secret"));
    invocation.completion.as_mut().unwrap().heartbeat = Some(CallHeartbeatPolicy {
        version: 1,
        epoch: "attempt-1".into(),
        interval_ms: 10000,
        execution_deadline_ms: 1000,
        delivery_deadline_ms: 31000,
    });
    assert!(
        invocation.validate("app", 0).is_err(),
        "renewable authority requires a selected instance"
    );
    invocation.target_instance = Some(ServiceInstanceTarget {
        node_id: "n".into(),
        generation: "g".into(),
    });
    assert!(invocation.validate("app", 0).is_ok());
    invocation
        .completion
        .as_mut()
        .unwrap()
        .heartbeat
        .as_mut()
        .unwrap()
        .delivery_deadline_ms += 1;
    assert!(
        invocation.validate("app", 0).is_err(),
        "delivery cannot exceed its finite budget"
    );
    invocation.completion = None;
    assert!(invocation.validate("app", 0).is_err());
}
