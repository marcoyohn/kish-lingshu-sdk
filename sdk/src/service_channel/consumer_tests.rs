use super::*;
use crate::event_dispatch::{ConsumerSelector, EventConsumer, EventContext};
use crate::service_channel::call::tests::{authority, endpoint};
use crate::services::execution::tests::fixture;
use kish_lingshu_event_dispatch_contract::{
    DeliveryMode, EventEnvelopeV1, InvocationConsumptionV1, InvocationTraceV1, InvocationV1,
    INVOCATION_CONTRACT_VERSION,
};
use kish_lingshu_foundation_contract::service_auth::{
    verify_client_transport_message, ServiceSigner,
};
use kish_lingshu_foundation_contract::service_transport::RouteIdentity;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(serde::Deserialize)]
struct Payload {
    value: u64,
}
struct Handler {
    group: &'static str,
    calls: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl EventConsumer for Handler {
    type Event = Payload;
    type Output = Value;
    fn selector(&self) -> ConsumerSelector {
        ConsumerSelector::new("consumer-security", "order.created")
            .unwrap()
            .with_consumer_group(self.group)
            .unwrap()
    }
    async fn consume(&self, context: EventContext, event: Payload) -> Result<Value, ConsumerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match event.value {
            2 => {
                return Err(ConsumerError::retryable(
                    "application-private-code",
                    "secret message",
                ))
            }
            3 => {
                return Err(ConsumerError::permanent(
                    "application-private-code",
                    "secret message",
                ))
            }
            4 => {
                return Err(ConsumerError::throttled(
                    "application-private-code",
                    "secret message",
                    None,
                ))
            }
            5 => std::future::pending::<()>().await,
            _ => {}
        }
        if event.value == 999 {
            return Ok(Value::String("x".repeat(MAX_BUSINESS_PAYLOAD_BYTES)));
        }
        Ok(json!({"value": event.value, "key":context.idempotency_key()}))
    }
}
pub(crate) fn input(ep: &ServiceEndpoint) -> NativeConsumerRequest {
    let ServiceEndpoint::Zenoh {
        lanes,
        route_revision,
        ..
    } = ep
    else {
        unreachable!()
    };
    let now = chrono::Utc::now();
    NativeConsumerRequest {
        route_revision: *route_revision,
        lane: lanes[0].clone(),
        member_id: 11,
        membership_generation: 12,
        invocation: InvocationV1 {
            contract_version: INVOCATION_CONTRACT_VERSION.into(),
            event: EventEnvelopeV1 {
                event_id: 1,
                app_id: "app".into(),
                topic: "consumer-security".into(),
                event_type: "order.created".into(),
                schema_version: "1".into(),
                source: "test".into(),
                subject: None,
                occurred_at: now,
                published_at: now,
                not_before: now,
                partition_key: None,
                correlation_id: None,
                causation_id: None,
                headers: Default::default(),
                payload: json!({"value":1}),
                schedule: None,
            },
            consumption: InvocationConsumptionV1 {
                group_key: Some("chosen".into()),
                consumption_id: 2,
                subscription_id: 3,
                group_id: 4,
                subscription_epoch: 1,
                queue_epoch: 1,
                queue_id: 0,
                queue_offset: 0,
                invocation_id: 5,
                attempt_generation: 1,
                mode: DeliveryMode::Sync,
                idempotency_key: "original-business-key".into(),
                invocation_deadline: now + chrono::Duration::seconds(5),
            },
            completion: None,
            trace: InvocationTraceV1::default(),
        },
    }
}
pub(crate) fn signed(
    platform: &ServiceSigner,
    ep: &ServiceEndpoint,
    input: &NativeConsumerRequest,
) -> TransportEnvelope {
    let ServiceEndpoint::Zenoh { route, lanes, .. } = ep else {
        unreachable!()
    };
    let target = route.invoke_key(&lanes[0]).unwrap();
    let now = chrono::Utc::now().timestamp_millis();
    let deadline = input
        .invocation
        .consumption
        .invocation_deadline
        .timestamp_millis();
    let request_id = RouteIdentity::new(uuid::Uuid::new_v4().to_string()).unwrap();
    let payload = serde_json::value::to_raw_value(input).unwrap();
    let proof = platform
        .sign_transport_message(
            "app",
            MessageKind::InvokeEvent,
            &target,
            &request_id,
            payload.get().as_bytes(),
            now,
            deadline,
        )
        .unwrap();
    TransportEnvelope {
        protocol_version: ProtocolVersion::V1,
        kind: MessageKind::InvokeEvent,
        request_id,
        application_id: RouteIdentity::new("app").unwrap(),
        target,
        deadline_unix_ms: deadline,
        proof,
        trace_parent: None,
        payload,
    }
}
pub(crate) fn decode(
    bytes: &[u8],
    request: &TransportEnvelope,
    signer: &ChannelMessageSigner,
    initial: &ChannelBootstrapResponse,
) -> NativeConsumerResponse {
    let now = chrono::Utc::now().timestamp_millis();
    let reply =
        TransportEnvelope::decode(bytes, &request.target, &request.application_id, now).unwrap();
    assert_eq!(reply.request_id, request.request_id);
    assert_eq!(reply.deadline_unix_ms, request.deadline_unix_ms);
    verify_client_transport_message(
        signer.public_key(),
        &ClientChannelIdentity {
            application_id: initial.application_id.clone(),
            instance_id: RouteIdentity::new("sdk").unwrap(),
            base_generation: RouteIdentity::new("base").unwrap(),
            certificate_identity: initial.certificate.certificate_identity.clone(),
        },
        &reply.proof,
        MessageKind::InvokeEvent,
        &reply.target,
        &reply.request_id,
        reply.payload.get().as_bytes(),
        now,
    )
    .unwrap();
    serde_json::from_str(reply.payload.get()).unwrap()
}
#[tokio::test]
async fn consumer_checks_role_membership_group_signature_and_replay_before_execution() {
    let capture = super::super::observation::tests::Capture::default();
    capture.observe(consumer_observation_fixture()).await;
    for (outcome, count) in [
        ("permanent_failure", 3.0),
        ("completed", 2.0),
        ("throttled", 2.0),
        ("retryable_failure", 1.0),
        ("timed_out", 1.0),
        ("unknown", 0.0),
    ] {
        assert_eq!(
            capture.sum(
                "lingshu_sdk_channel_business_results_total",
                Some("consumer"),
                Some(outcome)
            ),
            count,
            "{outcome}"
        );
    }
    // The oversized completed result becomes a permanent wire failure. A timed
    // out Handler cannot seal a reply past the original envelope deadline.
    for (outcome, count) in [
        ("permanent_failure", 4.0),
        ("completed", 1.0),
        ("throttled", 2.0),
        ("retryable_failure", 1.0),
        ("timed_out", 0.0),
    ] {
        assert_eq!(
            capture.sum(
                "lingshu_sdk_channel_prepared_responses_total",
                Some("consumer"),
                Some(outcome)
            ),
            count,
            "{outcome}"
        );
    }
    capture.assert_balanced_resource_gauges();
    capture.assert_bounded_labels();
}
async fn consumer_observation_fixture() {
    let f = fixture(1).await;
    let platform = ServiceSigner::new(&[73; 32]).unwrap();
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
    let signer = ChannelMessageSigner::from_pkcs8(&key.serialize_der()).unwrap();
    let initial = authority(&platform, &signer);
    let ep = endpoint();
    let calls = Arc::new(AtomicUsize::new(0));
    let sibling = Arc::new(AtomicUsize::new(0));
    let mut registry = ConsumerRegistry::new("app").unwrap();
    registry
        .register(Handler {
            group: "chosen",
            calls: calls.clone(),
        })
        .unwrap();
    registry
        .register(Handler {
            group: "sibling",
            calls: sibling.clone(),
        })
        .unwrap();
    assert!(registry.supports_group("chosen"));
    assert!(!registry.supports_group("undeclared"));
    let binding = NativeConsumerExecution {
        received_lanes: Default::default(),
        registry: Arc::new(registry),
        total: f.budget.clone(),
        capacity: tokio::sync::Semaphore::new(1),
        group_key: "chosen".into(),
        group_id: 4,
        member_id: 11,
        membership_generation: 12,
    };
    for mismatch in [
        "revision",
        "lane",
        "member",
        "membership",
        "group_id",
        "group_key",
        "deadline",
    ] {
        let mut invocation = input(&ep);
        match mismatch {
            "revision" => invocation.route_revision += 1,
            "lane" => invocation.lane.epoch = RouteIdentity::new("foreign").unwrap(),
            "member" => invocation.member_id += 1,
            "membership" => invocation.membership_generation += 1,
            "group_id" => invocation.invocation.consumption.group_id += 1,
            "group_key" => invocation.invocation.consumption.group_key = Some("sibling".into()),
            "deadline" => {
                invocation.invocation.consumption.invocation_deadline +=
                    chrono::Duration::seconds(30)
            }
            _ => unreachable!(),
        }
        let req = signed(&platform, &ep, &invocation);
        assert!(
            binding
                .reply(
                    &ep,
                    &req.target,
                    &req.encode(chrono::Utc::now().timestamp_millis()).unwrap(),
                    initial.authorization_expires_unix_ms,
                    &f.core.connection,
                    &initial,
                    &signer
                )
                .await
                .is_err(),
            "{mismatch}"
        );
    }
    let req = signed(&ServiceSigner::new(&[74; 32]).unwrap(), &ep, &input(&ep));
    assert!(binding
        .reply(
            &ep,
            &req.target,
            &req.encode(chrono::Utc::now().timestamp_millis()).unwrap(),
            initial.authorization_expires_unix_ms,
            &f.core.connection,
            &initial,
            &signer
        )
        .await
        .is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut asynchronous = input(&ep);
    asynchronous.invocation.consumption.mode = DeliveryMode::Async;
    let req = signed(&platform, &ep, &asynchronous);
    let reply = binding
        .reply(
            &ep,
            &req.target,
            &req.encode(chrono::Utc::now().timestamp_millis()).unwrap(),
            initial.authorization_expires_unix_ms,
            &f.core.connection,
            &initial,
            &signer,
        )
        .await
        .unwrap();
    assert!(matches!(
        decode(&reply, &req, &signer, &initial),
        NativeConsumerResponse::PermanentFailure { .. }
    ));
    for (body, expected) in [
        (json!({"value":"invalid"}), "permanent"),
        (json!({"value":999}), "permanent"),
        (json!({"value":1}), "completed"),
    ] {
        let mut invocation = input(&ep);
        invocation.invocation.event.payload = body;
        let req = signed(&platform, &ep, &invocation);
        let wire = req.encode(chrono::Utc::now().timestamp_millis()).unwrap();
        let reply = binding
            .reply(
                &ep,
                &req.target,
                &wire,
                initial.authorization_expires_unix_ms,
                &f.core.connection,
                &initial,
                &signer,
            )
            .await
            .unwrap();
        match (expected, decode(&reply, &req, &signer, &initial)) {
            ("permanent", NativeConsumerResponse::PermanentFailure { .. }) => {}
            ("completed", NativeConsumerResponse::Completed { result }) => {
                assert_eq!(result["key"], "original-business-key")
            }
            _ => panic!("wrong typed Consumer outcome"),
        }
        assert!(binding
            .reply(
                &ep,
                &req.target,
                &wire,
                initial.authorization_expires_unix_ms,
                &f.core.connection,
                &initial,
                &signer
            )
            .await
            .is_err());
    }
    for value in [2, 3, 4, 5] {
        let mut invocation = input(&ep);
        invocation.invocation.event.payload = json!({"value":value});
        if value == 5 {
            invocation.invocation.consumption.invocation_deadline =
                chrono::Utc::now() + chrono::Duration::milliseconds(30);
        }
        let req = signed(&platform, &ep, &invocation);
        let response = binding
            .reply(
                &ep,
                &req.target,
                &req.encode(chrono::Utc::now().timestamp_millis()).unwrap(),
                initial.authorization_expires_unix_ms,
                &f.core.connection,
                &initial,
                &signer,
            )
            .await;
        if value == 5 {
            assert!(
                response.is_err(),
                "an expired reply cannot manufacture receipt evidence"
            );
            continue;
        }
        let response = decode(&response.unwrap(), &req, &signer, &initial);
        assert!(matches!(
            (value, response),
            (2, NativeConsumerResponse::RetryableFailure { .. })
                | (3, NativeConsumerResponse::PermanentFailure { .. })
                | (4, NativeConsumerResponse::Throttled { .. })
        ));
    }
    // Occupying the Call core's shared budget throttles Event without executing.
    let _call_capacity = f.budget.acquire().unwrap();
    let req = signed(&platform, &ep, &input(&ep));
    let reply = binding
        .reply(
            &ep,
            &req.target,
            &req.encode(chrono::Utc::now().timestamp_millis()).unwrap(),
            initial.authorization_expires_unix_ms,
            &f.core.connection,
            &initial,
            &signer,
        )
        .await
        .unwrap();
    assert!(matches!(
        decode(&reply, &req, &signer, &initial),
        NativeConsumerResponse::Throttled { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 6);
    assert_eq!(sibling.load(Ordering::SeqCst), 0);
    drop(_call_capacity);
    assert_eq!(f.budget.semaphore.available_permits(), 1);
    f.core.connection.shutdown().await;
}
