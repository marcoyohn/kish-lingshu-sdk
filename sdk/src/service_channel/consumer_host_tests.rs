//! Real TLS/Router/SQL/Redis Dispatch acceptance; no SDK inbound listener.
#[path = "consumer_crash_host_tests.rs"]
mod crash_tests;
use super::super::{ChannelCertificateRotationConfig, ChannelSessionConfig};
use crate::{
    event_dispatch::{ConsumerError, ConsumerRegistry, EventContext, PublishEvent},
    ClientBuilder, ClientConfig, MutationOptions, ServiceConnection, ServiceCredential,
    ServiceExecutionBudget,
};
use kish_lingshu_foundation_contract::{
    service_transport::RouteIdentity, ServiceInstanceRegistration,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Deserialize, Serialize, JsonSchema, crate::event_dispatch::EventPayload)]
#[event(
    key = "native.order.created",
    topic = "native.events",
    event_type = "native.order.created",
    schema_version = "1",
    topic_name = "Native Events"
)]
struct Order {
    sequence: u64,
    fail_once: bool,
}

// Existing application Eventing retry wire shape; the adapter never interprets
// it as a publication or a request to rebroadcast an original business Event.
#[derive(Deserialize, Serialize, JsonSchema, crate::event_dispatch::EventPayload)]
#[event(
    key = "native.eventing.retry",
    topic = "platform.eventing",
    event_type = "eventing.handler.retry.requested",
    schema_version = "1",
    topic_name = "Platform Eventing"
)]
struct TargetedRetry {
    request: RetryRequest,
}
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RetryRequest {
    target_application: String,
    target_consumer_group: String,
    original_event: OriginalEvent,
    failure: LocalFailure,
}
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct OriginalEvent {
    event_id: String,
    topic: String,
    event_type: String,
    schema_version: String,
    occurred_at: u64,
    partition_key: Option<String>,
    correlation_id: Option<String>,
    payload_json: String,
}
#[derive(Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LocalFailure {
    code: String,
    permanent: bool,
    attempted_at: u64,
}
#[derive(Default)]
struct Handlers {
    crash: Option<Arc<crash_tests::CrashHandler>>,
    drain_gate: Option<Arc<tokio::sync::Semaphore>>,
    rotation_drain: bool,
    good: AtomicUsize,
    retry: AtomicUsize,
    identities: Mutex<Vec<(u64, String, u64)>>,
    bridge: AtomicUsize,
    originals: Mutex<Vec<(String, String)>>,
    traces: Mutex<Vec<Value>>,
}
impl Handlers {
    async fn observe_trace(&self, context: &EventContext, sequence: u64, group: &str) {
        let wire = super::super::trace::current_trace_parent().unwrap();
        tokio::task::yield_now().await;
        assert_eq!(
            super::super::trace::current_trace_parent().as_deref(),
            Some(wire.as_str())
        );
        self.traces.lock().unwrap().push(json!({
            "event_id": context.event_id(), "sequence": sequence, "group": group,
            "attempt": context.attempt_generation(), "trace": wire,
            "dispatch_parent": context.traceparent(), "headers_empty": context.headers().is_empty(),
        }));
    }
}
#[crate::event_dispatch(maximum_concurrency = 1)]
impl Handlers {
    #[event_consumer(consumer_group = "native-eventing-bridge", event = TargetedRetry)]
    async fn bridge(
        &self,
        context: EventContext,
        event: TargetedRetry,
    ) -> Result<(), ConsumerError> {
        let request = event.request;
        if request.target_application != context.app_id()
            || request.target_consumer_group != "native-retry"
        {
            return Err(ConsumerError::permanent(
                "wrong_retry_target",
                "foreign retry target",
            ));
        }
        self.bridge.fetch_add(1, Ordering::SeqCst);
        self.originals.lock().unwrap().push((
            request.original_event.event_id,
            request.original_event.payload_json,
        ));
        // One bounded original failed-group attempt per Dispatch invocation.
        // The bridge has no publisher and never republishes itself/the Event.
        if context.attempt_generation() == 1 {
            return Err(ConsumerError::retryable(
                "original_group_retry",
                "Dispatch must own the next attempt",
            ));
        }
        Ok(())
    }
    #[event_consumer(consumer_group = "native-good", event = Order)]
    async fn good(&self, context: EventContext, event: Order) -> Result<Value, ConsumerError> {
        if let Some(crash) = &self.crash {
            return crash.consume(context, event).await;
        }
        self.observe_trace(&context, event.sequence, "native-good")
            .await;
        self.good.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.drain_gate {
            if !self.rotation_drain || context.attempt_generation() == 1 {
                assert_eq!(context.attempt_generation(), 1);
                gate.acquire().await.unwrap().forget();
            }
        }
        self.identities.lock().unwrap().push((
            context.event_id(),
            context.idempotency_key().to_owned(),
            event.sequence,
        ));
        Ok(json!({"sequence": event.sequence}))
    }
    #[event_consumer(consumer_group = "native-retry", event = Order)]
    async fn retry(&self, context: EventContext, event: Order) -> Result<Value, ConsumerError> {
        self.observe_trace(&context, event.sequence, "native-retry")
            .await;
        self.retry.fetch_add(1, Ordering::SeqCst);
        self.identities.lock().unwrap().push((
            context.event_id(),
            context.idempotency_key().to_owned(),
            event.sequence,
        ));
        if event.fail_once && context.attempt_generation() == 1 {
            return Err(ConsumerError::retryable(
                "bounded_step_retry",
                "Dispatch schedules the next bounded step",
            ));
        }
        Ok(json!({"sequence": event.sequence}))
    }
}
async fn wait_for(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
async fn pool(
    connection: &ServiceConnection,
    instance: &str,
) -> super::super::ServiceChannelSessions {
    connection
        .bootstrap_channel(
            ServiceInstanceRegistration {
                instance_id: instance.into(),
                incarnation_id: format!("{instance}-boot"),
                generation: None,
            },
            Some(RouteIdentity::new("dev").unwrap()),
        )
        .await
        .unwrap()
        .open_sessions(ChannelSessionConfig::host_test())
        .await
        .unwrap()
}
fn registry(app: &str, handlers: Arc<Handlers>) -> Arc<ConsumerRegistry> {
    let mut builder = ConsumerRegistry::builder(app).unwrap();
    builder.bind(handlers).unwrap();
    Arc::new(builder.build().unwrap())
}

async fn governance(url: &str, invocation_seconds: u64) -> u64 {
    let client = reqwest::Client::new();
    let token = std::env::var("LINGSHU_EVENT_TEST_TOKEN").unwrap();
    let post = |path: String, body: Value, key: &str| {
        client
            .post(format!("{url}/api/user/event-dispatch/v1{path}"))
            .header("x-token", &token)
            .header(
                "x-kish-app-id",
                std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap(),
            )
            .header("Idempotency-Key", key)
            .json(&body)
            .send()
    };
    async fn response(response: reqwest::Response) -> Value {
        let status = response.status();
        let body = response.text().await.unwrap();
        assert!(status.is_success(), "{status}: {body}");
        let value: Value = serde_json::from_str(&body).expect("JSON governance response");
        value
    }
    let topic = response(post("/topics".into(), json!({"topic":"native.events", "name":"Native Events", "partition_count":1, "consumption_order":"unordered", "state":"active"}), "native-event-topic").await.unwrap()).await;
    response(post(format!("/topics/{}/events", topic["topic_id"].as_u64().unwrap()), json!({"event_type":"native.order.created", "schema_version":"1", "payload_schema": schemars::schema_for!(Order), "state":"active"}), "native-event-definition").await.unwrap()).await;
    let mut retry_subscription = 0;
    for group in ["native-good", "native-retry"] {
        let value = response(
            post(
                "/groups".into(),
                json!({"group_key":group,"maximum_concurrency":1}),
                &format!("native-group-{group}"),
            )
            .await
            .unwrap(),
        )
        .await;
        let subscription = response(post("/subscriptions".into(), json!({
            "topic":"native.events", "group_id":value["group_id"], "delivery_mode":"sync", "initial_position":{"type":"latest"}, "filter":null, "pause_policy":"retain",
            "timeouts":{"invocation_seconds":invocation_seconds,"maximum_completion_seconds":3600},
            "retry":{"maximum_failure_attempts":3,"initial_delay_seconds":1,"maximum_delay_seconds":2,"multiplier":2.0,"jitter_ratio":0.0},
            "throttle":{"minimum_cooldown_seconds":1,"maximum_cooldown_seconds":2,"maximum_throttle_duration_seconds":10,"half_open_probe_limit":1}
        }), &format!("native-subscription-{group}")).await.unwrap()).await;
        if group == "native-retry" {
            retry_subscription = subscription["subscription_id"].as_u64().unwrap();
        }
    }
    let topic = response(post("/topics".into(), json!({"topic":"platform.eventing", "name":"Platform Eventing", "partition_count":1, "consumption_order":"unordered", "state":"active"}), "native-eventing-topic").await.unwrap()).await;
    response(post(format!("/topics/{}/events", topic["topic_id"].as_u64().unwrap()), json!({"event_type":"eventing.handler.retry.requested", "schema_version":"1", "payload_schema": schemars::schema_for!(TargetedRetry), "state":"active"}), "native-eventing-definition").await.unwrap()).await;
    let group = response(
        post(
            "/groups".into(),
            json!({"group_key":"native-eventing-bridge","maximum_concurrency":1}),
            "native-eventing-group",
        )
        .await
        .unwrap(),
    )
    .await;
    response(post("/subscriptions".into(), json!({
        "topic":"platform.eventing", "group_id":group["group_id"], "delivery_mode":"sync", "initial_position":{"type":"latest"}, "filter":null, "pause_policy":"retain",
        "timeouts":{"invocation_seconds":2,"maximum_completion_seconds":3600},
        "retry":{"maximum_failure_attempts":3,"initial_delay_seconds":1,"maximum_delay_seconds":2,"multiplier":2.0,"jitter_ratio":0.0},
        "throttle":{"minimum_cooldown_seconds":1,"maximum_cooldown_seconds":2,"maximum_throttle_duration_seconds":10,"half_open_probe_limit":1}
    }), "native-eventing-subscription").await.unwrap()).await;
    retry_subscription
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "licensed native Event host; run zenss_channel_bootstrap_acceptance.py --event-test"]
async fn native_event_targets_one_member_per_group_retries_only_failed_group_and_rotates() {
    let resources = super::super::observation::tests::Capture::for_native_fixture();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    let retry_subscription = governance(&url, 2).await;
    let credential = || {
        ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap()).unwrap()
    };
    let primary_connection = ServiceConnection::connect(&url, credential())
        .await
        .unwrap();
    let secondary_connection = ServiceConnection::connect(&url, credential())
        .await
        .unwrap();
    let primary = pool(&primary_connection, "native-event-primary").await;
    let mut secondary = pool(&secondary_connection, "native-event-secondary").await;
    let first = Arc::new(Handlers::default());
    let second = Arc::new(Handlers::default());
    let first_registry = registry(&app, first.clone());
    let second_registry = registry(&app, second.clone());
    let budget = ServiceExecutionBudget::new(1).unwrap();
    let mut good = primary
        .register_consumer_role("native-good", "event-primary-good", 1)
        .await
        .unwrap();
    let mut retry = primary
        .register_consumer_role("native-retry", "event-primary-retry", 1)
        .await
        .unwrap();
    let mut bridge = primary
        .register_consumer_role("native-eventing-bridge", "event-primary-bridge", 1)
        .await
        .unwrap();
    primary
        .enable_sync_consumers(&mut bridge, first_registry.clone(), budget.clone())
        .unwrap();
    primary
        .enable_sync_consumers(&mut good, first_registry.clone(), budget.clone())
        .unwrap();
    primary
        .enable_sync_consumers(&mut retry, first_registry.clone(), budget.clone())
        .unwrap();
    let mut other = secondary
        .register_consumer_role("native-good", "event-secondary-good", 1)
        .await
        .unwrap();
    secondary
        .enable_sync_consumers(
            &mut other,
            second_registry,
            ServiceExecutionBudget::new(1).unwrap(),
        )
        .unwrap();
    if std::env::var_os("LINGSHU_GATEWAY_CONTROL_URL").is_some() {
        super::super::reconnect_host_tests::interrupt_and_reprove(
            &primary,
            &mut [&mut good, &mut retry, &mut bridge],
        )
        .await;
        super::super::reconnect_host_tests::observe(&secondary, true).await;
        secondary.confirm_role_route(&mut other).await.unwrap();
        for handlers in [&first, &second] {
            assert_eq!(handlers.good.load(Ordering::SeqCst), 0);
            assert_eq!(handlers.retry.load(Ordering::SeqCst), 0);
            assert_eq!(handlers.bridge.load(Ordering::SeqCst), 0);
        }
    }
    let client = ClientBuilder::new(ClientConfig::new(&url).with_retry_limit(1))
        .service_credential(credential())
        .connect()
        .unwrap();
    let lanes = primary.lane_count();
    let network_markers = std::env::var("LINGSHU_NETWORK_MARKER_DIR")
        .ok()
        .map(std::path::PathBuf::from);
    let observed_sessions = primary.sessions.clone();
    let original_router = observed_sessions[0]
        .info()
        .routers_zid()
        .await
        .next()
        .unwrap();
    let rotates_on_b = std::env::var("LINGSHU_NETWORK_ROTATE_ON_B").as_deref() == Ok("true");
    let ca_overlap = std::env::var("LINGSHU_CA_ISSUER_OVERLAP").as_deref() == Ok("true");
    let original_bootstrap = primary.identity().bootstrap_response().clone();
    if ca_overlap {
        assert!(rotates_on_b);
        let initial = &primary.identity().bootstrap_response().certificate;
        std::fs::write(
            network_markers.as_ref().unwrap().join("ca-initial.json"),
            serde_json::to_vec(
                &json!({"certificate": initial.certificate_pem, "roots": initial.root_ca_pem}),
            )
            .unwrap(),
        )
        .unwrap();
    }
    if rotates_on_b {
        assert!(network_markers.is_some());
        assert!((1..=4).contains(&primary.session_config().session_count()));
    }
    let original_certificate = primary
        .identity()
        .bootstrap_response()
        .certificate
        .certificate_identity
        .clone();
    let original_session_ids = primary.session_ids();
    let rotates = network_markers.is_none() || rotates_on_b;
    let original_good_generation = good
        .lifecycle_status(primary.authorization_deadline())
        .role_generation;
    let observed_good = good.consumers.lock().unwrap().as_ref().unwrap().clone();
    let observed_retry = retry.consumers.lock().unwrap().as_ref().unwrap().clone();
    // Full pools use bounded drain-before-open, retaining the four-Session cap.
    let mut managed = if rotates {
        primary
            .manage_roles_with_rotation(
                vec![good, retry, bridge],
                ChannelCertificateRotationConfig::new(Duration::from_secs(if rotates_on_b {
                    50
                } else {
                    20
                }))
                .unwrap(),
            )
            .unwrap()
    } else {
        primary.manage_roles(vec![good, retry, bridge]).unwrap()
    };
    let dispatch = client
        .event_dispatch()
        .with_managed_channel(&managed)
        .unwrap();
    let mut event = PublishEvent::typed(
        "native-fixture",
        &Order {
            sequence: 1,
            fail_once: true,
        },
    )
    .unwrap();
    // Durable delay ensures the original publication scope has already exited.
    event.delivery =
        kish_lingshu_event_dispatch_contract::DeliveryTime::after(Duration::from_millis(200));
    let options = MutationOptions::new("native-event/one").unwrap();
    dispatch
        .native_publication
        .as_ref()
        .unwrap()
        .discard_next_acceptance();
    let trace_root = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
    let receipt = super::super::trace::scope(
        Some(trace_root),
        dispatch.publish(event.clone(), options.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        dispatch
            .native_publication
            .as_ref()
            .unwrap()
            .wire_attempts(),
        2
    );
    assert_eq!(
        receipt.mutation.disposition,
        kish_lingshu_runtime_contract::MutationDisposition::Duplicate
    );
    let replay = super::super::trace::scope(
        Some("00-0123456789abcdef0123456789abcdef-1111111111111111-01"),
        dispatch.publish(event, options.clone()),
    )
    .await
    .unwrap();
    assert_eq!(receipt.event_id, replay.event_id);
    assert_eq!(
        replay.mutation.disposition,
        kish_lingshu_runtime_contract::MutationDisposition::Duplicate
    );
    let changed = dispatch
        .publish(
            PublishEvent::typed(
                "native-fixture",
                &Order {
                    sequence: 99,
                    fail_once: false,
                },
            )
            .unwrap(),
            options,
        )
        .await
        .unwrap_err();
    assert_eq!(
        changed.application_code(),
        Some(kish_lingshu_runtime_contract::CONFLICT_PROBLEM)
    );
    wait_for(|| {
        first.good.load(Ordering::SeqCst) + second.good.load(Ordering::SeqCst) == 1
            && first.retry.load(Ordering::SeqCst) == 2
    })
    .await;
    use kish_lingshu_foundation_contract::trace::TraceParent;
    let observed_traces: Vec<Value> = [&first, &second]
        .into_iter()
        .flat_map(|handlers| handlers.traces.lock().unwrap().clone())
        .filter(|value| value["event_id"] == receipt.event_id.get())
        .collect();
    assert_eq!(
        observed_traces.len(),
        3,
        "one successful group plus two Dispatch retry attempts"
    );
    let root = TraceParent::parse(trace_root).unwrap();
    let mut span_ids = std::collections::BTreeSet::new();
    for value in &observed_traces {
        let trace = TraceParent::parse(value["trace"].as_str().unwrap()).unwrap();
        let dispatch = TraceParent::parse(value["dispatch_parent"].as_str().unwrap()).unwrap();
        assert_eq!(trace.trace_id(), root.trace_id());
        assert_eq!(dispatch.trace_id(), root.trace_id());
        assert_eq!(trace.flags(), root.flags());
        assert_eq!(dispatch.flags(), root.flags());
        assert_ne!(trace.span_id(), dispatch.span_id());
        assert!(span_ids.insert(trace.span_id().to_owned()));
        assert_eq!(value["headers_empty"], true);
    }
    if let Some(directory) = std::env::var_os("LINGSHU_EVENT_TRACE_MARKER_DIR") {
        std::fs::write(std::path::Path::new(&directory).join("event-trace.json"),
            serde_json::to_vec(&json!({"event_id":receipt.event_id.get(), "root":trace_root, "handlers":observed_traces, "verified_control_reply_traces": super::super::trace::VERIFIED_CONTROL_TRACES.load(Ordering::SeqCst)})).unwrap()).unwrap();
    }
    let good_before_bridge = first.good.load(Ordering::SeqCst) + second.good.load(Ordering::SeqCst);
    let payload_json = "{ \"sequence\" : 7, \"fail_once\" : true }";
    dispatch
        .publish(
            PublishEvent::typed(
                "eventing",
                &TargetedRetry {
                    request: RetryRequest {
                        target_application: app.clone(),
                        target_consumer_group: "native-retry".into(),
                        original_event: OriginalEvent {
                            event_id: "original-local-event-7".into(),
                            topic: "native.events".into(),
                            event_type: "native.order.created".into(),
                            schema_version: "1".into(),
                            occurred_at: 7,
                            partition_key: None,
                            correlation_id: Some("original-correlation".into()),
                            payload_json: payload_json.into(),
                        },
                        failure: LocalFailure {
                            code: "local_handler_failed".into(),
                            permanent: false,
                            attempted_at: 8,
                        },
                    },
                },
            )
            .unwrap(),
            MutationOptions::new("native-eventing/failed-group-only").unwrap(),
        )
        .await
        .unwrap();
    wait_for(|| first.bridge.load(Ordering::SeqCst) == 2).await;
    assert_eq!(
        first.good.load(Ordering::SeqCst) + second.good.load(Ordering::SeqCst),
        good_before_bridge
    );
    assert_eq!(
        *first.originals.lock().unwrap(),
        vec![
            ("original-local-event-7".into(), payload_json.into()),
            ("original-local-event-7".into(), payload_json.into()),
        ]
    );
    let retry_keys = first.identities.lock().unwrap().clone();
    let retry_keys: Vec<_> = retry_keys
        .iter()
        .filter(|(_, key, _)| key.contains(&format!("/{retry_subscription}/")))
        .collect();
    assert_eq!(retry_keys.len(), 2);
    assert_eq!(retry_keys[0], retry_keys[1]);
    assert!(first
        .identities
        .lock()
        .unwrap()
        .iter()
        .chain(second.identities.lock().unwrap().iter())
        .all(|(id, _, sequence)| *id == receipt.event_id.get() && *sequence == 1));
    // Stop only the second replica. The original member generations survive
    // certificate adoption; later deliveries use the newly proved lane.
    secondary.deregister_role(&mut other).await.unwrap();
    secondary.close().await.unwrap();
    if let Some(markers) = network_markers.as_ref() {
        assert_eq!(
            std::env::var("LINGSHU_NETWORK_TRUSTED_TRANSFER").as_deref(),
            Ok("true")
        );
        let original_generations: Vec<_> = managed
            .role_statuses()
            .iter()
            .map(|s| s.role_generation.clone())
            .collect();
        let original_deadline = managed
            .role_statuses()
            .iter()
            .map(|s| s.authorization_deadline)
            .max()
            .unwrap();
        async fn marker(directory: &std::path::Path, name: &str) {
            tokio::time::timeout(Duration::from_secs(15), async {
                while !directory.join(name).exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("Event Router fixture coordination deadline");
        }
        std::fs::write(
            markers.join("sdk-ready"),
            "original group deliveries confirmed",
        )
        .unwrap();
        marker(&markers, "sdk-partitioned").await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while managed
                .connectivity_status()
                .connected_data_lanes
                .iter()
                .any(|v| *v)
                || managed.role_statuses().iter().any(|s| s.route_confirmed())
            {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("Consumer disconnect must invalidate route readiness");
        assert_eq!(
            first.good.load(Ordering::SeqCst) + second.good.load(Ordering::SeqCst),
            1
        );
        assert_eq!(first.retry.load(Ordering::SeqCst), 2);
        assert_eq!(first.bridge.load(Ordering::SeqCst), 2);
        std::fs::write(markers.join("sdk-blocked"), "no consumer replay").unwrap();
        marker(&markers, "sdk-restored").await;
        tokio::time::timeout(Duration::from_secs(20), async {
            while managed
                .connectivity_status()
                .connected_data_lanes
                .iter()
                .any(|v| !*v)
                || managed.role_statuses().iter().any(|s| !s.route_confirmed())
            {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("original managed Consumer roles must freshly prove on Router B");
        for session in &observed_sessions {
            assert!(session
                .info()
                .routers_zid()
                .await
                .any(|id| id != original_router));
        }
        assert_eq!(managed.rotation_status().session_ids, original_session_ids);
        assert_eq!(managed.rotation_status().completed, 0);
        // Continuous source renewal on B must outlive the original role window.
        tokio::time::sleep_until(original_deadline + Duration::from_secs(2)).await;
        // The serial owner may be inside fresh probing at this instant. Require
        // actual renewed authority and proof within a finite observation window;
        // do not confuse a coalesced in-progress snapshot with terminal loss.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let statuses = managed.role_statuses();
                assert!(statuses
                    .iter()
                    .all(|s| s.state == super::super::RoleLifecycleState::Active));
                if statuses
                    .iter()
                    .all(|s| s.authorization_deadline > original_deadline && s.route_confirmed())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("original roles must renew and freshly prove beyond their initial window");
        assert_eq!(
            managed
                .role_statuses()
                .iter()
                .map(|s| s.role_generation.clone())
                .collect::<Vec<_>>(),
            original_generations
        );
        assert_eq!(
            first.good.load(Ordering::SeqCst) + second.good.load(Ordering::SeqCst),
            1
        );
        assert_eq!(first.retry.load(Ordering::SeqCst), 2);
        if rotates_on_b {
            // Wait for an actual new CSR/pool on B after the old finite window.
            // Source A stays authoritative and its SDK entrance remains closed.
            // Rotation is scheduled at 50s and its finite candidate may use
            // 30s plus cleanup. The earlier 35s observation from the first
            // role's ~32s deadline left almost no margin on Linux.
            tokio::time::timeout(Duration::from_secs(50), async {
                while managed.rotation_status().completed == 0 {
                    let rotation = managed.rotation_status();
                    assert_ne!(
                        rotation.phase,
                        super::super::CertificateRotationPhase::Failed,
                        "rotation failed on B: {:?}; supervisor {:?}",
                        rotation.last_error,
                        managed.status()
                    );
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                while observed_sessions.iter().any(|session| !session.is_closed()) {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "rotation did not finish: phase {:?}, completed {}, predecessor closed {:?}, supervisor {:?}",
                    managed.rotation_status().phase,
                    managed.rotation_status().completed,
                    observed_sessions.iter().map(|s| s.is_closed()).collect::<Vec<_>>(),
                    managed.status()
                )
            });
            let rotation = managed.rotation_status();
            assert_eq!(rotation.completed, 1);
            if ca_overlap {
                let current = managed.publication.current.borrow().clone().unwrap();
                let adopted = current.test_bootstrap();
                let original = &original_bootstrap;
                assert_eq!(
                    adopted.certificate.root_ca_pem,
                    original.certificate.root_ca_pem
                );
                assert_eq!(adopted.instance.instance_id, original.instance.instance_id);
                assert_eq!(adopted.instance.generation, original.instance.generation);
                assert_ne!(
                    adopted.certificate.message_public_key,
                    original.certificate.message_public_key
                );
                std::fs::write(
                    markers.join("ca-adopted.json"),
                    serde_json::to_vec(&json!({"certificate": adopted.certificate.certificate_pem, "roots": adopted.certificate.root_ca_pem})).unwrap(),
                ).unwrap();
            }
            assert_ne!(rotation.certificate_identity, original_certificate);
            assert_eq!(rotation.session_ids.len(), original_session_ids.len());
            assert!(rotation
                .session_ids
                .iter()
                .all(|id| !original_session_ids.contains(id)));
            assert!(managed.role_statuses().iter().all(|s| s.route_confirmed()));
            assert_eq!(
                managed
                    .role_statuses()
                    .iter()
                    .map(|s| s.role_generation.clone())
                    .collect::<Vec<_>>(),
                original_generations
            );
            assert_eq!(
                primary_connection
                    .channel_session_budget()
                    .available_permits(),
                4 - original_session_ids.len()
            );
            assert_eq!(
                first.good.load(Ordering::SeqCst) + second.good.load(Ordering::SeqCst),
                1
            );
            assert_eq!(first.retry.load(Ordering::SeqCst), 2);
            assert_eq!(first.bridge.load(Ordering::SeqCst), 2);
            std::fs::write(markers.join("router-rotation-facts.json"), serde_json::to_vec(&json!({
                "physical_sessions":rotation.session_ids.len(), "completed_rotations":rotation.completed,
                "same_member_generations":true, "old_sessions_closed":true, "no_group_replay":true,
            })).unwrap()).unwrap();
        }
        std::fs::write(
            markers.join("router-transfer-facts.json"),
            serde_json::to_vec(&json!({
                "physical_sessions":observed_sessions.len(), "data_lanes":lanes,
                "past_original_role_window":true, "same_member_generations":true,
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            markers.join("sdk-completed"),
            "fresh Consumer reproof; no group replay",
        )
        .unwrap();
    }
    if rotates {
        tokio::time::timeout(Duration::from_secs(35), async {
            while managed.rotation_status().completed == 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
    }
    assert!(managed.role_statuses().iter().all(|s| s.route_confirmed()));
    dispatch
        .publish(
            PublishEvent::typed(
                "native-fixture",
                &Order {
                    sequence: 2,
                    fail_once: false,
                },
            )
            .unwrap(),
            MutationOptions::new("native-event/two").unwrap(),
        )
        .await
        .unwrap();
    wait_for(|| {
        first.good.load(Ordering::SeqCst) + second.good.load(Ordering::SeqCst) == 2
            && first.retry.load(Ordering::SeqCst) == 3
    })
    .await;
    if std::env::var("LINGSHU_VERIFY_DATA_LANES").as_deref() == Ok("true") {
        for index in 0..lanes {
            dispatch
                .publish(
                    PublishEvent::typed(
                        "native-fixture",
                        &Order {
                            sequence: 3 + index as u64,
                            fail_once: false,
                        },
                    )
                    .unwrap(),
                    MutationOptions::new(format!("native-event/lane-{index}")).unwrap(),
                )
                .await
                .unwrap();
            wait_for(|| {
                first.good.load(Ordering::SeqCst) + second.good.load(Ordering::SeqCst) == 3 + index
                    && first.retry.load(Ordering::SeqCst) == 4 + index
            })
            .await;
        }
        for observed in [&observed_good, &observed_retry] {
            assert!(observed.received_lanes[..lanes]
                .iter()
                .all(|n| n.load(Ordering::SeqCst) > 0));
            assert!(observed.received_lanes[lanes..]
                .iter()
                .all(|n| n.load(Ordering::SeqCst) == 0));
        }
        eprintln!("PASS {lanes} physical Consumer lanes retain group targeting and one shared execution budget");
    }
    if std::env::var("LINGSHU_VERIFY_DYNAMIC_ROLES").as_deref() == Ok("true") {
        let good_before = first.good.load(Ordering::SeqCst) + second.good.load(Ordering::SeqCst);
        let retry_before = first.retry.load(Ordering::SeqCst);
        let removed = managed
            .deregister_role(RouteIdentity::new(original_good_generation.clone()).unwrap())
            .await
            .unwrap();
        assert!(removed.remote_deregistered);
        assert_eq!(managed.role_statuses().len(), 2);
        assert!(managed.role_statuses().iter().all(|s| s.route_confirmed()));
        let added = managed
            .enroll_consumer(
                "native-good",
                "event-primary-dynamic",
                1,
                first_registry,
                budget.clone(),
            )
            .await
            .unwrap();
        assert_ne!(added.role_generation, original_good_generation);
        assert!(added.route_confirmed());
        dispatch
            .publish(
                PublishEvent::typed(
                    "native-fixture",
                    &Order {
                        sequence: 100,
                        fail_once: false,
                    },
                )
                .unwrap(),
                MutationOptions::new("native-event/dynamic-role").unwrap(),
            )
            .await
            .unwrap();
        wait_for(|| {
            first.good.load(Ordering::SeqCst) + second.good.load(Ordering::SeqCst)
                == good_before + 1
                && first.retry.load(Ordering::SeqCst) == retry_before + 1
        })
        .await;
        eprintln!("PASS dynamic Consumer removal/addition preserves sibling group, Dispatch custody and shared business budget");
    }
    if std::env::var("LINGSHU_NETWORK_SOURCE_AUTHORITY_LOSS").as_deref() == Ok("true") {
        let markers = network_markers.as_ref().unwrap();
        let generations: Vec<_> = managed
            .role_statuses()
            .iter()
            .map(|s| s.role_generation.clone())
            .collect();
        let before_counts = (
            first.good.load(Ordering::SeqCst) + second.good.load(Ordering::SeqCst),
            first.retry.load(Ordering::SeqCst),
            first.bridge.load(Ordering::SeqCst),
        );
        let super::super::ChannelSupervisorStatus::Active {
            authorization_deadline,
            ..
        } = managed.status()
        else {
            panic!("source-loss fixture requires a live adopted pool");
        };
        assert!(authorization_deadline <= tokio::time::Instant::now() + Duration::from_secs(30));
        std::fs::write(
            markers.join("source-loss-ready"),
            "new directed deliveries committed on B",
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !markers.join("source-lost").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let redis_state_loss =
            std::env::var("LINGSHU_NETWORK_REDIS_STATE_LOSS").as_deref() == Ok("true");
        let source_authority_restart =
            std::env::var("LINGSHU_NETWORK_SOURCE_AUTHORITY_RESTART").as_deref() == Ok("true");
        if source_authority_restart {
            tokio::time::timeout_at(authorization_deadline, async {
                while !markers.join("source-restarted").exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("the same source Router must be ready before the original finite expiry");
            assert!(matches!(
                managed.status(),
                super::super::ChannelSupervisorStatus::Active { .. }
            ));
        }
        let publish_center_work = || async {
            // A separate HTTP producer deliberately creates new durable work.
            // This is test setup, not fallback of the managed native publisher.
            let response = reqwest::Client::new()
                .post(format!("{url}/api/user/event-dispatch/v1/events"))
                .header(
                    "x-token",
                    std::env::var("LINGSHU_EVENT_TEST_TOKEN").unwrap(),
                )
                .header("x-kish-app-id", &app)
                .header("Idempotency-Key", "native-event/authority-lost-new-work")
                .json(&json!({
                    "topic": "native.events", "event_type": "native.order.created",
                    "schema_version": "1", "source": "native-fixture",
                    "payload": { "sequence": 201, "fail_once": false },
                }))
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), reqwest::StatusCode::CREATED);
            let receipt: Value = response.json().await.unwrap();
            assert!(receipt["event_id"].as_u64().is_some());
            assert_eq!(receipt["status"], "published");
            assert_eq!(receipt["duplicate"], false);
        };
        if redis_state_loss {
            // Shared domain authority is absent immediately, even before TLS expiry.
            publish_center_work().await;
        }
        tokio::time::timeout_at(authorization_deadline + Duration::from_secs(5), async {
            loop {
                match managed.status() {
                    super::super::ChannelSupervisorStatus::Active {
                        authorization_deadline: current,
                        ..
                    } => assert!(
                        current <= authorization_deadline,
                        "lost original authority cannot extend the last finite deadline"
                    ),
                    super::super::ChannelSupervisorStatus::Closed {
                        reason: super::super::ChannelCloseReason::AuthorityExpired,
                    } => break,
                    super::super::ChannelSupervisorStatus::Stopping {
                        reason: super::super::ChannelCloseReason::AuthorityExpired,
                    } => {}
                    unexpected => panic!("unexpected source-loss status: {unexpected:?}"),
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("foreign authority must close within the original deadline and bounded cleanup");
        assert!(managed.role_statuses().iter().all(|s| !s.route_confirmed()));
        if source_authority_restart {
            // Source loss alone leaves B's original finite permissions valid.
            // Test new admission after that exact deadline, not during its lease.
            publish_center_work().await;
        }
        if redis_state_loss || source_authority_restart {
            // Let a directed scan run after the physical pool has also closed.
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        assert_eq!(
            managed
                .role_statuses()
                .iter()
                .map(|s| s.role_generation.clone())
                .collect::<Vec<_>>(),
            generations
        );
        let published = dispatch
            .publish(
                PublishEvent::typed(
                    "native-fixture",
                    &Order {
                        sequence: 200,
                        fail_once: false,
                    },
                )
                .unwrap(),
                MutationOptions::new("native-event/source-lost").unwrap(),
            )
            .await;
        assert!(
            published.is_err(),
            "expired managed publisher cannot silently fall back to HTTP"
        );
        assert_eq!(
            (
                first.good.load(Ordering::SeqCst) + second.good.load(Ordering::SeqCst),
                first.retry.load(Ordering::SeqCst),
                first.bridge.load(Ordering::SeqCst)
            ),
            before_counts
        );
        primary_connection.ensure_open().unwrap();
        let mut facts = json!({
            "hard_deadline_preserved":true, "same_member_generations":true, "no_group_replay":true, "publication_rejected":true,
        });
        if redis_state_loss || source_authority_restart {
            facts["center_http_publication_confirmed"] = json!(true);
            facts["pending_event_not_delivered"] = json!(true);
        }
        if source_authority_restart {
            facts["source_ready_before_expiry"] = json!(true);
        }
        std::fs::write(
            markers.join("source-loss-facts.json"),
            serde_json::to_vec(&facts).unwrap(),
        )
        .unwrap();
        std::fs::write(
            markers.join("source-loss-completed"),
            "foreign permission expired; no reenrollment",
        )
        .unwrap();
    }
    managed.close().await.unwrap();
    assert_eq!(budget.semaphore.available_permits(), 1);
    assert!(
        primary_connection
            .channel_session_budget()
            .available_permits()
            == 4
    );
    if let Some(resources) = resources {
        resources.assert_balanced_resource_gauges();
        resources.assert_bounded_labels();
        for outcome in ["completed", "retryable_failure"] {
            assert!(
                resources.sum(
                    "lingshu_sdk_channel_business_results_total",
                    Some("consumer"),
                    Some(outcome)
                ) >= 1.0
            );
        }
        assert!(
            resources.sum(
                "lingshu_sdk_channel_inbound_exchanges_total",
                Some("consumer"),
                Some("reply_submitted")
            ) >= 1.0
        );
        assert!(
            resources.sum(
                "lingshu_sdk_channel_verified_dispositions_total",
                Some("publication"),
                Some("accepted")
            ) >= 2.0
        );
        assert!(
            resources.sum(
                "lingshu_sdk_channel_exchanges_total",
                Some("publication"),
                Some("unknown")
            ) >= 1.0
        );
        assert!(
            resources.sum(
                "lingshu_sdk_channel_exchanges_total",
                Some("publication"),
                Some("reply_verified")
            ) >= 1.0
        );
        if std::env::var("LINGSHU_VERIFY_DYNAMIC_ROLES").as_deref() == Ok("true") {
            assert!(resources.count("lingshu_sdk_channel_command_queue_wait_seconds") >= 2);
        }
        eprintln!("PASS SDK Event publication: verified center custody plus lost ACK unknown, directed consumers and balanced resources");
        eprintln!("PASS SDK Consumer results and submitted replies preserve Dispatch-directed failed-group retry");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "licensed matched Host drain; run acceptance --event-drain-test"]
async fn native_event_original_reply_commits_during_product_drain() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    governance(&url, 10).await;
    let credential = || {
        ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap()).unwrap()
    };
    let connection = ServiceConnection::connect(&url, credential())
        .await
        .unwrap();
    let mut sessions = pool(&connection, "native-event-drain").await;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let handlers = Arc::new(Handlers {
        drain_gate: Some(gate.clone()),
        ..Default::default()
    });
    let mut builder = ConsumerRegistry::builder(&app).unwrap();
    builder.bind(handlers.clone()).unwrap();
    let mut role = sessions
        .register_consumer_role("native-good", "event-drain-good", 1)
        .await
        .unwrap();
    let budget = ServiceExecutionBudget::new(1).unwrap();
    sessions
        .enable_sync_consumers(
            &mut role,
            Arc::new(builder.build().unwrap()),
            budget.clone(),
        )
        .unwrap();
    let client = ClientBuilder::new(ClientConfig::new(&url).with_retry_limit(0))
        .service_credential(credential())
        .connect()
        .unwrap();
    client
        .event_dispatch()
        .with_channel(&sessions)
        .unwrap()
        .publish(
            PublishEvent::typed(
                "native-drain",
                &Order {
                    sequence: 1,
                    fail_once: false,
                },
            )
            .unwrap(),
            MutationOptions::new("native-event/drain").unwrap(),
        )
        .await
        .unwrap();
    wait_for(|| handlers.good.load(Ordering::SeqCst) == 1).await;
    assert_eq!(budget.semaphore.available_permits(), 0);
    let markers = std::path::PathBuf::from(std::env::var("LINGSHU_DRAIN_MARKER_DIR").unwrap());
    std::fs::write(markers.join("accepted"), "one directed Consumer executing").unwrap();
    wait_for(|| markers.join("draining").exists()).await;
    gate.add_permits(1);
    // Keep the original listener until Dispatch has persisted the result;
    // there is no HTTP completion request or SDK publication retry here.
    wait_for(|| markers.join("persisted").exists()).await;
    assert_eq!(handlers.good.load(Ordering::SeqCst), 1);
    assert_eq!(budget.semaphore.available_permits(), 1);
    std::fs::write(
        markers.join("completed"),
        "one original Consumer reply committed",
    )
    .unwrap();
    role.close().await.unwrap();
    sessions.close().await.unwrap();
    connection.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "licensed host; run acceptance --full-pool-event-drain-test"]
async fn native_full_pool_rotation_drains_consumer_and_dispatch_recovers_fenced_reply() {
    assert!(ChannelSessionConfig::host_test().session_count() > 2);
    let _ = rustls::crypto::ring::default_provider().install_default();
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    governance(&url, 10).await;
    let credential = || {
        ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap()).unwrap()
    };
    let connection = ServiceConnection::connect(&url, credential())
        .await
        .unwrap();
    let sessions = pool(&connection, "native-event-full-drain").await;
    let original_sessions = sessions.session_ids();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let handlers = Arc::new(Handlers {
        drain_gate: Some(gate.clone()),
        rotation_drain: true,
        ..Default::default()
    });
    let mut role = sessions
        .register_consumer_role("native-good", "event-full-drain-good", 1)
        .await
        .unwrap();
    let generation = role
        .lifecycle_status(sessions.authorization_deadline())
        .role_generation;
    let budget = ServiceExecutionBudget::new(1).unwrap();
    sessions
        .enable_sync_consumers(&mut role, registry(&app, handlers.clone()), budget.clone())
        .unwrap();
    let mut managed = sessions
        .manage_roles_with_rotation(
            vec![role],
            ChannelCertificateRotationConfig::new(Duration::from_secs(20)).unwrap(),
        )
        .unwrap();
    let client = ClientBuilder::new(ClientConfig::new(&url).with_retry_limit(0))
        .service_credential(credential())
        .connect()
        .unwrap();
    let publisher = client
        .event_dispatch()
        .with_managed_channel(&managed)
        .unwrap();
    // Begin one bounded request just before the scheduled rotation, keeping its
    // original ten-second delivery window intact throughout declaration drain.
    tokio::time::sleep(Duration::from_secs(18)).await;
    publisher
        .publish(
            PublishEvent::typed(
                "native-full-drain",
                &Order {
                    sequence: 1,
                    fail_once: false,
                },
            )
            .unwrap(),
            MutationOptions::new("native-event/full-drain").unwrap(),
        )
        .await
        .unwrap();
    wait_for(|| handlers.good.load(Ordering::SeqCst) == 1).await;
    tokio::time::timeout(Duration::from_secs(7), async {
        loop {
            assert_eq!(managed.rotation_status().last_error, None);
            if managed
                .role_statuses()
                .iter()
                .all(|s| s.state == super::super::RoleLifecycleState::Stopped)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        budget.semaphore.available_permits(),
        0,
        "handoff must not cancel the accepted Consumer"
    );
    assert_eq!(managed.rotation_status().completed, 0);
    assert_eq!(
        connection.channel_session_budget().available_permits(),
        4 - original_sessions.len()
    );
    gate.add_permits(1);
    wait_for(|| managed.rotation_status().completed == 1).await;
    // Reply submission is not durable completion. A route handoff may fence
    // the old reply; only Dispatch can schedule the same-event recovery.
    let markers = std::path::PathBuf::from(std::env::var("LINGSHU_FULL_POOL_MARKER_DIR").unwrap());
    std::fs::write(
        markers.join("rotated"),
        "accepted Consumer finished without local cancellation",
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        while !markers.join("persisted").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let executions = handlers.good.load(Ordering::SeqCst);
    assert!((1..=2).contains(&executions));
    let identities = handlers.identities.lock().unwrap().clone();
    assert_eq!(identities.len(), executions);
    assert!(identities.iter().all(|identity| identity == &identities[0]));
    let attempts: Vec<_> = handlers
        .traces
        .lock()
        .unwrap()
        .iter()
        .map(|trace| trace["attempt"].as_u64().unwrap())
        .collect();
    assert_eq!(attempts, if executions == 1 { vec![1] } else { vec![1, 2] });
    eprintln!("PASS full-pool Consumer: original execution finished; {executions} Dispatch attempts; original event/idempotency identity preserved; durable cursor verified externally");
    assert_eq!(budget.semaphore.available_permits(), 1);
    assert_eq!(managed.role_statuses()[0].role_generation, generation);
    assert!(managed.role_statuses()[0].route_confirmed());
    assert!(managed
        .rotation_status()
        .session_ids
        .iter()
        .all(|id| !original_sessions.contains(id)));
    assert_eq!(
        managed.rotation_status().session_ids.len(),
        original_sessions.len()
    );
    managed.close().await.unwrap();
    assert_eq!(connection.channel_session_budget().available_permits(), 4);
    connection.shutdown().await;
}
