#![cfg(feature = "event-consumer-http")]

use std::{
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use chrono::Utc;
use kish_lingshu_event_dispatch_contract::{
    ConsumerInstanceDeregisterRequestV1, ConsumerInstanceHeartbeatRequestV1,
    ConsumerInstanceLeaseV1, ConsumerInstanceRegistrationRequestV1,
};
use kish_lingshu_sdk::{
    event_dispatch::{
        ConsumerNode, ConsumerNodeConfig, ConsumerNodeConfigError, ConsumerNodeError,
        ConsumerNodeStatus,
    },
    ConsumerGroupRegistrationCredential,
};
use serde_json::json;

const REGISTRATION_SECRET: &str = "edrk_v1_test-registration-secret";
const SUCCESS: u8 = 0;
const FENCE: u8 = 1;
const FAIL_ONCE: u8 = 2;

#[derive(Default)]
struct MockControlPlane {
    mode: AtomicU8,
    registrations: AtomicUsize,
    heartbeats: AtomicUsize,
    deregistrations: AtomicUsize,
    registration: Mutex<Option<ConsumerInstanceRegistrationRequestV1>>,
}

impl MockControlPlane {
    fn lease() -> ConsumerInstanceLeaseV1 {
        ConsumerInstanceLeaseV1 {
            member_id: 91,
            node_id: "orders-pod-1".into(),
            membership_generation: 7,
            lease_seconds: 6,
            heartbeat_interval_seconds: 1,
            lease_expires_at: Utc::now() + chrono::Duration::seconds(6),
        }
    }
}

fn authenticated(headers: &HeaderMap) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        == Some(&format!("Bearer {REGISTRATION_SECRET}"))
}

async fn register(
    State(state): State<Arc<MockControlPlane>>,
    headers: HeaderMap,
    Json(request): Json<ConsumerInstanceRegistrationRequestV1>,
) -> Response {
    if !authenticated(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"code": "consumer_registration_unauthorized", "message": "rejected"})),
        )
            .into_response();
    }
    state.registrations.fetch_add(1, Ordering::SeqCst);
    *state.registration.lock().unwrap() = Some(request);
    (StatusCode::CREATED, Json(MockControlPlane::lease())).into_response()
}

async fn heartbeat(
    State(state): State<Arc<MockControlPlane>>,
    Path(member_id): Path<u64>,
    headers: HeaderMap,
    Json(request): Json<ConsumerInstanceHeartbeatRequestV1>,
) -> Response {
    if !authenticated(&headers)
        || member_id != 91
        || request.group_id != 41
        || request.node_id != "orders-pod-1"
        || request.membership_generation != 7
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"code": "consumer_registration_unauthorized", "message": "rejected"})),
        )
            .into_response();
    }
    let count = state.heartbeats.fetch_add(1, Ordering::SeqCst) + 1;
    match state.mode.load(Ordering::SeqCst) {
        FENCE => (
            StatusCode::CONFLICT,
            Json(json!({"code": "membership_generation_conflict", "message": "replaced"})),
        )
            .into_response(),
        FAIL_ONCE if count == 1 => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"code": "temporarily_unavailable", "message": "retry"})),
        )
            .into_response(),
        _ => Json(MockControlPlane::lease()).into_response(),
    }
}

async fn deregister(
    State(state): State<Arc<MockControlPlane>>,
    Path(member_id): Path<u64>,
    headers: HeaderMap,
    Json(request): Json<ConsumerInstanceDeregisterRequestV1>,
) -> StatusCode {
    if authenticated(&headers)
        && member_id == 91
        && request.group_id == 41
        && request.node_id == "orders-pod-1"
        && request.membership_generation == 7
    {
        state.deregistrations.fetch_add(1, Ordering::SeqCst);
        StatusCode::NO_CONTENT
    } else {
        StatusCode::UNAUTHORIZED
    }
}

async fn start_control_plane(
    state: Arc<MockControlPlane>,
) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route(
            "/api/user/event-dispatch/v1/consumer-instances",
            post(register),
        )
        .route(
            "/api/user/event-dispatch/v1/consumer-instances/{member_id}/heartbeat",
            post(heartbeat),
        )
        .route(
            "/api/user/event-dispatch/v1/consumer-instances/{member_id}/deregister",
            post(deregister),
        )
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (
        format!("http://{address}/api/user/event-dispatch/v1"),
        server,
    )
}

fn node_config(control_plane_url: String) -> ConsumerNodeConfig {
    ConsumerNodeConfig {
        control_plane_url,
        app_id: "orders".into(),
        group_id: 41,
        registration_credential: ConsumerGroupRegistrationCredential::new(REGISTRATION_SECRET)
            .unwrap(),
        node_id: "orders-pod-1".into(),
        invocation_url: "http://orders-pod-1.internal/events".into(),
        maximum_in_flight: 8,
    }
}

fn client() -> reqwest::Client {
    let _ = rustls::crypto::ring::default_provider().install_default();
    reqwest::Client::new()
}

async fn wait_for_status(
    status: &mut tokio::sync::watch::Receiver<ConsumerNodeStatus>,
    predicate: impl Fn(&ConsumerNodeStatus) -> bool,
) -> ConsumerNodeStatus {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = status.borrow().clone();
            if predicate(&current) {
                return current;
            }
            status.changed().await.unwrap();
        }
    })
    .await
    .expect("node status transition")
}

#[tokio::test]
async fn registers_heartbeats_and_deregisters_with_the_group_credential() {
    let state = Arc::new(MockControlPlane::default());
    state.mode.store(SUCCESS, Ordering::SeqCst);
    let (url, server) = start_control_plane(state.clone()).await;
    let node = ConsumerNode::start_with_client(node_config(url), client())
        .await
        .unwrap();
    assert!(matches!(
        node.status(),
        ConsumerNodeStatus::Registered { .. }
    ));
    let mut status = node.subscribe();
    wait_for_status(&mut status, |_| state.heartbeats.load(Ordering::SeqCst) > 0).await;

    let request = state.registration.lock().unwrap().clone().unwrap();
    assert_eq!(request.app_id, "orders");
    assert_eq!(request.group_id, 41);
    assert_eq!(request.node_id, "orders-pod-1");
    assert_eq!(request.maximum_in_flight, 8);

    node.shutdown().await;
    assert_eq!(state.registrations.load(Ordering::SeqCst), 1);
    assert_eq!(state.deregistrations.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn generation_conflict_fences_the_old_node() {
    let state = Arc::new(MockControlPlane::default());
    state.mode.store(FENCE, Ordering::SeqCst);
    let (url, server) = start_control_plane(state).await;
    let node = ConsumerNode::start_with_client(node_config(url), client())
        .await
        .unwrap();
    let mut status = node.subscribe();
    let fenced = wait_for_status(&mut status, |value| {
        matches!(value, ConsumerNodeStatus::Fenced { .. })
    })
    .await;
    assert!(fenced.is_terminal());
    assert_eq!(fenced.lease().membership_generation, 7);
    node.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn transient_heartbeat_failure_retries_within_the_lease() {
    let state = Arc::new(MockControlPlane::default());
    state.mode.store(FAIL_ONCE, Ordering::SeqCst);
    let (url, server) = start_control_plane(state.clone()).await;
    let node = ConsumerNode::start_with_client(node_config(url), client())
        .await
        .unwrap();
    let mut status = node.subscribe();
    let retrying = wait_for_status(&mut status, |value| {
        matches!(value, ConsumerNodeStatus::HeartbeatRetrying { .. })
    })
    .await;
    assert!(!retrying.is_terminal());
    wait_for_status(&mut status, |value| {
        state.heartbeats.load(Ordering::SeqCst) >= 2
            && matches!(value, ConsumerNodeStatus::Registered { .. })
    })
    .await;
    node.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn invalid_node_fails_before_network_io_and_secret_is_redacted() {
    let mut config = node_config("http://127.0.0.1:9/api/user/event-dispatch/v1".into());
    assert!(!format!("{:?}", config.registration_credential).contains(REGISTRATION_SECRET));
    assert!(!format!("{config:?}").contains(REGISTRATION_SECRET));
    config.node_id.clear();
    assert_eq!(
        ConsumerNode::start_with_client(config, client())
            .await
            .unwrap_err(),
        ConsumerNodeError::InvalidConfig(ConsumerNodeConfigError::InvalidNodeId)
    );
}
