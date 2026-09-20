#![cfg(feature = "service-auth")]

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{header, HeaderMap, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use kish_lingshu_event_dispatch_contract::{
    ConsumerEnrollmentRequest, ConsumerInstanceLeaseV1, ConsumerSession,
};
use kish_lingshu_foundation_contract::service_auth::{ServiceSigner, SIGNATURE_HEADER};
use kish_lingshu_sdk::{
    event_dispatch::{EnrolledConsumerNodeConfig, EnrolledConsumerNodeStatus},
    ServiceAuthError, ServiceConnection, ServiceCredential,
};
use tokio::task::JoinHandle;
use tower::ServiceExt;

const ROOT_KEY: &str = "application-root-do-not-disclose";
const APP: &str = "orders";
const TARGET: &str = "https://callback.example/prefix/callback?tenant=orders&value=%2F";

#[derive(Default)]
struct Control {
    mode: AtomicU8,
    signing_root: AtomicU8,
    trust_calls: AtomicUsize,
    enrollments: AtomicUsize,
    heartbeats: AtomicUsize,
    deregistrations: AtomicUsize,
    sessions: Mutex<HashMap<String, ConsumerSession>>,
}

impl Control {
    fn signer(&self) -> ServiceSigner {
        ServiceSigner::new(&[self.signing_root.load(Ordering::SeqCst); 32]).unwrap()
    }
}

fn check_root(headers: &HeaderMap) {
    assert_eq!(
        headers.get(header::AUTHORIZATION).unwrap(),
        &format!("Bearer {ROOT_KEY}")
    );
    assert_eq!(headers.get("x-kish-app-id").unwrap(), APP);
}

async fn trust(State(control): State<Arc<Control>>, headers: HeaderMap) -> Response {
    check_root(&headers);
    control.trust_calls.fetch_add(1, Ordering::SeqCst);
    let mut trust = control.signer().trust(APP, Utc::now().timestamp()).unwrap();
    match control.mode.load(Ordering::SeqCst) {
        20 => trust.app_id = "foreign".into(),
        21 => {
            return (
                StatusCode::FOUND,
                [(header::LOCATION, "/redirect-secret-trap")],
            )
                .into_response()
        }
        22 => return "x".repeat(128 * 1024).into_response(),
        23 => trust.expires_at = Utc::now().timestamp() + 2,
        24 => return (StatusCode::SERVICE_UNAVAILABLE, ROOT_KEY).into_response(),
        25 => return (StatusCode::UNAUTHORIZED, ROOT_KEY).into_response(),
        26 => {
            return Response::new(Body::from_stream(futures::stream::iter([
                Ok::<_, std::io::Error>(Bytes::from(vec![b'x'; 40 * 1024])),
                Ok(Bytes::from(vec![b'x'; 40 * 1024])),
            ])))
        }
        _ => {}
    }
    Json(trust).into_response()
}

async fn enroll(
    State(control): State<Arc<Control>>,
    headers: HeaderMap,
    Json(input): Json<ConsumerEnrollmentRequest>,
) -> Response {
    check_root(&headers);
    let generation = control.enrollments.fetch_add(1, Ordering::SeqCst) as u64 + 1;
    if control.mode.load(Ordering::SeqCst) == 27 {
        return (StatusCode::UNAUTHORIZED, ROOT_KEY).into_response();
    }
    let session = ConsumerSession {
        group_id: 17,
        group_key: input.group_key,
        lease: ConsumerInstanceLeaseV1 {
            member_id: generation,
            node_id: input.node_id.clone(),
            membership_generation: generation,
            lease_seconds: 3,
            heartbeat_interval_seconds: 1,
            lease_expires_at: Utc::now() + chrono::Duration::seconds(3),
        },
        credential: format!("session-secret-{}-{generation}-0", input.node_id),
        expires_at: Utc::now().timestamp() + 300,
    };
    control
        .sessions
        .lock()
        .unwrap()
        .insert(input.node_id, session.clone());
    Json(session).into_response()
}

fn check_session(control: &Control, headers: &HeaderMap, body: &Bytes) -> Option<ConsumerSession> {
    assert!(body.is_empty(), "session operations must not send a body");
    assert!(!headers.contains_key("x-kish-app-id"));
    let auth = headers
        .get(header::AUTHORIZATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(!auth.contains(ROOT_KEY));
    let token = auth.strip_prefix("Bearer ").unwrap();
    control
        .sessions
        .lock()
        .unwrap()
        .values()
        // A cancelled renewal may leave an older, still valid same-generation proof.
        .find(|s| {
            token.starts_with(&format!(
                "session-secret-{}-{}-",
                s.lease.node_id, s.lease.membership_generation
            ))
        })
        .cloned()
}

async fn heartbeat(
    State(control): State<Arc<Control>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(mut session) = check_session(&control, &headers, &body) else {
        return StatusCode::CONFLICT.into_response();
    };
    assert_eq!(
        headers.get(header::AUTHORIZATION).unwrap(),
        &format!("Bearer {}", session.credential)
    );
    let count = control.heartbeats.fetch_add(1, Ordering::SeqCst) + 1;
    match control.mode.load(Ordering::SeqCst) {
        1 => return (StatusCode::UNAUTHORIZED, ROOT_KEY).into_response(),
        2 => return (StatusCode::CONFLICT, session.credential).into_response(),
        3 => session.group_id += 1,
        4 => session.group_key = "foreign".into(),
        5 => session.lease.node_id = "other-node".into(),
        6 => session.lease.member_id += 1,
        7 => session.lease.membership_generation += 1,
        8 => {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        9 if count == 1 => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        10 => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        _ => {}
    }
    session.credential = format!(
        "session-secret-{}-{}-{count}",
        session.lease.node_id, session.lease.membership_generation
    );
    session.lease.lease_expires_at = Utc::now() + chrono::Duration::seconds(3);
    session.expires_at = Utc::now().timestamp() + 300;
    control
        .sessions
        .lock()
        .unwrap()
        .insert(session.lease.node_id.clone(), session.clone());
    Json(session).into_response()
}

async fn deregister(
    State(control): State<Arc<Control>>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    if check_session(&control, &headers, &body).is_none() {
        return StatusCode::CONFLICT;
    }
    control.deregistrations.fetch_add(1, Ordering::SeqCst);
    StatusCode::NO_CONTENT
}

struct Server {
    base: String,
    control: Arc<Control>,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn start(mode: u8) -> Self {
        let control = Arc::new(Control::default());
        control.mode.store(mode, Ordering::SeqCst);
        let app = Router::new()
            .route("/api/user/event-dispatch/v1/service-auth/trust", get(trust))
            .route(
                "/api/user/event-dispatch/v1/consumer-enrollments",
                post(enroll),
            )
            .route(
                "/api/user/event-dispatch/v1/consumer-sessions/heartbeat",
                post(heartbeat),
            )
            .route(
                "/api/user/event-dispatch/v1/consumer-sessions/deregister",
                post(deregister),
            )
            .route(
                "/redirect-secret-trap",
                get(|| async {
                    panic!("redirect followed");
                    #[allow(unreachable_code)]
                    StatusCode::OK
                }),
            )
            .with_state(control.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            base,
            control,
            task,
        }
    }

    async fn connect(&self) -> Result<ServiceConnection, ServiceAuthError> {
        ServiceConnection::connect(&self.base, ServiceCredential::new(APP, ROOT_KEY).unwrap()).await
    }
}

fn config(node: &str) -> EnrolledConsumerNodeConfig {
    EnrolledConsumerNodeConfig {
        group_key: "workers".into(),
        node_id: node.into(),
        invocation_url: TARGET.into(),
        maximum_in_flight: 4,
    }
}

fn echo_router() -> Router {
    Router::new().route("/callback", post(|body: Bytes| async move { body }))
}

fn callback(proof: Option<&str>, path: &str, body: &[u8]) -> Request<Body> {
    let mut request = Request::builder().method("POST").uri(path);
    if let Some(proof) = proof {
        request = request.header(SIGNATURE_HEADER, proof);
    }
    request.body(Body::from(body.to_vec())).unwrap()
}

async fn terminal(
    mut status: tokio::sync::watch::Receiver<EnrolledConsumerNodeStatus>,
) -> EnrolledConsumerNodeStatus {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let value = status.borrow_and_update().clone();
            if value.is_terminal() {
                return value;
            }
            status.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

async fn wait_count(counter: &AtomicUsize, minimum: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while counter.load(Ordering::SeqCst) < minimum {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn bootstrap_rejects_insecure_transport_redirect_foreign_app_and_oversized_response() {
    for url in [
        "http://remote.example/",
        "https://user:secret@example.test/",
        "https://example.test/?secret=x",
        "https://example.test/#fragment",
    ] {
        let error = ServiceConnection::connect(url, ServiceCredential::new(APP, ROOT_KEY).unwrap())
            .await
            .unwrap_err();
        assert_eq!(error, ServiceAuthError::InvalidUrl);
        assert!(!format!("{error:?} {error}").contains("secret"));
    }
    for (mode, expected) in [
        (20, ServiceAuthError::InvalidTrust),
        (21, ServiceAuthError::Http(302)),
        (22, ServiceAuthError::InvalidResponse),
        (26, ServiceAuthError::InvalidResponse),
        (24, ServiceAuthError::Http(503)),
    ] {
        let server = Server::start(mode).await;
        assert_eq!(server.connect().await.unwrap_err(), expected);
    }
}

#[tokio::test]
async fn middleware_preserves_exact_bytes_and_original_nested_uri_and_rejects_replay() {
    let server = Server::start(0).await;
    let connection = server.connect().await.unwrap();
    let router = Router::new().nest(
        "/prefix",
        connection.protect(echo_router(), TARGET).unwrap(),
    );
    let bytes = b"{ \"payload\": [1, 2] }\n";
    let proof = server
        .control
        .signer()
        .sign_callback(APP, "POST", TARGET, bytes, Utc::now().timestamp())
        .unwrap();
    let response = router
        .clone()
        .oneshot(callback(
            Some(&proof),
            "/prefix/callback?tenant=orders&value=%2F",
            bytes,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .as_ref(),
        bytes
    );
    let response = router
        .oneshot(callback(
            Some(&proof),
            "/prefix/callback?tenant=orders&value=%2F",
            bytes,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn rejects_missing_tampered_expired_future_foreign_and_wrong_destination_proofs() {
    let server = Server::start(0).await;
    let connection = server.connect().await.unwrap();
    let router = Router::new().nest(
        "/prefix",
        connection.protect(echo_router(), TARGET).unwrap(),
    );
    let signer = server.control.signer();
    let now = Utc::now().timestamp();
    for (app, method, target, signed_body, issued_at) in [
        (APP, "POST", TARGET, "original", now),
        ("foreign", "POST", TARGET, "changed", now),
        (APP, "GET", TARGET, "changed", now),
        (
            APP,
            "POST",
            "https://other.example/prefix/callback?tenant=orders&value=%2F",
            "changed",
            now,
        ),
        (APP, "POST", TARGET, "changed", now - 61),
        (APP, "POST", TARGET, "changed", now + 120),
    ] {
        let proof = signer
            .sign_callback(app, method, target, signed_body.as_bytes(), issued_at)
            .unwrap();
        let response = router
            .clone()
            .oneshot(callback(
                Some(&proof),
                "/prefix/callback?tenant=orders&value=%2F",
                b"changed",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    for path in [
        "/prefix/callback?tenant=orders&value=/",
        "/prefix/callback?value=%2F&tenant=orders",
        "/prefix/callback",
    ] {
        let proof = signer
            .sign_callback(APP, "POST", TARGET, b"changed", now)
            .unwrap();
        assert_eq!(
            router
                .clone()
                .oneshot(callback(Some(&proof), path, b"changed"))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
    }
    assert_eq!(
        router
            .clone()
            .oneshot(callback(
                None,
                "/prefix/callback?tenant=orders&value=%2F",
                b"changed"
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    let proof = signer
        .sign_callback(APP, "POST", TARGET, b"changed", now)
        .unwrap();
    let large = vec![0; 1024 * 1024 + 1];
    assert_eq!(
        router
            .oneshot(callback(
                Some(&proof),
                "/prefix/callback?tenant=orders&value=%2F",
                &large
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn cloned_connections_share_atomic_replay_protection() {
    let server = Server::start(0).await;
    let connection = server.connect().await.unwrap();
    let target = "https://callback.example/callback";
    let proof = server
        .control
        .signer()
        .sign_callback(APP, "POST", target, b"", Utc::now().timestamp())
        .unwrap();
    let first = connection.protect(echo_router(), target).unwrap();
    let second = connection.clone().protect(echo_router(), target).unwrap();
    let (first, second) = tokio::join!(
        first.oneshot(callback(Some(&proof), "/callback", b"")),
        second.oneshot(callback(Some(&proof), "/callback", b""))
    );
    let mut statuses = [
        first.unwrap().status().as_u16(),
        second.unwrap().status().as_u16(),
    ];
    statuses.sort();
    assert_eq!(statuses, [200, 401]);
}

#[tokio::test]
async fn refresh_rotates_trust_and_last_clone_drop_cancels_refresh() {
    let server = Server::start(23).await;
    let connection = server.connect().await.unwrap();
    let clone = connection.clone();
    drop(connection);
    server.control.signing_root.store(7, Ordering::SeqCst);
    wait_count(&server.control.trust_calls, 2).await;
    let target = "https://callback.example/callback";
    let router = clone.protect(echo_router(), target).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let proof = server
                .control
                .signer()
                .sign_callback(APP, "POST", target, b"", Utc::now().timestamp())
                .unwrap();
            if router
                .clone()
                .oneshot(callback(Some(&proof), "/callback", b""))
                .await
                .unwrap()
                .status()
                == StatusCode::OK
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    drop(router);
    drop(clone);
    let calls = server.control.trust_calls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(server.control.trust_calls.load(Ordering::SeqCst), calls);
}

#[tokio::test]
async fn expired_trust_and_explicit_shutdown_fail_closed() {
    let server = Server::start(23).await;
    let connection = server.connect().await.unwrap();
    let target = "https://callback.example/callback";
    let router = connection.protect(echo_router(), target).unwrap();
    server.control.mode.store(24, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let proof = server
        .control
        .signer()
        .sign_callback(APP, "POST", target, b"", Utc::now().timestamp())
        .unwrap();
    assert_eq!(
        router
            .clone()
            .oneshot(callback(Some(&proof), "/callback", b""))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    connection.shutdown().await;
    assert_eq!(
        connection.protect(echo_router(), target).unwrap_err(),
        ServiceAuthError::Closed
    );
    assert_eq!(
        router
            .oneshot(callback(Some(&proof), "/callback", b""))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn replicas_renew_independently_rotate_credentials_and_deregister_latest_session() {
    let server = Server::start(0).await;
    let connection = server.connect().await.unwrap();
    let first = connection.enroll_consumer(config("pod-a")).await.unwrap();
    let second = connection.enroll_consumer(config("pod-b")).await.unwrap();
    assert_ne!(
        first.status().lease().member_id,
        second.status().lease().member_id
    );
    wait_count(&server.control.heartbeats, 4).await;
    first.shutdown().await;
    second.shutdown().await;
    assert_eq!(server.control.deregistrations.load(Ordering::SeqCst), 2);
    assert_eq!(server.control.enrollments.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn heartbeat_rejects_every_identity_mismatch_and_never_reenrolls() {
    let mut tests = tokio::task::JoinSet::new();
    for mode in 1..=7 {
        tests.spawn(async move {
            let server = Server::start(mode).await;
            let connection = server.connect().await.unwrap();
            let node = connection.enroll_consumer(config("pod-a")).await.unwrap();
            let status = terminal(node.subscribe()).await;
            match mode {
                1 => assert!(matches!(
                    status,
                    EnrolledConsumerNodeStatus::CredentialRejected { .. }
                )),
                2 | 3 | 6 | 7 => {
                    assert!(matches!(status, EnrolledConsumerNodeStatus::Fenced { .. }))
                }
                4 | 5 => assert!(matches!(
                    status,
                    EnrolledConsumerNodeStatus::Failed {
                        failure: ServiceAuthError::InvalidResponse,
                        ..
                    }
                )),
                _ => unreachable!(),
            }
            assert_eq!(server.control.enrollments.load(Ordering::SeqCst), 1);
            assert!(!format!("{status:?} {node:?}").contains(ROOT_KEY));
            assert!(!format!("{status:?} {node:?}").contains("session-secret"));
            node.shutdown().await;
            assert_eq!(server.control.heartbeats.load(Ordering::SeqCst), 1);
            assert_eq!(server.control.deregistrations.load(Ordering::SeqCst), 0);
        });
    }
    while let Some(result) = tests.join_next().await {
        result.unwrap();
    }
}

#[tokio::test]
async fn restart_fences_previous_generation_without_affecting_replacement() {
    let server = Server::start(0).await;
    let connection = server.connect().await.unwrap();
    let old = connection.enroll_consumer(config("pod-a")).await.unwrap();
    let replacement = connection.enroll_consumer(config("pod-a")).await.unwrap();
    assert!(matches!(
        terminal(old.subscribe()).await,
        EnrolledConsumerNodeStatus::Fenced { .. }
    ));
    old.shutdown().await;
    assert!(!replacement.status().is_terminal());
    replacement.shutdown().await;
    assert_eq!(server.control.deregistrations.load(Ordering::SeqCst), 1);
    assert_eq!(server.control.enrollments.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn lease_expiry_interrupts_hung_heartbeat_and_transient_retries() {
    for mode in [8, 10] {
        let server = Server::start(mode).await;
        let connection = server.connect().await.unwrap();
        let node = connection.enroll_consumer(config("pod-a")).await.unwrap();
        assert!(matches!(
            terminal(node.subscribe()).await,
            EnrolledConsumerNodeStatus::LeaseExpired { .. }
        ));
        node.shutdown().await;
        assert_eq!(server.control.enrollments.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn cancellation_interrupts_inflight_renewal_and_drop_deregisters() {
    let server = Server::start(8).await;
    let connection = server.connect().await.unwrap();
    let node = connection.enroll_consumer(config("pod-a")).await.unwrap();
    wait_count(&server.control.heartbeats, 1).await;
    tokio::time::timeout(Duration::from_secs(1), node.shutdown())
        .await
        .unwrap();
    assert_eq!(server.control.deregistrations.load(Ordering::SeqCst), 1);
    let node = connection.enroll_consumer(config("pod-b")).await.unwrap();
    drop(node);
    wait_count(&server.control.deregistrations, 2).await;
}

#[tokio::test]
async fn transient_failure_recovers_without_reenrollment() {
    let server = Server::start(9).await;
    let connection = server.connect().await.unwrap();
    let node = connection.enroll_consumer(config("pod-a")).await.unwrap();
    let mut status = node.subscribe();
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            status.changed().await.unwrap();
            if matches!(
                *status.borrow_and_update(),
                EnrolledConsumerNodeStatus::HeartbeatRetrying { .. }
            ) {
                break;
            }
        }
        loop {
            status.changed().await.unwrap();
            if matches!(
                *status.borrow_and_update(),
                EnrolledConsumerNodeStatus::Registered { .. }
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    node.shutdown().await;
    assert_eq!(server.control.enrollments.load(Ordering::SeqCst), 1);
    assert_eq!(server.control.deregistrations.load(Ordering::SeqCst), 1);
}

#[cfg(all(feature = "event-consumer-http", feature = "user-task-completion-http"))]
#[tokio::test]
async fn protects_existing_event_and_task_adapters_before_json_handling() {
    use kish_lingshu_sdk::{
        event_dispatch::{ConsumerHttpAdapter, ConsumerRegistry},
        user_task::completion::{CompletionHttpAdapter, CompletionRegistry},
    };
    let server = Server::start(0).await;
    let connection = server.connect().await.unwrap();
    let event = ConsumerHttpAdapter::new(Arc::new(
        ConsumerRegistry::builder(APP).unwrap().build().unwrap(),
    ))
    .router();
    let task = CompletionHttpAdapter::new(Arc::new(
        CompletionRegistry::builder(APP).unwrap().build().unwrap(),
    ))
    .router();
    for (router, path) in [
        (
            Router::new().nest("/internal/events", event),
            "/internal/events",
        ),
        (
            task,
            kish_lingshu_runtime_contract::USER_TASK_COMPLETION_PATH,
        ),
    ] {
        let target = format!("https://callback.example{path}");
        let router = connection.protect(router, &target).unwrap();
        assert_eq!(
            router
                .clone()
                .oneshot(callback(None, path, b"not-json"))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let proof = server
            .control
            .signer()
            .sign_callback(APP, "POST", &target, b"not-json", Utc::now().timestamp())
            .unwrap();
        let response = router
            .oneshot(callback(Some(&proof), path, b"not-json"))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "the authenticated request reaches the existing adapter's JSON validation"
        );
    }
}

#[tokio::test]
async fn duplicate_signature_headers_fail_closed_without_consuming_valid_proof() {
    let server = Server::start(0).await;
    let connection = server.connect().await.unwrap();
    let target = "https://callback.example/callback";
    let router = connection.protect(echo_router(), target).unwrap();
    let proof = server
        .control
        .signer()
        .sign_callback(APP, "POST", target, b"", Utc::now().timestamp())
        .unwrap();
    let mut request = callback(Some(&proof), "/callback", b"");
    request
        .headers_mut()
        .append(SIGNATURE_HEADER, proof.parse().unwrap());
    assert_eq!(
        router.clone().oneshot(request).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        router
            .oneshot(callback(Some(&proof), "/callback", b""))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn normalizes_callback_urls_like_reqwest_and_rejects_remote_plaintext() {
    let server = Server::start(0).await;
    let connection = server.connect().await.unwrap();
    let configured = "HTTPS://CALLBACK.EXAMPLE:443/prefix/../callback?value=hello world";
    let canonical = reqwest::Url::parse(configured).unwrap().to_string();
    let router = connection.protect(echo_router(), configured).unwrap();
    let proof = server
        .control
        .signer()
        .sign_callback(APP, "POST", &canonical, b"", Utc::now().timestamp())
        .unwrap();
    assert_eq!(
        router
            .oneshot(callback(Some(&proof), "/callback?value=hello%20world", b""))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        connection
            .protect(echo_router(), "http://remote.example/callback")
            .unwrap_err(),
        ServiceAuthError::InvalidUrl
    );
    let mut insecure = config("pod-a");
    insecure.invocation_url = "http://remote.example/callback".into();
    assert_eq!(
        connection.enroll_consumer(insecure).await.unwrap_err(),
        ServiceAuthError::InvalidNodeConfig
    );
    assert_eq!(server.control.enrollments.load(Ordering::SeqCst), 0);
    for local in [
        "http://127.0.0.1/callback",
        "http://[::1]/callback",
        "http://localhost/callback",
    ] {
        let _ = connection.protect(echo_router(), local).unwrap();
    }
}

#[tokio::test]
async fn application_header_uses_existing_percent_encoded_protocol() {
    let application = "订单/app%one";
    let router = Router::new().route(
        "/api/user/event-dispatch/v1/service-auth/trust",
        get(move |headers: HeaderMap| async move {
            assert_eq!(
                headers.get("x-kish-app-id").unwrap(),
                "%E8%AE%A2%E5%8D%95%2Fapp%25one"
            );
            Json(
                ServiceSigner::new(&[0; 32])
                    .unwrap()
                    .trust(application, Utc::now().timestamp())
                    .unwrap(),
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let connection = ServiceConnection::connect(
        &base,
        ServiceCredential::new(application, ROOT_KEY).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(connection.application_id(), application);
    connection.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn revoked_root_closes_clones_and_notifies_nodes_without_reenrollment() {
    let server = Server::start(23).await;
    let connection = server.connect().await.unwrap();
    let clone = connection.clone();
    let node = connection.enroll_consumer(config("pod-a")).await.unwrap();
    let target = "https://callback.example/callback";
    let router = clone.protect(echo_router(), target).unwrap();
    server.control.mode.store(25, Ordering::SeqCst);
    assert!(matches!(
        terminal(node.subscribe()).await,
        EnrolledConsumerNodeStatus::CredentialRejected { .. }
    ));
    assert_eq!(
        clone.enroll_consumer(config("pod-b")).await.unwrap_err(),
        ServiceAuthError::Http(401)
    );
    let proof = server
        .control
        .signer()
        .sign_callback(APP, "POST", target, b"", Utc::now().timestamp())
        .unwrap();
    assert_eq!(
        router
            .oneshot(callback(Some(&proof), "/callback", b""))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(server.control.enrollments.load(Ordering::SeqCst), 1);
    node.shutdown().await;
}

#[tokio::test]
async fn enrollment_root_rejection_closes_connection_without_repeated_root_requests() {
    let server = Server::start(27).await;
    let connection = server.connect().await.unwrap();
    let clone = connection.clone();
    assert_eq!(
        connection
            .enroll_consumer(config("pod-a"))
            .await
            .unwrap_err(),
        ServiceAuthError::Http(401)
    );
    assert_eq!(
        clone.enroll_consumer(config("pod-b")).await.unwrap_err(),
        ServiceAuthError::Http(401)
    );
    assert_eq!(
        clone.protect(echo_router(), TARGET).unwrap_err(),
        ServiceAuthError::Http(401)
    );
    assert_eq!(server.control.enrollments.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn session_auth_rejection_closes_shared_callback_connection() {
    let server = Server::start(1).await;
    let connection = server.connect().await.unwrap();
    let node = connection.enroll_consumer(config("pod-a")).await.unwrap();
    let peer = connection.enroll_consumer(config("pod-b")).await.unwrap();
    let target = "https://callback.example/callback";
    let router = connection.protect(echo_router(), target).unwrap();
    assert!(matches!(
        terminal(node.subscribe()).await,
        EnrolledConsumerNodeStatus::CredentialRejected { .. }
    ));
    assert!(matches!(
        terminal(peer.subscribe()).await,
        EnrolledConsumerNodeStatus::CredentialRejected { .. }
    ));
    let proof = server
        .control
        .signer()
        .sign_callback(APP, "POST", target, b"", Utc::now().timestamp())
        .unwrap();
    assert_eq!(
        router
            .oneshot(callback(Some(&proof), "/callback", b""))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        connection
            .enroll_consumer(config("pod-c"))
            .await
            .unwrap_err(),
        ServiceAuthError::Http(401)
    );
    assert_eq!(server.control.enrollments.load(Ordering::SeqCst), 2);
    node.shutdown().await;
    peer.shutdown().await;
}
