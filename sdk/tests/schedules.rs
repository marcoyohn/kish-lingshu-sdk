#![cfg(all(feature = "http-client", feature = "event-consumer-http"))]
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use kish_lingshu_sdk::{
    event_dispatch::{
        IntervalBasis, MisfirePolicy, OverlapPolicy, ScheduleDefinition, ScheduleTrigger,
    },
    ClientBuilder, ClientConfig, Error, MutationOptions, ServiceCredential,
};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

fn definition() -> ScheduleDefinition {
    ScheduleDefinition {
        name: "recovery.publisher".into(),
        trigger: ScheduleTrigger::Interval {
            every_seconds: 30,
            anchor_at: "2026-01-01T00:00:00Z".parse().unwrap(),
            basis: IntervalBasis::TriggeredAt,
        },
        event: json!({"topic":"recovery","event_type":"recover","payload":{"publisher_id":"publisher"}}),
        misfire_policy: MisfirePolicy::FireOnce,
        misfire_batch_cap: 1,
        overlap_policy: OverlapPolicy::Serialize,
        enabled: true,
    }
}
struct Capture {
    mode: u16,
    requests: Mutex<Vec<(HeaderMap, Value)>>,
}
async fn endpoint(
    State(state): State<Arc<Capture>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let count = {
        let mut r = state.requests.lock().unwrap();
        r.push((headers, body.clone()));
        r.len()
    };
    if state.mode == 307 {
        return (StatusCode::TEMPORARY_REDIRECT, [("location", "/trap")]).into_response();
    }
    if state.mode == 600 {
        return (StatusCode::OK, "x".repeat(65537)).into_response();
    }
    if state.mode == 601 {
        return (StatusCode::OK, "invalid JSON").into_response();
    }
    if state.mode == 401 || state.mode == 409 || (state.mode == 503 && count == 1) {
        return (StatusCode::from_u16(state.mode).unwrap(),Json(json!({"code":if state.mode==409 {"schedule_conflict"} else {"temporarily_unavailable"},"message":"fixture","retryable":state.mode==503,"request_id":"schedule-test"}))).into_response();
    }
    Json(json!({"schedule_id":42,"app_id":if state.mode==602 {"foreign-app"} else {"app"},
        "name":body["name"],"trigger":body["trigger"],"event_template":body["event"],
        "misfire_policy":body["misfire_policy"],"misfire_batch_cap":body["misfire_batch_cap"],
        "overlap_policy":body["overlap_policy"],"state":if state.mode==603 {"paused"} else {"active"},"next_fire_at":null,"revision":1})).into_response()
}
struct Fixture {
    url: String,
    state: Arc<Capture>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Fixture {
    async fn start(mode: u16) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Capture {
            mode,
            requests: Mutex::new(Vec::new()),
        });
        let router = Router::new()
            .route("/api/user/event-dispatch/v1/schedules", post(endpoint))
            .route(
                "/trap",
                post(|| async {
                    panic!("must not follow credential-bearing redirect");
                    #[allow(unreachable_code)]
                    StatusCode::OK
                }),
            )
            .with_state(state.clone());
        Self {
            url,
            state,
            task: tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
        }
    }
    fn client(&self) -> kish_lingshu_sdk::Client<kish_lingshu_sdk::ServicePrincipal> {
        ClientBuilder::new(ClientConfig::new(&self.url))
            .service_credential(ServiceCredential::new("app", "root-secret").unwrap())
            .connect()
            .unwrap()
    }
}
#[tokio::test]
async fn schedule_retries_preserve_scope_key_and_body() {
    let fixture = Fixture::start(503).await;
    let receipt = fixture
        .client()
        .event_dispatch()
        .ensure_schedule(definition(), MutationOptions::new("stable-key").unwrap())
        .await
        .unwrap();
    assert_eq!(receipt.schedule_id, 42);
    let requests = fixture.state.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].1, requests[1].1);
    for (headers, _) in requests.iter() {
        assert_eq!(headers["authorization"], "Bearer root-secret");
        assert_eq!(headers["x-kish-app-id"], "app");
        assert_eq!(headers["idempotency-key"], "stable-key");
    }
}
#[tokio::test]
async fn schedule_rejects_permanent_errors_invalid_receipts_and_redirects_without_retry() {
    for mode in [401, 409, 307, 600, 601, 602, 603] {
        let fixture = Fixture::start(mode).await;
        let error = fixture
            .client()
            .event_dispatch()
            .ensure_schedule(definition(), MutationOptions::new("stable-key").unwrap())
            .await
            .unwrap_err();
        if mode == 409 {
            assert!(matches!(error,Error::Application(ref e) if e.http_status==Some(409)))
        }
        assert_eq!(
            fixture.state.requests.lock().unwrap().len(),
            1,
            "mode {mode}: {error}"
        );
    }
}
