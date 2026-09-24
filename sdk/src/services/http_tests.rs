use super::*;
use axum::{
    body::{to_bytes, Body},
    extract::State,
    http::{HeaderMap, Request},
    routing::{get, post},
};
use kish_lingshu_foundation_contract::service_auth::{ServiceSigner, SIGNATURE_HEADER};
use serde_json::{json, Value};
use tower::ServiceExt;

#[derive(Clone)]
struct Platform {
    signer: Arc<ServiceSigner>,
    results: Arc<tokio::sync::Mutex<Vec<ServiceCompletion>>>,
    reports: Arc<AtomicUsize>,
    enrollments: Arc<AtomicUsize>,
}
async fn trust(
    State(p): State<Platform>,
    headers: HeaderMap,
) -> Json<kish_lingshu_foundation_contract::service_auth::ServiceTrust> {
    assert_eq!(headers["authorization"], "Bearer app-key");
    assert_eq!(headers["x-kish-app-id"], "app");
    Json(
        p.signer
            .trust("app", chrono::Utc::now().timestamp())
            .unwrap(),
    )
}
async fn enroll(
    State(p): State<Platform>,
    Json(request): Json<ServiceEnrollment>,
) -> Json<ServiceSession> {
    assert_eq!(request.node_id, "node");
    assert_eq!(request.maximum_in_flight, 1);
    p.enrollments.fetch_add(1, Ordering::SeqCst);
    Json(ServiceSession {
        instance: request.instance.map(|r| ServiceInstanceIdentity {
            instance_id: r.instance_id,
            generation: r.generation.unwrap_or_else(|| "shared-generation".into()),
        }),
        node_id: request.node_id,
        generation: "g1".into(),
        credential: "session".into(),
        lease_expires_at_ms: chrono::Utc::now().timestamp_millis() + 30_000,
        heartbeat_interval_ms: 10_000,
    })
}
async fn complete(
    State(p): State<Platform>,
    headers: HeaderMap,
    Json(result): Json<ServiceCompletion>,
) -> Response {
    assert_eq!(headers["authorization"], "Bearer completion-secret");
    if p.reports.fetch_add(1, Ordering::SeqCst) == 0 {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    p.results.lock().await.push(result);
    Json(CompletionDisposition::Recorded).into_response()
}
fn registry(calls: Arc<AtomicUsize>, hold: Arc<tokio::sync::Semaphore>) -> Arc<ServiceRegistry> {
    role_registry(calls, hold, true, false)
}
fn role_registry(
    calls: Arc<AtomicUsize>,
    hold: Arc<Semaphore>,
    call: bool,
    event: bool,
) -> Arc<ServiceRegistry> {
    let manifest = ServiceManifest {
        contract_version: 1,
        application_id: "app".into(),
        services: vec![ServiceDefinition {
            service_key: "refunds".into(),
            description: String::new(),
            operations: vec![OperationDefinition {
                user_task_completion: None,
                operation_key: "execute".into(),
                version: "v1".into(),
                description: String::new(),
                action: "refund.execute".into(),
                idempotent: true,
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
                error_schema: service_error_schema(),
                call: call.then(|| CallBinding {
                    modes: [CallMode::Sync, CallMode::Async].into_iter().collect(),
                    maximum_concurrency: 1,
                    timeout_ms: 5000,
                }),
                events: if event {
                    [EventBinding {
                        topic: "refunds".into(),
                        event_type: "created".into(),
                        consumer_group: "projection".into(),
                    }]
                    .into_iter()
                    .collect()
                } else {
                    Default::default()
                },
            }],
        }],
    };
    let mut builder = ServiceRegistryBuilder::new(manifest).unwrap();
    builder
        .bind(
            "refunds",
            "execute",
            "v1",
            move |context: ServiceContext, input: Value| {
                let calls = calls.clone();
                let hold = hold.clone();
                async move {
                    assert_eq!(context.idempotency_key(), "business-refund");
                    assert!(!serde_json::to_string(&context)
                        .unwrap()
                        .contains("completion-secret"));
                    calls.fetch_add(1, Ordering::SeqCst);
                    if input.get("panic") == Some(&json!(true)) {
                        panic!("provider failure");
                    }
                    if input.get("hold") == Some(&json!(true)) {
                        hold.acquire().await.unwrap().forget();
                    }
                    Ok(json!({"refund_id":"refund-42"}))
                }
            },
        )
        .unwrap();
    Arc::new(builder.build().unwrap())
}
fn invocation(registry: &ServiceRegistry, mode: CallMode, input: Value) -> ServiceInvocation {
    ServiceInvocation {
        admission: Some(ServiceCallAdmission {
            policy_revision: 1,
            maximum_concurrency: 1,
        }),
        target_instance: Some(ServiceInstanceTarget {
            node_id: "node".into(),
            generation: "g1".into(),
        }),
        contract_version: 1,
        operation: registry.capabilities()[0].operation.clone(),
        context: ServiceContext {
            application_id: "app".into(),
            idempotency_key: "business-refund".into(),
            deadline_ms: chrono::Utc::now().timestamp_millis() + 5000,
            trace_id: None,
            invocation: InvocationRole::Call(CallContext {
                user_task_completion: None,
                call_id: "call-1".into(),
                attempt: 1,
                caller: "workflow".into(),
                mode,
                workflow: None,
            }),
        },
        input,
        completion: (mode == CallMode::Async).then(|| CompletionTarget {
            heartbeat: None,
            token: "completion-secret".into(),
        }),
    }
}
async fn send(
    router: &Router,
    p: &Platform,
    target: &str,
    invocation: &ServiceInvocation,
) -> Response {
    let body = serde_json::to_vec(invocation).unwrap();
    let proof = p
        .signer
        .sign_callback("app", "POST", target, &body, chrono::Utc::now().timestamp())
        .unwrap();
    router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/invoke")
                .method("POST")
                .header("content-type", "application/json")
                .header(SIGNATURE_HEADER, proof)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}
#[tokio::test]
async fn service_http_sync_async_capacity_fencing_and_automatic_reporting_without_dispatch() {
    let p = Platform {
        signer: Arc::new(ServiceSigner::new(&[67; 32]).unwrap()),
        results: Default::default(),
        reports: Default::default(),
        enrollments: Default::default(),
    };
    // No Event Dispatch routes exist in this fixture.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let platform = Router::new()
        .route("/api/user/services/v1/service-auth/trust", get(trust))
        .route("/api/user/services/v1/enrollments", post(enroll))
        .route("/api/user/services/v1/completions", post(complete))
        .route(
            "/api/user/services/v1/sessions/deregister",
            post(|| async { StatusCode::NO_CONTENT }),
        )
        .with_state(p.clone());
    let server = tokio::spawn(async move { axum::serve(listener, platform).await.unwrap() });
    let connection = ServiceConnection::connect(
        &base,
        crate::ServiceCredential::new("app", "app-key").unwrap(),
    )
    .await
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let hold = Arc::new(Semaphore::new(0));
    let registry = registry(calls.clone(), hold.clone());
    let adapter = ServiceHttpAdapter::new(registry.clone(), connection, 1).unwrap();
    let target = format!("{base}invoke");
    let router = Router::new().nest("/invoke", adapter.router(&target).unwrap());
    let sync = invocation(&registry, CallMode::Sync, json!({}));
    assert!(!adapter.status().accepting);
    assert_eq!(
        send(&router, &p, &target, &sync).await.status(),
        StatusCode::CONFLICT
    );
    let registration = adapter.enroll("node", &target).await.unwrap();
    assert!(adapter.status().accepting);
    let response = send(&router, &p, &target, &sync).await;
    assert_eq!(response.status(), StatusCode::OK);
    let result: InvocationResponse =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
    assert_eq!(
        result,
        InvocationResponse::Completed {
            outcome: ServiceOutcome::Succeeded {
                result: json!({"refund_id":"refund-42"})
            }
        }
    );
    let mut missing_governance = sync.clone();
    missing_governance.admission = None;
    assert_eq!(
        send(&router, &p, &target, &missing_governance)
            .await
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    missing_governance.admission = Some(ServiceCallAdmission {
        policy_revision: 1,
        maximum_concurrency: 2,
    });
    assert_eq!(
        send(&router, &p, &target, &missing_governance)
            .await
            .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let asynchronous = invocation(&registry, CallMode::Async, json!({"hold":true}));
    assert_eq!(
        send(&router, &p, &target, &asynchronous).await.status(),
        StatusCode::ACCEPTED
    );
    assert_eq!(
        send(&router, &p, &target, &sync).await.status(),
        StatusCode::TOO_MANY_REQUESTS
    );
    let mut stale = sync.clone();
    stale.target_instance.as_mut().unwrap().generation = "old".into();
    assert_eq!(
        send(&router, &p, &target, &stale).await.status(),
        StatusCode::CONFLICT
    );
    hold.add_permits(1);
    tokio::time::timeout(Duration::from_secs(3), async {
        while p.results.lock().await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(p.reports.load(Ordering::SeqCst), 2);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(matches!(
        p.results.lock().await[0].outcome,
        ServiceOutcome::Succeeded { .. }
    ));
    // The platform has recorded the result, but its HTTP response must reach
    // the SDK before that task releases the instance's execution permit.
    tokio::time::timeout(Duration::from_secs(3), async {
        while adapter.status().active != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let failed = send(
        &router,
        &p,
        &target,
        &invocation(&registry, CallMode::Sync, json!({"panic":true})),
    )
    .await;
    assert_eq!(failed.status(), StatusCode::OK);
    let result: InvocationResponse =
        serde_json::from_slice(&to_bytes(failed.into_body(), 4096).await.unwrap()).unwrap();
    assert!(
        matches!(result,InvocationResponse::Completed{outcome:ServiceOutcome::Failed{error}} if error.code=="handler_panicked")
    );
    registration.shutdown().await;
    assert!(!adapter.status().accepting);
    assert_eq!(
        send(&router, &p, &target, &sync).await.status(),
        StatusCode::CONFLICT
    );
    adapter.shutdown().await;
    assert_eq!(adapter.status().active, 0);
    server.abort();
}

#[cfg(feature = "service-event-http")]
#[tokio::test]
async fn event_only_and_both_roles_keep_independent_admission_and_shared_shutdown() {
    use crate::event_dispatch::EnrolledConsumerNodeStatus;
    use kish_lingshu_event_dispatch_contract::ConsumerInstanceLeaseV1;
    let p = Platform {
        signer: Arc::new(ServiceSigner::new(&[71; 32]).unwrap()),
        results: Default::default(),
        reports: Default::default(),
        enrollments: Default::default(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/api/user/services/v1/service-auth/trust", get(trust))
        .with_state(p.clone());
    let platform = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    for both in [false, true] {
        let connection = ServiceConnection::connect(
            &base,
            crate::ServiceCredential::new("app", "app-key").unwrap(),
        )
        .await
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let registry = role_registry(calls.clone(), Arc::new(Semaphore::new(0)), both, true);
        let adapter = ServiceHttpAdapter::new(registry.clone(), connection.clone(), 2).unwrap();
        let target = format!("{base}invoke");
        let event_url = format!("{base}events");
        let router = Router::new()
            .nest("/events", adapter.event_router(&event_url).unwrap())
            .nest("/invoke", adapter.router(&target).unwrap());
        // Controlled lease signals exercise the production admission bridge;
        // legacy enrollment networking is covered by its existing adapter tests.
        let ready = ServiceEnrollmentStatus::Ready {
            node_id: "node".into(),
            generation: "g1".into(),
            lease_expires_at_ms: chrono::Utc::now().timestamp_millis() + 30000,
        };
        let (call_status, rx) = watch::channel(ready.clone());
        if both {
            *adapter.state.enrollment.write().unwrap() = Some(rx);
        }
        let lease = ConsumerInstanceLeaseV1 {
            member_id: 1,
            node_id: "node".into(),
            membership_generation: 1,
            lease_seconds: 30,
            heartbeat_interval_seconds: 10,
            lease_expires_at: chrono::Utc::now() + chrono::Duration::seconds(30),
        };
        let (event_status, rx) = watch::channel(EnrolledConsumerNodeStatus::Registered {
            lease: lease.clone(),
        });
        adapter
            .state
            .events
            .memberships
            .write()
            .unwrap()
            .insert("projection".into(), rx);
        assert!(adapter.status().accepting);
        assert!(adapter.status().event_accepting);
        assert_eq!(adapter.status().call_accepting, both);
        assert_eq!(
            send_event(&router, &p, &event_url, false).await.status(),
            StatusCode::OK,
            "Event-only does not need a Call lease"
        );
        let busy_router = router.clone();
        let busy_platform = p.clone();
        let busy_url = event_url.clone();
        let busy =
            tokio::spawn(
                async move { send_event(&busy_router, &busy_platform, &busy_url, true).await },
            );
        tokio::time::timeout(Duration::from_secs(2), async {
            while adapter.status().active != 1 {
                tokio::task::yield_now().await
            }
        })
        .await
        .unwrap();
        if both {
            // The Event occupies one of two instance slots, but cannot consume
            // the single Call role permit for the same logical Operation.
            assert_eq!(
                send(
                    &router,
                    &p,
                    &target,
                    &invocation(&registry, CallMode::Sync, json!({}))
                )
                .await
                .status(),
                StatusCode::OK
            );
            call_status.send_replace(ServiceEnrollmentStatus::Stopped);
            assert_eq!(
                send_event(&router, &p, &event_url, false).await.status(),
                StatusCode::OK
            );
            call_status.send_replace(ready);
        }
        event_status.send_replace(EnrolledConsumerNodeStatus::Stopped { lease });
        assert_eq!(
            busy.await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            send_event(&router, &p, &event_url, false).await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        if both {
            assert_eq!(
                send(
                    &router,
                    &p,
                    &target,
                    &invocation(&registry, CallMode::Sync, json!({}))
                )
                .await
                .status(),
                StatusCode::OK
            );
        }
        connection.shutdown().await;
        assert!(!adapter.status().accepting);
        adapter.shutdown().await;
        assert_eq!(adapter.status().active, 0);
    }
    platform.abort();
}

#[cfg(feature = "service-event-http")]
async fn send_event(router: &Router, p: &Platform, target: &str, hold: bool) -> Response {
    let now = chrono::Utc::now();
    let value = json!({
        "contract_version":"1.0",
        "event":{"event_id":1,"app_id":"app","topic":"refunds","event_type":"created","schema_version":"1","source":"fixture","occurred_at":now,"published_at":now,"not_before":now,"payload":{"hold":hold}},
        "consumption":{"group_key":"projection","consumption_id":1,"subscription_id":1,"group_id":1,"subscription_epoch":1,"queue_epoch":1,"queue_id":0,"queue_offset":0,"invocation_id":1,"attempt_generation":1,"mode":"sync","idempotency_key":"business-refund","invocation_deadline":now+chrono::Duration::seconds(10)},"trace":{}
    });
    let mut value = value;
    value["contract_version"] =
        json!(kish_lingshu_event_dispatch_contract::INVOCATION_CONTRACT_VERSION);
    let body = serde_json::to_vec(&value).unwrap();
    let proof = p
        .signer
        .sign_callback("app", "POST", target, &body, now.timestamp())
        .unwrap();
    router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/events")
                .method("POST")
                .header("content-type", "application/json")
                .header("Idempotency-Key", "business-refund")
                .header("X-Event-Invocation-Id", "1")
                .header(SIGNATURE_HEADER, proof)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn signed_attempt_cancel_and_progress_invalidation_stop_only_matching_work() {
    let p = Platform {
        signer: Arc::new(ServiceSigner::new(&[74; 32]).unwrap()),
        results: Default::default(),
        reports: Default::default(),
        enrollments: Default::default(),
    };
    let progress_calls = Arc::new(AtomicUsize::new(0));
    let invalid = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let observations = progress_calls.clone();
    let invalidate = invalid.clone();
    let platform_router = Router::new()
        .route("/api/user/services/v1/service-auth/trust", get(trust))
        .with_state(p.clone())
        .route(
            "/api/user/services/v1/progress",
            post(
                move |headers: HeaderMap, Json(progress): Json<ServiceProgress>| {
                    let observations = observations.clone();
                    let invalidate = invalidate.clone();
                    async move {
                        assert_eq!(headers["authorization"], "Bearer completion-secret");
                        assert_eq!(progress.call_id, "call-1");
                        observations.fetch_add(1, Ordering::SeqCst);
                        Json(if invalidate.load(Ordering::SeqCst) {
                            ProgressDisposition::Invalidated
                        } else {
                            ProgressDisposition::Running
                        })
                    }
                },
            ),
        );
    let server = tokio::spawn(async move { axum::serve(listener, platform_router).await.unwrap() });
    let connection = ServiceConnection::connect(
        &base,
        crate::ServiceCredential::new("app", "app-key").unwrap(),
    )
    .await
    .unwrap();
    let registry = registry(Arc::new(AtomicUsize::new(0)), Arc::new(Semaphore::new(0)));
    let adapter = ServiceHttpAdapter::new(registry.clone(), connection, 1).unwrap();
    let (lease, rx) = watch::channel(ServiceEnrollmentStatus::Ready {
        node_id: "node".into(),
        generation: "g1".into(),
        lease_expires_at_ms: chrono::Utc::now().timestamp_millis() + 30_000,
    });
    *adapter.state.enrollment.write().unwrap() = Some(rx);
    let target = format!("{base}invoke");
    let router = Router::new().nest("/invoke", adapter.router(&target).unwrap());
    let invocation = invocation(&registry, CallMode::Async, json!({"hold":true}));
    assert_eq!(
        send(&router, &p, &target, &invocation).await.status(),
        StatusCode::ACCEPTED
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while progress_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut control = ServiceCancellation {
        contract_version: 1,
        target_instance: invocation.target_instance.clone().unwrap(),
        operation: invocation.operation.clone(),
        call_id: "call-1".into(),
        attempt: 2,
    };
    assert_eq!(
        send_cancel(&router, &p, &target, &control, "POST")
            .await
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send_cancel(&router, &p, &target, &control, "DELETE")
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        adapter.status().active,
        1,
        "old/other attempt must not cancel the running call"
    );
    control.attempt = 1;
    control.target_instance.generation = "old".into();
    assert_eq!(
        send_cancel(&router, &p, &target, &control, "DELETE")
            .await
            .status(),
        StatusCode::CONFLICT
    );
    control.target_instance.generation = "g1".into();
    assert_eq!(
        send_cancel(&router, &p, &target, &control, "DELETE")
            .await
            .status(),
        StatusCode::NO_CONTENT
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while adapter.status().active != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(adapter.state.attempts.lock().unwrap().is_empty());
    invalid.store(true, Ordering::SeqCst);
    assert_eq!(
        send(&router, &p, &target, &invocation).await.status(),
        StatusCode::ACCEPTED
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while adapter.status().active != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(progress_calls.load(Ordering::SeqCst) >= 2);
    assert_eq!(p.reports.load(Ordering::SeqCst), 0);
    assert!(adapter.state.attempts.lock().unwrap().is_empty());
    drop(lease);
    adapter.shutdown().await;
    server.abort();
}

async fn send_cancel(
    router: &Router,
    p: &Platform,
    target: &str,
    control: &ServiceCancellation,
    signed_method: &str,
) -> Response {
    let body = serde_json::to_vec(control).unwrap();
    let proof = p
        .signer
        .sign_callback(
            "app",
            signed_method,
            target,
            &body,
            chrono::Utc::now().timestamp(),
        )
        .unwrap();
    router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/invoke")
                .method("DELETE")
                .header("content-type", "application/json")
                .header(SIGNATURE_HEADER, proof)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn signed_call_governance_drains_lower_quota_and_fences_old_policy() {
    // Exercise the same atomic admission used by the HTTP handler with a Both registry.
    let p = Platform {
        signer: Arc::new(ServiceSigner::new(&[68; 32]).unwrap()),
        results: Default::default(),
        reports: Default::default(),
        enrollments: Default::default(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let platform = Router::new()
        .route("/api/user/services/v1/service-auth/trust", get(trust))
        .with_state(p);
    let server = tokio::spawn(async move { axum::serve(listener, platform).await.unwrap() });
    let connection = ServiceConnection::connect(
        &base,
        crate::ServiceCredential::new("app", "app-key").unwrap(),
    )
    .await
    .unwrap();
    let registry = role_registry(Default::default(), Arc::new(Semaphore::new(0)), true, true);
    let adapter = ServiceHttpAdapter::new(registry.clone(), connection, 4).unwrap();
    let mut call = invocation(&registry, CallMode::Sync, json!({}));
    // A published ceiling of four, with a mutable per-replica ceiling of two.
    let budget = Arc::new(Semaphore::new(4));
    call.admission = Some(ServiceCallAdmission {
        policy_revision: 2,
        maximum_concurrency: 2,
    });
    let first = acquire_call_permits(&adapter.state, &call, &budget, 4).unwrap();
    let second = acquire_call_permits(&adapter.state, &call, &budget, 4).unwrap();
    assert_eq!(
        acquire_call_permits(&adapter.state, &call, &budget, 4)
            .unwrap_err()
            .code,
        "capacity_exhausted"
    );
    call.admission = Some(ServiceCallAdmission {
        policy_revision: 3,
        maximum_concurrency: 1,
    });
    assert!(acquire_call_permits(&adapter.state, &call, &budget, 4).is_err());
    drop(first);
    assert!(acquire_call_permits(&adapter.state, &call, &budget, 4).is_err());
    drop(second);
    let third = acquire_call_permits(&adapter.state, &call, &budget, 4).unwrap();
    drop(third);
    call.admission = Some(ServiceCallAdmission {
        policy_revision: 2,
        maximum_concurrency: 4,
    });
    assert_eq!(
        acquire_call_permits(&adapter.state, &call, &budget, 4)
            .unwrap_err()
            .code,
        "stale_governance"
    );
    call.admission = Some(ServiceCallAdmission {
        policy_revision: 3,
        maximum_concurrency: 4,
    });
    assert_eq!(
        acquire_call_permits(&adapter.state, &call, &budget, 4)
            .unwrap_err()
            .code,
        "stale_governance"
    );
    call.admission = None;
    assert_eq!(
        acquire_call_permits(&adapter.state, &call, &budget, 4)
            .unwrap_err()
            .code,
        "invalid_admission"
    );
    // Call quotas do not reserve the independent Event role's shared permits.
    assert_eq!(adapter.state.total.available_permits(), 4);
    server.abort();
}

#[cfg(feature = "event-consumer-http")]
#[tokio::test]
async fn concurrent_call_and_event_enrollment_share_one_instance_generation() {
    use kish_lingshu_event_dispatch_contract::{
        ConsumerEnrollmentRequest, ConsumerInstanceLeaseV1, ConsumerSession,
    };
    let registrations = Arc::new(tokio::sync::Mutex::new(
        Vec::<ServiceInstanceRegistration>::new(),
    ));
    let event_records = registrations.clone();
    let call_records = registrations.clone();
    let p = Platform {
        signer: Arc::new(ServiceSigner::new(&[67; 32]).unwrap()),
        results: Default::default(),
        reports: Default::default(),
        enrollments: Default::default(),
    };
    let app = Router::new()
        .route("/api/user/services/v1/service-auth/trust", get(trust))
        .route(
            "/api/user/services/v1/enrollments",
            post(move |Json(input): Json<ServiceEnrollment>| {
                let records = call_records.clone();
                async move {
                    let registration = input.instance.unwrap();
                    records.lock().await.push(registration.clone());
                    Json(ServiceSession {
                        instance: Some(ServiceInstanceIdentity {
                            instance_id: registration.instance_id,
                            generation: "common".into(),
                        }),
                        node_id: input.node_id,
                        generation: "call".into(),
                        credential: "call-token".into(),
                        heartbeat_interval_ms: 10000,
                        lease_expires_at_ms: chrono::Utc::now().timestamp_millis() + 30000,
                    })
                }
            }),
        )
        .route(
            "/api/user/event-dispatch/v1/consumer-enrollments",
            post(move |Json(input): Json<ConsumerEnrollmentRequest>| {
                let records = event_records.clone();
                async move {
                    let registration = input.instance.unwrap();
                    records.lock().await.push(registration.clone());
                    Json(ConsumerSession {
                        instance: Some(ServiceInstanceIdentity {
                            instance_id: registration.instance_id,
                            generation: "common".into(),
                        }),
                        group_id: 1,
                        group_key: input.group_key,
                        credential: "event-token".into(),
                        expires_at: chrono::Utc::now().timestamp() + 300,
                        lease: ConsumerInstanceLeaseV1 {
                            member_id: 1,
                            node_id: input.node_id,
                            membership_generation: 1,
                            lease_seconds: 30,
                            heartbeat_interval_seconds: 10,
                            lease_expires_at: chrono::Utc::now() + chrono::Duration::seconds(30),
                        },
                    })
                }
            }),
        )
        .route(
            "/api/user/services/v1/sessions/deregister",
            post(|| async { StatusCode::NO_CONTENT }),
        )
        .route(
            "/api/user/event-dispatch/v1/consumer-sessions/deregister",
            post(|| async { StatusCode::NO_CONTENT }),
        )
        .with_state(p);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async { axum::serve(listener, app).await.unwrap() });
    let connection = ServiceConnection::connect(
        &base,
        crate::ServiceCredential::new("app", "app-key").unwrap(),
    )
    .await
    .unwrap();
    let registry = registry(Arc::new(AtomicUsize::new(0)), Arc::new(Semaphore::new(1)));
    let (call, event) = tokio::join!(
        connection.enroll_service("call-node", format!("{base}call"), 1, registry),
        connection.enroll_consumer(crate::event_dispatch::EnrolledConsumerNodeConfig {
            group_key: "events".into(),
            node_id: "event-node".into(),
            invocation_url: format!("{base}event"),
            maximum_in_flight: 1,
        })
    );
    let call = call.unwrap();
    let event = event.unwrap();
    let requests = registrations.lock().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].instance_id, requests[1].instance_id);
    assert_eq!(requests[0].incarnation_id, requests[1].incarnation_id);
    assert!(requests[0].generation.is_none());
    assert_eq!(requests[1].generation.as_deref(), Some("common"));
    drop(requests);
    call.shutdown().await;
    event.shutdown().await;
    connection.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn call_enrollment_root_rejection_revokes_the_shared_connection() {
    for status in [StatusCode::UNAUTHORIZED, StatusCode::FORBIDDEN] {
        let p = Platform {
            signer: Arc::new(ServiceSigner::new(&[67; 32]).unwrap()),
            results: Default::default(),
            reports: Default::default(),
            enrollments: Default::default(),
        };
        let attempts = p.enrollments.clone();
        let app = Router::new()
            .route("/api/user/services/v1/service-auth/trust", get(trust))
            .route(
                "/api/user/services/v1/enrollments",
                post(move || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async move { status }
                }),
            )
            .with_state(p.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async { axum::serve(listener, app).await.unwrap() });
        let connection = ServiceConnection::connect(
            &base,
            crate::ServiceCredential::new("app", "app-key").unwrap(),
        )
        .await
        .unwrap();
        let registry = registry(Arc::new(AtomicUsize::new(0)), Arc::new(Semaphore::new(1)));
        let peer = connection.clone();
        for attempt in [&connection, &peer] {
            assert!(
                matches!(attempt.enroll_service("node", format!("{base}call"), 1, registry.clone()).await,
                Err(crate::ServiceAuthError::Http(code)) if code == status.as_u16())
            );
        }
        assert_eq!(p.enrollments.load(Ordering::SeqCst), 1);
        assert!(peer.protect(Router::new(), &format!("{base}call")).is_err());
        connection.shutdown().await;
        server.abort();
    }
}

#[tokio::test]
async fn renewable_call_heartbeats_are_sdk_owned_and_results_retry_without_reexecuting_handler() {
    let p = Platform {
        signer: Arc::new(ServiceSigner::new(&[69; 32]).unwrap()),
        results: Default::default(),
        reports: Default::default(),
        enrollments: Default::default(),
    };
    let heartbeats = Arc::new(tokio::sync::Mutex::new(Vec::<ServiceHeartbeat>::new()));
    let recorded = heartbeats.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/", listener.local_addr().unwrap());
    let platform = Router::new()
        .route("/api/user/services/v1/service-auth/trust", get(trust))
        .route("/api/user/services/v1/enrollments", post(enroll))
        .route("/api/user/services/v1/completions", post(complete))
        .route(
            "/api/user/services/v1/heartbeats",
            post(
                move |headers: HeaderMap, Json(input): Json<ServiceHeartbeat>| {
                    let recorded = recorded.clone();
                    async move {
                        assert_eq!(headers["authorization"], "Bearer completion-secret");
                        let mut seen = recorded.lock().await;
                        seen.push(input);
                        if seen.len() == 1 {
                            return StatusCode::SERVICE_UNAVAILABLE.into_response();
                        }
                        let now = chrono::Utc::now().timestamp_millis();
                        Json(HeartbeatDisposition::Renewed {
                            store_now_ms: now,
                            liveness_until_ms: now + 33000,
                        })
                        .into_response()
                    }
                },
            ),
        )
        .route(
            "/api/user/services/v1/sessions/deregister",
            post(|| async { StatusCode::NO_CONTENT }),
        )
        .with_state(p.clone());
    let server = tokio::spawn(async move { axum::serve(listener, platform).await.unwrap() });
    let connection = ServiceConnection::connect(
        &base,
        crate::ServiceCredential::new("app", "app-key").unwrap(),
    )
    .await
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let hold = Arc::new(Semaphore::new(0));
    let registry = registry(calls.clone(), hold.clone());
    let adapter = ServiceHttpAdapter::new(registry.clone(), connection, 1).unwrap();
    let target = format!("{base}invoke");
    let router = Router::new().nest("/invoke", adapter.router(&target).unwrap());
    let registration = adapter.enroll("node", &target).await.unwrap();
    let (status, rx) = watch::channel(registration.status());
    *adapter.state.enrollment.write().unwrap() = Some(rx);
    let mut request = invocation(&registry, CallMode::Async, json!({"hold":true}));
    request.completion.as_mut().unwrap().heartbeat = Some(CallHeartbeatPolicy {
        version: 1,
        epoch: "attempt-epoch".into(),
        interval_ms: 10000,
        execution_deadline_ms: request.context.deadline_ms,
        delivery_deadline_ms: request.context.deadline_ms + 3000,
    });
    assert_eq!(
        send(&router, &p, &target, &request).await.status(),
        StatusCode::ACCEPTED
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while heartbeats.lock().await.len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Losing discovery admission must not revoke the separately granted call.
    status.send_replace(ServiceEnrollmentStatus::Unavailable);
    assert!(!adapter.status().accepting);
    hold.add_permits(1);
    tokio::time::timeout(Duration::from_secs(3), async {
        while p.results.lock().await.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let phases: Vec<_> = heartbeats.lock().await.iter().map(|h| h.phase).collect();
    assert!(phases.contains(&CallPhase::Running));
    assert!(phases.contains(&CallPhase::Completing));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(p.reports.load(Ordering::SeqCst), 2);
    drop(status);
    registration.shutdown().await;
    adapter.shutdown().await;
    server.abort();
}
