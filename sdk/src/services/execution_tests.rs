use super::*;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct MemoryReporter {
    results: Mutex<Vec<ServiceCompletion>>,
    heartbeats: Mutex<Vec<ServiceHeartbeat>>,
    failures: AtomicUsize,
    hold_reports: std::sync::atomic::AtomicBool,
    gate: Semaphore,
}
impl Default for MemoryReporter {
    fn default() -> Self {
        Self {
            results: Default::default(),
            heartbeats: Default::default(),
            failures: Default::default(),
            hold_reports: Default::default(),
            gate: Semaphore::new(0),
        }
    }
}
#[async_trait::async_trait]
impl CallReporter for MemoryReporter {
    async fn progress(
        &self,
        _: &CompletionTarget,
        _: &ServiceProgress,
    ) -> Result<ProgressDisposition, CallReportError> {
        Ok(ProgressDisposition::Running)
    }
    async fn heartbeat(
        &self,
        _: &CompletionTarget,
        request: &ServiceHeartbeat,
    ) -> Result<HeartbeatDisposition, CallReportError> {
        self.heartbeats.lock().unwrap().push(request.clone());
        let now = chrono::Utc::now().timestamp_millis();
        Ok(HeartbeatDisposition::Renewed {
            store_now_ms: now,
            liveness_until_ms: now + CALL_LIVENESS_MS,
        })
    }
    async fn complete(
        &self,
        _: &CompletionTarget,
        request: &ServiceCompletion,
        _: Option<Duration>,
    ) -> Result<CompletionDisposition, CallReportError> {
        self.results.lock().unwrap().push(request.clone());
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(CallReportError::Unavailable);
        }
        if self.hold_reports.load(Ordering::SeqCst) {
            self.gate.acquire().await.unwrap().forget();
        }
        Ok(CompletionDisposition::Recorded)
    }
}
pub(crate) struct Fixture {
    pub(crate) core: Arc<CallExecutionCore>,
    pub(crate) stop: watch::Sender<bool>,
    pub(crate) registration: watch::Sender<ServiceEnrollmentStatus>,
    pub(crate) calls: Arc<AtomicUsize>,
    pub(crate) hold: Arc<Semaphore>,
    reporter: Arc<MemoryReporter>,
    pub(crate) budget: crate::ServiceExecutionBudget,
}
pub(crate) async fn fixture(capacity: u32) -> Fixture {
    // This is a bootstrap server for an outbound SDK connection, with no Axum
    // adapter or application invocation listener in the execution-only build.
    let signer =
        kish_lingshu_foundation_contract::service_auth::ServiceSigner::new(&[73; 32]).unwrap();
    let trust =
        serde_json::to_vec(&signer.trust("app", chrono::Utc::now().timestamp()).unwrap()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let byte = socket.read_u8().await.unwrap();
            request.push(byte);
            assert!(request.len() < 8192);
        }
        assert!(request.starts_with(b"GET /api/user/services/v1/service-auth/trust "));
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", trust.len()).as_bytes()).await.unwrap();
        socket.write_all(&trust).await.unwrap();
    });
    let connection = ServiceConnection::connect(
        &format!("http://{address}/"),
        crate::ServiceCredential::new("app", "fixture-key").unwrap(),
    )
    .await
    .unwrap();
    server.await.unwrap();
    let manifest = ServiceManifest {
        contract_version: 1,
        application_id: "app".into(),
        services: vec![ServiceDefinition {
            service_key: "test".into(),
            description: String::new(),
            operations: vec![OperationDefinition {
                user_task_completion: None,
                operation_key: "run".into(),
                version: "v1".into(),
                description: String::new(),
                action: "test.run".into(),
                idempotent: true,
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
                error_schema: service_error_schema(),
                call: Some(CallBinding {
                    modes: [CallMode::Sync, CallMode::Async].into_iter().collect(),
                    maximum_concurrency: capacity,
                    timeout_ms: 5000,
                }),
                events: Default::default(),
            }],
        }],
    };
    let mut builder = ServiceRegistryBuilder::new(manifest).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let hold = Arc::new(Semaphore::new(0));
    let observed = calls.clone();
    let held = hold.clone();
    builder
        .bind(
            "test",
            "run",
            "v1",
            move |context: ServiceContext, input: Value| {
                let observed = observed.clone();
                let held = held.clone();
                async move {
                    assert_eq!(context.idempotency_key(), "business-key");
                    assert!(!serde_json::to_string(&context)
                        .unwrap()
                        .contains("result-secret"));
                    observed.fetch_add(1, Ordering::SeqCst);
                    if input["hold"] == true {
                        held.acquire().await.unwrap().forget();
                    }
                    if input["panic"] == true {
                        panic!("fixture handler failure");
                    }
                    if input["invalid_output"] == true {
                        return Ok(json!(false));
                    }
                    if input["oversize"] == true {
                        return Ok(json!({"large":"x".repeat(MAX_SERVICE_PAYLOAD_BYTES)}));
                    }
                    Ok(json!({"applied":true}))
                }
            },
        )
        .unwrap();
    let registry = Arc::new(builder.build().unwrap());
    let reporter = Arc::new(MemoryReporter::default());
    let budget = crate::ServiceExecutionBudget::new(capacity).unwrap();
    let (core, stop) =
        CallExecutionCore::new(registry, connection, budget.clone(), reporter.clone()).unwrap();
    let (registration, rx) = watch::channel(ServiceEnrollmentStatus::Ready {
        node_id: "node".into(),
        generation: "generation".into(),
        lease_expires_at_ms: chrono::Utc::now().timestamp_millis() + 30000,
    });
    *core.enrollment.write().unwrap() = Some(rx);
    Fixture {
        core,
        stop,
        registration,
        calls,
        hold,
        reporter,
        budget,
    }
}
pub(crate) fn invocation(f: &Fixture, mode: CallMode, input: Value) -> ServiceInvocation {
    ServiceInvocation {
        contract_version: 1,
        admission: Some(ServiceCallAdmission {
            policy_revision: 1,
            maximum_concurrency: f.budget.maximum_in_flight(),
        }),
        target_instance: Some(ServiceInstanceTarget {
            node_id: "node".into(),
            generation: "generation".into(),
        }),
        operation: f.core.registry.capabilities()[0].operation.clone(),
        context: ServiceContext {
            application_id: "app".into(),
            idempotency_key: "business-key".into(),
            deadline_ms: chrono::Utc::now().timestamp_millis() + 5000,
            trace_id: None,
            invocation: InvocationRole::Call(CallContext {
                user_task_completion: None,
                call_id: "call-one".into(),
                attempt: 1,
                caller: "workflow".into(),
                mode,
                workflow: None,
            }),
        },
        input,
        completion: (mode == CallMode::Async).then(|| CompletionTarget {
            token: "result-secret".into(),
            heartbeat: None,
        }),
    }
}
pub(crate) async fn wait_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn rejection_code(result: Result<InvocationResponse, CallExecutionError>) -> String {
    match result {
        Err(CallExecutionError::Rejected { error, .. }) => error.code,
        _ => panic!("expected pre-acceptance rejection"),
    }
}

#[tokio::test]
async fn execution_only_validates_contract_and_bounds_panic_output_and_deadline() {
    let f = fixture(1).await;
    let invalid = invocation(&f, CallMode::Sync, json!([]));
    assert_eq!(
        rejection_code(f.core.invoke(invalid).await),
        "invalid_input"
    );
    let mut stale = invocation(&f, CallMode::Sync, json!({}));
    stale.target_instance.as_mut().unwrap().generation = "old".into();
    assert_eq!(rejection_code(f.core.invoke(stale).await), "stale_instance");
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        f.core
            .invoke(invocation(&f, CallMode::Sync, json!({})))
            .await,
        Ok(InvocationResponse::Completed {
            outcome: ServiceOutcome::Succeeded { .. }
        })
    ));
    for (input, code) in [
        (json!({"panic":true}), "handler_panicked"),
        (json!({"invalid_output":true}), "invalid_output"),
        (json!({"oversize":true}), "output_too_large"),
    ] {
        match f.core.invoke(invocation(&f, CallMode::Sync, input)).await {
            Ok(InvocationResponse::Completed {
                outcome: ServiceOutcome::Failed { error },
            }) => assert_eq!(error.code, code),
            _ => panic!("expected bounded handler outcome"),
        }
    }
    let mut short = invocation(&f, CallMode::Sync, json!({"hold":true}));
    short.context.deadline_ms = chrono::Utc::now().timestamp_millis() + 30;
    assert!(
        matches!(f.core.invoke(short).await, Ok(InvocationResponse::Completed { outcome: ServiceOutcome::Failed { error } }) if error.code == "deadline_exceeded")
    );
    assert_eq!(f.budget.semaphore.available_permits(), 1);
    assert!(f.core.attempts.lock().unwrap().is_empty());
    f.core.connection.shutdown().await;
}

#[tokio::test]
async fn shared_owner_fences_duplicate_attempts_and_matches_cancellation_exactly() {
    let f = fixture(2).await;
    let first = invocation(&f, CallMode::Async, json!({"hold":true}));
    assert!(matches!(
        f.core.invoke(first.clone()).await,
        Ok(InvocationResponse::Accepted { .. })
    ));
    wait_until(|| f.calls.load(Ordering::SeqCst) == 1).await;
    assert_eq!(
        rejection_code(f.core.invoke(first.clone()).await),
        "attempt_running"
    );
    let mut second = first.clone();
    if let InvocationRole::Call(c) = &mut second.context.invocation {
        c.call_id = "call-two".into();
    }
    assert!(matches!(
        f.core.invoke(second).await,
        Ok(InvocationResponse::Accepted { .. })
    ));
    wait_until(|| f.calls.load(Ordering::SeqCst) == 2).await;
    let mut third = first.clone();
    if let InvocationRole::Call(c) = &mut third.context.invocation {
        c.call_id = "call-three".into();
    }
    assert_eq!(
        rejection_code(f.core.invoke(third).await),
        "capacity_exhausted"
    );
    let mut cancel = ServiceCancellation {
        contract_version: 1,
        target_instance: first.target_instance.unwrap(),
        operation: first.operation,
        call_id: "call-one".into(),
        attempt: 2,
    };
    assert!(f.core.cancel_attempt(cancel.clone()));
    assert_eq!(f.core.active.load(Ordering::Acquire), 2);
    cancel.attempt = 1;
    cancel.target_instance.generation = "old".into();
    assert!(!f.core.cancel_attempt(cancel.clone()));
    cancel.target_instance.generation = "generation".into();
    assert!(f.core.cancel_attempt(cancel));
    wait_until(|| f.core.active.load(Ordering::Acquire) == 1).await;
    assert_eq!(f.budget.semaphore.available_permits(), 1);
    f.stop.send_replace(true);
    wait_until(|| f.core.active.load(Ordering::Acquire) == 0).await;
    assert_eq!(f.budget.semaphore.available_permits(), 2);
    assert!(f.reporter.results.lock().unwrap().is_empty());
    f.core.connection.shutdown().await;
}

#[tokio::test]
async fn result_retry_retains_shared_capacity_and_same_result_without_reexecuting_handler() {
    let f = fixture(1).await;
    f.reporter.failures.store(1, Ordering::SeqCst);
    f.reporter.hold_reports.store(true, Ordering::SeqCst);
    let request = invocation(&f, CallMode::Async, json!({}));
    assert!(matches!(
        f.core.invoke(request.clone()).await,
        Ok(InvocationResponse::Accepted { .. })
    ));
    wait_until(|| f.reporter.results.lock().unwrap().len() == 2).await;
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.core.active.load(Ordering::Acquire), 1);
    assert!(f.budget.acquire().is_err());
    assert_eq!(
        rejection_code(f.core.invoke(request).await),
        "capacity_exhausted"
    );
    let results = f.reporter.results.lock().unwrap().clone();
    assert_eq!(results[0], results[1]);
    f.reporter.gate.add_permits(1);
    wait_until(|| f.core.active.load(Ordering::Acquire) == 0).await;
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.core.unconfirmed.load(Ordering::Acquire), 0);
    assert_eq!(f.budget.semaphore.available_permits(), 1);
    f.core.connection.shutdown().await;
}

#[tokio::test]
async fn renewable_accepted_call_survives_discovery_unavailability_under_attempt_authority() {
    let f = fixture(1).await;
    let mut request = invocation(&f, CallMode::Async, json!({"hold":true}));
    request.completion.as_mut().unwrap().heartbeat = Some(CallHeartbeatPolicy {
        version: CALL_HEARTBEAT_VERSION,
        epoch: "attempt-epoch".into(),
        interval_ms: CALL_HEARTBEAT_INTERVAL_MS,
        execution_deadline_ms: request.context.deadline_ms,
        delivery_deadline_ms: request.context.deadline_ms + CALL_DELIVERY_MS,
    });
    assert!(matches!(
        f.core.invoke(request).await,
        Ok(InvocationResponse::Accepted { .. })
    ));
    wait_until(|| {
        f.calls.load(Ordering::SeqCst) == 1 && !f.reporter.heartbeats.lock().unwrap().is_empty()
    })
    .await;
    f.registration
        .send_replace(ServiceEnrollmentStatus::Unavailable);
    assert_eq!(
        rejection_code(
            f.core
                .invoke(invocation(&f, CallMode::Sync, json!({})))
                .await
        ),
        "stale_instance"
    );
    f.hold.add_permits(1);
    wait_until(|| f.core.active.load(Ordering::Acquire) == 0).await;
    {
        let results = f.reporter.results.lock().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].call_id, "call-one");
    }
    assert!(f
        .reporter
        .heartbeats
        .lock()
        .unwrap()
        .iter()
        .any(|h| h.phase == CallPhase::Completing && h.epoch == "attempt-epoch"));
    assert_eq!(f.budget.semaphore.available_permits(), 1);
    f.core.connection.shutdown().await;
}

#[tokio::test]
async fn exhausted_reporting_remains_unconfirmed_without_replaying_business_effects() {
    let f = fixture(1).await;
    f.reporter.failures.store(10, Ordering::SeqCst);
    assert!(matches!(
        f.core
            .invoke(invocation(&f, CallMode::Async, json!({})))
            .await,
        Ok(InvocationResponse::Accepted { .. })
    ));
    wait_until(|| f.core.active.load(Ordering::Acquire) == 0).await;
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    {
        let results = f.reporter.results.lock().unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r == &results[0]));
    }
    assert_eq!(f.core.unconfirmed.load(Ordering::Acquire), 1);
    assert_eq!(f.budget.semaphore.available_permits(), 1);
    assert!(f.core.attempts.lock().unwrap().is_empty());
    f.core.connection.shutdown().await;
}

#[tokio::test]
async fn root_closure_interrupts_an_accepted_sync_call_as_unknown_and_releases_ownership() {
    let f = fixture(1).await;
    let request = invocation(&f, CallMode::Sync, json!({"hold":true}));
    let core = f.core.clone();
    let task = tokio::spawn(async move { core.invoke(request).await });
    wait_until(|| f.calls.load(Ordering::SeqCst) == 1).await;
    f.core.connection.shutdown().await;
    assert!(matches!(
        task.await.unwrap(),
        Err(CallExecutionError::OutcomeUnknown)
    ));
    assert_eq!(f.budget.semaphore.available_permits(), 1);
    assert!(f.core.attempts.lock().unwrap().is_empty());
    assert_eq!(
        rejection_code(
            f.core
                .invoke(invocation(&f, CallMode::Sync, json!({})))
                .await
        ),
        "service_stopping"
    );
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
}

#[cfg(feature = "service-call-zenoh")]
#[tokio::test]
async fn accepted_async_attempt_is_fenced_by_root_closure_instance_replacement_and_execution_deadline(
) {
    for cause in ["root", "replacement", "deadline"] {
        let f = fixture(1).await;
        let mut request = invocation(&f, CallMode::Async, json!({"hold":true}));
        if cause == "deadline" {
            request.context.deadline_ms = chrono::Utc::now().timestamp_millis() + 50;
        }
        request.completion.as_mut().unwrap().heartbeat = Some(CallHeartbeatPolicy {
            version: CALL_HEARTBEAT_VERSION,
            epoch: "original-epoch".into(),
            interval_ms: CALL_HEARTBEAT_INTERVAL_MS,
            execution_deadline_ms: request.context.deadline_ms,
            delivery_deadline_ms: request.context.deadline_ms + CALL_DELIVERY_MS,
        });
        assert!(matches!(
            f.core.invoke(request).await,
            Ok(InvocationResponse::Accepted { .. })
        ));
        wait_until(|| f.calls.load(Ordering::SeqCst) == 1).await;
        match cause {
            "root" => f.core.connection.shutdown().await,
            "replacement" => {
                f.registration.send_replace(ServiceEnrollmentStatus::Ready {
                    node_id: "node".into(),
                    generation: "replacement".into(),
                    lease_expires_at_ms: chrono::Utc::now().timestamp_millis() + 30000,
                });
            }
            _ => {}
        }
        wait_until(|| f.core.active.load(Ordering::Acquire) == 0).await;
        assert!(f.core.join_accepted().await);
        assert!(f.core.attempts.lock().unwrap().is_empty());
        assert_eq!(f.budget.semaphore.available_permits(), 1);
        assert_eq!(f.calls.load(Ordering::SeqCst), 1);
        let results = f.reporter.results.lock().unwrap();
        if cause == "deadline" {
            assert_eq!(results.len(), 1);
            assert!(
                matches!(&results[0].outcome, ServiceOutcome::Failed { error } if error.code == "deadline_exceeded")
            );
        } else {
            assert!(
                results.is_empty(),
                "{cause} must not confirm a business result"
            );
        }
        drop(results);
        f.core.connection.shutdown().await;
    }
}

#[cfg(feature = "service-call-zenoh")]
#[tokio::test]
async fn accepted_task_join_timeout_and_cancellation_never_confirm_cleanup() {
    let f = fixture(1).await;
    tokio::time::pause();
    let task = tokio::spawn(std::future::pending::<()>());
    f.core.accepted_tasks.lock().unwrap().push(task);
    assert!(!f.core.join_accepted().await);
    assert_eq!(f.core.accepted_tasks.lock().unwrap().len(), 1);
    f.core.accepted_tasks.lock().unwrap()[0].abort();
    assert!(!f.core.join_accepted().await);
    // A later empty join must retain the prior failed-cleanup evidence.
    assert!(!f.core.join_accepted().await);
    tokio::time::resume();
    f.core.connection.shutdown().await;
}
