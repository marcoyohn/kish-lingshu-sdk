//! Release-only end-to-end comparison through the actual product Workflow owner.
//! No synthetic echo server, skipped authentication, or local Invoke replay.
use super::*;
use crate::services::{ServiceHttpAdapter, ServiceRegistryBuilder};
use crate::{ClientBuilder, ClientConfig, MutationOptions, RequestOptions, ServiceCredential};
use kish_lingshu_runtime_contract::{provider::*, service::*, WorkflowId, WorkflowRunResult};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::time::Instant;

async fn admin<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> T {
    let value: serde_json::Value = response.error_for_status().unwrap().json().await.unwrap();
    assert_eq!(value["status"], true, "{value:?}");
    serde_json::from_value(value["data"].clone()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "release licensed Host matrix; run zenss_transport_performance.py"]
async fn licensed_release_workflow_transport_case() {
    assert!(
        !cfg!(debug_assertions),
        "performance evidence requires --release"
    );
    let metrics = super::super::observation::tests::Capture::for_native_fixture()
        .expect("matrix requires its isolated metric recorder");
    let mode = std::env::var("LINGSHU_PERF_TRANSPORT").unwrap();
    assert!(matches!(mode.as_str(), "http" | "zenoh"));
    let number = |name: &str| std::env::var(name).unwrap().parse::<usize>().unwrap();
    let payload_ceiling = number("LINGSHU_PERF_PAYLOAD_BYTES");
    let rejection_case = std::env::var("LINGSHU_PERF_REJECTION_CASE").as_deref() == Ok("true");
    // The 1MiB contract ceiling includes invocation metadata. Reserve explicit
    // headroom for a legal near-ceiling case; separately measure full-body rejection.
    let payload_bytes = if payload_ceiling == 1048576 && !rejection_case {
        payload_ceiling - 16384
    } else {
        payload_ceiling
    };
    let concurrency = number("LINGSHU_PERF_CONCURRENCY");
    let samples = number("LINGSHU_PERF_SAMPLES");
    let warmup = number("LINGSHU_PERF_WARMUP");
    let handler_delay_ms = number("LINGSHU_PERF_HANDLER_DELAY_MS");
    assert!((1..=8).contains(&concurrency) && (20..=1000).contains(&samples));
    assert!((1024..=1048576).contains(&payload_ceiling) && (4..=100).contains(&warmup));
    assert!(!rejection_case || payload_ceiling == 1048576);
    assert!(matches!(handler_delay_ms, 0 | 250));
    let window = std::env::var("LINGSHU_PERF_WINDOW").unwrap_or_default();
    assert!(matches!(window.as_str(), "" | "capacity" | "link-loss"));
    assert!(window.is_empty() || (!rejection_case && concurrency == 8 && payload_ceiling == 1024));
    let capacity = if window == "capacity" { 2 } else { concurrency };
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    let key = std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap();
    let token = std::env::var("LINGSHU_CHANNEL_TEST_PREVIEW_TOKEN").unwrap();
    let markers = std::path::PathBuf::from(std::env::var("LINGSHU_PERF_MARKERS").unwrap());
    let connection = ServiceConnection::connect(&url, ServiceCredential::new(&app, &key).unwrap())
        .await
        .unwrap();
    let identity = connection
        .bootstrap_channel(
            ServiceInstanceRegistration {
                instance_id: "release-performance".into(),
                incarnation_id: "one-case".into(),
                generation: None,
            },
            None,
        )
        .await
        .unwrap();
    let mut pool = identity
        .open_sessions(super::super::ChannelSessionConfig::host_test())
        .await
        .unwrap();
    let mut manifest: ServiceManifest =
        serde_json::from_str(&std::env::var("LINGSHU_CHANNEL_TEST_MANIFEST").unwrap()).unwrap();
    let service = "release-performance-fixture";
    manifest.services[0].service_key = service.into();
    let binding = manifest.services[0].operations[0].call.as_mut().unwrap();
    binding.maximum_concurrency = capacity as u32;
    binding.timeout_ms = 10000;
    let reference = manifest.services[0].operations[0]
        .reference(service)
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let handler_active = active.clone();
    let handler_peak = peak.clone();
    let mut builder = ServiceRegistryBuilder::new(manifest.clone()).unwrap();
    builder.bind::<serde_json::Value, serde_json::Value, _, _>(service, "echo", "1", move |context, input| {
        assert!(matches!(&context.invocation, InvocationRole::Call(c) if c.mode == CallMode::Sync && c.workflow.is_some()));
        assert_eq!(input["pad"].as_str().unwrap().len(), payload_bytes);
        observed.fetch_add(1, Ordering::SeqCst);
        handler_peak.fetch_max(handler_active.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
        let guard = crate::services::execution::ActiveGuard(handler_active.clone());
        async move {
            let _guard = guard;
            if handler_delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(handler_delay_ms as u64)).await;
            }
            Ok(input)
        }
    }).unwrap();
    let registry = Arc::new(builder.build().unwrap());
    let catalog = ProviderCatalog {
        format_version: 1, application_id: app.clone(), provider_key: "release-performance-source".into(), release: "1".into(),
        services: Some(manifest), events: None, workflows: vec![ProviderWorkflow {
            key: "benchmark".into(), name: "Release transport performance".into(), description: None,
            define_schema: serde_json::from_value(serde_json::json!({"type":"ReactFlow","config":{"reactflow":{
                "nodes":[
                    {"id":"start","type":"startEvent","data":{"id":"start","type":"startEvent","name":"Start","trigger":{"type":"manual"}}},
                    {"id":"invoke","type":"serviceTask","data":{"id":"invoke","type":"serviceTask","name":"Echo","operation":reference,"mode":"sync",
                        "input":{"ScriptExpr":format!("#{{pad: \"{}\"}}", "x".repeat(payload_bytes))},
                        "idempotency_key":{"LiteralString":"release-performance-effect"},"deadline_ms":10000,"maximum_attempts":1,"remote_retry":{"maximum_attempts":1}}},
                    {"id":"end","type":"endEvent","data":{"id":"end","type":"endEvent","name":"End"}}
                ],"edges":[{"id":"a","source":"start","target":"invoke"},{"id":"b","source":"invoke","target":"end"}]
            }}})).unwrap(),
        }],
    };
    let mut provider = pool.register_provider_role(&catalog).await.unwrap();
    let ChannelRoleEnrollmentResponse::Provider(response) = provider.registration() else {
        unreachable!()
    };
    let http = reqwest::Client::new();
    let post = |action: &str| {
        http.post(format!(
            "{url}/api/admin/apps/{app}/providers/{}/{action}",
            catalog.provider_key
        ))
        .header("x-token", &token)
        .header("x-kish-app-id", &app)
        .timeout(Duration::from_secs(15))
    };
    let plan: ProviderPlan = admin(
        post("preview")
            .json(&ProviderPreviewRequest {
                instance_id: response.session.instance.instance_id.clone(),
                generation: response.session.generation.clone(),
                catalog_digest: catalog.digest().unwrap(),
                environment_bindings: BTreeMap::new(),
                workflow_bindings: BTreeMap::new(),
            })
            .send()
            .await
            .unwrap(),
    )
    .await;
    let receipt: ProviderReceipt = admin(
        post("apply")
            .json(&ProviderApplyRequest {
                plan_id: plan.id,
                publish: true,
                event_retirement: Default::default(),
            })
            .send()
            .await
            .unwrap(),
    )
    .await;
    assert!(receipt.complete);
    let id = receipt.items["workflow:benchmark"]["workflow_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let mut call = None;
    let mut adapter = None;
    let mut enrollment = None;
    let mut http_connection = None;
    let mut server = None;
    let mut http_port = None;
    let http_peers = Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::new()));
    let http_requests = Arc::new(AtomicUsize::new(0));
    if mode == "zenoh" {
        let mut role = pool
            .register_service_role(
                "release-performance-call",
                capacity as u32,
                registry.as_ref(),
            )
            .await
            .unwrap();
        pool.enable_sync_calls(
            &mut role,
            registry.clone(),
            crate::ServiceExecutionBudget::new(capacity as u32).unwrap(),
        )
        .unwrap();
        call = Some(role);
    } else {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        http_port = Some(listener.local_addr().unwrap().port());
        let endpoint = format!("http://127.0.0.1:{}/", http_port.unwrap());
        // Provider uses a native instance. The HTTP baseline needs its own
        // registration identity: a live instance cannot change endpoint transport.
        let callback_connection =
            ServiceConnection::connect(&url, ServiceCredential::new(&app, &key).unwrap())
                .await
                .unwrap();
        let runtime = ServiceHttpAdapter::new(
            registry.clone(),
            callback_connection.clone(),
            capacity as u32,
        )
        .unwrap();
        http_connection = Some(callback_connection);
        let peers = http_peers.clone();
        let requests = http_requests.clone();
        let router = runtime
            .router(&endpoint)
            .unwrap()
            .layer(axum::middleware::from_fn(
                move |axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<
                    std::net::SocketAddr,
                >,
                      request: axum::extract::Request,
                      next: axum::middleware::Next| {
                    assert_eq!(request.version(), axum::http::Version::HTTP_11);
                    assert_ne!(
                        request
                            .headers()
                            .get("connection")
                            .and_then(|h| h.to_str().ok()),
                        Some("close")
                    );
                    peers.lock().unwrap().insert(peer);
                    requests.fetch_add(1, Ordering::SeqCst);
                    async move { next.run(request).await }
                },
            ));
        server = Some(tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        }));
        enrollment = Some(
            runtime
                .enroll("release-performance-http", &endpoint)
                .await
                .unwrap(),
        );
        adapter = Some(runtime);
    }
    let sdk = ClientBuilder::new(ClientConfig::new(&url).with_retry_limit(0))
        .service_credential(ServiceCredential::new(&app, &key).unwrap())
        .connect()
        .unwrap();
    let workflow = sdk.workflows().select(WorkflowId(id)).unwrap();
    std::fs::write(
        markers.join("transport-ready.json"),
        serde_json::json!({"http_callback_port":http_port}).to_string(),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !markers.join("transport-configured").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut latencies = Vec::new();
    let mut recovery_latencies = Vec::new();
    let mut dispositions = Vec::new();
    let mut warmup_rejections = Vec::new();
    let mut measured_successes = 0usize;
    let mut measured_started = None;
    let mut encoding_offset = 0;
    let mut bytes_offset = 0;
    let mut queue_offset = 0;
    let mut renewed = Instant::now();
    let mut renewals = 0;
    let original_sessions = pool.session_ids();
    let mut native_recovery_due = false;
    let mut native_recovery = None;
    let phases = if window.is_empty() {
        vec![("warmup", warmup), ("measured", samples)]
    } else {
        vec![("warmup", warmup), ("measured", samples), ("recovery", 8)]
    };
    for (phase, count) in phases {
        if phase == "measured" {
            encoding_offset = metrics
                .values("lingshu_sdk_call_response_encoding_seconds", None)
                .len();
            bytes_offset = metrics
                .values("lingshu_sdk_call_response_encoded_bytes", None)
                .len();
            queue_offset = metrics
                .values(
                    "lingshu_sdk_channel_queue_wait_seconds",
                    Some("role_business"),
                )
                .len();
            std::fs::write(markers.join("measured-start"), "start").unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while !markers.join("sampled-start").exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            measured_started = Some(Instant::now());
        }
        let batch_size = if !window.is_empty() && phase != "measured" {
            1
        } else {
            concurrency
        };
        for first in (0..count).step_by(batch_size) {
            if window == "link-loss" && phase == "measured" && first == 0 {
                std::fs::write(markers.join("fault-request"), "drop data TCP only").unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while !markers.join("fault-applied").exists() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
            }
            if native_recovery_due || renewed.elapsed() > Duration::from_secs(8) {
                let mut recovery_context = None;
                // Explicit harness work between batches, using actual signed
                // authority/role renewal and route proof. No Invoke is retried.
                if native_recovery_due {
                    // Removing loss does not synchronously restore TCP/Zenoh.
                    // Retry only signed control observation, inside the ORIGINAL
                    // authority window. A rejection/expiry is terminal; never
                    // create a new Session, register again, or replay an Invoke.
                    let started = Instant::now();
                    let deadline =
                        (started + Duration::from_secs(15)).min(pool.authorization_deadline());
                    recovery_context = Some((started, deadline));
                    let mut attempts = 0usize;
                    tokio::time::timeout_at(deadline, async {
                        loop {
                            attempts += 1;
                            match pool.refresh_authorization().await {
                                Ok(_) => break,
                                Err(super::super::ChannelSessionError::Transport) => {
                                    tokio::time::sleep(Duration::from_millis(250)).await;
                                }
                                Err(error) => {
                                    panic!("native recovery authority rejected: {error:?}")
                                }
                            }
                        }
                    })
                    .await
                    .expect("native recovery must fit original authority and fifteen seconds");
                    assert!(Instant::now() < deadline);
                    assert_eq!(pool.session_ids(), original_sessions);
                    native_recovery = Some(serde_json::json!({
                        "control_observation_attempts": attempts,
                        "elapsed_seconds": started.elapsed().as_secs_f64(),
                        "original_authority_budget_seconds": deadline.duration_since(started).as_secs_f64(),
                        "original_sessions_preserved": true,
                        "bound": "min(original authority deadline, fifteen seconds)",
                        "invoke_retries": 0
                    }));
                } else {
                    pool.refresh_authorization().await.unwrap();
                }
                let mut roles = vec![&mut provider];
                if let Some(role) = call.as_mut() {
                    roles.push(role);
                }
                let renewal = pool.renew_role_leases(&mut roles);
                let results = if let Some((_, deadline)) = recovery_context {
                    tokio::time::timeout_at(deadline, renewal)
                        .await
                        .expect("role renewal must fit original recovery budget")
                        .unwrap()
                } else {
                    renewal.await.unwrap()
                };
                assert!(results.iter().all(|r| r.status == 200));
                let mut proof_attempts = 0usize;
                for role in roles {
                    if let Some((_, deadline)) = recovery_context {
                        // Physical observations are coalesced once per second;
                        // a control Reply alone cannot confirm a data topology.
                        // Re-probe only within the same original recovery budget.
                        tokio::time::timeout_at(deadline, async {
                            loop {
                                proof_attempts += 1;
                                match pool.confirm_role_route(role).await {
                                    Ok(()) => break,
                                    Err(super::super::ChannelSessionError::Transport) => {
                                        tokio::time::sleep(Duration::from_millis(250)).await;
                                    }
                                    Err(error) => {
                                        panic!("native recovery proof rejected: {error:?}")
                                    }
                                }
                            }
                        })
                        .await
                        .expect("fresh route proof must fit original recovery budget");
                        assert!(role.route_confirmed());
                    } else {
                        pool.confirm_role_route(role).await.unwrap();
                    }
                }
                if let Some((started, deadline)) = recovery_context {
                    assert!(Instant::now() < deadline);
                    assert_eq!(pool.session_ids(), original_sessions);
                    let record = native_recovery.as_mut().unwrap();
                    record["elapsed_seconds"] = started.elapsed().as_secs_f64().into();
                    record["route_proof_attempts"] = proof_attempts.into();
                    native_recovery_due = false;
                }
                renewed = Instant::now();
                renewals += 1;
            }
            let mut jobs = tokio::task::JoinSet::new();
            for index in first..(first + batch_size).min(count) {
                let workflow = workflow.clone();
                let mutation = format!("release-performance-{phase}-{index}");
                let allow_failure = (window == "capacity" && phase == "measured")
                    || (window == "link-loss" && phase == "measured" && first == 0);
                let allow_admission_deadline =
                    window == "link-loss" && phase == "measured" && first == 0;
                jobs.spawn(async move {
                    let started = Instant::now();
                    let run = workflow
                        .start(
                            crate::workflow::WorkflowStart::message("benchmark"),
                            MutationOptions::new(mutation).unwrap(),
                        )
                        .await
                        .unwrap();
                    let result = run
                        .wait(
                            RequestOptions::new()
                                .with_deadline(chrono::Utc::now() + chrono::Duration::seconds(20)),
                        )
                        .await;
                    // A runner that rejects immediately may disappear before
                    // SSE attachment, or a replacement Runner may reject its
                    // old cursor. Neither observation error proves a business result.
                    // Only in an explicit negative case, require a Failed durable
                    // snapshot below and let the independent SQL recorder verify
                    // the actual failure identity and reason before acceptance.
                    let observation_failure = match &result {
                        Err(crate::Error::Transport(failure))
                            if (allow_failure || rejection_case) && failure.kind == crate::TransportKind::Stream =>
                        {
                            Some("stream_ended_without_terminal_detail")
                        }
                        Err(crate::Error::Application(failure))
                            if (allow_failure || rejection_case)
                                && failure.http_status == Some(410)
                                && failure.problem.code.as_ref() == "replay_unavailable"
                                && failure.problem.details.as_ref().and_then(|details|
                                    details.get("workflow_instance_id").and_then(serde_json::Value::as_u64))
                                    == Some(run.handle().workflow_instance_id.0) =>
                        {
                            Some("replay_unavailable_for_original_occurrence")
                        }
                        Err(error) => panic!("unexpected observation error: {error:?}"),
                        _ => None,
                    };
                    let completed = matches!(&result, Ok(WorkflowRunResult::Completed { .. }));
                    let failure_message = match &result {
                        Ok(WorkflowRunResult::Failed { failure, .. }) => {
                            Some(failure.message.clone())
                        }
                        _ => None,
                    };
                    if allow_failure {
                        assert!(
                            completed
                                || observation_failure.is_some()
                                || (allow_admission_deadline && failure_message.as_ref().is_some_and(|m|
                                    m.contains("admission_failed: Service total deadline exceeded while waiting for admission")))
                                || failure_message.as_ref().is_some_and(|m| [
                                    "capacity_exhausted",
                                    "service_unavailable",
                                    "native_submission_unavailable",
                                    "outcome_unknown",
                                    "deadline_exceeded"
                                ]
                                .iter()
                                .any(|code| m.contains(code))),
                            "{result:?}"
                        );
                    } else if rejection_case {
                        assert!(
                            observation_failure.is_some() || matches!(&result, Ok(WorkflowRunResult::Failed { failure, .. })
                            if failure.message.contains("payload_too_large")),
                            "{result:?}"
                        );
                    } else {
                        assert!(
                            matches!(result, Ok(WorkflowRunResult::Completed { .. })),
                            "{result:?}"
                        );
                    }
                    let attached = workflow.attach(run.handle().clone(), None).unwrap();
                    tokio::time::timeout(Duration::from_secs(5), async {
                        loop {
                            if attached
                                .snapshot(RequestOptions::new())
                                .await
                                .unwrap()
                                .state
                                == if !completed {
                                    kish_lingshu_runtime_contract::WorkflowState::Failed
                                } else {
                                    kish_lingshu_runtime_contract::WorkflowState::Completed
                                }
                            {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    })
                    .await
                    .unwrap();
                    (
                        started.elapsed().as_secs_f64() * 1000.0,
                        completed,
                        failure_message,
                        run.handle().workflow_instance_id.0.to_string(),
                        observation_failure,
                    )
                });
            }
            while let Some(result) = jobs.join_next().await {
                let (latency, completed, failure, occurrence, observation_failure) =
                    result.unwrap();
                if phase == "measured" {
                    latencies.push(latency);
                    measured_successes += usize::from(completed);
                    dispositions.push(serde_json::json!({"phase": if window == "link-loss" && first == 0 { "fault" } else { "measured" }, "completed":completed,"failure":failure,"latency_ms":latency,"occurrence":occurrence,"observation_failure":observation_failure}));
                } else if phase == "recovery" {
                    recovery_latencies.push(latency);
                } else if rejection_case {
                    warmup_rejections.push(serde_json::json!({"occurrence":occurrence,"completed":completed,"failure":failure,"observation_failure":observation_failure}));
                }
            }
            if window == "link-loss" && phase == "measured" && first == 0 {
                std::fs::write(markers.join("fault-restore-request"), "restore data path").unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while !markers.join("fault-restored").exists() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                native_recovery_due = mode == "zenoh";
            }
        }
    }
    let elapsed = measured_started.unwrap().elapsed().as_secs_f64();
    let encoding = metrics.values("lingshu_sdk_call_response_encoding_seconds", None)
        [encoding_offset..]
        .to_vec();
    let encoded_bytes =
        metrics.values("lingshu_sdk_call_response_encoded_bytes", None)[bytes_offset..].to_vec();
    let queues = metrics.values(
        "lingshu_sdk_channel_queue_wait_seconds",
        Some("role_business"),
    )[queue_offset..]
        .to_vec();
    let successful = if rejection_case { 0 } else { samples };
    let executed = if rejection_case { 0 } else { warmup + samples };
    if window.is_empty() {
        assert_eq!(encoding.len(), successful);
        assert_eq!(encoded_bytes.len(), successful);
        assert_eq!(calls.load(Ordering::SeqCst), executed);
    } else {
        assert!(
            measured_successes < samples,
            "window never applied pressure or link loss"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            warmup + measured_successes + 8
        );
        assert_eq!(recovery_latencies.len(), 8);
        assert!(peak.load(Ordering::SeqCst) <= capacity);
        if window == "capacity" {
            assert!(
                dispositions.iter().all(|row| !row["failure"]
                    .as_str()
                    .is_some_and(|m| m.contains("outcome_unknown"))),
                "unconfirmed submission is not an overload rejection"
            );
        }
    }
    if mode == "http" && window.is_empty() {
        assert_eq!(http_requests.load(Ordering::SeqCst), executed);
        assert!(
            http_peers.lock().unwrap().len() <= concurrency,
            "HTTP connections were not reused"
        );
    }
    std::fs::write(markers.join("case.json"), serde_json::json!({
        "transport":mode,"lanes":pool.lane_count(),"concurrency":concurrency,"payload_padding_bytes":payload_bytes,
        "payload_nominal_ceiling_bytes":payload_ceiling,"rejection_case":rejection_case,
        "samples":samples,"warmup":warmup,"window":window,"capacity":capacity,
        "measured_successes":measured_successes,"measured_failures":samples-measured_successes,
        "recovery_latencies_ms":recovery_latencies,"dispositions":dispositions,"peak_handler_concurrency":peak.load(Ordering::SeqCst),"elapsed_seconds":elapsed,"latencies_ms":latencies,
        "warmup_rejections":warmup_rejections,
        "native_transport_recovery":native_recovery,
        "handler_delay_ms":handler_delay_ms,"signed_renewal_batches":renewals,
        "handler_executions":calls.load(Ordering::SeqCst),"http_callback_port":http_port,
            "http_connections":http_peers.lock().unwrap().len(),"http_requests":http_requests.load(Ordering::SeqCst),
            "response_encoding_seconds":encoding,"response_encoded_bytes":encoded_bytes,"native_worker_queue_seconds":queues,
        "scope":"Workflow Start to durable terminal SQL snapshot; includes orchestration and observation",
        "invoke_retries":0,"workflow_name":"Release transport performance"
    }).to_string()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !markers.join("sampled-end").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    if let Some(enrollment) = enrollment {
        enrollment.shutdown().await;
    }
    if let Some(adapter) = adapter {
        adapter.shutdown().await;
    }
    if let Some(connection) = http_connection {
        connection.shutdown().await;
    }
    if let Some(server) = server {
        server.abort();
        let _ = server.await;
    }
    if let Some(mut call) = call {
        pool.deregister_role(&mut call).await.unwrap();
        call.close().await.unwrap();
    }
    pool.deregister_role(&mut provider).await.unwrap();
    provider.close().await.unwrap();
    pool.close().await.unwrap();
    connection.shutdown().await;
}
