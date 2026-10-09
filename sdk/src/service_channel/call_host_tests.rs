//! Actual licensed host/TLS/Owner/Workflow fixture, run through the exported SDK.
use super::*;
use crate::services::ServiceRegistryBuilder;
use crate::{ClientBuilder, ClientConfig, MutationOptions, RequestOptions, ServiceCredential};
use kish_lingshu_runtime_contract::{provider::*, service::*, WorkflowId, WorkflowRunResult};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
};

async fn admin<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> T {
    let value: serde_json::Value = response.error_for_status().unwrap().json().await.unwrap();
    assert_eq!(value["status"], true, "{value:?}");
    serde_json::from_value(value["data"].clone()).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "isolated licensed native host; run zenss_channel_bootstrap_acceptance.py --sync-call-test"]
async fn native_sync_workflow_uses_authorized_role_and_saved_result_without_callback_listener() {
    // Selected single-test process only: the ordinary SDK still installs no
    // recorder. Capture callbacks/worker threads across the real TLS fixture.
    let resources = super::super::observation::tests::Capture::for_native_fixture();
    native_workflow(CallMode::Sync, None, None).await;
    if let Some(resources) = resources {
        resources.assert_balanced_resource_gauges();
        resources.assert_bounded_labels();
        for plane in ["role_business", "role_control", "catalog"] {
            assert!(
                resources.sum("lingshu_sdk_channel_reservation_seconds", Some(plane), None) > 0.0
            );
            assert!(
                resources.sum("lingshu_sdk_channel_queue_wait_seconds", Some(plane), None) > 0.0
            );
        }
        for name in [
            "lingshu_sdk_channel_reserved_roles",
            "lingshu_sdk_channel_reserved_declaration_keys",
        ] {
            assert!(resources.count(name) > 0);
        }
        assert_eq!(
            resources.sum(
                "lingshu_sdk_channel_declaration_reservations_retained_total",
                None,
                None
            ),
            0.0
        );
        assert!(
            resources.sum(
                "lingshu_sdk_channel_business_results_total",
                Some("call"),
                Some("succeeded")
            ) >= 1.0
        );
        assert!(
            resources.sum(
                "lingshu_sdk_channel_inbound_exchanges_total",
                Some("call"),
                Some("reply_submitted")
            ) >= 1.0
        );
        eprintln!("PASS SDK role ingress/declaration observations: business/control/catalog, static labels, nonnegative and balanced after cleanup");
        eprintln!("PASS SDK Sync business results are separate from submitted replies and platform receipts");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "isolated licensed native host; run zenss_channel_bootstrap_acceptance.py --async-call-test"]
async fn native_async_workflow_uses_independent_report_and_saved_result_without_callback_listener()
{
    let resources = super::super::observation::tests::Capture::for_native_fixture();
    native_workflow(CallMode::Async, None, None).await;
    if let Some(resources) = resources {
        resources.assert_balanced_resource_gauges();
        resources.assert_bounded_labels();
        let durable = |outcome| {
            resources.sum(
                "lingshu_sdk_channel_verified_dispositions_total",
                Some("completion"),
                Some(outcome),
            )
        };
        assert!(durable("recorded") + durable("duplicate") >= 1.0);
        assert!(
            resources.sum(
                "lingshu_sdk_channel_business_results_total",
                Some("call"),
                Some("succeeded")
            ) >= 1.0
        );
        assert!(
            resources.sum(
                "lingshu_sdk_channel_prepared_responses_total",
                Some("call"),
                Some("accepted")
            ) >= 1.0
        );
        assert!(
            resources.sum(
                "lingshu_sdk_channel_exchanges_total",
                Some("completion"),
                Some("reply_verified")
            ) >= 1.0
        );
        if std::env::var("LINGSHU_VERIFY_DYNAMIC_ROLES").as_deref() == Ok("true") {
            assert!(resources.count("lingshu_sdk_channel_command_queue_wait_seconds") >= 2);
        }
        eprintln!("PASS SDK native reports: verified original durable completion, balanced exchanges and lifecycle commands, static labels");
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "two product processes and active Writer markers; run multirouter --remote-writer-test"]
async fn native_async_completion_forwards_to_active_remote_writer_with_parallel_sync_work() {
    native_workflow_with_parallel(CallMode::Async, None, None, true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "two products and a real SQLite checkpoint fault; run multirouter --completion-persistence-failure-test"]
async fn native_async_checkpoint_failure_retries_original_result_without_reexecution() {
    native_workflow_with_parallel(
        CallMode::Async,
        Some(faults::FaultMode::ObserveCheckpointFailure),
        None,
        true,
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "licensed host native Async faults; run acceptance --async-call-fault-test"]
async fn native_async_lost_and_early_replies_preserve_one_execution_and_durable_result() {
    let resources = super::super::observation::tests::Capture::for_native_fixture();
    for mode in [
        faults::FaultMode::LoseAccepted,
        faults::FaultMode::CompleteBeforeAccepted,
        faults::FaultMode::LoseCompletionAck,
        faults::FaultMode::CapacityRejected,
    ] {
        native_workflow(CallMode::Async, Some(mode), None).await;
    }
    if let Some(resources) = resources {
        // Four original successful executions; only the capacity case adds
        // two affirmative pre-acceptance rejections. ACK retries add no result.
        assert_eq!(
            resources.sum(
                "lingshu_sdk_channel_business_results_total",
                Some("call"),
                Some("succeeded")
            ),
            4.0
        );
        assert_eq!(
            resources.sum(
                "lingshu_sdk_channel_business_results_total",
                Some("call"),
                Some("rejected")
            ),
            2.0
        );
        assert_eq!(
            resources.sum(
                "lingshu_sdk_channel_business_results_total",
                Some("call"),
                Some("unknown")
            ),
            0.0
        );
        assert!(
            resources.sum(
                "lingshu_sdk_channel_inbound_exchanges_total",
                Some("call"),
                Some("unknown")
            ) >= 1.0
        );
        resources.assert_balanced_resource_gauges();
        resources.assert_bounded_labels();
        eprintln!("PASS SDK Async result ownership: four executions, two capacity rejections, lost Accepted/ACK and early completion never recount results");
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum CallLifecycle {
    RoleExpiry,
    CertificateRotation,
    ManagedRotation,
    ProductDrain,
    ProductStop,
    WriterRestart,
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "two products, SIGKILL and real Writer TTL; run multirouter --writer-restart-test"]
async fn native_accepted_call_reports_after_original_writer_process_restarts() {
    native_workflow(
        CallMode::Async,
        Some(faults::FaultMode::LoseCompletionAck),
        Some(CallLifecycle::WriterRestart),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "two products, original Writer remains dead; run multirouter --writer-takeover-test"]
async fn native_accepted_call_reports_after_other_node_takes_over_dead_writer() {
    assert_eq!(
        std::env::var("LINGSHU_WRITER_TAKEOVER_TEST").as_deref(),
        Ok("true")
    );
    native_workflow(
        CallMode::Async,
        Some(faults::FaultMode::LoseCompletionAck),
        Some(CallLifecycle::WriterRestart),
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "licensed matched Host final stop; run acceptance --final-stop-test"]
async fn native_accepted_call_cannot_confirm_after_final_product_stop() {
    native_workflow(
        CallMode::Async,
        Some(faults::FaultMode::LoseCompletionAck),
        Some(CallLifecycle::ProductStop),
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "licensed matched Host drain; run acceptance --drain-test"]
async fn native_accepted_call_completes_after_drain_and_replays_one_lost_ack() {
    native_workflow(
        CallMode::Async,
        Some(faults::FaultMode::LoseCompletionAck),
        Some(CallLifecycle::ProductDrain),
    )
    .await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "licensed matched Host drain; run acceptance --sync-drain-test"]
async fn native_sync_call_keeps_original_reply_after_product_drain() {
    native_workflow(CallMode::Sync, None, Some(CallLifecycle::ProductDrain)).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "licensed host accepted-call lifecycle; run acceptance --call-lifecycle-test"]
async fn native_accepted_call_survives_role_expiry_and_inflight_certificate_handoff() {
    for lifecycle in [
        CallLifecycle::RoleExpiry,
        CallLifecycle::CertificateRotation,
        CallLifecycle::ManagedRotation,
    ] {
        native_workflow(CallMode::Async, None, Some(lifecycle)).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "licensed host; run acceptance --full-pool-rotation-test"]
async fn native_full_pool_defers_rotation_until_original_accepted_report_finishes() {
    assert!(super::super::ChannelSessionConfig::host_test().session_count() > 2);
    native_workflow(CallMode::Async, None, Some(CallLifecycle::ManagedRotation)).await;
}
async fn native_workflow(
    mode: CallMode,
    fault: Option<faults::FaultMode>,
    lifecycle: Option<CallLifecycle>,
) {
    native_workflow_with_parallel(mode, fault, lifecycle, false).await;
}
async fn native_workflow_with_parallel(
    mode: CallMode,
    fault: Option<faults::FaultMode>,
    lifecycle: Option<CallLifecycle>,
    parallel_owner: bool,
) {
    let asynchronous = mode == CallMode::Async;
    let writer_takeover = std::env::var("LINGSHU_WRITER_TAKEOVER_TEST").as_deref() == Ok("true");
    let wire_mode = if asynchronous { "async" } else { "sync" };
    let label = if parallel_owner {
        "async-remote-writer".into()
    } else {
        lifecycle.map_or_else(
            || fault.map_or_else(|| wire_mode.to_owned(), |f| format!("async-{}", f.label())),
            |l| {
                format!(
                    "{wire_mode}-{}",
                    if l == CallLifecycle::RoleExpiry {
                        "role-expiry"
                    } else if l == CallLifecycle::ManagedRotation {
                        "managed-certificate-handoff"
                    } else if l == CallLifecycle::ProductDrain {
                        "product-drain"
                    } else if l == CallLifecycle::ProductStop {
                        "product-stop"
                    } else if l == CallLifecycle::WriterRestart {
                        if writer_takeover {
                            "writer-takeover"
                        } else {
                            "writer-restart"
                        }
                    } else {
                        "certificate-handoff"
                    }
                )
            },
        )
    };
    let expected_executions = if parallel_owner { 2 } else { 1 };
    let timeout_ms = if parallel_owner {
        10_000
    } else if asynchronous
        && matches!(
            lifecycle,
            Some(
                CallLifecycle::RoleExpiry
                    | CallLifecycle::ManagedRotation
                    | CallLifecycle::ProductDrain
                    | CallLifecycle::ProductStop
                    | CallLifecycle::WriterRestart
            )
        )
    {
        45_000
    } else {
        5000
    };
    let service_key = format!("native-{label}-workflow-fixture");
    let source = format!("native-{label}-workflow-source");
    let instance = format!("native-{label}-workflow");
    let call_node = format!("native-{label}-workflow-call");
    let workflow_key = format!("native-{label}-workflow");
    let workflow_name = if parallel_owner {
        "Native active remote Writer Workflow".into()
    } else if lifecycle.is_some() {
        format!(
            "Native {} lifecycle {label}",
            if asynchronous { "Async" } else { "Sync" }
        )
    } else if fault.is_some() {
        format!("Native Async fault {label}")
    } else if asynchronous {
        "Native asynchronous Workflow".into()
    } else {
        "Native synchronous Workflow".into()
    };
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    let dispatch_url =
        std::env::var("LINGSHU_CHANNEL_TEST_DISPATCH_URL").unwrap_or_else(|_| url.clone());
    let key = std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap();
    let token = std::env::var("LINGSHU_CHANNEL_TEST_PREVIEW_TOKEN").unwrap();
    let connection = ServiceConnection::connect(&url, ServiceCredential::new(&app, &key).unwrap())
        .await
        .unwrap();
    let identity = connection
        .bootstrap_test_channel(
            ServiceInstanceRegistration {
                instance_id: instance,
                incarnation_id: "workflow-boot".into(),
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
    if std::env::var_os("LINGSHU_VERIFY_REPORT_INSTALL_ISOLATION").is_some() {
        let key = format!(
            "{}/report-install",
            pool.identity()
                .bootstrap_response()
                .control_route
                .key()
                .unwrap()
                .as_str()
        );
        let forged = serde_json::to_vec(&serde_json::json!({
            "request_id": uuid::Uuid::new_v4().to_string(),
            "application_id": app,
            "deadline_ms": chrono::Utc::now().timestamp_millis() + 3000,
            "action": {"action": "cancel", "install_key": key},
        }))
        .unwrap();
        let replies = pool.sessions[0]
            .get(key)
            .payload(forged)
            .timeout(Duration::from_millis(500))
            .await
            .unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(1), replies.recv_async()).await;
        assert!(
            !matches!(reply, Ok(Ok(ref reply)) if reply.result().is_ok()),
            "SDK cannot use a Platform-only report installation route"
        );
    }
    let mut manifest: ServiceManifest =
        serde_json::from_str(&std::env::var("LINGSHU_CHANNEL_TEST_MANIFEST").unwrap()).unwrap();
    manifest.services[0].service_key = service_key.clone();
    manifest.services[0].operations[0]
        .call
        .as_mut()
        .unwrap()
        .timeout_ms = timeout_ms;
    manifest.services[0].operations[0]
        .call
        .as_mut()
        .unwrap()
        .maximum_concurrency = expected_executions;
    manifest.services[0].operations[0]
        .call
        .as_mut()
        .unwrap()
        .modes = if asynchronous {
        [CallMode::Sync, CallMode::Async].into_iter().collect()
    } else {
        [CallMode::Sync].into_iter().collect()
    };
    let reference = manifest.services[0].operations[0]
        .reference(&service_key)
        .unwrap();
    let executions = Arc::new(AtomicUsize::new(0));
    let observed = executions.clone();
    let handler_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let handler_hold = handler_gate.clone();
    let mut builder = ServiceRegistryBuilder::new(manifest.clone()).unwrap();
    builder.bind::<serde_json::Value, serde_json::Value, _, _>(&service_key, "echo", "1", move |context, value| {
        let branch = value.get("branch").and_then(|v| v.as_str()).unwrap_or("normal");
        let branch_mode = if parallel_owner && branch == "slow" { CallMode::Sync } else { mode };
        assert_eq!(context.idempotency_key, if parallel_owner { format!("native-workflow-effect-{branch}") } else { "native-workflow-effect".into() });
        assert!(matches!(&context.invocation, InvocationRole::Call(c) if c.mode == branch_mode && c.workflow.is_some()));
        let root = match &context.invocation {
            InvocationRole::Call(c) => c.workflow.as_ref().unwrap().root_workflow_instance_id.clone(),
            _ => unreachable!(),
        };
        let first = observed.fetch_add(1, Ordering::SeqCst) == 0;
        let gate = handler_hold.clone();
        async move {
            let trace = crate::service_channel::trace::current_trace_parent().expect("native Handler causal scope");
            let parsed = kish_lingshu_foundation_contract::trace::TraceParent::parse(&trace).unwrap();
            if parallel_owner {
                let markers = std::path::PathBuf::from(std::env::var("LINGSHU_REMOTE_WRITER_MARKER_DIR").unwrap());
                std::fs::write(markers.join(if branch_mode == CallMode::Sync { "slow-trace" } else { "fast-trace" }), &trace).unwrap();
                let marker = if branch_mode == CallMode::Sync {
                    std::fs::write(markers.join("slow-started"), root).unwrap();
                    "release-slow"
                } else {
                    std::fs::write(markers.join("fast-started"), &root).unwrap();
                    "release-fast"
                };
                tokio::time::timeout(Duration::from_secs(8), async {
                    while !markers.join(marker).exists() { tokio::time::sleep(Duration::from_millis(10)).await; }
                }).await.expect("active Writer fixture coordination budget");
            }
            if first && lifecycle.is_some() { gate.acquire().await.unwrap().forget(); }
            assert_eq!(crate::service_channel::trace::current_trace_parent().as_deref(), Some(trace.as_str()));
            assert!(!parsed.trace_id().is_empty());
            Ok(value)
        }
    }).unwrap();
    let registry = Arc::new(builder.build().unwrap());
    let mut catalog = ProviderCatalog {
        format_version: 1, application_id: app.clone(), provider_key: source, release: "1".into(),
        services: Some(manifest), events: None, workflows: vec![ProviderWorkflow {
            key: workflow_key.clone(), name: workflow_name.into(), description: None,
            define_schema: serde_json::from_value(serde_json::json!({"type":"ReactFlow","config":{"reactflow":{
                "nodes":[
                    {"id":"start","type":"startEvent","data":{"id":"start","type":"startEvent","name":"Start","trigger":{"type":"manual"}}},
                    {"id":"invoke","type":"serviceTask","data":{"id":"invoke","type":"serviceTask","name":"Native echo","operation":reference,"mode":wire_mode,"input":{"ScriptExpr":"#{case_id: 42}"},"idempotency_key":{"LiteralString":"native-workflow-effect"},"deadline_ms":timeout_ms,"maximum_attempts":1,"remote_retry":{"maximum_attempts":3,"retry_delay_ms":{"ScriptExpr":"[100, 100]"}}}},
                    {"id":"end","type":"endEvent","data":{"id":"end","type":"endEvent","name":"End"}}
                ],"edges":[{"id":"a","source":"start","target":"invoke"},{"id":"b","source":"invoke","target":"end"}]
            }}})).unwrap(),
        }],
    };
    if parallel_owner {
        let graph = catalog.workflows[0]
            .define_schema
            .get_mut("config")
            .unwrap();
        let graph = graph.get_mut("reactflow").unwrap();
        let original = graph["nodes"][1].clone();
        let mut fast = original.clone();
        fast["id"] = serde_json::json!("fast");
        fast["data"]["id"] = serde_json::json!("fast");
        fast["data"]["input"] =
            serde_json::json!({"ScriptExpr":"#{case_id: 42, branch: \"fast\"}"});
        fast["data"]["idempotency_key"] =
            serde_json::json!({"LiteralString":"native-workflow-effect-fast"});
        let mut slow = original;
        slow["id"] = serde_json::json!("slow");
        slow["data"]["id"] = serde_json::json!("slow");
        slow["data"]["mode"] = serde_json::json!("sync");
        slow["data"]["input"] =
            serde_json::json!({"ScriptExpr":"#{case_id: 42, branch: \"slow\"}"});
        slow["data"]["idempotency_key"] =
            serde_json::json!({"LiteralString":"native-workflow-effect-slow"});
        graph["nodes"] = serde_json::json!([
            {"id":"start","type":"startEvent","data":{"id":"start","type":"startEvent","name":"Start","trigger":{"type":"manual"}}},
            {"id":"fork","type":"parallelGateway","data":{"id":"fork","type":"parallelGateway","name":"Fork"}}, fast, slow,
            {"id":"join","type":"join","data":{"id":"join","type":"join","name":"Join"}},
            {"id":"end","type":"endEvent","data":{"id":"end","type":"endEvent","name":"End"}}
        ]);
        graph["edges"] = serde_json::json!([
            {"id":"start-fork","source":"start","target":"fork"},
            {"id":"fork-fast","source":"fork","target":"fast"},
            {"id":"fork-slow","source":"fork","target":"slow"},
            {"id":"fast-join","source":"fast","target":"join"},
            {"id":"slow-join","source":"slow","target":"join"},
            {"id":"join-end","source":"join","target":"end"}
        ]);
    }
    let mut provider = pool.register_provider_role(&catalog).await.unwrap();
    let ChannelRoleEnrollmentResponse::Provider(response) = provider.registration() else {
        unreachable!()
    };
    let client = reqwest::Client::new();
    let base = format!(
        "{dispatch_url}/api/admin/apps/{app}/providers/{}",
        catalog.provider_key
    );
    let post = |path: &str| {
        client
            .post(format!("{base}/{path}"))
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
    assert!(receipt.complete, "{receipt:?}");
    let id = receipt.items[&format!("workflow:{workflow_key}")]["workflow_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let mut call = pool
        .register_service_role(&call_node, expected_executions, registry.as_ref())
        .await
        .unwrap();
    if let kish_lingshu_foundation_contract::service_transport::ServiceEndpoint::Zenoh {
        lanes,
        ..
    } = call.endpoint()
    {
        assert_eq!(lanes.len(), pool.lane_count());
    } else {
        panic!("native Call must retain its issued lanes");
    }
    let sdk = ClientBuilder::new(ClientConfig::new(&dispatch_url).with_retry_limit(0))
        .service_credential(ServiceCredential::new(&app, &key).unwrap())
        .connect()
        .unwrap();
    let mut workflow = sdk.workflows().select(WorkflowId(id)).unwrap();
    let budget = crate::ServiceExecutionBudget::new(expected_executions).unwrap();
    // Real shared execution capacity, not a fabricated Rejected response.
    let held =
        (fault == Some(faults::FaultMode::CapacityRejected)).then(|| budget.acquire().unwrap());
    if held.is_some() {
        // Control proof remains independent of a completely occupied shared
        // business budget, including when its physical Session is dedicated.
        pool.refresh_authorization().await.unwrap();
        assert!(budget.acquire().is_err());
    }
    if fault.is_none() && lifecycle.is_none() && !parallel_owner {
        // Async must reject a Sync-only execution owner, even with a current route.
        if asynchronous {
            pool.enable_sync_calls(&mut call, registry.clone(), budget.clone())
                .unwrap();
        }
        // A route proof or Sync-only binding cannot make Async admission ready.
        let unready = workflow
            .start(
                crate::workflow::WorkflowStart::message("probe-only fixture"),
                MutationOptions::new(format!("native-{label}-workflow-probe-only")).unwrap(),
            )
            .await
            .unwrap();
        let unavailable = unready
            .wait(
                RequestOptions::new()
                    .with_deadline(chrono::Utc::now() + chrono::Duration::seconds(15)),
            )
            .await
            .unwrap();
        assert!(
            matches!(unavailable, WorkflowRunResult::Failed { .. }),
            "{unavailable:?}"
        );
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        if asynchronous {
            // Replacement on the same binding is forbidden. Withdraw and create a
            // fresh role after the negative Sync-only admission check.
            pool.deregister_role(&mut call).await.unwrap();
            call.close().await.unwrap();
            call = pool
                .register_service_role(&call_node, 1, registry.as_ref())
                .await
                .unwrap();
            pool.enable_async_calls(&mut call, registry.clone(), budget.clone())
                .unwrap();
        } else {
            pool.enable_sync_calls(&mut call, registry.clone(), budget.clone())
                .unwrap();
        }
    } else if asynchronous {
        pool.enable_async_calls(&mut call, registry.clone(), budget.clone())
            .unwrap();
    } else {
        pool.enable_sync_calls(&mut call, registry.clone(), budget.clone())
            .unwrap();
    }
    let injected = fault.map(faults::Faults::new);
    let execution = call.calls.lock().unwrap().as_ref().unwrap().clone();
    if let Some(injected) = &injected {
        *execution.faults.lock().unwrap() = Some(injected.clone());
    }
    let release_capacity = held.map(|held| {
        let injected = injected.as_ref().unwrap().clone();
        tokio::spawn(async move {
            let mut rejected = injected.rejected.subscribe();
            let result =
                tokio::time::timeout(Duration::from_secs(5), rejected.wait_for(|n| *n == 2)).await;
            assert!(
                matches!(result, Ok(Ok(_))),
                "two real capacity rejections must arrive"
            );
            drop(held);
        })
    });
    if std::env::var_os("LINGSHU_GATEWAY_CONTROL_URL").is_some() {
        super::super::reconnect_host_tests::interrupt_and_reprove(
            &pool,
            &mut [&mut provider, &mut call],
        )
        .await;
        assert_eq!(
            executions.load(Ordering::SeqCst),
            0,
            "reconnect cannot replay business work"
        );
    }
    let mut run = workflow
        .start(
            crate::workflow::WorkflowStart::message("native fixture"),
            MutationOptions::new(format!("native-{label}-workflow-start")).unwrap(),
        )
        .await
        .unwrap();
    let mut retired = None;
    if let Some(lifecycle) = lifecycle {
        tokio::time::timeout(Duration::from_secs(5), async {
            while executions.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(execution.core.active.load(Ordering::SeqCst), 1);
        if lifecycle == CallLifecycle::WriterRestart {
            let markers = std::path::PathBuf::from(
                std::env::var("LINGSHU_WRITER_RESTART_MARKER_DIR").unwrap(),
            );
            tokio::time::timeout(Duration::from_secs(3), async {
                while injected
                    .as_ref()
                    .unwrap()
                    .accepted_released
                    .load(Ordering::SeqCst)
                    != 1
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            std::fs::write(
                markers.join("accepted"),
                run.handle().root_workflow_instance_id.to_string(),
            )
            .unwrap();
            tokio::time::timeout(Duration::from_secs(38), async {
                while !markers.join("writer-lease-expired").exists() {
                    pool.refresh_authorization().await.unwrap();
                    pool.renew_role_leases(&mut [&mut provider, &mut call])
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            })
            .await
            .expect("original Writer lease must expire without fixture mutation");
            if writer_takeover {
                // Attach the same occurrence through surviving A. Never issue
                // another Start command or execute the accepted Handler again.
                let surviving = ClientBuilder::new(ClientConfig::new(&url).with_retry_limit(0))
                    .service_credential(ServiceCredential::new(&app, &key).unwrap())
                    .connect()
                    .unwrap();
                workflow = surviving.workflows().select(WorkflowId(id)).unwrap();
                run = workflow.attach(run.handle().clone(), None).unwrap();
            }
            assert_eq!(execution.core.active.load(Ordering::SeqCst), 1);
            assert_eq!(executions.load(Ordering::SeqCst), 1);
            handler_gate.add_permits(1);
        } else if matches!(
            lifecycle,
            CallLifecycle::ProductDrain | CallLifecycle::ProductStop
        ) {
            let markers =
                std::path::PathBuf::from(std::env::var("LINGSHU_DRAIN_MARKER_DIR").unwrap());
            if asynchronous {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while injected
                        .as_ref()
                        .unwrap()
                        .accepted_released
                        .load(Ordering::SeqCst)
                        != 1
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            std::fs::write(markers.join("accepted"), "one original handler").unwrap();
            tokio::time::timeout(Duration::from_secs(12), async {
                while !markers.join("draining").exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            // HTTP has exited. The original Session and report capability are
            // the only return channel, including the deliberately lost ACK.
            handler_gate.add_permits(1);
            if lifecycle == CallLifecycle::ProductStop {
                // The harness has observed final Host cleanup before releasing
                // this original Handler. A locally computed result is not a
                // business checkpoint acknowledgement.
                tokio::time::timeout(Duration::from_secs(12), execution.core.wait_idle())
                    .await
                    .unwrap();
                assert!(execution.join_accepted().await);
                let injected = injected.as_ref().unwrap();
                assert_eq!(executions.load(Ordering::SeqCst), 1);
                assert_eq!(injected.invocations.load(Ordering::SeqCst), 1);
                assert_eq!(injected.recorded.load(Ordering::SeqCst), 0);
                assert_eq!(injected.duplicates.load(Ordering::SeqCst), 0);
                std::fs::write(
                    markers.join("completed"),
                    "no durable ACK after final stopping",
                )
                .unwrap();
                let _ = call.close().await;
                let _ = provider.close().await;
                let _ = pool.close().await;
                connection.shutdown().await;
                return;
            }
            if let Some(injected) = &injected {
                injected.wait_settled().await;
                assert!(execution.join_accepted().await);
                assert_eq!(injected.invocations.load(Ordering::SeqCst), 1);
                assert_eq!(injected.recorded.load(Ordering::SeqCst), 1);
                assert_eq!(injected.completions.load(Ordering::SeqCst), 2);
                assert_eq!(injected.duplicates.load(Ordering::SeqCst), 1);
            } else {
                // The script observes the authoritative terminal SQL result.
                // Keep the original Session alive until its Reply has committed.
                tokio::time::timeout(Duration::from_secs(10), async {
                    while !markers.join("persisted").exists() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .expect("original Sync reply must persist during drain");
            }
            assert_eq!(executions.load(Ordering::SeqCst), 1);
            std::fs::write(markers.join("completed"), "same durable native result").unwrap();
            call.close().await.unwrap();
            provider.close().await.unwrap();
            pool.close().await.unwrap();
            connection.shutdown().await;
            return;
        }
        if lifecycle == CallLifecycle::ManagedRotation {
            let full = pool.session_config().session_count() > 2;
            let count = pool.session_config().session_count();
            let mut managed = pool
                .manage_roles_with_rotation(
                    vec![call, provider],
                    super::super::ChannelCertificateRotationConfig::new(Duration::from_secs(20))
                        .unwrap(),
                )
                .unwrap();
            let mut rotation = managed.subscribe_rotation();
            if full {
                tokio::time::sleep(Duration::from_secs(22)).await;
                assert_eq!(managed.rotation_status().completed, 0);
                assert!(execution.pending_reports());
                assert_eq!(
                    connection.channel_session_budget().available_permits(),
                    4 - count
                );
                handler_gate.add_permits(1);
                assert!(matches!(
                    run.wait(
                        RequestOptions::new()
                            .with_deadline(chrono::Utc::now() + chrono::Duration::seconds(10))
                    )
                    .await
                    .unwrap(),
                    WorkflowRunResult::Completed { .. }
                ));
                tokio::time::timeout(Duration::from_secs(5), execution.wait_idle())
                    .await
                    .unwrap();
                assert!(!execution.pending_reports());
            }
            tokio::time::timeout(Duration::from_secs(35), async {
                loop {
                    let status = rotation.borrow_and_update().clone();
                    assert_eq!(status.last_error, None, "{status:?}");
                    if status.completed == 1 {
                        break;
                    }
                    rotation.changed().await.unwrap();
                }
            })
            .await
            .unwrap();
            if !full {
                assert_eq!(execution.core.active.load(Ordering::SeqCst), 1);
                assert!(execution.pending_reports());
                assert_eq!(
                    connection.channel_session_budget().available_permits(),
                    4 - 2 * count,
                    "both configured pools remain owned during report drain"
                );
                handler_gate.add_permits(1);
            }
            assert!(matches!(
                run.wait(
                    RequestOptions::new()
                        .with_deadline(chrono::Utc::now() + chrono::Duration::seconds(10))
                )
                .await
                .unwrap(),
                WorkflowRunResult::Completed { .. }
            ));
            let second = workflow
                .start(
                    crate::workflow::WorkflowStart::message("after managed certificate handoff"),
                    MutationOptions::new(format!("native-{label}-workflow-new-certificate"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(matches!(
                second
                    .wait(
                        RequestOptions::new()
                            .with_deadline(chrono::Utc::now() + chrono::Duration::seconds(10))
                    )
                    .await
                    .unwrap(),
                WorkflowRunResult::Completed { .. }
            ));
            assert_eq!(executions.load(Ordering::SeqCst), 2);
            managed.close().await.unwrap();
            assert_eq!(connection.channel_session_budget().available_permits(), 4);
            connection.shutdown().await;
            return;
        }
        if lifecycle == CallLifecycle::WriterRestart {
            // Keep original role, report authority and result; generic completion
            // checks below read the restored Writer's durable checkpoint.
        } else if lifecycle == CallLifecycle::RoleExpiry {
            let expires = call.lease_window.borrow().expires_at_ms;
            // Renew only Provider/common base and physical control. Call must
            // actually expire in platform Redis, while its original attempt is
            // kept live by signed independent heartbeats.
            while chrono::Utc::now().timestamp_millis() <= expires + 100 {
                pool.refresh_authorization().await.unwrap();
                pool.renew_role_leases(&mut [&mut provider]).await.unwrap();
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            assert!(!call.route_confirmed());
            assert_eq!(execution.core.active.load(Ordering::SeqCst), 1);
        } else {
            let identity = pool.prepare_certificate_rotation().await.unwrap();
            let mut candidate = identity
                .open_sessions(super::super::ChannelSessionConfig::host_test())
                .await
                .unwrap();
            candidate.adopt_role(&mut provider).await.unwrap();
            candidate.adopt_role(&mut call).await.unwrap();
            candidate.finalize_certificate_rotation().await.unwrap();
            assert_eq!(execution.core.active.load(Ordering::SeqCst), 1);
            assert!(execution.pending_reports());
            let current = call.calls.lock().unwrap().clone().unwrap();
            assert!(
                Arc::ptr_eq(&current.core, &execution.core),
                "handoff keeps the execution/attempt/budget owner"
            );
            pool.retain_reports_until(execution.report_deadline())
                .unwrap();
            assert!(
                pool.refresh_authorization().await.is_err(),
                "retired report pool cannot refresh control"
            );
            retired = Some(std::mem::replace(&mut pool, candidate));
        }
        handler_gate.add_permits(1);
    }
    let result = run
        .wait(
            RequestOptions::new().with_deadline(chrono::Utc::now() + chrono::Duration::seconds(15)),
        )
        .await
        .unwrap();
    assert!(
        matches!(result, WorkflowRunResult::Completed { .. }),
        "{result:?}"
    );
    if let Some(release_capacity) = release_capacity {
        release_capacity.await.unwrap();
    }
    assert_eq!(
        executions.load(Ordering::SeqCst),
        expected_executions as usize
    );
    // A terminal runner may already have released its transient event buffer.
    // Reattaching and reading the durable snapshot must not execute a command.
    let attached = workflow.attach(run.handle().clone(), None).unwrap();
    // The completion notification precedes the runner's durable state write.
    // Observe that write within a finite fixture budget; never start another run.
    let replay = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = attached.snapshot(RequestOptions::new()).await.unwrap();
            if snapshot.state == kish_lingshu_runtime_contract::WorkflowState::Completed {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("completed Workflow must persist its terminal result");
    let WorkflowRunResult::Completed { output, .. } = result else {
        unreachable!()
    };
    assert_eq!(replay.output, Some(output));
    assert_eq!(
        executions.load(Ordering::SeqCst),
        expected_executions as usize
    );
    if let Some(injected) = &injected {
        injected.wait_settled().await;
        assert!(execution.join_accepted().await);
        assert_eq!(
            injected.invocations.load(Ordering::SeqCst),
            if injected.mode == faults::FaultMode::CapacityRejected {
                3
            } else {
                expected_executions as usize
            },
            "only authenticated pre-acceptance rejection permits another submission"
        );
        assert_eq!(injected.accepted.load(Ordering::SeqCst), 1);
        assert_eq!(
            injected.accepted_released.load(Ordering::SeqCst),
            usize::from(injected.mode != faults::FaultMode::LoseAccepted),
            "early completion must actually release Accepted after the durable ACK"
        );
        if injected.mode == faults::FaultMode::ObserveCheckpointFailure {
            assert!(injected.unavailable.load(Ordering::SeqCst) >= 1);
            assert!(injected.completions.load(Ordering::SeqCst) >= 2);
            assert_eq!(
                injected.recorded.load(Ordering::SeqCst)
                    + injected.duplicates.load(Ordering::SeqCst),
                1,
                "only a successful original checkpoint or its durable duplicate settles reporting"
            );
        } else {
            assert_eq!(
                injected.recorded.load(Ordering::SeqCst),
                1,
                "one original durable result"
            );
        }
        if injected.mode == faults::FaultMode::LoseCompletionAck {
            assert_eq!(injected.completions.load(Ordering::SeqCst), 2);
            assert_eq!(
                injected.duplicates.load(Ordering::SeqCst),
                1,
                "retry reads the original durable receipt"
            );
        } else if injected.mode != faults::FaultMode::ObserveCheckpointFailure {
            assert_eq!(injected.completions.load(Ordering::SeqCst), 1);
            assert_eq!(injected.duplicates.load(Ordering::SeqCst), 0);
        }
        let requests = injected.requests.lock().unwrap();
        for request in requests.iter() {
            let first = requests
                .iter()
                .find(|original| {
                    original.context.idempotency_key == request.context.idempotency_key
                })
                .unwrap();
            assert_eq!(request.input, first.input);
            assert_eq!(request.operation, first.operation);
            assert_eq!(
                request.context.idempotency_key,
                first.context.idempotency_key
            );
            let InvocationRole::Call(call) = &request.context.invocation else {
                panic!()
            };
            let InvocationRole::Call(original) = &first.context.invocation else {
                panic!()
            };
            assert_eq!(call.call_id, original.call_id);
            assert_eq!(
                call.attempt, 1,
                "submission retries cannot consume business attempts"
            );
            assert_eq!(call.workflow, original.workflow);
        }
        let rejected = *injected.rejected.borrow();
        assert_eq!(
            rejected,
            if injected.mode == faults::FaultMode::CapacityRejected {
                2
            } else {
                0
            }
        );
        if rejected > 0 {
            let lane_hits = execution.received_lanes[..pool.lane_count()]
                .iter()
                .filter(|hits| hits.load(Ordering::SeqCst) > 0)
                .count();
            assert_eq!(
                lane_hits,
                pool.lane_count().min(3),
                "different physical lanes must observe the same occupied execution budget"
            );
            assert_eq!(
                budget.semaphore.available_permits(),
                1,
                "lane count cannot multiply the logical business capacity"
            );
            let epochs: std::collections::BTreeSet<_> = requests
                .iter()
                .map(|r| {
                    r.completion
                        .as_ref()
                        .unwrap()
                        .heartbeat
                        .as_ref()
                        .unwrap()
                        .epoch
                        .clone()
                })
                .collect();
            assert_eq!(
                epochs.len(),
                3,
                "each rejected submission fences its old completion authority"
            );
        }
        assert_eq!(
            executions.load(Ordering::SeqCst),
            expected_executions as usize
        );
    }
    if lifecycle == Some(CallLifecycle::CertificateRotation) {
        let second = workflow
            .start(
                crate::workflow::WorkflowStart::message("after certificate handoff"),
                MutationOptions::new(format!("native-{label}-workflow-new-certificate")).unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            second
                .wait(
                    RequestOptions::new()
                        .with_deadline(chrono::Utc::now() + chrono::Duration::seconds(10))
                )
                .await
                .unwrap(),
            WorkflowRunResult::Completed { .. }
        ));
        assert_eq!(
            executions.load(Ordering::SeqCst),
            2,
            "new call uses adopted route and new CSR"
        );
    }
    if let Ok(markers) = std::env::var("LINGSHU_NETWORK_MARKER_DIR") {
        assert!(!asynchronous && lifecycle.is_none() && fault.is_none());
        let markers = std::path::PathBuf::from(markers);
        let original_call = call.registration().clone();
        let original_provider = provider.registration().clone();
        async fn marker(directory: &std::path::Path, name: &str) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !directory.join(name).exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("network fixture coordination deadline");
        }
        let router_reselection =
            std::env::var("LINGSHU_NETWORK_ROUTER_RESELECTION").as_deref() == Ok("true");
        let links = if router_reselection {
            &["sdk"][..]
        } else {
            &["sdk", "peer"][..]
        };
        let original_sessions = pool.session_ids();
        let original_router = pool.sessions[0].info().routers_zid().await.next().unwrap();
        for (index, link) in links.iter().enumerate() {
            std::fs::write(markers.join(format!("{link}-ready")), "original roles live").unwrap();
            marker(&markers, &format!("{link}-partitioned")).await;
            let unavailable = workflow
                .start(
                    crate::workflow::WorkflowStart::message("partition"),
                    MutationOptions::new(format!("native-network-{link}-partition")).unwrap(),
                )
                .await
                .unwrap();
            assert!(matches!(
                unavailable
                    .wait(
                        RequestOptions::new()
                            .with_deadline(chrono::Utc::now() + chrono::Duration::seconds(10))
                    )
                    .await
                    .unwrap(),
                WorkflowRunResult::Failed { .. }
            ));
            assert_eq!(
                executions.load(Ordering::SeqCst),
                index + 1,
                "partition never submits a business Handler"
            );
            std::fs::write(
                markers.join(format!("{link}-blocked")),
                "no business execution",
            )
            .unwrap();
            marker(&markers, &format!("{link}-restored")).await;
            tokio::time::timeout(Duration::from_secs(8), async {
                while pool.connected_lanes().await != pool.lane_count()
                    || pool
                        .subscribe_connectivity()
                        .borrow()
                        .connected_data_lanes
                        .iter()
                        .any(|v| !*v)
                {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("original SDK Session did not physically reconnect");
            assert_eq!(pool.session_ids(), original_sessions);
            if router_reselection {
                assert_ne!(
                    pool.sessions[0].info().routers_zid().await.next().unwrap(),
                    original_router
                );
                assert!(!call.route_confirmed() && !provider.route_confirmed());
            }
            eprintln!("network {link}: original physical Session reconnected");
            if router_reselection
                && std::env::var("LINGSHU_NETWORK_TRUSTED_TRANSFER").as_deref() != Ok("true")
            {
                // Current /2 candidate imports business routing observations,
                // but not an original SDK's ingress control grant on Router B.
                // Keep this negative as a production gate until trusted channel
                // admission is implemented; physical relocation is not readiness.
                assert_eq!(
                    pool.refresh_authorization().await.unwrap_err(),
                    ChannelSessionError::Transport
                );
                assert!(!call.route_confirmed() && !provider.route_confirmed());
                assert_eq!(executions.load(Ordering::SeqCst), 1);
                assert_eq!(
                    serde_json::to_value(call.registration()).unwrap(),
                    serde_json::to_value(&original_call).unwrap()
                );
                assert_eq!(
                    serde_json::to_value(provider.registration()).unwrap(),
                    serde_json::to_value(&original_provider).unwrap()
                );
                call.close().await.unwrap();
                provider.close().await.unwrap();
                pool.close().await.unwrap();
                connection.ensure_open().unwrap();
                connection.shutdown().await;
                std::fs::write(markers.join(format!("{link}-completed")), "Router B rejects missing original ingress grant; no reenrollment or Handler replay").unwrap();
                return;
            }
            let current = pool.refresh_authorization().await.unwrap();
            assert_eq!(
                current.instance,
                pool.identity().bootstrap_response().instance
            );
            pool.confirm_role_route(&mut call).await.unwrap();
            pool.confirm_role_route(&mut provider).await.unwrap();
            assert!(call.route_confirmed() && provider.route_confirmed());
            if router_reselection
                && std::env::var("LINGSHU_NETWORK_TRUSTED_TRANSFER").as_deref() == Ok("true")
            {
                // Stay on B beyond the original grant window. Only source A's
                // fresh finite authority can renew it; relocation grants no lease.
                for _ in 0..3 {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    pool.refresh_authorization().await.unwrap();
                    let renewed = pool
                        .renew_role_leases(&mut [&mut call, &mut provider])
                        .await
                        .unwrap();
                    assert!(renewed.iter().all(|result| result.status == 200));
                    pool.confirm_role_route(&mut call).await.unwrap();
                    pool.confirm_role_route(&mut provider).await.unwrap();
                    assert_eq!(pool.session_ids(), original_sessions);
                    assert_eq!(executions.load(Ordering::SeqCst), 1);
                }
            }
            assert_eq!(
                serde_json::to_value(call.registration()).unwrap(),
                serde_json::to_value(&original_call).unwrap()
            );
            assert_eq!(
                serde_json::to_value(provider.registration()).unwrap(),
                serde_json::to_value(&original_provider).unwrap()
            );
            let restored = workflow
                .start(
                    crate::workflow::WorkflowStart::message("after native reconnect"),
                    MutationOptions::new(format!("native-network-{link}-restored")).unwrap(),
                )
                .await
                .unwrap();
            assert!(matches!(
                restored
                    .wait(
                        RequestOptions::new()
                            .with_deadline(chrono::Utc::now() + chrono::Duration::seconds(10))
                    )
                    .await
                    .unwrap(),
                WorkflowRunResult::Completed { .. }
            ));
            assert_eq!(
                executions.load(Ordering::SeqCst),
                index + 2,
                "reconnect itself cannot replay Handler work"
            );
            if router_reselection {
                assert_eq!(
                    original_sessions
                        .iter()
                        .collect::<std::collections::HashSet<_>>()
                        .len(),
                    original_sessions.len()
                );
                for session in &pool.sessions {
                    assert!(session
                        .info()
                        .routers_zid()
                        .await
                        .any(|id| id != original_router));
                }
                std::fs::write(
                    markers.join("router-transfer-facts.json"),
                    serde_json::to_vec(&serde_json::json!({
                        "physical_sessions":original_sessions.len(), "data_lanes":pool.lane_count(),
                        "source_renewal_rounds":3, "new_workflow_executions":1,
                    }))
                    .unwrap(),
                )
                .unwrap();
            }
            std::fs::write(
                markers.join(format!("{link}-completed")),
                "same original roles and durable result",
            )
            .unwrap();
        }
    }
    if std::env::var("LINGSHU_VERIFY_DATA_LANES").as_deref() == Ok("true")
        && fault.is_none()
        && lifecycle.is_none()
        && !parallel_owner
    {
        let before_lane_calls = executions.load(Ordering::SeqCst);
        for index in 0..pool.lane_count() {
            let repeated = workflow
                .start(
                    crate::workflow::WorkflowStart::message("another lane"),
                    MutationOptions::new(format!("native-{label}-data-lane-{index}")).unwrap(),
                )
                .await
                .unwrap();
            assert!(matches!(
                repeated
                    .wait(
                        RequestOptions::new()
                            .with_deadline(chrono::Utc::now() + chrono::Duration::seconds(10))
                    )
                    .await
                    .unwrap(),
                WorkflowRunResult::Completed { .. }
            ));
        }
        assert!(
            execution.received_lanes[..pool.lane_count()]
                .iter()
                .all(|n| n.load(Ordering::SeqCst) > 0),
            "each authorized physical lane must execute a real business invocation"
        );
        assert!(execution.received_lanes[pool.lane_count()..]
            .iter()
            .all(|n| n.load(Ordering::SeqCst) == 0));
        assert_eq!(
            executions.load(Ordering::SeqCst),
            before_lane_calls + pool.lane_count()
        );
        eprintln!("PASS {} physical lanes execute distinct Workflow calls through one shared execution owner", pool.lane_count());
    }
    if std::env::var("LINGSHU_VERIFY_DYNAMIC_ROLES").as_deref() == Ok("true")
        && fault.is_none()
        && lifecycle.is_none()
        && !parallel_owner
    {
        let generation = call
            .lifecycle_status(pool.authorization_deadline())
            .role_generation;
        let before = executions.load(Ordering::SeqCst);
        let mut managed = pool.manage_roles(vec![call, provider]).unwrap();
        let removed = managed
            .deregister_role(RouteIdentity::new(generation.clone()).unwrap())
            .await
            .unwrap();
        assert!(removed.remote_deregistered);
        assert_eq!(managed.role_statuses().len(), 1);
        assert!(managed.role_statuses()[0].route_confirmed());
        let added = managed
            .enroll_service(
                format!("{call_node}-dynamic"),
                registry,
                budget.clone(),
                asynchronous,
            )
            .await
            .unwrap();
        assert_ne!(added.role_generation, generation);
        assert!(added.route_confirmed());
        let new_call = workflow
            .start(
                crate::workflow::WorkflowStart::message("dynamic service"),
                MutationOptions::new(format!("native-{label}-dynamic-role")).unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            new_call
                .wait(
                    RequestOptions::new()
                        .with_deadline(chrono::Utc::now() + chrono::Duration::seconds(10))
                )
                .await
                .unwrap(),
            WorkflowRunResult::Completed { .. }
        ));
        assert_eq!(executions.load(Ordering::SeqCst), before + 1);
        managed.close().await.unwrap();
        assert_eq!(
            budget.semaphore.available_permits(),
            expected_executions as usize
        );
        connection.shutdown().await;
        eprintln!("PASS explicit dynamic service removal/addition keeps Provider and the shared execution budget; same Workflow business path");
        return;
    }
    if lifecycle != Some(CallLifecycle::RoleExpiry) {
        pool.deregister_role(&mut call).await.unwrap();
    }
    pool.deregister_role(&mut provider).await.unwrap();
    call.close().await.unwrap();
    provider.close().await.unwrap();
    pool.close().await.unwrap();
    if let Some(retired) = retired.as_mut() {
        retired.close().await.unwrap();
    }
    connection.shutdown().await;
}
