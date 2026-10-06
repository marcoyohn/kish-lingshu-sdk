//! Actual scale-out keeps old Sessions and uses both independently budgeted SDKs.
use super::*;
use crate::services::ServiceRegistryBuilder;
use crate::{ClientBuilder, ClientConfig, MutationOptions, RequestOptions, ServiceCredential};
use kish_lingshu_runtime_contract::{provider::*, service::*, WorkflowId, WorkflowRunResult};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct Active(Arc<AtomicUsize>);
async fn admin<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> T {
    let value: serde_json::Value = response.error_for_status().unwrap().json().await.unwrap();
    assert_eq!(value["status"], true, "{value:?}");
    serde_json::from_value(value["data"].clone()).unwrap()
}
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
fn registry(
    manifest: ServiceManifest,
    expanded: Arc<AtomicBool>,
    global_active: Arc<AtomicUsize>,
    global_peak: Arc<AtomicUsize>,
    executed: Arc<std::sync::Mutex<std::collections::BTreeSet<(String, u32)>>>,
    count: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
) -> Arc<crate::services::ServiceRegistry> {
    let mut builder = ServiceRegistryBuilder::new(manifest).unwrap();
    builder.bind::<serde_json::Value, serde_json::Value, _, _>("scale-fixture", "echo", "1", move |context,input| {
        assert!(matches!(&context.invocation, InvocationRole::Call(c) if c.mode == CallMode::Sync && c.workflow.is_some()));
        let InvocationRole::Call(call) = &context.invocation else { unreachable!() };
        assert!(executed.lock().unwrap().insert((call.call_id.clone(),call.attempt)), "one attempt cannot run twice after expansion");
        count.fetch_add(1, Ordering::SeqCst);
        peak.fetch_max(active.fetch_add(1, Ordering::SeqCst)+1, Ordering::SeqCst);
        let guard = Active(active.clone());
        global_peak.fetch_max(global_active.fetch_add(1,Ordering::SeqCst)+1,Ordering::SeqCst);
        let global_guard=Active(global_active.clone());
        let hold = expanded.load(Ordering::SeqCst);
        async move {
            let _guard = guard;
            let _global_guard=global_guard;
            if hold {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(input)
        }
    }).unwrap();
    Arc::new(builder.build().unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real matched Kubernetes scale-out; run zenss_kubernetes_component.py --scale-test"]
async fn native_scale_out_retains_old_sessions_and_distributes_calls_with_shared_limits() {
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let url_a = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    let url_b = std::env::var("LINGSHU_CHANNEL_TEST_DISPATCH_URL").unwrap();
    let key = std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap();
    let token = std::env::var("LINGSHU_CHANNEL_TEST_PREVIEW_TOKEN").unwrap();
    let markers = std::path::PathBuf::from(std::env::var("LINGSHU_SCALE_MARKERS").unwrap());
    let connection_a =
        ServiceConnection::connect(&url_a, ServiceCredential::new(&app, &key).unwrap())
            .await
            .unwrap();
    let identity = connection_a
        .bootstrap_channel(
            ServiceInstanceRegistration {
                instance_id: "scale-a".into(),
                incarnation_id: "one-scale-fixture".into(),
                generation: None,
            },
            None,
        )
        .await
        .unwrap();
    let mut pool_a = identity
        .open_sessions(super::super::ChannelSessionConfig::host_test())
        .await
        .unwrap();
    let old_sessions = pool_a.session_ids();
    let old_router = pool_a.sessions[0]
        .info()
        .routers_zid()
        .await
        .next()
        .unwrap();
    let mut manifest: ServiceManifest =
        serde_json::from_str(&std::env::var("LINGSHU_CHANNEL_TEST_MANIFEST").unwrap()).unwrap();
    manifest.services[0].service_key = "scale-fixture".into();
    let binding = manifest.services[0].operations[0].call.as_mut().unwrap();
    binding.maximum_concurrency = 8;
    binding.timeout_ms = 10_000;
    binding.modes = [CallMode::Sync].into_iter().collect();
    let reference = manifest.services[0].operations[0]
        .reference("scale-fixture")
        .unwrap();
    let catalog=ProviderCatalog {
        format_version:1,application_id:app.clone(),provider_key:"scale-source".into(),release:"1".into(),
        services:Some(manifest.clone()),events:None,workflows:vec![ProviderWorkflow {
            key:"scale".into(),name:"Native Kubernetes scale Workflow".into(),description:None,
            define_schema:serde_json::from_value(serde_json::json!({"type":"ReactFlow","config":{"reactflow":{
                "nodes":[
                    {"id":"start","type":"startEvent","data":{"id":"start","type":"startEvent","name":"Start","trigger":{"type":"manual"}}},
                    {"id":"invoke","type":"serviceTask","data":{"id":"invoke","type":"serviceTask","name":"Echo","operation":reference,"mode":"sync",
                        "input":{"ScriptExpr":"#{case_id: 42}"},"idempotency_key":{"LiteralString":"scale-effect"},"deadline_ms":10_000,"maximum_attempts":1,"remote_retry":{"maximum_attempts":1}}},
                    {"id":"end","type":"endEvent","data":{"id":"end","type":"endEvent","name":"End"}}
                ],"edges":[{"id":"a","source":"start","target":"invoke"},{"id":"b","source":"invoke","target":"end"}]
            }}})).unwrap(),
        }],
    };
    let mut provider = pool_a.register_provider_role(&catalog).await.unwrap();
    let ChannelRoleEnrollmentResponse::Provider(response) = provider.registration() else {
        unreachable!()
    };
    let http = reqwest::Client::new();
    let post = |action: &str| {
        http.post(format!(
            "{url_a}/api/admin/apps/{app}/providers/{}/{action}",
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
    let id = receipt.items["workflow:scale"]["workflow_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let expanded = Arc::new(AtomicBool::new(false));
    let global_active = Arc::new(AtomicUsize::new(0));
    let global_peak = Arc::new(AtomicUsize::new(0));
    let executed = Arc::new(std::sync::Mutex::new(std::collections::BTreeSet::new()));
    let count_a = Arc::new(AtomicUsize::new(0));
    let count_b = Arc::new(AtomicUsize::new(0));
    let active_a = Arc::new(AtomicUsize::new(0));
    let active_b = Arc::new(AtomicUsize::new(0));
    let peak_a = Arc::new(AtomicUsize::new(0));
    let peak_b = Arc::new(AtomicUsize::new(0));
    let registry_a = registry(
        manifest.clone(),
        expanded.clone(),
        global_active.clone(),
        global_peak.clone(),
        executed.clone(),
        count_a.clone(),
        active_a.clone(),
        peak_a.clone(),
    );
    let mut role_a = pool_a
        .register_service_role("scale-call-a", 8, registry_a.as_ref())
        .await
        .unwrap();
    pool_a
        .enable_sync_calls(
            &mut role_a,
            registry_a,
            crate::ServiceExecutionBudget::new(8).unwrap(),
        )
        .unwrap();
    let sdk_a = ClientBuilder::new(ClientConfig::new(&url_a).with_retry_limit(0))
        .service_credential(ServiceCredential::new(&app, &key).unwrap())
        .connect()
        .unwrap();
    let mut workflow = sdk_a.workflows().select(WorkflowId(id)).unwrap();
    let mut second = None;
    let mut after_elapsed = 0.0;
    let mut roots = Vec::new();
    for (phase, total, concurrency) in [("before", 4, 1), ("after", 32, 8)] {
        if phase == "after" {
            assert_eq!(count_a.load(Ordering::SeqCst), 4);
            std::fs::write(
                markers.join("scale-request"),
                "old Session and four durable calls already live",
            )
            .unwrap();
            tokio::time::timeout(Duration::from_secs(40), async {
                while !markers.join("scaled-ready").exists() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            pool_a.refresh_authorization().await.unwrap();
            assert!(pool_a
                .renew_role_leases(&mut [&mut provider, &mut role_a])
                .await
                .unwrap()
                .iter()
                .all(|r| r.status == 200));
            pool_a.confirm_role_route(&mut provider).await.unwrap();
            pool_a.confirm_role_route(&mut role_a).await.unwrap();
            let connection_b =
                ServiceConnection::connect(&url_b, ServiceCredential::new(&app, &key).unwrap())
                    .await
                    .unwrap();
            let identity = connection_b
                .bootstrap_channel(
                    ServiceInstanceRegistration {
                        instance_id: "scale-b".into(),
                        incarnation_id: "one-scale-fixture".into(),
                        generation: None,
                    },
                    None,
                )
                .await
                .unwrap();
            let mut pool_b = identity
                .open_sessions(super::super::ChannelSessionConfig::host_test())
                .await
                .unwrap();
            assert_ne!(
                old_router,
                pool_b.sessions[0]
                    .info()
                    .routers_zid()
                    .await
                    .next()
                    .unwrap()
            );
            let registry_b = registry(
                manifest.clone(),
                expanded.clone(),
                global_active.clone(),
                global_peak.clone(),
                executed.clone(),
                count_b.clone(),
                active_b.clone(),
                peak_b.clone(),
            );
            let mut role_b = pool_b
                .register_service_role("scale-call-b", 8, registry_b.as_ref())
                .await
                .unwrap();
            pool_b
                .enable_sync_calls(
                    &mut role_b,
                    registry_b,
                    crate::ServiceExecutionBudget::new(8).unwrap(),
                )
                .unwrap();
            let sdk_b = ClientBuilder::new(ClientConfig::new(&url_b).with_retry_limit(0))
                .service_credential(ServiceCredential::new(&app, &key).unwrap())
                .connect()
                .unwrap();
            workflow = sdk_b.workflows().select(WorkflowId(id)).unwrap();
            second = Some((connection_b, pool_b, role_b));
            expanded.store(true, Ordering::SeqCst);
        }
        let start = tokio::time::Instant::now();
        for first in (0..total).step_by(concurrency) {
            let mut jobs = tokio::task::JoinSet::new();
            for index in first..first + concurrency {
                let workflow = workflow.clone();
                jobs.spawn(async move {
                    let run = workflow
                        .start(
                            crate::workflow::WorkflowStart::message("scale"),
                            MutationOptions::new(format!("scale-{phase}-{index}")).unwrap(),
                        )
                        .await
                        .unwrap();
                    let result = run
                        .wait(
                            RequestOptions::new()
                                .with_deadline(chrono::Utc::now() + chrono::Duration::seconds(15)),
                        )
                        .await
                        .unwrap();
                    assert!(
                        matches!(result, WorkflowRunResult::Completed { .. }),
                        "{phase}/{index}: {result:?}"
                    );
                    let attached = workflow.attach(run.handle().clone(), None).unwrap();
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while attached
                            .snapshot(RequestOptions::new())
                            .await
                            .unwrap()
                            .state
                            != kish_lingshu_runtime_contract::WorkflowState::Completed
                        {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    })
                    .await
                    .unwrap();
                    run.handle().workflow_instance_id.0.to_string()
                });
            }
            while let Some(job) = jobs.join_next().await {
                roots.push(job.unwrap());
            }
        }
        if phase == "after" {
            after_elapsed = start.elapsed().as_secs_f64();
        }
    }
    assert_eq!(
        count_a.load(Ordering::SeqCst) + count_b.load(Ordering::SeqCst),
        36
    );
    assert!(count_a.load(Ordering::SeqCst) > 4 && count_b.load(Ordering::SeqCst) > 0);
    assert!(peak_a.load(Ordering::SeqCst) <= 8 && peak_b.load(Ordering::SeqCst) <= 8);
    assert!(global_peak.load(Ordering::SeqCst) <= 8);
    assert_eq!(executed.lock().unwrap().len(), 36);
    assert_eq!(
        active_a.load(Ordering::SeqCst) + active_b.load(Ordering::SeqCst),
        0
    );
    assert_eq!(pool_a.session_ids(), old_sessions);
    assert_eq!(
        pool_a.sessions[0]
            .info()
            .routers_zid()
            .await
            .next()
            .unwrap(),
        old_router
    );
    std::fs::write(markers.join("scale-facts.json"),serde_json::json!({
        "before_calls":4,"after_calls":32,"old_instance_after_calls":count_a.load(Ordering::SeqCst)-4,"new_instance_after_calls":count_b.load(Ordering::SeqCst),
        "old_sessions":old_sessions,"old_sessions_preserved":true,"old_router_preserved":true,
        "new_sessions":second.as_ref().unwrap().1.session_ids(),"new_router_distinct":true,
        "per_instance_peak_in_flight":[peak_a.load(Ordering::SeqCst),peak_b.load(Ordering::SeqCst)],
        "aggregate_peak_in_flight":global_peak.load(Ordering::SeqCst),"per_instance_shared_execution_capacity":8,
        "per_instance_operation_cap":8,"workload_concurrency":8,"total_configured_sdk_capacity":16,"after_elapsed_seconds":after_elapsed,
        "selection":"existing rendezvous hash; distribution need not be exactly equal",
        "after_workflows_per_second":32.0/after_elapsed,"durable_occurrences":roots,"handler_reexecutions":0,
        "new_connection_allocation":"explicit bootstrap to added Router; existing connections stay on old Router",
        "automatic_connection_rebalance":false,"scale_to_zero":false,
    }).to_string()).unwrap();
    let (connection_b, mut pool_b, mut role_b) = second.unwrap();
    pool_b.deregister_role(&mut role_b).await.unwrap();
    role_b.close().await.unwrap();
    pool_b.close().await.unwrap();
    connection_b.shutdown().await;
    pool_a.deregister_role(&mut role_a).await.unwrap();
    role_a.close().await.unwrap();
    pool_a.deregister_role(&mut provider).await.unwrap();
    provider.close().await.unwrap();
    pool_a.close().await.unwrap();
    connection_a.shutdown().await;
}
