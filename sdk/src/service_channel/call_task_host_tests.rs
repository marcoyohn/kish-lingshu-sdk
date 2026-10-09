//! Licensed host acceptance: public typed Handler, native Service transport and
//! durable User Task business state. Interactive user actions still use HTTP.
use super::*;
use crate::{
    ClientBuilder, ClientConfig, MutationOptions, RequestOptions, ServiceCredential, UserCredential,
};
use kish_lingshu_runtime_contract::{provider::*, ApplicationTaskQuery, UserTaskState, WorkflowId};
use std::{collections::BTreeMap, sync::Mutex};

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct Submission {
    behavior: String,
}
struct TaskProvider {
    actor: String,
    calls: Mutex<BTreeMap<String, usize>>,
}
#[crate::lingshu_service(key = "native-task-completion")]
impl TaskProvider {
    #[user_task_completion_handler(operation="complete",version="1",task_type="native.review",modes=["sync","async"],idempotent=true)]
    async fn complete(
        &self,
        context: crate::user_task::completion::CompletionContext,
        input: Submission,
    ) -> crate::user_task::completion::CompletionResult<serde_json::Value> {
        use crate::user_task::completion::CompletionError;
        assert_eq!(context.actor_user_id(), self.actor);
        assert_eq!(context.task().task_type, "native.review");
        assert_eq!(context.invocation_id(), context.idempotency_key());
        let attempt = {
            let mut calls = self.calls.lock().unwrap();
            let count = calls.entry(context.idempotency_key().into()).or_default();
            *count += 1;
            *count
        };
        match input.behavior.as_str() {
            "rejected" => Err(CompletionError::rejected(
                "correct_submission",
                "Please correct the review",
            )),
            "failed" => Err(CompletionError::failed(
                "binding_failed",
                "Review cannot be applied",
            )),
            "retry" if attempt == 1 => Err(CompletionError::retryable_after(
                "busy",
                "Try later",
                Duration::from_millis(100),
            )),
            _ => Ok(serde_json::json!({"reviewed":true})),
        }
    }
}
async fn admin<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> T {
    let value: serde_json::Value = response.error_for_status().unwrap().json().await.unwrap();
    assert_eq!(value["status"], true, "{value:?}");
    serde_json::from_value(value["data"].clone()).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "licensed host native User Task; run acceptance --user-task-test"]
async fn native_user_task_sync_and_async_preserve_four_business_outcomes() {
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    let key = std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap();
    let token = std::env::var("LINGSHU_CHANNEL_TEST_PREVIEW_TOKEN").unwrap();
    let user_token = std::env::var("LINGSHU_CHANNEL_TEST_USER_TOKEN").unwrap();
    let connection = ServiceConnection::connect(&url, ServiceCredential::new(&app, &key).unwrap())
        .await
        .unwrap();
    let identity = connection
        .bootstrap_channel(
            ServiceInstanceRegistration {
                instance_id: "native-user-task".into(),
                incarnation_id: "task-boot".into(),
                generation: None,
            },
            None,
        )
        .await
        .unwrap();
    let mut pool = identity
        .open_sessions(super::super::ChannelSessionConfig::default())
        .await
        .unwrap();
    let implementation = Arc::new(TaskProvider {
        actor: app.clone(),
        calls: Default::default(),
    });
    let manifest = ServiceManifest {
        contract_version: SERVICE_CONTRACT_VERSION,
        application_id: app.clone(),
        services: vec![TaskProvider::lingshu_service_definition()],
    };
    let reference = manifest.services[0].operations[0]
        .reference(&manifest.services[0].service_key)
        .unwrap();
    let mut builder = ServiceRegistryBuilder::new(manifest.clone()).unwrap();
    implementation
        .clone()
        .bind_lingshu_services(&mut builder)
        .unwrap();
    let registry = Arc::new(builder.build().unwrap());
    let mut workflows = Vec::new();
    for mode in [CallMode::Sync, CallMode::Async] {
        for behavior in ["applied", "rejected", "retry", "failed"] {
            let label = format!(
                "{}-{behavior}",
                if mode == CallMode::Sync {
                    "sync"
                } else {
                    "async"
                }
            );
            workflows.push(ProviderWorkflow {
                key: format!("native-user-task-{label}"), name: format!("Native User Task {label}"), description: None,
                define_schema: serde_json::from_value(serde_json::json!({"type":"ReactFlow","config":{"reactflow":{
                    "nodes":[
                        {"id":"start","type":"startEvent","data":{"name":"Start","trigger":{"type":"manual"}}},
                        {"id":"review","type":"userTask","data":{"name":"Review","title":{"LiteralString":"Native review"},"task_type":"native.review","task_mode":"todo","assignee_type":"fixed_users","assignee_user_ids":{"LiteralString":serde_json::to_string(&vec![&app]).unwrap()},"html_type":"inline_html","html_value":{"LiteralString":"Review"},"completion_handler":{"service_key":reference.service_key,"operation_key":reference.operation_key,"version":reference.version,"contract_digest":reference.contract_digest,"mode":mode,"deadline_ms":10000,"maximum_attempts":3}}},
                        {"id":"end","type":"endEvent","data":{"name":"End"}}
                    ],"edges":[{"id":"a","source":"start","target":"review"},{"id":"b","source":"review","target":"end"}]
                }}})).unwrap(),
            });
        }
    }
    let catalog = ProviderCatalog {
        format_version: 1,
        application_id: app.clone(),
        provider_key: "native-user-task-source".into(),
        release: "1".into(),
        services: Some(manifest),
        events: None,
        workflows,
    };
    let mut provider = pool.register_provider_role(&catalog).await.unwrap();
    let ChannelRoleEnrollmentResponse::Provider(enrollment) = provider.registration() else {
        unreachable!()
    };
    let http = reqwest::Client::new();
    let base = format!(
        "{url}/api/admin/apps/{app}/providers/{}",
        catalog.provider_key
    );
    let post = |path: &str| {
        http.post(format!("{base}/{path}"))
            .header("x-token", &token)
            .header("x-kish-app-id", &app)
            .timeout(Duration::from_secs(15))
    };
    let plan: ProviderPlan = admin(
        post("preview")
            .json(&ProviderPreviewRequest {
                instance_id: enrollment.session.instance.instance_id.clone(),
                generation: enrollment.session.generation.clone(),
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
    let mut call = pool
        .register_service_role("native-user-task-call", 1, registry.as_ref())
        .await
        .unwrap();
    pool.enable_async_calls(
        &mut call,
        registry,
        crate::ServiceExecutionBudget::new(1).unwrap(),
    )
    .unwrap();
    let mut managed = pool.manage_roles(vec![call, provider]).unwrap();
    let service = ClientBuilder::new(ClientConfig::new(&url).with_retry_limit(0))
        .service_credential(ServiceCredential::new(&app, &key).unwrap())
        .connect()
        .unwrap();
    let user = ClientBuilder::new(
        ClientConfig::new(&url)
            .with_retry_limit(0)
            .with_selected_application(&app),
    )
    .user_credential(UserCredential::new(user_token).unwrap())
    .connect()
    .unwrap();
    for workflow_spec in &catalog.workflows {
        let id = WorkflowId(
            receipt.items[&format!("workflow:{}", workflow_spec.key)]["workflow_id"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
        );
        let workflow = service.workflows().select(id).unwrap();
        let run = workflow
            .start(
                crate::workflow::WorkflowStart::message("native task fixture"),
                MutationOptions::new(format!("{}-start", workflow_spec.key)).unwrap(),
            )
            .await
            .unwrap();
        let task = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let page = service
                    .user_tasks()
                    .list(
                        ApplicationTaskQuery {
                            workflow_id: Some(id),
                            ..Default::default()
                        },
                        RequestOptions::new(),
                    )
                    .await
                    .unwrap();
                if let Some(task) = page.items.into_iter().next() {
                    break task;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(task.state, UserTaskState::PendingClaimed);
        let handle = user
            .user_tasks()
            .get(task.id, RequestOptions::new())
            .await
            .unwrap();
        let behavior = workflow_spec.key.rsplit('-').next().unwrap();
        let submitted = handle
            .submit_json(
                serde_json::json!({"behavior":behavior}),
                MutationOptions::new(format!("{}-submit", workflow_spec.key)).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(submitted.state, UserTaskState::Completing);
        let expected = match behavior {
            "rejected" => UserTaskState::PendingClaimed,
            "failed" => UserTaskState::CompletionFailed,
            _ => UserTaskState::Completed,
        };
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let task = service
                    .user_tasks()
                    .get(task.id, RequestOptions::new())
                    .await
                    .unwrap();
                if task.summary.state == expected
                    && task.summary.revision.get() > handle.revision().get()
                {
                    break task;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await;
        if result.is_err() {
            let observed = service
                .user_tasks()
                .get(task.id, RequestOptions::new())
                .await
                .unwrap();
            panic!(
                "native User Task {} did not reach {:?}: task={observed:?}, calls={:?}",
                workflow_spec.key,
                expected,
                implementation.calls.lock().unwrap()
            );
        }
        let result = result.unwrap();
        assert_eq!(result.summary.state, expected);
        if expected == UserTaskState::Completed {
            // Task custody can precede the Workflow's terminal write. The
            // transient event buffer may already have been released by the
            // time we observe the task; verify the durable snapshot instead
            // of requiring a late SSE attachment to replay that buffer.
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let snapshot = run.snapshot(RequestOptions::new()).await.unwrap();
                    if snapshot.state == kish_lingshu_runtime_contract::WorkflowState::Completed {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("applied task must persist the completed Workflow");
        } else {
            let snapshot = run.snapshot(RequestOptions::new()).await.unwrap();
            assert_ne!(
                snapshot.state,
                kish_lingshu_runtime_contract::WorkflowState::Completed,
                "successful wire reply cannot finish a rejected/failed task"
            );
        }
        // Same submission response is replayable even after its business outcome;
        // it cannot issue a second Invoke or consume a business attempt.
        let replay = handle
            .submit_json(
                serde_json::json!({"behavior":behavior}),
                MutationOptions::new(format!("{}-submit", workflow_spec.key)).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replay, submitted);
        let conflict = handle
            .submit_json(
                serde_json::json!({"behavior":"changed"}),
                MutationOptions::new(format!("{}-submit", workflow_spec.key)).unwrap(),
            )
            .await
            .unwrap_err();
        assert!(matches!(conflict, crate::Error::Application(error)
            if error.http_status == Some(409)
                && error.problem.code.as_ref() == "conflict"));
        let count = if behavior == "retry" { 2 } else { 1 };
        let calls = implementation.calls.lock().unwrap();
        assert_eq!(
            calls.values().sum::<usize>(),
            catalog
                .workflows
                .iter()
                .take_while(|w| w.key != workflow_spec.key)
                .map(|w| if w.key.ends_with("retry") { 2 } else { 1 })
                .sum::<usize>()
                + count
        );
        assert!(calls.values().all(|n| *n <= 2));
    }
    managed.close().await.unwrap();
    connection.shutdown().await;
}
