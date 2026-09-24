use super::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Deserialize, Serialize, JsonSchema)]
struct Input {
    value: u32,
}
struct Provider(AtomicUsize);
#[crate::lingshu_service(key = "reports")]
impl Provider {
    #[service_call(operation="generate",version="v1",action="report.generate",idempotent=true,modes=["sync","async"])]
    async fn generate(&self, context: ServiceContext, input: Input) -> Result<u32, ServiceError> {
        assert_eq!(context.idempotency_key(), "business-1");
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(input.value + 1)
    }
    #[service_event(
        operation = "consume",
        version = "v1",
        action = "report.consume",
        topic = "reports",
        event_type = "created",
        consumer_group = "projection"
    )]
    async fn consume(&self, _context: ServiceContext, input: Input) -> Result<u32, ServiceError> {
        Ok(input.value)
    }
}
fn manifest() -> ServiceManifest {
    ServiceManifest {
        contract_version: 1,
        application_id: "app".into(),
        services: vec![Provider::lingshu_service_definition()],
    }
}
fn invocation(mode: CallMode) -> ServiceInvocation {
    let definition = Provider::lingshu_service_definition();
    ServiceInvocation {
        admission: None,
        target_instance: None,
        contract_version: 1,
        operation: definition.operations[0].reference("reports").unwrap(),
        context: ServiceContext {
            application_id: "app".into(),
            idempotency_key: "business-1".into(),
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
        input: json!({"value":2}),
        completion: if mode == CallMode::Async {
            Some(CompletionTarget {
                heartbeat: None,
                token: "secret".into(),
            })
        } else {
            None
        },
    }
}
#[test]
fn unbound_and_duplicate_implementations_fail_before_readiness() {
    assert!(ServiceRegistryBuilder::new(manifest())
        .unwrap()
        .build()
        .is_err());
    let mut builder = ServiceRegistryBuilder::new(manifest()).unwrap();
    let provider = Arc::new(Provider(AtomicUsize::new(0)));
    provider
        .clone()
        .bind_lingshu_services(&mut builder)
        .unwrap();
    assert_eq!(
        provider
            .bind_lingshu_services(&mut builder)
            .unwrap_err()
            .code,
        "duplicate_handler"
    );
}
#[tokio::test]
async fn typed_handler_serves_both_call_modes_and_invalid_input_never_executes() {
    let mut builder = ServiceRegistryBuilder::new(manifest()).unwrap();
    let provider = Arc::new(Provider(AtomicUsize::new(0)));
    provider
        .clone()
        .bind_lingshu_services(&mut builder)
        .unwrap();
    let registry = builder.build().unwrap();
    for mode in [CallMode::Sync, CallMode::Async] {
        assert_eq!(
            registry.invoke(invocation(mode)).await,
            ServiceOutcome::Succeeded { result: json!(3) }
        );
    }
    let mut invalid = invocation(CallMode::Sync);
    invalid.input = json!({"value":"bad"});
    assert!(matches!(
        registry.invoke(invalid).await,
        ServiceOutcome::Failed { .. }
    ));
    let mut mismatch = invocation(CallMode::Sync);
    mismatch.operation.contract_digest = "wrong".into();
    assert!(matches!(
        registry.invoke(mismatch).await,
        ServiceOutcome::Failed { .. }
    ));
    assert_eq!(provider.0.load(Ordering::Relaxed), 2);
    assert_eq!(registry.capabilities().len(), 2);
    assert!(registry
        .capabilities()
        .iter()
        .any(|c| !c.call && !c.events.is_empty()));
}
#[test]
fn source_manifest_exports_without_live_instances_or_credentials() {
    let manifest = export_manifest("app").unwrap();
    assert!(manifest.services.iter().any(|s| s.service_key == "reports"));
    let text = serde_json::to_string(&manifest).unwrap();
    assert!(!text.contains("invocation_url"));
    assert!(!text.contains("credential"));
}

struct TaskProvider;
#[crate::lingshu_service(key = "task-reviews")]
impl TaskProvider {
    #[user_task_completion_handler(operation="complete",version="v1",task_type="review.v1",modes=["sync","async"],idempotent=true)]
    async fn complete(
        &self,
        context: crate::user_task::completion::CompletionContext,
        input: Input,
    ) -> crate::user_task::completion::CompletionResult<u32> {
        use crate::user_task::completion::CompletionError;
        assert_eq!(context.actor_user_id(), "reviewer");
        match input.value {
            0 => Err(CompletionError::rejected("invalid", "correct submission")),
            1 => Err(CompletionError::retryable("busy", "try later")),
            2 => Err(CompletionError::failed("invalid_binding", "stop")),
            value => Ok(value),
        }
    }
}
#[tokio::test]
async fn native_completion_is_a_typed_call_with_four_business_outcomes() {
    let definition = TaskProvider::lingshu_service_definition();
    assert!(definition.operations[0].action.is_empty());
    let operation = definition.operations[0].reference("task-reviews").unwrap();
    let mut builder = ServiceRegistryBuilder::new(ServiceManifest {
        contract_version: 1,
        application_id: "app".into(),
        services: vec![definition],
    })
    .unwrap();
    Arc::new(TaskProvider)
        .bind_lingshu_services(&mut builder)
        .unwrap();
    let registry = builder.build().unwrap();
    for (value, expected) in [(0, "rejected"), (1, "retry"), (2, "failed"), (3, "applied")] {
        let mut call = invocation(CallMode::Sync);
        call.operation = operation.clone();
        call.input = json!({"value":value});
        let InvocationRole::Call(c) = &mut call.context.invocation else {
            unreachable!()
        };
        c.caller = "user_task".into();
        c.workflow = Some(WorkflowCallTarget {
            workflow_id: "1".into(),
            workflow_version: "1".into(),
            root_workflow_instance_id: "2".into(),
            workflow_instance_id: "2".into(),
            execution_path: "3@2".into(),
            execution_seq: 1,
            node_id: "review".into(),
        });
        c.user_task_completion=Some(Box::new(serde_json::from_value(json!({
            "contract_version":kish_lingshu_runtime_contract::USER_TASK_COMPLETION_CONTRACT_VERSION,
            "invocation_id":"submission-1","idempotency_key":"business-1","application_id":"app",
            "task":{"id":1,"task_type":"review.v1","expected_revision":1,"staged_revision":2},
            "actor_user_id":"reviewer","workflow":{"workflow_id":1,"workflow_instance_id":2,"flow_node_id":"review"},
            "submission":call.input,"request_id":"r","correlation_id":"c",
            "invocation_deadline":chrono::Utc::now()+chrono::Duration::minutes(1)
        })).unwrap()));
        let outcome = registry.invoke(call.clone()).await;
        let ServiceOutcome::Succeeded { result } = outcome else {
            panic!("business outcome mapped to transport error: {outcome:?}")
        };
        assert_eq!(result["outcome"], expected);
        let InvocationRole::Call(c) = &mut call.context.invocation else {
            unreachable!()
        };
        c.user_task_completion = None;
        assert!(matches!(
            registry.invoke(call).await,
            ServiceOutcome::Failed { .. }
        ));
    }
}
