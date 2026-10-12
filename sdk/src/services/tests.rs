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
                human_task: None,
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

#[tokio::test]
async fn operation_projection_preserves_handler_and_rejects_sibling_or_digest_mismatch() {
    let mut builder = ServiceRegistryBuilder::new(manifest()).unwrap();
    let provider = Arc::new(Provider(AtomicUsize::new(0)));
    provider
        .clone()
        .bind_lingshu_services(&mut builder)
        .unwrap();
    let registry = builder.build().unwrap();
    let request = invocation(CallMode::Sync);
    let selected = registry.select_operation(&request.operation).unwrap();
    assert_eq!(selected.capabilities().len(), 1);
    assert_eq!(selected.manifest().services[0].operations.len(), 1);
    assert_eq!(
        selected.invoke(request.clone()).await,
        ServiceOutcome::Succeeded { result: json!(3) }
    );
    let sibling = registry
        .capabilities()
        .into_iter()
        .find(|c| c.operation != request.operation)
        .unwrap();
    assert!(selected.definition(&sibling.operation).is_none());
    let mut wrong = request.operation;
    wrong.contract_digest = "wrong".into();
    assert!(registry.select_operation(&wrong).is_err());
    assert_eq!(provider.0.load(Ordering::Relaxed), 1);
    assert_eq!(registry.capabilities().len(), 2);
}
