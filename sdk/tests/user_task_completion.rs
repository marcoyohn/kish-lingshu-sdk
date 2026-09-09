#![cfg(feature = "user-task-completion-http")]

use std::{sync::Arc, time::Duration};

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use chrono::Utc;
use kish_lingshu_runtime_contract::{
    CorrelationId, RequestId, UserTaskCompletionInvocationV1, UserTaskCompletionOutcomeV1,
    UserTaskCompletionProblem, UserTaskCompletionTaskV1, UserTaskCompletionWorkflowV1, UserTaskId,
    UserTaskRevision, WorkflowId, WorkflowInstanceId, USER_TASK_COMPLETION_CONTRACT_VERSION,
    USER_TASK_COMPLETION_IDEMPOTENCY_HEADER, USER_TASK_COMPLETION_INVOCATION_ID_HEADER,
    USER_TASK_COMPLETION_PATH,
};
use kish_lingshu_sdk::{
    user_task::completion::{
        CompletionContext, CompletionError, CompletionHttpAdapter, CompletionProducerMetadata,
        CompletionRegistry, CompletionRegistryError, CompletionResult, CompletionSourceCatalog,
    },
    user_task_handlers,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tower::ServiceExt;

#[derive(Debug, Deserialize, JsonSchema)]
struct Decision {
    decision: String,
}

#[derive(Debug, JsonSchema, Serialize)]
struct AppliedOutput {
    recorded_decision: String,
    actor_user_id: String,
}

struct ApprovalHandlers;

#[user_task_handlers]
impl ApprovalHandlers {
    #[completion_handler(task_type = "approval.applied.v1")]
    async fn applied(
        &self,
        context: CompletionContext,
        submission: Decision,
    ) -> CompletionResult<AppliedOutput> {
        Ok(AppliedOutput {
            recorded_decision: submission.decision,
            actor_user_id: context.actor_user_id().to_string(),
        })
    }

    #[completion_handler(task_type = "approval.rejected.v1")]
    async fn rejected(
        &self,
        _context: CompletionContext,
        _submission: Decision,
    ) -> CompletionResult<AppliedOutput> {
        Err(CompletionError::rejected_problem(
            UserTaskCompletionProblem::new("decision_rejected", "decision needs revision")
                .with_field_error("decision", "choose approve or reject"),
        ))
    }

    #[completion_handler(task_type = "approval.retry.v1")]
    async fn retry(
        &self,
        _context: CompletionContext,
        _submission: Decision,
    ) -> CompletionResult<AppliedOutput> {
        Err(CompletionError::retryable_after(
            "approval_busy",
            "approval service is busy",
            Duration::from_millis(250),
        ))
    }

    #[completion_handler(task_type = "approval.failed.v1")]
    async fn failed(
        &self,
        _context: CompletionContext,
        _submission: Decision,
    ) -> CompletionResult<AppliedOutput> {
        Err(CompletionError::failed(
            "approval_configuration_invalid",
            "approval configuration is invalid",
        ))
    }
}

struct UnregisteredHandlers;

fn registry() -> Arc<CompletionRegistry> {
    let mut builder = CompletionRegistry::builder("approval-app").unwrap();
    builder.bind(Arc::new(ApprovalHandlers)).unwrap();
    Arc::new(builder.build().unwrap())
}

fn invocation(task_type: &str) -> UserTaskCompletionInvocationV1 {
    UserTaskCompletionInvocationV1 {
        contract_version: USER_TASK_COMPLETION_CONTRACT_VERSION.to_string(),
        invocation_id: "user-task-completion/9001".to_string(),
        idempotency_key: "user-task-completion/9001".to_string(),
        application_id: "approval-app".to_string(),
        task: UserTaskCompletionTaskV1 {
            id: UserTaskId::new(123).unwrap(),
            task_type: task_type.to_string(),
            expected_revision: UserTaskRevision::new(3).unwrap(),
            staged_revision: UserTaskRevision::new(4).unwrap(),
            display: Some(json!({"approval_case_id": "case-123"})),
        },
        actor_user_id: "principal-1".to_string(),
        workflow: UserTaskCompletionWorkflowV1 {
            workflow_id: WorkflowId(10),
            workflow_instance_id: WorkflowInstanceId(20),
            root_workflow_instance_id: None,
            flow_node_id: "manager-approval".to_string(),
        },
        submission: json!({"decision": "approve"}),
        request_id: RequestId::from("req-1"),
        correlation_id: CorrelationId::from("corr-1"),
        invocation_deadline: Utc::now() + chrono::Duration::seconds(30),
        trace: None,
    }
}

#[tokio::test]
async fn typed_handlers_dispatch_and_map_all_completion_outcomes() {
    let registry = registry();

    let applied = registry.dispatch(invocation("approval.applied.v1")).await;
    assert_eq!(
        applied,
        UserTaskCompletionOutcomeV1::Applied {
            output: json!({
                "recorded_decision": "approve",
                "actor_user_id": "principal-1"
            }),
        }
    );

    let rejected = registry.dispatch(invocation("approval.rejected.v1")).await;
    assert!(matches!(
        rejected,
        UserTaskCompletionOutcomeV1::Rejected { problem }
            if problem.code == "decision_rejected"
                && problem.field_errors.contains_key("decision")
    ));

    let retry = registry.dispatch(invocation("approval.retry.v1")).await;
    assert!(matches!(
        retry,
        UserTaskCompletionOutcomeV1::Retry {
            problem,
            retry_after_milliseconds: Some(250),
        } if problem.code == "approval_busy"
    ));

    let failed = registry.dispatch(invocation("approval.failed.v1")).await;
    assert!(matches!(
        failed,
        UserTaskCompletionOutcomeV1::Failed { problem }
            if problem.code == "approval_configuration_invalid"
    ));

    let unknown = registry.dispatch(invocation("approval.unknown.v1")).await;
    assert!(matches!(
        unknown,
        UserTaskCompletionOutcomeV1::Failed { problem }
            if problem.code == "unsupported_task_type"
    ));
}

#[test]
fn registry_requires_exact_handler_instance_bindings() {
    let missing = match CompletionRegistry::builder("approval-app").unwrap().build() {
        Ok(_) => panic!("registry unexpectedly accepted missing bindings"),
        Err(error) => error,
    };
    assert!(matches!(
        missing,
        CompletionRegistryError::MissingBinding { .. }
    ));

    let mut unused = CompletionRegistry::builder("approval-app").unwrap();
    unused.bind(Arc::new(UnregisteredHandlers)).unwrap();
    let unused = match unused.build_bound_handlers() {
        Ok(_) => panic!("registry unexpectedly accepted an unused binding"),
        Err(error) => error,
    };
    assert!(matches!(
        unused,
        CompletionRegistryError::UnusedBinding { .. }
    ));

    let mut duplicate = CompletionRegistry::builder("approval-app").unwrap();
    duplicate.bind(Arc::new(ApprovalHandlers)).unwrap();
    let duplicate = match duplicate.bind(Arc::new(ApprovalHandlers)) {
        Ok(_) => panic!("registry unexpectedly accepted a duplicate binding"),
        Err(error) => error,
    };
    assert!(matches!(
        duplicate,
        CompletionRegistryError::DuplicateBinding { .. }
    ));
}

#[test]
fn completion_source_export_is_stable_and_environment_neutral() {
    let producer = CompletionProducerMetadata {
        package_name: "approval-application".to_string(),
        package_version: "1.2.3".to_string(),
    };
    let first = CompletionSourceCatalog::collect()
        .unwrap()
        .export_json(producer.clone())
        .unwrap();
    let second = CompletionSourceCatalog::collect()
        .unwrap()
        .export_json(producer)
        .unwrap();
    assert_eq!(first, second);

    let contract: Value = serde_json::from_slice(&first).unwrap();
    assert_eq!(
        contract["operation"]["path"],
        Value::String(USER_TASK_COMPLETION_PATH.to_string())
    );
    assert_eq!(contract["handlers"].as_array().unwrap().len(), 4);
    let encoded = String::from_utf8(first).unwrap();
    for forbidden in [
        "application_id",
        "base_url",
        "credential",
        "authorization",
        "environment",
        "handler_instance",
    ] {
        assert!(!encoded.contains(forbidden), "contract leaked {forbidden}");
    }
}

async fn post(invocation: UserTaskCompletionInvocationV1) -> (StatusCode, Value) {
    let body = serde_json::to_vec(&invocation).unwrap();
    let request = Request::post(USER_TASK_COMPLETION_PATH)
        .header("content-type", "application/json")
        .header(
            USER_TASK_COMPLETION_IDEMPOTENCY_HEADER,
            invocation.idempotency_key.as_str(),
        )
        .header(
            USER_TASK_COMPLETION_INVOCATION_ID_HEADER,
            invocation.invocation_id.as_str(),
        )
        .body(Body::from(body))
        .unwrap();
    let response = CompletionHttpAdapter::new(registry())
        .router()
        .oneshot(request)
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn canonical_http_route_validates_protocol_identity() {
    let (status, body) = post(invocation("approval.applied.v1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "applied");

    let mut wrong_application = invocation("approval.applied.v1");
    wrong_application.application_id = "other-app".to_string();
    let (status, body) = post(wrong_application).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "completion_handler_not_registered");

    let mut expired = invocation("approval.applied.v1");
    expired.invocation_deadline = Utc::now() - chrono::Duration::seconds(1);
    let (status, body) = post(expired).await;
    assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
    assert_eq!(body["code"], "completion_deadline_exceeded");
}

#[tokio::test]
async fn canonical_http_route_rejects_mismatched_headers() {
    let invocation = invocation("approval.applied.v1");
    let request = Request::post(USER_TASK_COMPLETION_PATH)
        .header("content-type", "application/json")
        .header(USER_TASK_COMPLETION_IDEMPOTENCY_HEADER, "another-key")
        .header(
            USER_TASK_COMPLETION_INVOCATION_ID_HEADER,
            invocation.invocation_id.as_str(),
        )
        .body(Body::from(serde_json::to_vec(&invocation).unwrap()))
        .unwrap();
    let response = CompletionHttpAdapter::new(registry())
        .router()
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["code"], "idempotency_key_mismatch");
}
