#![cfg(all(feature = "http-client", feature = "service-auth"))]
use axum::{Json, Router, http::HeaderMap, routing::post};
use kish_lingshu_sdk::human_tasks::HumanTaskUserDelegation;
use kish_lingshu_sdk::workflow::WorkflowInstanceId;
use kish_lingshu_sdk::{ClientBuilder, ClientConfig, Error, RequestOptions, ServiceCredential};
use serde_json::{Value, json};

#[tokio::test]
async fn exchange_binds_application_subject_instance_task_and_short_lifetime() {
    for case in [
        "valid",
        "clock-skew",
        "application",
        "subject",
        "instance",
        "task",
        "expired",
        "long-lived",
        "wrong-purpose",
    ] {
        let router = Router::new().route("/api/user/services/v1/human-task-users", post(move |headers:HeaderMap, Json(request):Json<Value>| async move {
            assert_eq!(headers["authorization"], "Bearer application-key");
            assert_eq!(request, json!({"subject":"business-user","workflow_instance_id":7,"task_id":"task-1"}));
            let mut authority = request;
            match case {
                "subject" => authority["subject"] = json!("foreign-user"),
                "instance" => authority["workflow_instance_id"] = json!(8),
                "task" => authority["task_id"] = json!("foreign-task"),
                _ => {}
            }
            let lifetime = match case { "expired" => -1, "long-lived" => 100, "clock-skew" => 32, _ => 30 };
            Json(json!({"application_id":if case=="application" {"foreign-app"}else{"app"}, "authority":authority,
                "expires_at":chrono::Utc::now().timestamp()+lifetime,
                "credential":if case=="wrong-purpose" {"service:not-a-user"}else{"human:opaque-proof"}}))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let result = ClientBuilder::new(ClientConfig::new(endpoint).with_retry_limit(0))
            .service_credential(ServiceCredential::new("app", "application-key").unwrap())
            .delegate_human_task_user(
                HumanTaskUserDelegation {
                    subject: "business-user".into(),
                    workflow_instance_id: WorkflowInstanceId(7),
                    task_id: "task-1".into(),
                },
                RequestOptions::new(),
            )
            .await;
        if matches!(case, "valid" | "clock-skew") {
            assert!(result.is_ok(), "{case}");
        } else {
            assert!(matches!(result, Err(Error::ContractViolation(_))), "{case}");
        }
        server.abort();
    }
}
