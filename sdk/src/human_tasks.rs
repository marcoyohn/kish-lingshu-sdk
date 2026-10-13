//! Human tasks in workflow instances: draft editing, approval and reading steps.
//! Execution observations and durable command receipts; business inboxes remain external.
use crate::{
    Error, Principal, RequestOptions, ServicePrincipal, UserPrincipal, client::ClientInner,
};
use kish_lingshu_runtime_contract::WorkflowInstanceId;
pub use kish_lingshu_runtime_contract::human_task::{
    HumanTaskAction, HumanTaskCommand, HumanTaskCommandReceipt, HumanTaskCommandState,
    HumanTaskCommandStatus, HumanTaskKind, HumanTaskPhase, HumanTaskProblem, HumanTaskSnapshot,
    HumanTaskUserDelegation,
};
use std::{marker::PhantomData, sync::Arc};
#[derive(Clone)]
pub struct HumanTasks<P: Principal> {
    inner: Arc<ClientInner>,
    principal: PhantomData<P>,
}
impl<P: Principal> HumanTasks<P> {
    pub(crate) fn new(inner: Arc<ClientInner>) -> Self {
        Self {
            inner,
            principal: PhantomData,
        }
    }
    fn validate(
        &self,
        expected_instance: WorkflowInstanceId,
        app: &str,
        instance: WorkflowInstanceId,
        task_id: &str,
        expected_task_id: Option<&str>,
    ) -> Result<(), Error> {
        if instance != expected_instance
            || self
                .inner
                .application_id
                .as_deref()
                .is_some_and(|selected| selected != app)
            || task_id.is_empty()
            || expected_task_id.is_some_and(|expected| expected != task_id)
        {
            return Err(protocol("Human task receipt ownership mismatch"));
        }
        Ok(())
    }
    fn validate_receipt(
        &self,
        instance: WorkflowInstanceId,
        task_id: &str,
        receipt: &HumanTaskCommandReceipt,
    ) -> Result<(), Error> {
        self.validate(
            instance,
            &receipt.application_id,
            receipt.workflow_instance_id,
            &receipt.task_id,
            Some(task_id),
        )?;
        if receipt.command_id.is_empty()
            || receipt.operation_id.is_empty()
            || receipt.task_revision == 0
        {
            return Err(protocol("Invalid command receipt"));
        }
        Ok(())
    }
    pub async fn list(
        &self,
        instance: WorkflowInstanceId,
        options: RequestOptions,
    ) -> Result<Vec<HumanTaskSnapshot>, Error> {
        let results = self
            .inner
            .binding
            .list_human_tasks(instance, options)
            .await?;
        for result in &results {
            self.validate(
                instance,
                &result.application_id,
                result.workflow_instance_id,
                &result.task_id,
                None,
            )?;
        }
        Ok(results)
    }
    pub async fn get(
        &self,
        instance: WorkflowInstanceId,
        task_id: &str,
        options: RequestOptions,
    ) -> Result<HumanTaskSnapshot, Error> {
        let result = self
            .inner
            .binding
            .get_human_task(instance, task_id.to_owned(), options)
            .await?;
        self.validate(
            instance,
            &result.application_id,
            result.workflow_instance_id,
            &result.task_id,
            Some(task_id),
        )?;
        Ok(result)
    }
    pub async fn command_status(
        &self,
        instance: WorkflowInstanceId,
        task_id: &str,
        command: &str,
        options: RequestOptions,
    ) -> Result<HumanTaskCommandStatus, Error> {
        let result = self
            .inner
            .binding
            .human_task_command_status(instance, task_id.to_owned(), command.to_owned(), options)
            .await?;
        self.validate_receipt(instance, task_id, &result.receipt)?;
        if result.receipt.command_id != command {
            return Err(protocol("Command receipt identity mismatch"));
        }
        Ok(result)
    }
}
impl HumanTasks<UserPrincipal> {
    /// Acceptance is durable, but is not a successful business outcome. Poll status explicitly.
    pub async fn command(
        &self,
        instance: WorkflowInstanceId,
        task_id: &str,
        command: HumanTaskCommand,
        options: RequestOptions,
    ) -> Result<HumanTaskCommandReceipt, Error> {
        let result = self
            .inner
            .binding
            .human_task_command(instance, task_id.to_owned(), command, options)
            .await?;
        self.validate_receipt(instance, task_id, &result)?;
        Ok(result)
    }
}
impl HumanTasks<ServicePrincipal> {
    pub async fn retry(
        &self,
        instance: WorkflowInstanceId,
        task_id: &str,
        command: &str,
        options: RequestOptions,
    ) -> Result<HumanTaskCommandReceipt, Error> {
        let result = self
            .inner
            .binding
            .retry_human_task(instance, task_id.to_owned(), command.to_owned(), options)
            .await?;
        self.validate_receipt(instance, task_id, &result)?;
        if result.command_id != command {
            return Err(protocol("Retry receipt identity mismatch"));
        }
        Ok(result)
    }
}

fn protocol(message: &str) -> Error {
    Error::Protocol(crate::ProtocolError {
        direction: crate::ProtocolDirection::DecodeResponse,
        message: message.into(),
        request_id: None,
    })
}

#[cfg(all(test, feature = "http-client", feature = "service-http"))]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::{Path, State},
        http::StatusCode,
        routing::{get, post},
    };
    use serde_json::json;
    #[derive(Clone)]
    struct Platform {
        bad_identity: bool,
    }
    async fn command(
        State(p): State<Platform>,
        Path((instance, task_id)): Path<(u64, String)>,
        Json(request): Json<HumanTaskCommand>,
    ) -> (StatusCode, Json<HumanTaskCommandReceipt>) {
        assert_eq!(request.action, HumanTaskAction::Complete);
        (
            StatusCode::ACCEPTED,
            Json(HumanTaskCommandReceipt {
                application_id: if p.bad_identity { "other" } else { "app" }.into(),
                workflow_instance_id: WorkflowInstanceId(instance),
                task_id,
                command_id: "command".into(),
                operation_id: "operation".into(),
                task_revision: 2,
                state: HumanTaskCommandState::Accepted,
            }),
        )
    }
    async fn status(
        Path((instance, task_id, command)): Path<(u64, String, String)>,
    ) -> Json<HumanTaskCommandStatus> {
        Json(HumanTaskCommandStatus {
            receipt: HumanTaskCommandReceipt {
                application_id: "app".into(),
                workflow_instance_id: WorkflowInstanceId(instance),
                task_id,
                command_id: command,
                operation_id: "operation".into(),
                task_revision: 2,
                state: HumanTaskCommandState::Accepted,
            },
            state: HumanTaskCommandState::Rejected,
            action: HumanTaskAction::Complete,
            problem: Some(HumanTaskProblem::new("policy", "No business effect")),
        })
    }
    #[tokio::test]
    async fn http_receipt_is_not_final_outcome_and_wrong_application_is_rejected() {
        for bad_identity in [false, true] {
            let router = Router::new().route("/openapi/workflow-instances/{instance}/human-tasks/{task_id}/commands",post(command)).route("/openapi/workflow-instances/{instance}/human-tasks/{task_id}/commands/{command}",get(status)).with_state(Platform { bad_identity });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                axum::serve(listener, router).await.unwrap();
            });
            let client = crate::ClientBuilder::new(
                crate::ClientConfig::new(&url).with_selected_application("app"),
            )
            .user_credential(crate::UserCredential::new("verified-user-token").unwrap())
            .connect()
            .unwrap();
            let request = HumanTaskCommand {
                action: HumanTaskAction::Complete,
                expected_revision: 1,
                idempotency_key: kish_lingshu_runtime_contract::IdempotencyKey::new("submit")
                    .unwrap(),
                input: json!({}),
                expected_business_revision: None,
            };
            let result = client
                .human_tasks()
                .command(
                    WorkflowInstanceId(42),
                    "occurrence",
                    request,
                    RequestOptions::default(),
                )
                .await;
            if bad_identity {
                assert!(matches!(result, Err(Error::Protocol(_))));
            } else {
                let receipt = result.unwrap();
                assert_eq!(receipt.state, HumanTaskCommandState::Accepted);
                assert_eq!(receipt.task_id, "occurrence");
                assert_eq!(receipt.task_revision, 2);
                let wire = serde_json::to_value(&receipt).unwrap();
                assert_eq!(wire["task_id"], "occurrence");
                assert_eq!(wire["task_revision"], 2);
                assert!(wire.get("interaction_id").is_none());
                let final_result = client
                    .human_tasks()
                    .command_status(
                        WorkflowInstanceId(42),
                        "occurrence",
                        "command",
                        RequestOptions::default(),
                    )
                    .await
                    .unwrap();
                assert_eq!(final_result.state, HumanTaskCommandState::Rejected);
                assert_eq!(final_result.receipt.task_id, "occurrence");
                assert_eq!(final_result.receipt.state, HumanTaskCommandState::Accepted);
            }
            server.abort();
        }
    }
}
