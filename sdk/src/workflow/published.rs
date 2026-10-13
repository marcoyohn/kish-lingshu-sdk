use super::*;
impl Workflows<crate::ServicePrincipal> {
    /// Current enabled main-branch snapshots for the credential application.
    pub async fn list_published(
        &self,
        after: Option<u64>,
        limit: u32,
        options: RequestOptions,
    ) -> Result<PublishedWorkflowPage, Error> {
        if !(1..=100).contains(&limit) || after == Some(0) {
            return Err(Error::configuration(
                "published_workflows.pagination",
                "Invalid catalog pagination",
            ));
        }
        let page = self
            .inner
            .binding
            .list_published_workflows(after, limit, options)
            .await?;
        let mut previous = after.unwrap_or(0);
        if page.items.len() > limit as usize {
            return Err(Error::contract(
                "Published catalog exceeded requested limit",
                None,
            ));
        }
        for item in &page.items {
            self.validate_published(item)?;
            if item.workflow_id <= previous {
                return Err(Error::contract(
                    "Published catalog is unordered or repeated",
                    None,
                ));
            }
            previous = item.workflow_id;
        }
        if let Some(next) = page.next_workflow_id {
            if page.items.len() != limit as usize
                || page.items.last().map(|item| item.workflow_id) != Some(next)
            {
                return Err(Error::contract("Published catalog cursor mismatch", None));
            }
        }
        Ok(page)
    }
    /// Exact immutable published definition; never resolves another version.
    pub async fn get_published(
        &self,
        workflow: WorkflowId,
        definition: u64,
        options: RequestOptions,
    ) -> Result<PublishedWorkflowDefinition, Error> {
        if workflow.0 == 0 || definition == 0 {
            return Err(Error::configuration(
                "published_workflows.identity",
                "Invalid definition identity",
            ));
        }
        let value = self
            .inner
            .binding
            .get_published_workflow(workflow.0, definition, options)
            .await?;
        self.validate_published(&value)?;
        if value.workflow_id != workflow.0 || value.definition_id != definition {
            return Err(Error::contract(
                "Published definition identity mismatch",
                None,
            ));
        }
        Ok(value)
    }
    fn validate_published(&self, value: &PublishedWorkflowDefinition) -> Result<(), Error> {
        if value.workflow_id == 0
            || value.definition_id == 0
            || !value.define_schema.is_object()
            || self.inner.application_id.as_deref() != Some(value.application_id.as_str())
        {
            return Err(Error::contract(
                "Published definition ownership mismatch",
                None,
            ));
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "http-client"))]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn server(
        value: Value,
    ) -> (
        crate::Client<crate::ServicePrincipal>,
        tokio::task::JoinHandle<String>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![0; 16384];
            let size = socket.read(&mut bytes).await.unwrap();
            let body = value.to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
            String::from_utf8(bytes[..size].to_vec()).unwrap()
        });
        let client =
            crate::ClientBuilder::new(crate::ClientConfig::new(endpoint).with_retry_limit(0))
                .service_credential(
                    crate::ServiceCredential::new("owner", "isolated-test-key").unwrap(),
                )
                .connect()
                .unwrap();
        (client, task)
    }
    fn definition() -> Value {
        serde_json::json!({"application_id":"owner","workflow_id":1,"definition_id":10,"name":"Published","define_schema":{"type":"ReactFlow"}})
    }
    #[tokio::test]
    async fn exact_reads_validate_application_workflow_and_revision() {
        for changed in [
            None,
            Some(("application_id", serde_json::json!("foreign"))),
            Some(("workflow_id", serde_json::json!(2))),
            Some(("definition_id", serde_json::json!(11))),
        ] {
            let mut body = definition();
            if let Some((field, value)) = changed.clone() {
                body[field] = value;
            }
            let (client, task) = server(body).await;
            let result = client
                .workflows()
                .get_published(WorkflowId(1), 10, RequestOptions::new())
                .await;
            assert_eq!(result.is_ok(), changed.is_none());
            let request = task.await.unwrap();
            assert!(request.starts_with("GET /openapi/workflows/1/published-definitions/10 "));
            assert!(request
                .to_ascii_lowercase()
                .contains("x-kish-app-id: owner"));
        }
    }
    #[tokio::test]
    async fn pagination_rejects_unordered_repeated_foreign_and_bad_cursors() {
        for mode in 0..5 {
            let mut first = definition();
            let mut second = definition();
            second["workflow_id"] = serde_json::json!(3);
            if mode == 1 {
                first["application_id"] = serde_json::json!("foreign");
            }
            if mode == 2 {
                second["workflow_id"] = serde_json::json!(1);
            }
            let next = if mode == 3 {
                Some(4)
            } else if mode == 4 {
                Some(1)
            } else {
                Some(3)
            };
            let (client, task) =
                server(serde_json::json!({"items":[first,second],"next_workflow_id":next})).await;
            assert_eq!(
                client
                    .workflows()
                    .list_published(None, 2, RequestOptions::new())
                    .await
                    .is_ok(),
                mode == 0
            );
            task.await.unwrap();
        }
    }
}
