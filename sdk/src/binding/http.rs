//! Authenticated HTTP and SSE Product Runtime binding implementation.
use kish_lingshu_runtime_contract::WorkspaceApprovalMode;

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;

use base64::Engine as _;
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::StreamExt;
use kish_lingshu_event_dispatch_contract::{EventId, EventRecord, PublishEvent, PublishReceipt};
use kish_lingshu_runtime_contract::{
    ApplicationProblem, ApplicationTaskQuery, ApplicationTaskSummary, CommandAck,
    CurrentUserTaskQuery, EventCursor, Page, PrincipalKind, RequestId, RetryUserTaskCompletion,
    RuntimeBindings, RuntimeError, SessionHandle, SessionId, SessionSnapshot, TaskActionReceipt,
    UserTask, UserTaskAction, UserTaskCompletionRetryReceipt, UserTaskId, UserTaskSummary,
    WorkflowEvent, WorkflowId, WorkflowInput, WorkflowInstanceId, WorkflowRunHandle,
    WorkflowSignal, WorkflowSnapshot,
};

use kish_lingshu_runtime_contract::{AssetRef, AssetUpload};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use rand::Rng;
use reqwest::{header, Method, Response};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use url::Url;

use super::{
    retry::{execute_with_retry, ReplaySafety, RetryFailure, RetryPolicy, RetryReason},
    runtime::runtime_error,
    sse::{JsonSseParser, SseItem, SseReattachment},
    ProductBinding, WorkflowEventStream,
};

use crate::workflow::{
    AgentTaskPlan, ConversationScope, SessionMessage, SessionRecord, WorkflowRecord,
};
use crate::{
    auth::ClientCredential, config::ClientConfig, request::TransportAttempt, ApplicationFailure,
    AuthenticatedUser, Error, MutationOptions, ProtocolDirection, ProtocolError, RequestOptions,
    TransportFailure, TransportKind,
};

const APPLICATION_HEADER: &str = "x-kish-app-id";
const CORRELATION_ID_HEADER: &str = "x-correlation-id";
const DEADLINE_HEADER: &str = "x-kish-request-deadline";
const IDEMPOTENCY_HEADER: &str = "idempotency-key";
const REQUEST_ID_HEADER: &str = "x-request-id";
const TRANSPORT_ATTEMPT_HEADER: &str = "x-kish-transport-attempt";

#[derive(Clone)]
pub(crate) struct HttpBinding {
    state: Arc<HttpBindingState>,
}

struct HttpBindingState {
    endpoint: Url,
    client: reqwest::Client,
    credential: Option<Arc<str>>,
    principal: PrincipalKind,
    application_scope: Option<String>,
    auth_application: Option<String>,
    auth_brand: Option<String>,
    user_agent: String,
    timeout: Duration,
    retry: RetryPolicy,
}

impl HttpBinding {
    pub(crate) fn new(
        config: &ClientConfig,
        credential: ClientCredential,
        principal: PrincipalKind,
    ) -> Result<Self, Error> {
        let endpoint = config
            .endpoint()
            .ok_or_else(|| Error::configuration("endpoint", "an HTTP endpoint is required"))?;
        let mut endpoint = Url::parse(endpoint).map_err(|error| {
            Error::configuration("endpoint", format!("cannot parse endpoint: {error}"))
        })?;
        if !endpoint.path().ends_with('/') {
            let path = format!("{}/", endpoint.path());
            endpoint.set_path(&path);
        }
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::Client::builder()
            .connect_timeout(config.timeout())
            .build()
            .map_err(|error| {
                Error::Transport(TransportFailure {
                    kind: TransportKind::Connect,
                    message: error.to_string(),
                    request_id: None,
                    retryable: false,
                })
            })?;
        let application_scope = credential
            .service_application_id()
            .map(str::to_owned)
            .or_else(|| config.selected_application().map(str::to_owned));
        Ok(Self {
            state: Arc::new(HttpBindingState {
                endpoint,
                client,
                credential: credential.expose().map(Arc::from),
                principal,
                application_scope,
                auth_application: config.auth_application().map(str::to_owned),
                auth_brand: config.auth_brand().map(str::to_owned),
                user_agent: config.metadata().user_agent(),
                timeout: config.timeout(),
                retry: RetryPolicy::new(config.retry_limit()),
            }),
        })
    }

    fn endpoint(&self, path: &str) -> Result<Url, Error> {
        self.state
            .endpoint
            .join(path.trim_start_matches('/'))
            .map_err(|error| Error::configuration("endpoint", error.to_string()))
    }

    fn api_root(&self) -> &'static str {
        match self.state.principal {
            PrincipalKind::Service => "openapi",
            PrincipalKind::User => "api/user",
            PrincipalKind::Internal => unreachable!("Clients cannot use an internal principal"),
        }
    }

    fn request(
        &self,
        method: Method,
        url: Url,
        options: &RequestOptions,
        attempt: &TransportAttempt,
        body: &[u8],
        idempotency_key: Option<&str>,
        accept: &'static str,
    ) -> Result<reqwest::RequestBuilder, Error> {
        let mut request = self
            .state
            .client
            .request(method, url)
            .header(header::USER_AGENT, &self.state.user_agent)
            .header(header::ACCEPT, accept)
            .header(REQUEST_ID_HEADER, attempt.request_id.as_ref())
            .header(CORRELATION_ID_HEADER, attempt.correlation_id.as_ref())
            .header(TRANSPORT_ATTEMPT_HEADER, attempt.number);
        match self.state.principal {
            PrincipalKind::Service => {
                let credential = self.state.credential.as_deref().ok_or_else(|| {
                    Error::configuration("credential", "a service credential is required")
                })?;
                request = request.header(header::AUTHORIZATION, format!("Bearer {credential}"));
            }
            PrincipalKind::User => {
                request = request.header("x-user-profile", "is_admin_mode=false");
                if let Some(credential) = self.state.credential.as_deref() {
                    request = request
                        .header("x-kish-token-key", "x-token")
                        .header("x-token", credential);
                }
                if let Some(auth_application) = &self.state.auth_application {
                    request = request.header("x-auth-app", auth_application);
                }
                if let Some(auth_brand) = &self.state.auth_brand {
                    request = request.header("x-auth-brand", auth_brand);
                }
            }
            PrincipalKind::Internal => unreachable!("Clients cannot use an internal principal"),
        }
        if !body.is_empty() {
            request = request
                .header(header::CONTENT_TYPE, "application/json")
                .body(body.to_vec());
        }
        if let Some(idempotency_key) = idempotency_key {
            request = request.header(IDEMPOTENCY_HEADER, idempotency_key);
        }
        if let Some(application_id) = &self.state.application_scope {
            request = request.header(
                APPLICATION_HEADER,
                utf8_percent_encode(application_id, NON_ALPHANUMERIC).to_string(),
            );
        }
        if let Some(trace) = options.trace() {
            if let Some(traceparent) = &trace.traceparent {
                request = request.header("traceparent", traceparent);
            }
            if let Some(tracestate) = &trace.tracestate {
                request = request.header("tracestate", tracestate);
            }
        }
        if let Some(deadline) = options.deadline() {
            request = request.header(
                DEADLINE_HEADER,
                deadline
                    .as_datetime()
                    .to_rfc3339_opts(SecondsFormat::Millis, true),
            );
        }
        Ok(request.timeout(self.attempt_timeout(options)?))
    }

    fn attempt_timeout(&self, options: &RequestOptions) -> Result<Duration, Error> {
        let Some(deadline) = options.deadline() else {
            return Ok(self.state.timeout);
        };
        let remaining = (*deadline.as_datetime() - Utc::now())
            .to_std()
            .map_err(|_| {
                request_transport_error(
                    "operation deadline elapsed before the transport attempt",
                    options.request_id().clone(),
                    false,
                )
            })?;
        if remaining.is_zero() {
            return Err(request_transport_error(
                "operation deadline elapsed before the transport attempt",
                options.request_id().clone(),
                false,
            ));
        }
        Ok(remaining.min(self.state.timeout))
    }

    fn encode_body<T: Serialize + ?Sized>(
        &self,
        body: &T,
        request_id: &RequestId,
    ) -> Result<Arc<[u8]>, Error> {
        serde_json::to_vec(body).map(Arc::from).map_err(|error| {
            Error::Protocol(ProtocolError {
                direction: ProtocolDirection::EncodeRequest,
                message: error.to_string(),
                request_id: Some(request_id.clone()),
            })
        })
    }

    async fn execute_json<T>(
        &self,
        method: Method,
        url: Url,
        body: Arc<[u8]>,
        options: &RequestOptions,
        safety: ReplaySafety,
        idempotency_key: Option<&str>,
        not_found_as_null: bool,
    ) -> Result<T, Error>
    where
        T: DeserializeOwned + Send + 'static,
    {
        let binding = self.clone();
        let retry_options = options.clone();
        let closure_options = options.clone();
        let idempotency_key = idempotency_key.map(str::to_owned);
        execute_with_retry(
            self.state.retry,
            safety,
            &retry_options,
            body,
            move |attempt, body| {
                let binding = binding.clone();
                let method = method.clone();
                let url = url.clone();
                let options = closure_options.clone();
                let idempotency_key = idempotency_key.clone();
                async move {
                    let request = binding
                        .request(
                            method,
                            url,
                            &options,
                            &attempt,
                            &body,
                            idempotency_key.as_deref(),
                            "application/json",
                        )
                        .map_err(|error| RetryFailure {
                            error,
                            reason: RetryReason::Transport,
                        })?;
                    let response = request.send().await.map_err(|error| RetryFailure {
                        error: request_transport_error(
                            error.to_string(),
                            attempt.request_id.clone(),
                            error.is_timeout() || error.is_connect() || error.is_request(),
                        ),
                        reason: RetryReason::Transport,
                    })?;
                    binding
                        .decode_json_response(response, &attempt.request_id, not_found_as_null)
                        .await
                }
            },
        )
        .await
    }

    async fn decode_json_response<T>(
        &self,
        response: Response,
        request_id: &RequestId,
        not_found_as_null: bool,
    ) -> Result<T, RetryFailure>
    where
        T: DeserializeOwned,
    {
        let status = response.status();
        let retry_after = response
            .headers()
            .get(header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let bytes = response.bytes().await.map_err(|error| RetryFailure {
            error: request_transport_error(error.to_string(), request_id.clone(), true),
            reason: RetryReason::Transport,
        })?;
        if status.is_success() || (not_found_as_null && status.as_u16() == 404) {
            let bytes = if status.as_u16() == 404 {
                b"null".as_slice()
            } else {
                bytes.as_ref()
            };
            return serde_json::from_slice(bytes).map_err(|error| RetryFailure {
                error: Error::Protocol(ProtocolError {
                    direction: ProtocolDirection::DecodeResponse,
                    message: format!("HTTP {} success body is invalid: {error}", status.as_u16()),
                    request_id: Some(request_id.clone()),
                }),
                reason: RetryReason::HttpStatus {
                    status: status.as_u16(),
                    retry_after: None,
                },
            });
        }
        Err(RetryFailure {
            error: decode_application_error(
                status.as_u16(),
                &bytes,
                retry_after.as_deref(),
                request_id.clone(),
                Utc::now(),
            ),
            reason: RetryReason::HttpStatus {
                status: status.as_u16(),
                retry_after: retry_after
                    .as_deref()
                    .and_then(|value| parse_retry_after(value, Utc::now())),
            },
        })
    }

    async fn open_sse(
        &self,
        url: Url,
        after: Option<&EventCursor>,
        options: &RequestOptions,
        attempt_counter: Arc<AtomicU32>,
    ) -> Result<Response, Error> {
        let binding = self.clone();
        let retry_options = options.clone();
        let closure_options = options.clone();
        let after = after.cloned();
        execute_with_retry(
            self.state.retry,
            ReplaySafety::StreamReattachment,
            &retry_options,
            Arc::from([]),
            move |mut attempt, body| {
                let binding = binding.clone();
                let url = url.clone();
                let options = closure_options.clone();
                let after = after.clone();
                let attempt_counter = attempt_counter.clone();
                async move {
                    attempt.number = attempt_counter.fetch_add(1, Ordering::Relaxed);
                    let mut request = binding
                        .request(
                            Method::GET,
                            url,
                            &options,
                            &attempt,
                            &body,
                            None,
                            "text/event-stream",
                        )
                        .map_err(|error| RetryFailure {
                            error,
                            reason: RetryReason::Transport,
                        })?;
                    if let Some(after) = after {
                        request = request.header("last-event-id", after.as_ref());
                    }
                    let response = request.send().await.map_err(|error| RetryFailure {
                        error: request_transport_error(
                            error.to_string(),
                            attempt.request_id.clone(),
                            true,
                        ),
                        reason: RetryReason::Transport,
                    })?;
                    let status = response.status();
                    if !status.is_success() {
                        return binding
                            .decode_json_response::<serde_json::Value>(
                                response,
                                &attempt.request_id,
                                false,
                            )
                            .await
                            .map(|_| unreachable!("non-success SSE response decoded as success"));
                    }
                    let content_type = response
                        .headers()
                        .get(header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default();
                    if !content_type.starts_with("text/event-stream") {
                        return Err(RetryFailure {
                            error: Error::Protocol(ProtocolError {
                                direction: ProtocolDirection::DecodeEvent,
                                message: format!(
                                    "expected text/event-stream, received {content_type:?}"
                                ),
                                request_id: Some(attempt.request_id),
                            }),
                            reason: RetryReason::HttpStatus {
                                status: status.as_u16(),
                                retry_after: None,
                            },
                        });
                    }
                    Ok(response)
                }
            },
        )
        .await
    }

    fn application_task_url(&self, path: &str, query: &ApplicationTaskQuery) -> Result<Url, Error> {
        let mut url = self.endpoint(path)?;
        let mut pairs = url.query_pairs_mut();
        if let Some(state) = &query.state {
            pairs.append_pair("state", state.as_str());
        }
        if let Some(mode) = &query.mode {
            pairs.append_pair("mode", mode.as_str());
        }
        if let Some(workflow_id) = query.workflow_id {
            pairs.append_pair("workflow_id", &workflow_id.0.to_string());
        }
        if let Some(participant_user_id) = &query.participant_user_id {
            pairs.append_pair("participant_user_id", participant_user_id);
        }
        if let Some(participant_state) = &query.participant_state {
            pairs.append_pair("participant_state", participant_state.as_str());
        }
        if let Some(claimed_by) = &query.claimed_by {
            pairs.append_pair("claimed_by", claimed_by);
        }
        if let Some(created_from) = query.created.created_from {
            pairs.append_pair("created_from", &created_from.to_rfc3339());
        }
        if let Some(created_before) = query.created.created_before {
            pairs.append_pair("created_before", &created_before.to_rfc3339());
        }
        pairs.append_pair("page", &query.pagination.page.to_string());
        pairs.append_pair("page_size", &query.pagination.page_size.to_string());
        drop(pairs);
        Ok(url)
    }

    fn current_user_task_url(
        &self,
        path: &str,
        query: &CurrentUserTaskQuery,
    ) -> Result<Url, Error> {
        let mut url = self.endpoint(path)?;
        let mut pairs = url.query_pairs_mut();
        if let Some(participant_state) = &query.participant_state {
            pairs.append_pair("participant_state", participant_state.as_str());
        }
        if let Some(mode) = &query.mode {
            pairs.append_pair("mode", mode.as_str());
        }
        if let Some(created_from) = query.created.created_from {
            pairs.append_pair("created_from", &created_from.to_rfc3339());
        }
        if let Some(created_before) = query.created.created_before {
            pairs.append_pair("created_before", &created_before.to_rfc3339());
        }
        pairs.append_pair("page", &query.pagination.page.to_string());
        pairs.append_pair("page_size", &query.pagination.page_size.to_string());
        drop(pairs);
        Ok(url)
    }
}

#[derive(Serialize)]
struct StartWorkflowBody {
    input: WorkflowInput,
    bindings: RuntimeBindings,
}

#[derive(Serialize)]
struct SignalWorkflowBody {
    signal: WorkflowSignal,
}

#[derive(Serialize)]
struct ResumeWorkflowBody {
    suspension: kish_lingshu_runtime_contract::SuspensionHandle,
    payload: kish_lingshu_runtime_contract::SuspensionResponse,
}

#[derive(Serialize)]
struct TerminateWorkflowBody {
    reason: Option<String>,
    wait_timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
struct LegacyResponse<T> {
    status: bool,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    data: Option<T>,
    #[serde(default)]
    runtime_error: Option<RuntimeError>,
}

#[derive(Default, Deserialize)]
struct UploadPolicy {
    upload_url: String,
    #[serde(default)]
    method: Option<String>,
    oss_key: String,
    #[serde(default)]
    form_fields: std::collections::HashMap<String, String>,
    #[serde(default)]
    header_fields: std::collections::HashMap<String, String>,
    #[serde(default)]
    key_info_fields: Vec<serde_json::Value>,
    #[serde(default)]
    need_call_server: bool,
}

fn unwrap_legacy<T>(response: LegacyResponse<T>, request_id: &RequestId) -> Result<T, Error> {
    if response.status {
        return response.data.ok_or_else(|| {
            Error::contract(
                "successful legacy response omitted data",
                Some(request_id.clone()),
            )
        });
    }
    if let Some(error) = response.runtime_error {
        return Err(runtime_error(error, request_id.clone()));
    }
    Err(Error::contract(
        response
            .message
            .unwrap_or_else(|| "legacy API request failed".to_string()),
        Some(request_id.clone()),
    ))
}

fn unwrap_legacy_unit(
    response: LegacyResponse<serde_json::Value>,
    request_id: &RequestId,
) -> Result<(), Error> {
    if response.status {
        return Ok(());
    }
    if let Some(error) = response.runtime_error {
        return Err(runtime_error(error, request_id.clone()));
    }
    Err(Error::contract(
        response
            .message
            .unwrap_or_else(|| "legacy API request failed".to_string()),
        Some(request_id.clone()),
    ))
}

fn bounded_image(bytes: Vec<u8>) -> Result<Vec<u8>, Error> {
    if bytes.len() > 20 * 1024 * 1024 {
        return Err(Error::configuration(
            "download image",
            "image download exceeds the 20 MiB limit",
        ));
    }
    Ok(bytes)
}

#[derive(Deserialize)]
struct WorkspaceApprovalModeProfile {
    approval_mode: WorkspaceApprovalMode,
}

#[derive(Serialize)]
struct CreateSessionBody {
    workflow_id: u64,
    name: String,
    description: Option<String>,
    workflow_session_type: String,
    get_or_create: bool,
}

#[derive(Deserialize)]
struct LegacySession {
    id: u64,
    workflow_id: u64,
    name: String,
    description: Option<String>,
    workflow_session_type: String,
    app_id: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
    #[serde(default)]
    user_id: String,
    #[serde(default)]
    is_deleted: bool,
    #[serde(default)]
    active_workflow_instance_id: Option<u64>,
}

impl LegacySession {
    fn into_snapshot(self) -> SessionSnapshot {
        SessionSnapshot {
            session_id: SessionId(self.id),
            workflow_id: WorkflowId(self.workflow_id),
            created_at: self.created_at,
            updated_at: self.updated_at,
            actor_id: self.user_id,
            name: self.name,
            description: self.description,
            session_type: self.workflow_session_type,
            application_id: self.app_id,
            active_workflow_instance_id: self.active_workflow_instance_id.map(WorkflowInstanceId),
            deleted: self.is_deleted,
            extensions: Default::default(),
        }
    }
}

#[async_trait]
impl ProductBinding for HttpBinding {
    fn name(&self) -> &'static str {
        "http"
    }

    async fn authenticated_user(
        &self,
        options: RequestOptions,
    ) -> Result<AuthenticatedUser, Error> {
        let response: LegacyResponse<AuthenticatedUser> = self
            .execute_json(
                Method::GET,
                self.endpoint("api/user/profile")?,
                Arc::from([]),
                &options,
                ReplaySafety::Read,
                None,
                false,
            )
            .await?;
        unwrap_legacy(response, options.request_id())
    }

    async fn allocate_workspace_id(&self, options: RequestOptions) -> Result<u64, Error> {
        #[derive(Deserialize)]
        struct Response {
            workspace_id: u64,
        }
        let body = self.encode_body(&serde_json::json!({}), options.request_id())?;
        let response: LegacyResponse<Response> = self
            .execute_json(
                Method::POST,
                self.endpoint("api/user/client-workspaces/id")?,
                body,
                &options,
                ReplaySafety::NonIdempotentMutation,
                None,
                false,
            )
            .await?;
        Ok(unwrap_legacy(response, options.request_id())?.workspace_id)
    }

    async fn update_workspace_approval_mode(
        &self,
        application_id: &str,
        workspace_id: u64,
        approval_mode: WorkspaceApprovalMode,
        options: RequestOptions,
    ) -> Result<WorkspaceApprovalMode, Error> {
        let application_id = utf8_percent_encode(application_id.trim(), NON_ALPHANUMERIC);
        let path =
            format!("api/user/apps/{application_id}/workspaces/{workspace_id}/tool-approval-mode");
        let body = self.encode_body(
            &serde_json::json!({ "approval_mode": approval_mode }),
            options.request_id(),
        )?;
        let response: LegacyResponse<WorkspaceApprovalModeProfile> = self
            .execute_json(
                Method::PUT,
                self.endpoint(&path)?,
                body,
                &options,
                ReplaySafety::NonIdempotentMutation,
                None,
                false,
            )
            .await?;
        Ok(unwrap_legacy(response, options.request_id())?.approval_mode)
    }

    async fn list_workflow_records(
        &self,
        limit: u64,
        options: RequestOptions,
    ) -> Result<Vec<WorkflowRecord>, Error> {
        let mut url = self.endpoint("api/user/design/workflows")?;
        url.query_pairs_mut()
            .append_pair("limit", &limit.to_string())
            .append_pair("order", "desc");
        if let Some(application_id) = &self.state.application_scope {
            url.query_pairs_mut().append_pair("app_id", application_id);
        }
        let response: LegacyResponse<Vec<WorkflowRecord>> = self
            .execute_json(
                Method::GET,
                url,
                Arc::from([]),
                &options,
                ReplaySafety::Read,
                None,
                false,
            )
            .await?;
        unwrap_legacy(response, options.request_id())
    }

    async fn get_workflow_record(
        &self,
        workflow_id: u64,
        options: RequestOptions,
    ) -> Result<Option<WorkflowRecord>, Error> {
        let mut url = self.endpoint("api/user/design/workflows")?;
        url.query_pairs_mut()
            .append_pair("workflow_ids", &workflow_id.to_string())
            .append_pair("limit", "1")
            .append_pair("order", "desc");
        if let Some(application_id) = &self.state.application_scope {
            url.query_pairs_mut().append_pair("app_id", application_id);
        }
        let response: LegacyResponse<Vec<WorkflowRecord>> = self
            .execute_json(
                Method::GET,
                url,
                Arc::from([]),
                &options,
                ReplaySafety::Read,
                None,
                false,
            )
            .await?;
        Ok(unwrap_legacy(response, options.request_id())?
            .into_iter()
            .next())
    }

    async fn list_session_records(
        &self,
        workflow_id: Option<u64>,
        limit: u64,
        name: Option<&str>,
        options: RequestOptions,
    ) -> Result<Vec<SessionRecord>, Error> {
        let mut url = self.endpoint("api/user/sessions")?;
        if let Some(workflow_id) = workflow_id {
            url.query_pairs_mut()
                .append_pair("workflow_id", &workflow_id.to_string());
        }
        url.query_pairs_mut()
            .append_pair("limit", &limit.to_string())
            .append_pair("order", "desc");
        if let Some(name) = name {
            url.query_pairs_mut().append_pair("name", name);
        }
        let response: LegacyResponse<Vec<SessionRecord>> = self
            .execute_json(
                Method::GET,
                url,
                Arc::from([]),
                &options,
                ReplaySafety::Read,
                None,
                false,
            )
            .await?;
        unwrap_legacy(response, options.request_id())
    }

    async fn create_session_record(
        &self,
        workflow_id: u64,
        name: String,
        options: RequestOptions,
    ) -> Result<SessionRecord, Error> {
        let body = self.encode_body(
            &CreateSessionBody {
                workflow_id,
                name,
                description: None,
                workflow_session_type: "general".to_string(),
                get_or_create: true,
            },
            options.request_id(),
        )?;
        let response: LegacyResponse<SessionRecord> = self
            .execute_json(
                Method::POST,
                self.endpoint("api/user/sessions")?,
                body,
                &options,
                ReplaySafety::NonIdempotentMutation,
                None,
                false,
            )
            .await?;
        unwrap_legacy(response, options.request_id())
    }

    async fn rename_session_record(
        &self,
        session_id: u64,
        description: String,
        options: RequestOptions,
    ) -> Result<SessionRecord, Error> {
        let body = self.encode_body(
            &serde_json::json!({ "description": description }),
            options.request_id(),
        )?;
        let response: LegacyResponse<SessionRecord> = self
            .execute_json(
                Method::PATCH,
                self.endpoint(&format!("api/user/sessions/{session_id}"))?,
                body,
                &options,
                ReplaySafety::NonIdempotentMutation,
                None,
                false,
            )
            .await?;
        unwrap_legacy(response, options.request_id())
    }

    async fn delete_session_record(
        &self,
        session_id: u64,
        options: RequestOptions,
    ) -> Result<(), Error> {
        let response: LegacyResponse<serde_json::Value> = self
            .execute_json(
                Method::DELETE,
                self.endpoint(&format!("api/user/sessions/{session_id}"))?,
                Arc::from([]),
                &options,
                ReplaySafety::NonIdempotentMutation,
                None,
                false,
            )
            .await?;
        unwrap_legacy_unit(response, options.request_id())
    }

    async fn load_session_message_records(
        &self,
        session_id: u64,
        before_id: Option<u64>,
        limit: u64,
        conversation_scope_id: Option<u64>,
        options: RequestOptions,
    ) -> Result<Vec<SessionMessage>, Error> {
        let mut url = self.endpoint(&format!("api/user/sessions/{session_id}/messages"))?;
        url.query_pairs_mut()
            .append_pair("limit", &limit.to_string())
            .append_pair("order", if before_id.is_some() { "desc" } else { "asc" })
            .append_pair(
                "conversation_scope",
                &conversation_scope_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "main".to_string()),
            );
        if let Some(before_id) = before_id {
            url.query_pairs_mut()
                .append_pair("begin_id", &before_id.to_string());
        }
        let response: LegacyResponse<Vec<SessionMessage>> = self
            .execute_json(
                Method::GET,
                url,
                Arc::from([]),
                &options,
                ReplaySafety::Read,
                None,
                false,
            )
            .await?;
        unwrap_legacy(response, options.request_id())
    }

    async fn list_conversation_scope_records(
        &self,
        session_id: u64,
        options: RequestOptions,
    ) -> Result<Vec<ConversationScope>, Error> {
        let response: LegacyResponse<Vec<ConversationScope>> = self
            .execute_json(
                Method::GET,
                self.endpoint(&format!(
                    "api/user/sessions/{session_id}/conversation-scopes"
                ))?,
                Arc::from([]),
                &options,
                ReplaySafety::Read,
                None,
                false,
            )
            .await?;
        unwrap_legacy(response, options.request_id())
    }

    async fn load_agent_task_plan_records(
        &self,
        workflow_instance_id: u64,
        options: RequestOptions,
    ) -> Result<Vec<AgentTaskPlan>, Error> {
        let response: LegacyResponse<Vec<AgentTaskPlan>> = self
            .execute_json(
                Method::GET,
                self.endpoint(&format!(
                    "api/user/workflow-instances/{workflow_instance_id}/agent-plans"
                ))?,
                Arc::from([]),
                &options,
                ReplaySafety::Read,
                None,
                false,
            )
            .await?;
        unwrap_legacy(response, options.request_id())
    }

    async fn download_image(
        &self,
        source_url: &str,
        options: RequestOptions,
    ) -> Result<Vec<u8>, Error> {
        if let Some(payload) = source_url.strip_prefix("data:") {
            let (metadata, data) = payload
                .split_once(',')
                .ok_or_else(|| Error::configuration("download image", "invalid image data URL"))?;
            let bytes = if metadata
                .split(';')
                .any(|part| part.eq_ignore_ascii_case("base64"))
            {
                base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .map_err(|error| Error::configuration("download image", error.to_string()))?
            } else {
                percent_encoding::percent_decode_str(data)
                    .decode_utf8()
                    .map_err(|error| Error::configuration("download image", error.to_string()))?
                    .into_owned()
                    .into_bytes()
            };
            return bounded_image(bytes);
        }
        let url = Url::parse(source_url)
            .map_err(|error| Error::configuration("download image", error.to_string()))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(Error::configuration(
                "download image",
                "image URL must use http, https, or data",
            ));
        }
        let response = self
            .state
            .client
            .get(url)
            .timeout(self.attempt_timeout(&options)?)
            .send()
            .await
            .map_err(|error| {
                request_transport_error(error.to_string(), options.request_id().clone(), true)
            })?;
        if !response.status().is_success() {
            return Err(request_transport_error(
                format!("image server returned HTTP {}", response.status()),
                options.request_id().clone(),
                response.status().is_server_error(),
            ));
        }
        bounded_image(
            response
                .bytes()
                .await
                .map_err(|error| {
                    request_transport_error(error.to_string(), options.request_id().clone(), true)
                })?
                .to_vec(),
        )
    }

    async fn upload_asset(
        &self,
        upload: AssetUpload,
        options: RequestOptions,
    ) -> Result<AssetRef, Error> {
        let extension = upload
            .file_name
            .rsplit_once('.')
            .map(|(_, extension)| extension)
            .unwrap_or_default();
        if extension.is_empty()
            || extension.len() > 16
            || !extension
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
        {
            return Err(Error::configuration(
                "upload asset",
                "asset file name requires a short alphanumeric extension",
            ));
        }

        let mut policy_url = self.endpoint("api/user/osss/request-upload-url")?;
        policy_url
            .query_pairs_mut()
            .append_pair("file_name", &upload.file_name);
        let policy: LegacyResponse<UploadPolicy> = self
            .execute_json(
                Method::GET,
                policy_url,
                Arc::from([]),
                &options,
                ReplaySafety::Read,
                None,
                false,
            )
            .await?;
        let policy = unwrap_legacy(policy, options.request_id())?;

        let mut form = reqwest::multipart::Form::new();
        for (name, value) in &policy.form_fields {
            form = form.text(name.clone(), value.clone());
        }
        let part = reqwest::multipart::Part::bytes(upload.bytes)
            .file_name(upload.file_name.clone())
            .mime_str(&upload.media_type)
            .map_err(|error| Error::configuration("upload asset", error.to_string()))?;
        form = form.part("file", part);
        let method = policy
            .method
            .as_deref()
            .unwrap_or("POST")
            .parse::<Method>()
            .map_err(|error| Error::configuration("upload asset", error.to_string()))?;
        let mut upload_request = self.state.client.request(method, &policy.upload_url);
        for (name, value) in &policy.header_fields {
            if !name.eq_ignore_ascii_case("content-type")
                && !name.eq_ignore_ascii_case("content-length")
            {
                upload_request = upload_request.header(name, value);
            }
        }
        let upload_response = upload_request
            .multipart(form)
            .timeout(self.attempt_timeout(&options)?)
            .send()
            .await
            .map_err(|error| {
                request_transport_error(error.to_string(), options.request_id().clone(), false)
            })?;
        if !upload_response.status().is_success() {
            return Err(request_transport_error(
                format!("object storage returned HTTP {}", upload_response.status()),
                options.request_id().clone(),
                false,
            ));
        }
        let response_headers = upload_response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                Some(serde_json::json!({
                    "name": name.as_str(),
                    "value": value.to_str().ok()?,
                }))
            })
            .collect::<Vec<_>>();

        if policy.need_call_server {
            let application_id = policy
                .oss_key
                .rsplit_once('@')
                .map(|(_, application)| application.to_string())
                .or_else(|| self.state.application_scope.clone())
                .unwrap_or_default();
            let callback = serde_json::json!({
                "appId": application_id,
                "keyInfoFields": policy.key_info_fields,
                "headerFields": response_headers,
            });
            let body = self.encode_body(&callback, options.request_id())?;
            let response: LegacyResponse<serde_json::Value> = self
                .execute_json(
                    Method::POST,
                    self.endpoint("api/user/osss/upload-callback-notify")?,
                    body,
                    &options,
                    ReplaySafety::NonIdempotentMutation,
                    None,
                    false,
                )
                .await?;
            let _ = unwrap_legacy(response, options.request_id())?;
        }

        let mut download_url = self.endpoint("api/user/osss/request-download-url")?;
        download_url
            .query_pairs_mut()
            .append_pair("oss_key", &policy.oss_key)
            .append_pair("expire_seconds", "315360000");
        let response: LegacyResponse<String> = self
            .execute_json(
                Method::GET,
                download_url,
                Arc::from([]),
                &options,
                ReplaySafety::Read,
                None,
                false,
            )
            .await?;
        let url = unwrap_legacy(response, options.request_id())?;
        Ok(AssetRef {
            asset_id: policy.oss_key,
            url,
            media_type: upload.media_type,
            extensions: Default::default(),
        })
    }

    async fn publish_event(
        &self,
        event: PublishEvent,
        options: MutationOptions,
    ) -> Result<PublishReceipt, Error> {
        let url = self.endpoint("openapi/event-dispatch/v1/events")?;
        let body = self.encode_body(&event, options.request().request_id())?;
        self.execute_json(
            Method::POST,
            url,
            body,
            options.request(),
            ReplaySafety::IdempotentMutation,
            Some(options.idempotency_key().as_str()),
            false,
        )
        .await
    }

    async fn get_event(
        &self,
        event_id: EventId,
        options: RequestOptions,
    ) -> Result<Option<EventRecord>, Error> {
        let url = self.endpoint(&format!(
            "openapi/event-dispatch/v1/events/{}",
            event_id.get()
        ))?;
        self.execute_json(
            Method::GET,
            url,
            Arc::from([]),
            &options,
            ReplaySafety::Read,
            None,
            true,
        )
        .await
    }

    async fn start_workflow(
        &self,
        workflow_id: WorkflowId,
        session_id: Option<SessionId>,
        input: WorkflowInput,
        bindings: RuntimeBindings,
        options: MutationOptions,
    ) -> Result<WorkflowRunHandle, Error> {
        let mut url = self.endpoint(&format!(
            "{}/workflows/{}/run/async",
            self.api_root(),
            workflow_id.0
        ))?;
        if let Some(session_id) = session_id {
            url.query_pairs_mut()
                .append_pair("session_id", &session_id.0.to_string());
        }
        let body = self.encode_body(
            &StartWorkflowBody { input, bindings },
            options.request().request_id(),
        )?;
        self.execute_json(
            Method::POST,
            url,
            body,
            options.request(),
            ReplaySafety::IdempotentMutation,
            Some(options.idempotency_key().as_str()),
            false,
        )
        .await
    }

    async fn create_session(
        &self,
        workflow_id: WorkflowId,
        name: String,
        session_type: String,
        get_or_create: bool,
        options: RequestOptions,
    ) -> Result<SessionHandle, Error> {
        let body = self.encode_body(
            &CreateSessionBody {
                workflow_id: workflow_id.0,
                name,
                description: None,
                workflow_session_type: session_type,
                get_or_create,
            },
            options.request_id(),
        )?;
        let response: LegacyResponse<LegacySession> = self
            .execute_json(
                Method::POST,
                self.endpoint("api/user/sessions")?,
                body,
                &options,
                ReplaySafety::NonIdempotentMutation,
                None,
                false,
            )
            .await?;
        let session = unwrap_legacy(response, options.request_id())?;
        if session.workflow_id != workflow_id.0 {
            return Err(Error::contract(
                "Session creation returned another Workflow identity",
                Some(options.request_id().clone()),
            ));
        }
        Ok(SessionHandle {
            session_id: SessionId(session.id),
            workflow_id,
        })
    }

    async fn load_session(
        &self,
        session_id: SessionId,
        options: RequestOptions,
    ) -> Result<SessionSnapshot, Error> {
        let response: LegacyResponse<LegacySession> = self
            .execute_json(
                Method::GET,
                self.endpoint(&format!("api/user/sessions/{}", session_id.0))?,
                Arc::from([]),
                &options,
                ReplaySafety::Read,
                None,
                false,
            )
            .await?;
        let session = unwrap_legacy(response, options.request_id())?;
        if session.id != session_id.0 {
            return Err(Error::contract(
                "Session response identity does not match the requested Session",
                Some(options.request_id().clone()),
            ));
        }
        Ok(session.into_snapshot())
    }

    async fn signal_workflow(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        signal: WorkflowSignal,
        options: MutationOptions,
    ) -> Result<CommandAck, Error> {
        let (path, body) = match signal {
            WorkflowSignal::SuspensionResponse {
                suspension,
                response,
            } => (
                format!(
                    "{}/workflow-instances/{}/suspended-resume",
                    self.api_root(),
                    workflow_instance_id.0
                ),
                self.encode_body(
                    &ResumeWorkflowBody {
                        suspension,
                        payload: response,
                    },
                    options.request().request_id(),
                )?,
            ),
            signal => (
                format!(
                    "{}/workflow-instances/{}/append-message",
                    self.api_root(),
                    workflow_instance_id.0
                ),
                self.encode_body(
                    &SignalWorkflowBody { signal },
                    options.request().request_id(),
                )?,
            ),
        };
        self.execute_json(
            Method::POST,
            self.endpoint(&path)?,
            body,
            options.request(),
            ReplaySafety::IdempotentMutation,
            Some(options.idempotency_key().as_str()),
            false,
        )
        .await
    }

    async fn terminate_workflow(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        reason: Option<String>,
        wait_timeout_ms: Option<u64>,
        options: MutationOptions,
    ) -> Result<CommandAck, Error> {
        let path = format!(
            "{}/workflow-instances/{}/terminate",
            self.api_root(),
            workflow_instance_id.0
        );
        let body = self.encode_body(
            &TerminateWorkflowBody {
                reason,
                wait_timeout_ms,
            },
            options.request().request_id(),
        )?;
        self.execute_json(
            Method::POST,
            self.endpoint(&path)?,
            body,
            options.request(),
            ReplaySafety::IdempotentMutation,
            Some(options.idempotency_key().as_str()),
            false,
        )
        .await
    }

    async fn workflow_snapshot(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        options: RequestOptions,
    ) -> Result<WorkflowSnapshot, Error> {
        let path = format!(
            "{}/workflow-instances/{}",
            self.api_root(),
            workflow_instance_id.0
        );
        self.execute_json(
            Method::GET,
            self.endpoint(&path)?,
            Arc::from([]),
            &options,
            ReplaySafety::Read,
            None,
            false,
        )
        .await
    }

    async fn subscribe_workflow(
        &self,
        workflow_instance_id: WorkflowInstanceId,
        after: Option<EventCursor>,
        options: RequestOptions,
    ) -> Result<WorkflowEventStream, Error> {
        let path = format!(
            "{}/workflow-instances/{}/notifications/sse",
            self.api_root(),
            workflow_instance_id.0
        );
        let url = self.endpoint(&path)?;
        let attempt_counter = Arc::new(AtomicU32::new(1));
        let first = self
            .open_sse(
                url.clone(),
                after.as_ref(),
                &options,
                attempt_counter.clone(),
            )
            .await?;
        let binding = self.clone();
        let request_id = options.request_id().clone();
        let stream = async_stream::stream! {
            let mut state = SseReattachment::new(after);
            let mut response = first;
            loop {
                let mut parser = JsonSseParser::<WorkflowEvent>::new();
                let mut bytes = response.bytes_stream();
                let mut completed = false;
                let mut disconnect_error = None;
                while let Some(chunk) = bytes.next().await {
                    let chunk = match chunk {
                        Ok(chunk) => chunk,
                        Err(error) => {
                            disconnect_error = Some(Error::Transport(TransportFailure {
                                kind: TransportKind::Stream,
                                message: error.to_string(),
                                request_id: Some(request_id.clone()),
                                retryable: true,
                            }));
                            break;
                        }
                    };
                    for item in parser.push(&chunk) {
                        match item {
                            Ok(SseItem::Event(event)) => {
                                state.accept(event.cursor.clone());
                                yield Ok(event);
                            }
                            Ok(SseItem::Done) => {
                                completed = true;
                                break;
                            }
                            Err(error) => {
                                yield Err(Error::Protocol(ProtocolError {
                                    direction: ProtocolDirection::DecodeEvent,
                                    message: error.to_string(),
                                    request_id: Some(request_id.clone()),
                                }));
                                return;
                            }
                        }
                    }
                    if completed {
                        break;
                    }
                }
                if !completed && disconnect_error.is_none() {
                    for item in parser.finish() {
                        match item {
                            Ok(SseItem::Event(event)) => {
                                state.accept(event.cursor.clone());
                                yield Ok(event);
                            }
                            Ok(SseItem::Done) => completed = true,
                            Err(error) => {
                                yield Err(Error::Protocol(ProtocolError {
                                    direction: ProtocolDirection::DecodeEvent,
                                    message: error.to_string(),
                                    request_id: Some(request_id.clone()),
                                }));
                                return;
                            }
                        }
                    }
                }
                if completed {
                    return;
                }
                let error = disconnect_error.unwrap_or_else(|| Error::Transport(TransportFailure {
                    kind: TransportKind::Stream,
                    message: "SSE stream ended before a completion marker".to_string(),
                    request_id: Some(request_id.clone()),
                    retryable: true,
                }));
                let now = Utc::now();
                let jitter = rand::thread_rng().gen_range(0.5..=1.5);
                let delay = binding.state.retry.next_delay(
                    ReplaySafety::StreamReattachment,
                    state.connection_attempt(),
                    RetryReason::StreamDisconnected,
                    now,
                    options.deadline().map(|value| *value.as_datetime()),
                    jitter,
                );
                let Some(delay) = delay else {
                    yield Err(error);
                    return;
                };
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                state.reconnect();
                response = match binding
                    .open_sse(
                        url.clone(),
                        state.after(),
                        &options,
                        attempt_counter.clone(),
                    )
                    .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };
            }
        };
        Ok(Box::pin(stream))
    }

    async fn list_application_tasks(
        &self,
        query: ApplicationTaskQuery,
        options: RequestOptions,
    ) -> Result<Page<UserTaskSummary>, Error> {
        let url = self.application_task_url("openapi/user-tasks", &query)?;
        self.execute_json(
            Method::GET,
            url,
            Arc::from([]),
            &options,
            ReplaySafety::Read,
            None,
            false,
        )
        .await
    }

    async fn summarize_application_tasks(
        &self,
        query: ApplicationTaskQuery,
        options: RequestOptions,
    ) -> Result<ApplicationTaskSummary, Error> {
        let url = self.application_task_url("openapi/user-tasks/summary", &query)?;
        self.execute_json(
            Method::GET,
            url,
            Arc::from([]),
            &options,
            ReplaySafety::Read,
            None,
            false,
        )
        .await
    }

    async fn get_application_task(
        &self,
        task_id: UserTaskId,
        options: RequestOptions,
    ) -> Result<UserTask, Error> {
        let url = self.endpoint(&format!("openapi/user-tasks/{}", task_id.get()))?;
        self.execute_json(
            Method::GET,
            url,
            Arc::from([]),
            &options,
            ReplaySafety::Read,
            None,
            false,
        )
        .await
    }

    async fn retry_user_task_completion(
        &self,
        request: RetryUserTaskCompletion,
        options: RequestOptions,
    ) -> Result<UserTaskCompletionRetryReceipt, Error> {
        let url = self.endpoint(&format!(
            "openapi/user-tasks/{}/completion/retry",
            request.task_id.get()
        ))?;
        let body = self.encode_body(&request, options.request_id())?;
        self.execute_json(
            Method::POST,
            url,
            body,
            &options,
            ReplaySafety::NonIdempotentMutation,
            None,
            false,
        )
        .await
    }

    async fn list_current_user_tasks(
        &self,
        query: CurrentUserTaskQuery,
        options: RequestOptions,
    ) -> Result<Page<UserTaskSummary>, Error> {
        let url = self.current_user_task_url("api/user/user-tasks", &query)?;
        self.execute_json(
            Method::GET,
            url,
            Arc::from([]),
            &options,
            ReplaySafety::Read,
            None,
            false,
        )
        .await
    }

    async fn get_current_user_task(
        &self,
        task_id: UserTaskId,
        options: RequestOptions,
    ) -> Result<UserTask, Error> {
        let url = self.endpoint(&format!("api/user/user-tasks/{}", task_id.get()))?;
        self.execute_json(
            Method::GET,
            url,
            Arc::from([]),
            &options,
            ReplaySafety::Read,
            None,
            false,
        )
        .await
    }

    async fn act_on_user_task(
        &self,
        action: UserTaskAction,
        options: MutationOptions,
    ) -> Result<TaskActionReceipt, Error> {
        if &action.precondition().idempotency_key != options.idempotency_key() {
            return Err(Error::contract(
                "User Task action idempotency key does not match its mutation options",
                Some(options.request().request_id().clone()),
            ));
        }
        let task_id = action.precondition().task_id;
        let (method, suffix, body) = match action {
            UserTaskAction::Claim(request) => (
                Method::POST,
                "claim",
                self.encode_body(&request, options.request().request_id())?,
            ),
            UserTaskAction::MarkRead(request) => (
                Method::POST,
                "read",
                self.encode_body(&request, options.request().request_id())?,
            ),
            UserTaskAction::SaveDraft(request) => (
                Method::PUT,
                "draft",
                self.encode_body(&request, options.request().request_id())?,
            ),
            UserTaskAction::Submit(request) => (
                Method::POST,
                "submit",
                self.encode_body(&request, options.request().request_id())?,
            ),
        };
        let url = self.endpoint(&format!("api/user/user-tasks/{}/{suffix}", task_id.get()))?;
        self.execute_json(
            method,
            url,
            body,
            options.request(),
            ReplaySafety::IdempotentMutation,
            Some(options.idempotency_key().as_str()),
            false,
        )
        .await
    }
}

pub(crate) fn decode_application_error(
    status: u16,
    body: &[u8],
    retry_after: Option<&str>,
    request_id: RequestId,
    now: DateTime<Utc>,
) -> Error {
    match serde_json::from_slice::<ApplicationProblem>(body) {
        Ok(problem) => Error::Application(ApplicationFailure::new(problem).with_http_context(
            status,
            retry_after.and_then(|value| parse_retry_after(value, now)),
        )),
        Err(application_error) => match serde_json::from_slice::<RuntimeError>(body) {
            Ok(runtime) => match runtime_error(runtime, request_id) {
                Error::Application(failure) => Error::Application(
                    failure.with_http_context(
                        status,
                        retry_after.and_then(|value| parse_retry_after(value, now)),
                    ),
                ),
                other => other,
            },
            Err(runtime_error) => Error::Protocol(ProtocolError {
                direction: ProtocolDirection::DecodeResponse,
                message: format!(
                    "HTTP {status} did not contain an application problem: {application_error}; legacy runtime error decode also failed: {runtime_error}"
                ),
                request_id: Some(request_id),
            }),
        },
    }
}

pub(crate) fn request_transport_error(
    message: impl Into<String>,
    request_id: RequestId,
    retryable: bool,
) -> Error {
    Error::Transport(TransportFailure {
        kind: TransportKind::Request,
        message: message.into(),
        request_id: Some(request_id),
        retryable,
    })
}

fn parse_retry_after(value: &str, now: DateTime<Utc>) -> Option<Duration> {
    let value = value.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let retry_at = httpdate::parse_http_date(value).ok()?;
    retry_at.duration_since(SystemTime::from(now)).ok()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn now() -> DateTime<Utc> {
        "2026-09-06T08:00:00Z".parse().unwrap()
    }

    #[test]
    fn application_problem_decoder_preserves_unknown_codes_and_http_context() {
        let body = serde_json::to_vec(&json!({
            "code": "future_domain.limit_changed",
            "message": "a newer server returned a new problem",
            "retryable": true,
            "details": {"limit": 17},
            "request_id": "server-request-1"
        }))
        .unwrap();
        let error = decode_application_error(
            429,
            &body,
            Some("7"),
            RequestId::from("client-request-1"),
            now(),
        );
        let Error::Application(failure) = error else {
            panic!("expected application failure");
        };
        assert_eq!(failure.problem.code.as_str(), "future_domain.limit_changed");
        assert_eq!(failure.problem.request_id.as_ref(), "server-request-1");
        assert_eq!(failure.problem.details, Some(json!({"limit": 17})));
        assert!(failure.problem.retryable);
        assert_eq!(failure.http_status, Some(429));
        assert_eq!(failure.retry_after, Some(Duration::from_secs(7)));
    }

    #[test]
    fn retry_after_http_date_and_malformed_problem_have_typed_results() {
        let retry_after =
            httpdate::fmt_http_date(SystemTime::from(now()) + Duration::from_secs(11));
        let body = br#"{
            "code":"unavailable",
            "message":"try later",
            "retryable":true,
            "request_id":"server-request-2"
        }"#;
        let Error::Application(failure) = decode_application_error(
            503,
            body,
            Some(&retry_after),
            RequestId::from("client-request-2"),
            now(),
        ) else {
            panic!("expected application failure");
        };
        assert_eq!(failure.retry_after, Some(Duration::from_secs(11)));

        let malformed = decode_application_error(
            500,
            b"not-json",
            None,
            RequestId::from("client-request-3"),
            now(),
        );
        assert!(matches!(
            malformed,
            Error::Protocol(ProtocolError {
                direction: ProtocolDirection::DecodeResponse,
                request_id: Some(request_id),
                ..
            }) if request_id.as_ref() == "client-request-3"
        ));
    }

    #[test]
    fn transport_errors_preserve_request_identity_and_retry_classification() {
        let error = request_transport_error(
            "connection reset",
            RequestId::from("client-request-4"),
            true,
        );
        assert!(matches!(
            error,
            Error::Transport(TransportFailure {
                kind: TransportKind::Request,
                request_id: Some(request_id),
                retryable: true,
                ..
            }) if request_id.as_ref() == "client-request-4"
        ));
    }
}
