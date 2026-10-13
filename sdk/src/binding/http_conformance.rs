use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures::StreamExt;
use kish_lingshu_event_dispatch_contract::{
    DeliveryTime, DynamicEvent, EventId, EventRoute, EventVisibility, PublishEvent,
};
use kish_lingshu_runtime_contract::{
    ApplicationProblem, ApplicationResult, CommandDisposition, EventCursor,
    EventId as WorkflowEventId, IdempotencyKey, InvocationSource, MutationDisposition,
    PrincipalKind, ProductRuntimeFacade, RequestContext, RootWorkflowInstanceId, RuntimeBindings,
    RuntimeError, RuntimeErrorCode, RuntimeResult, SessionId, SignalWorkflowRequest,
    StartWorkflowRequest, SubscribeWorkflowRequest, TerminateWorkflowRequest,
    TrustedContextFactory, WorkflowContext, WorkflowEvent, WorkflowEventKind, WorkflowId,
    WorkflowInput, WorkflowInstanceId, WorkflowSignal, WorkflowSnapshotRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{BindingRef, HttpBinding, ProductBinding, ProductRuntimeBinding};
use crate::{
    auth::ClientCredential, ClientConfig, MutationOptions, RequestOptions, ServiceCredential,
    UserCredential,
};

#[derive(Debug, Eq, PartialEq)]
struct BindingContractReport {
    event_id: u64,
    workflow_id: u64,
    workflow_instance_id: u64,
    signal_cursor: String,
}

#[tokio::test]
async fn http_and_product_runtime_bindings_pass_the_same_capability_contract() {
    let in_process_fixture =
        kish_lingshu_runtime_contract::test_support::ContractProductRuntimeFixture::default();
    let in_process = binding_pair(in_process_fixture.facade);
    let expected = assert_binding_contract(in_process.0, in_process.1).await;

    let http_fixture =
        kish_lingshu_runtime_contract::test_support::ContractProductRuntimeFixture::default();
    let server = MockProductRuntimeServer::start(http_fixture.facade).await;
    let remote = http_binding_pair(server.endpoint());
    let actual = assert_binding_contract(remote.0, remote.1).await;

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn public_http_instance_termination_does_not_require_an_observer_handle() {
    let fixture =
        kish_lingshu_runtime_contract::test_support::ContractProductRuntimeFixture::default();
    let server = MockProductRuntimeServer::start(fixture.facade).await;
    let client = crate::ClientBuilder::new(
        ClientConfig::new(server.endpoint()).with_selected_application("contract-app"),
    )
    .user_credential(UserCredential::new("user-secret").unwrap())
    .connect()
    .unwrap();
    let started = client
        .workflows()
        .start(
            WorkflowId(42),
            crate::workflow::WorkflowStart::structured(json!({"prompt":"terminate"}))
                .in_session(SessionId(7)),
            MutationOptions::new("detached/start").unwrap(),
        )
        .await
        .unwrap();
    let id = started.handle().workflow_instance_id;
    drop(started);
    assert!(matches!(
        client
            .workflows()
            .terminate(
                WorkflowInstanceId(0),
                None,
                MutationOptions::new("detached/zero").unwrap()
            )
            .await,
        Err(crate::Error::Configuration(_))
    ));
    let ack = client
        .workflows()
        .terminate(
            id,
            Some("durable cancellation".into()),
            MutationOptions::new("detached/terminate").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ack.workflow_instance_id, id);
    let snapshot = client
        .workflows()
        .snapshot(id, RequestOptions::new())
        .await
        .unwrap();
    assert_eq!(
        snapshot.state,
        kish_lingshu_runtime_contract::WorkflowState::Terminated
    );
}

#[tokio::test]
async fn public_http_workflow_start_replays_and_rejects_changed_input() {
    let fixture =
        kish_lingshu_runtime_contract::test_support::ContractProductRuntimeFixture::default();
    let server = MockProductRuntimeServer::start(fixture.facade).await;
    let client = crate::ClientBuilder::new(
        ClientConfig::new(server.endpoint()).with_selected_application("contract-app"),
    )
    .user_credential(UserCredential::new("user-secret").unwrap())
    .connect()
    .unwrap();
    let workflow = client.workflows().select(WorkflowId(42)).unwrap();

    let first = workflow
        .start(
            crate::workflow::WorkflowStart::structured(json!({"prompt": "same"}))
                .in_session(SessionId(7)),
            MutationOptions::new("public-http/workflow/42/start").unwrap(),
        )
        .await
        .unwrap();
    let replay = workflow
        .start(
            crate::workflow::WorkflowStart::structured(json!({"prompt": "same"}))
                .in_session(SessionId(7)),
            MutationOptions::new("public-http/workflow/42/start").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replay.handle(), first.handle());

    let error = match workflow
        .start(
            crate::workflow::WorkflowStart::structured(json!({"prompt": "changed"}))
                .in_session(SessionId(7)),
            MutationOptions::new("public-http/workflow/42/start").unwrap(),
        )
        .await
    {
        Ok(_) => panic!("changed HTTP start unexpectedly replayed the original Workflow"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        crate::Error::Application(failure) if failure.problem.code.as_str() == "conflict"
    ));
}

#[tokio::test]
async fn public_http_event_publication_replays_and_rejects_changed_payload() {
    let fixture =
        kish_lingshu_runtime_contract::test_support::ContractProductRuntimeFixture::default();
    let server = MockProductRuntimeServer::start(fixture.facade).await;
    let client = crate::ClientBuilder::new(ClientConfig::new(server.endpoint()))
        .service_credential(ServiceCredential::new("contract-app", "service-secret").unwrap())
        .connect()
        .unwrap();
    let events = client.event_dispatch();
    let route = EventRoute::new("contract.orders", "contract.order.created", "1").unwrap();
    let mut event = PublishEvent::dynamic(
        "contract-checkout",
        DynamicEvent::new(route, json!({"order_id": 42})).unwrap(),
    );
    event.delivery = DeliveryTime::after(std::time::Duration::from_secs(5));

    let accepted = events
        .publish(
            event.clone(),
            MutationOptions::new("public-http/event/order-42").unwrap(),
        )
        .await
        .unwrap();
    let replay = events
        .publish(
            event.clone(),
            MutationOptions::new("public-http/event/order-42").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replay.event_id, accepted.event_id);
    assert_eq!(replay.mutation.disposition, MutationDisposition::Duplicate);

    let observed = events
        .get(accepted.event_id, RequestOptions::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.payload, json!({"order_id": 42}));

    event.payload = json!({"order_id": 43});
    let error = events
        .publish(
            event,
            MutationOptions::new("public-http/event/order-42").unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        crate::Error::Application(failure) if failure.problem.code.as_str() == "conflict"
    ));
}

#[tokio::test]
async fn http_sse_reattaches_after_the_last_accepted_cursor_with_stable_logical_ids() {
    let state = Arc::new(ReconnectServerState::default());
    let app = Router::new()
        .route(
            "/api/user/workflow-instances/{instance_id}/notifications/sse",
            get(reconnecting_sse),
        )
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let config = ClientConfig::new(format!("http://{address}/"))
        .with_selected_application("contract-app")
        .with_retry_limit(2);
    let binding = HttpBinding::new(
        &config,
        ClientCredential::User(UserCredential::new("user-secret").unwrap()),
        PrincipalKind::User,
    )
    .unwrap();
    let events = binding
        .subscribe_workflow(
            WorkflowInstanceId(9_001),
            Some(EventCursor::from("reconnect:0")),
            RequestOptions::new(),
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    task.abort();

    assert_eq!(
        events
            .iter()
            .map(|event| event.cursor.as_ref())
            .collect::<Vec<_>>(),
        ["reconnect:1", "reconnect:2"]
    );
    let requests = state.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].last_event_id, "reconnect:0");
    assert_eq!(requests[1].last_event_id, "reconnect:1");
    assert_eq!(requests[0].request_id, requests[1].request_id);
    assert_eq!(requests[0].correlation_id, requests[1].correlation_id);
    assert_eq!(requests[0].attempt, 1);
    assert_eq!(requests[1].attempt, 2);
}

#[derive(Default)]
struct ReconnectServerState {
    connections: AtomicUsize,
    requests: Mutex<Vec<ReconnectRequest>>,
}

struct ReconnectRequest {
    last_event_id: String,
    request_id: String,
    correlation_id: String,
    attempt: u32,
}

async fn reconnecting_sse(
    State(state): State<Arc<ReconnectServerState>>,
    headers: HeaderMap,
    Path(instance_id): Path<u64>,
) -> Response {
    assert_eq!(instance_id, 9_001);
    request_context(&headers, PrincipalKind::User);
    let connection = state.connections.fetch_add(1, Ordering::SeqCst) + 1;
    let last_event_id = headers
        .get("last-event-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    state.requests.lock().unwrap().push(ReconnectRequest {
        last_event_id,
        request_id: headers
            .get("x-request-id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string(),
        correlation_id: headers
            .get("x-correlation-id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string(),
        attempt: headers
            .get("x-kish-transport-attempt")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap(),
    });
    let event = WorkflowEvent {
        event_id: WorkflowEventId::from(format!("reconnect-event-{connection}")),
        cursor: EventCursor::from(format!("reconnect:{connection}")),
        workflow_id: WorkflowId(42),
        workflow_instance_id: WorkflowInstanceId(instance_id),
        root_workflow_instance_id: RootWorkflowInstanceId(instance_id),
        execution_id: None,
        execution_path: None,
        session_id: None,
        message_id: None,
        tool_call_id: None,
        plan_id: None,
        timestamp: "2026-09-06T08:00:00Z".to_string(),
        kind: WorkflowEventKind::Extension {
            kind: "reconnect_test".to_string(),
            payload: json!({"connection": connection}),
        },
    };
    let mut body = format!("data: {}\n\n", serde_json::to_string(&event).unwrap());
    if connection == 2 {
        body.push_str("data: [DONE]\n\n");
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(body))
        .unwrap()
}

fn binding_pair(facade: ProductRuntimeFacade) -> (BindingRef, BindingRef) {
    let service = Arc::new(ProductRuntimeBinding::new(
        facade.clone(),
        context_factory(PrincipalKind::Service),
    ));
    let user = Arc::new(ProductRuntimeBinding::new(
        facade,
        context_factory(PrincipalKind::User),
    ));
    (service, user)
}

fn http_binding_pair(endpoint: String) -> (BindingRef, BindingRef) {
    let service_config = ClientConfig::new(&endpoint).with_retry_limit(1);
    let service = HttpBinding::new(
        &service_config,
        ClientCredential::Service(
            ServiceCredential::new("contract-app", "service-secret").unwrap(),
        ),
        PrincipalKind::Service,
    )
    .unwrap();
    let user_config = ClientConfig::new(endpoint)
        .with_selected_application("contract-app")
        .with_retry_limit(1);
    let user = HttpBinding::new(
        &user_config,
        ClientCredential::User(UserCredential::new("user-secret").unwrap()),
        PrincipalKind::User,
    )
    .unwrap();
    (Arc::new(service), Arc::new(user))
}

fn context_factory(principal: PrincipalKind) -> TrustedContextFactory {
    TrustedContextFactory::new(
        match principal {
            PrincipalKind::Service => "contract-service",
            PrincipalKind::User => "contract-actor",
            PrincipalKind::Internal => unreachable!(),
        },
        principal,
        Some("contract-app".to_string()),
        InvocationSource::EmbeddedSdk,
    )
    .unwrap()
}

async fn assert_binding_contract(service: BindingRef, user: BindingRef) -> BindingContractReport {
    let mut event = PublishEvent::dynamic(
        "contract-checkout",
        DynamicEvent::new(
            EventRoute::new("contract.orders", "contract.order.created", "1").unwrap(),
            json!({"order_id": 42}),
        )
        .unwrap(),
    );
    event.delivery = DeliveryTime::after(std::time::Duration::from_secs(5));
    let accepted = service
        .publish_event(
            event.clone(),
            MutationOptions::new("contract/event/order-42").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.visibility, EventVisibility::Delayed);
    let observed = service
        .get_event(accepted.event_id, RequestOptions::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.payload, json!({"order_id": 42}));
    let duplicate = service
        .publish_event(
            event.clone(),
            MutationOptions::new("contract/event/order-42").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        duplicate.mutation.disposition,
        MutationDisposition::Duplicate
    );
    event.payload = json!({"order_id": 43});
    let conflict = service
        .publish_event(
            event,
            MutationOptions::new("contract/event/order-42").unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        conflict,
        crate::Error::Application(failure) if failure.problem.code.as_str() == "conflict"
    ));

    let run = user
        .start_workflow(
            WorkflowId(42),
            Some(SessionId(7)),
            WorkflowInput::structured(json!({"prompt": "contract"})),
            RuntimeBindings::default(),
            MutationOptions::new("contract/workflow/start").unwrap(),
        )
        .await
        .unwrap();
    let replayed_run = user
        .start_workflow(
            WorkflowId(42),
            Some(SessionId(7)),
            WorkflowInput::structured(json!({"prompt": "contract"})),
            RuntimeBindings::default(),
            MutationOptions::new("contract/workflow/start").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replayed_run, run);
    let start_conflict = user
        .start_workflow(
            WorkflowId(42),
            Some(SessionId(7)),
            WorkflowInput::structured(json!({"prompt": "changed"})),
            RuntimeBindings::default(),
            MutationOptions::new("contract/workflow/start").unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        start_conflict,
        crate::Error::Application(failure) if failure.problem.code.as_str() == "conflict"
    ));
    let snapshot = user
        .workflow_snapshot(run.workflow_instance_id, RequestOptions::new())
        .await
        .unwrap();
    assert_eq!(snapshot.workflow_id, WorkflowId(42));
    let initial_events = user
        .subscribe_workflow(
            run.workflow_instance_id,
            Some(run.subscription_anchor.clone()),
            RequestOptions::new(),
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert!(initial_events.into_iter().all(|event| event.is_ok()));
    let signal = user
        .signal_workflow(
            run.workflow_instance_id,
            WorkflowSignal::Business {
                name: "contract_progress".to_string(),
                payload: json!({"step": 1}),
            },
            MutationOptions::new("contract/workflow/signal").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(signal.disposition, CommandDisposition::Accepted);
    let signal_cursor = signal.cursor.unwrap();
    let events = user
        .subscribe_workflow(
            run.workflow_instance_id,
            Some(run.subscription_anchor.clone()),
            RequestOptions::new(),
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(events.last().unwrap().cursor, signal_cursor);
    let terminated = user
        .terminate_workflow(
            run.workflow_instance_id,
            Some("contract complete".to_string()),
            None,
            MutationOptions::new("contract/workflow/terminate").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(terminated.disposition, CommandDisposition::Terminated);

    BindingContractReport {
        event_id: accepted.event_id.get(),
        workflow_id: run.workflow_id.0,
        workflow_instance_id: run.workflow_instance_id.0,
        signal_cursor: signal_cursor.to_string(),
    }
}

struct MockProductRuntimeServer {
    address: std::net::SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl MockProductRuntimeServer {
    async fn start(facade: ProductRuntimeFacade) -> Self {
        let app = Router::new()
            .route("/openapi/event-dispatch/v1/events", post(publish_event))
            .route(
                "/openapi/event-dispatch/v1/events/{event_id}",
                get(get_event),
            )
            .route(
                "/api/user/workflows/{workflow_id}/run/async",
                post(start_workflow),
            )
            .route(
                "/api/user/workflow-instances/{instance_id}",
                get(workflow_snapshot),
            )
            .route(
                "/api/user/workflow-instances/{instance_id}/append-message",
                post(signal_workflow),
            )
            .route(
                "/api/user/workflow-instances/{instance_id}/terminate",
                post(terminate_workflow),
            )
            .route(
                "/api/user/workflow-instances/{instance_id}/notifications/sse",
                get(subscribe_workflow),
            )
            .with_state(facade);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self { address, task }
    }

    fn endpoint(&self) -> String {
        format!("http://{}/", self.address)
    }
}

impl Drop for MockProductRuntimeServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn request_context(headers: &HeaderMap, principal: PrincipalKind) -> RequestContext {
    match principal {
        PrincipalKind::Service => {
            let authorization = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap();
            assert_eq!(authorization, "Bearer service-secret");
            assert_eq!(
                headers
                    .get("x-kish-app-id")
                    .and_then(|value| value.to_str().ok()),
                Some("contract%2Dapp")
            );
        }
        PrincipalKind::User => {
            assert_eq!(
                headers
                    .get("x-kish-token-key")
                    .and_then(|value| value.to_str().ok()),
                Some("x-token")
            );
            assert_eq!(
                headers.get("x-token").and_then(|value| value.to_str().ok()),
                Some("user-secret")
            );
            assert!(headers.contains_key("x-kish-app-id"));
        }
        PrincipalKind::Internal => unreachable!(),
    }
    let request_id = headers.get("x-request-id").unwrap().to_str().unwrap();
    let correlation_id = headers.get("x-correlation-id").unwrap().to_str().unwrap();
    TrustedContextFactory::new(
        match principal {
            PrincipalKind::Service => "contract-service",
            PrincipalKind::User => "contract-actor",
            PrincipalKind::Internal => unreachable!(),
        },
        principal,
        Some("contract-app".to_string()),
        InvocationSource::Http,
    )
    .unwrap()
    .request_context(request_id, correlation_id)
    .unwrap()
}

fn status_for_runtime(error: &RuntimeError) -> StatusCode {
    match error.code {
        RuntimeErrorCode::InvalidRequest => StatusCode::BAD_REQUEST,
        RuntimeErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
        RuntimeErrorCode::Forbidden => StatusCode::FORBIDDEN,
        RuntimeErrorCode::NotFound => StatusCode::NOT_FOUND,
        RuntimeErrorCode::Conflict => StatusCode::CONFLICT,
        RuntimeErrorCode::StaleSuspension | RuntimeErrorCode::ReplayUnavailable => StatusCode::GONE,
        RuntimeErrorCode::Unsupported => StatusCode::UNPROCESSABLE_ENTITY,
        RuntimeErrorCode::Connectivity | RuntimeErrorCode::Unavailable => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        RuntimeErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn status_for_problem(error: &ApplicationProblem) -> StatusCode {
    match error.code.as_str() {
        "invalid_request" => StatusCode::BAD_REQUEST,
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "forbidden" => StatusCode::FORBIDDEN,
        "not_found" => StatusCode::NOT_FOUND,
        "conflict" => StatusCode::CONFLICT,
        "unsupported" => StatusCode::UNPROCESSABLE_ENTITY,
        "unavailable" => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn runtime_response<T: Serialize>(result: RuntimeResult<T>) -> Response {
    match result {
        Ok(value) => Json(value).into_response(),
        Err(error) => (status_for_runtime(&error), Json(error)).into_response(),
    }
}

fn application_response<T: Serialize>(result: ApplicationResult<T>) -> Response {
    match result {
        Ok(value) => Json(value).into_response(),
        Err(error) => (status_for_problem(&error), Json(error)).into_response(),
    }
}

async fn publish_event(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Json(event): Json<PublishEvent>,
) -> Response {
    let context = request_context(&headers, PrincipalKind::Service);
    let key =
        IdempotencyKey::new(headers.get("idempotency-key").unwrap().to_str().unwrap()).unwrap();
    application_response(facade.event_publisher().publish(context, event, key).await)
}

async fn get_event(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(event_id): Path<u64>,
) -> Response {
    let context = request_context(&headers, PrincipalKind::Service);
    application_response(
        facade
            .event_publisher()
            .get(context, EventId::new(event_id).unwrap())
            .await,
    )
}

#[derive(Deserialize)]
struct StartWorkflowBody {
    input: WorkflowInput,
    #[serde(default)]
    bindings: RuntimeBindings,
}

async fn start_workflow(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(workflow_id): Path<u64>,
    Json(body): Json<StartWorkflowBody>,
) -> Response {
    let idempotency_key =
        IdempotencyKey::new(headers.get("idempotency-key").unwrap().to_str().unwrap()).unwrap();
    let context =
        request_context(&headers, PrincipalKind::User).with_idempotency_key(idempotency_key);
    runtime_response(
        facade
            .workflow()
            .start(StartWorkflowRequest {
                context: WorkflowContext::new(context, WorkflowId(workflow_id)),
                session_id: Some(SessionId(7)),
                input: body.input,
                bindings: body.bindings,
            })
            .await,
    )
}

async fn workflow_snapshot(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(instance_id): Path<u64>,
) -> Response {
    let context = request_context(&headers, PrincipalKind::User);
    runtime_response(
        facade
            .workflow()
            .snapshot(WorkflowSnapshotRequest {
                context,
                workflow_instance_id: WorkflowInstanceId(instance_id),
            })
            .await,
    )
}

#[derive(Deserialize)]
struct SignalWorkflowBody {
    signal: WorkflowSignal,
}

async fn signal_workflow(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(instance_id): Path<u64>,
    Json(body): Json<SignalWorkflowBody>,
) -> Response {
    let idempotency_key =
        IdempotencyKey::new(headers.get("idempotency-key").unwrap().to_str().unwrap()).unwrap();
    let context =
        request_context(&headers, PrincipalKind::User).with_idempotency_key(idempotency_key);
    runtime_response(
        facade
            .workflow()
            .signal(SignalWorkflowRequest {
                context,
                workflow_instance_id: WorkflowInstanceId(instance_id),
                signal: body.signal,
            })
            .await,
    )
}

#[derive(Deserialize)]
struct TerminateWorkflowBody {
    reason: Option<String>,
    wait_timeout_ms: Option<u64>,
}

async fn terminate_workflow(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(instance_id): Path<u64>,
    Json(body): Json<TerminateWorkflowBody>,
) -> Response {
    let idempotency_key =
        IdempotencyKey::new(headers.get("idempotency-key").unwrap().to_str().unwrap()).unwrap();
    let context =
        request_context(&headers, PrincipalKind::User).with_idempotency_key(idempotency_key);
    runtime_response(
        facade
            .workflow()
            .terminate(TerminateWorkflowRequest {
                context,
                workflow_instance_id: WorkflowInstanceId(instance_id),
                reason: body.reason,
                wait_timeout_ms: body.wait_timeout_ms,
            })
            .await,
    )
}

async fn subscribe_workflow(
    State(facade): State<ProductRuntimeFacade>,
    headers: HeaderMap,
    Path(instance_id): Path<u64>,
) -> Response {
    let context = request_context(&headers, PrincipalKind::User);
    let after = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .map(EventCursor::from);
    let stream = match facade
        .workflow()
        .subscribe(SubscribeWorkflowRequest {
            context,
            workflow_instance_id: WorkflowInstanceId(instance_id),
            after,
        })
        .await
    {
        Ok(stream) => stream,
        Err(error) => return runtime_response::<()>(Err(error)),
    };
    let events = match stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<RuntimeResult<Vec<_>>>()
    {
        Ok(events) => events,
        Err(error) => return runtime_response::<()>(Err(error)),
    };
    let mut body = String::new();
    for event in events {
        body.push_str("id: ");
        body.push_str(event.cursor.as_ref());
        body.push_str("\ndata: ");
        body.push_str(&serde_json::to_string(&event).unwrap());
        body.push_str("\n\n");
    }
    body.push_str("data: [DONE]\n\n");
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .body(Body::from(body))
        .unwrap()
}

#[tokio::test]
async fn human_task_http_binding_uses_unified_routes_and_wire_fields() {
    use crate::human_tasks::{HumanTaskAction, HumanTaskCommand};
    let snapshot = json!({
        "application_id":"contract-app", "workflow_instance_id":42,
        "task_id":"draft-1", "task_type":"approval.v1", "task_revision":1,
        "node_kind":"draft", "node_id":"draft", "execution_seq":1, "phase":"waiting",
        "external_ref":null, "active_command_id":null, "supported_actions":["save"], "problem":null
    });
    let receipt = json!({
        "application_id":"contract-app", "workflow_instance_id":42, "task_id":"draft-1",
        "command_id":"command-1", "operation_id":"operation-1", "task_revision":2, "state":"accepted"
    });
    let list = json!([snapshot.clone()]);
    let status =
        json!({"receipt":receipt.clone(), "state":"applied", "action":"save", "problem":null});
    let retry_receipt = receipt.clone();
    let routes = Router::new()
        .route(
            "/openapi/workflow-instances/42/human-tasks",
            get(move || async move { Json(list) }),
        )
        .route(
            "/openapi/workflow-instances/42/human-tasks/draft-1",
            get(move || async move { Json(snapshot) }),
        )
        .route(
            "/openapi/workflow-instances/42/human-tasks/draft-1/commands",
            post(move |Json(command): Json<HumanTaskCommand>| async move {
                assert_eq!(command.action, HumanTaskAction::Save);
                assert_eq!(command.expected_revision, 1);
                (StatusCode::ACCEPTED, Json(receipt))
            }),
        )
        .route(
            "/openapi/workflow-instances/42/human-tasks/draft-1/commands/command-1",
            get(move || async move { Json(status) }),
        )
        .route(
            "/openapi/workflow-instances/42/human-tasks/draft-1/commands/command-1/retry",
            post(move || async move { (StatusCode::ACCEPTED, Json(retry_receipt)) }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, routes).await.unwrap();
    });
    let user = crate::ClientBuilder::new(
        ClientConfig::new(&endpoint).with_selected_application("contract-app"),
    )
    .user_credential(UserCredential::new("user-secret").unwrap())
    .connect()
    .unwrap();
    let tasks = user.human_tasks();
    let instance = WorkflowInstanceId(42);
    assert_eq!(
        tasks.list(instance, RequestOptions::new()).await.unwrap()[0].task_type,
        "approval.v1"
    );
    assert_eq!(
        tasks
            .get(instance, "draft-1", RequestOptions::new())
            .await
            .unwrap()
            .task_revision,
        1
    );
    let command = HumanTaskCommand {
        action: HumanTaskAction::Save,
        expected_revision: 1,
        idempotency_key: IdempotencyKey::new("save-1").unwrap(),
        input: json!({}),
        expected_business_revision: None,
    };
    assert_eq!(
        tasks
            .command(instance, "draft-1", command, RequestOptions::new())
            .await
            .unwrap()
            .task_revision,
        2
    );
    assert_eq!(
        tasks
            .command_status(instance, "draft-1", "command-1", RequestOptions::new())
            .await
            .unwrap()
            .state,
        crate::human_tasks::HumanTaskCommandState::Applied
    );
    let service = crate::ClientBuilder::new(ClientConfig::new(&endpoint))
        .service_credential(ServiceCredential::new("contract-app", "service-secret").unwrap())
        .connect()
        .unwrap();
    assert_eq!(
        service
            .human_tasks()
            .retry(instance, "draft-1", "command-1", RequestOptions::new())
            .await
            .unwrap()
            .operation_id,
        "operation-1"
    );
    assert_eq!(
        reqwest::get(format!(
            "{endpoint}/openapi/workflow-instances/42/interactions"
        ))
        .await
        .unwrap()
        .status(),
        StatusCode::NOT_FOUND
    );
    server.abort();
}
