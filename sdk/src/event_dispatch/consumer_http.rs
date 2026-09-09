use std::{sync::Arc, time::Duration};

use axum::{
    extract::{DefaultBodyLimit, FromRef, Json, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use chrono::Utc;
use kish_lingshu_event_dispatch_contract::{
    DeliveryMode, InvocationV1, IDEMPOTENCY_HEADER, INVOCATION_CONTRACT_VERSION,
    INVOCATION_ID_HEADER,
};
use serde::Serialize;
use serde_json::{json, Value};

use super::{ConsumerError, ConsumerRegistry, EventContext};

#[derive(Debug, Clone)]
pub struct ConsumerHttpConfig {
    pub maximum_request_bytes: usize,
}

impl Default for ConsumerHttpConfig {
    fn default() -> Self {
        Self {
            maximum_request_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone)]
struct ConsumerHttpState {
    registry: Arc<ConsumerRegistry>,
}

impl FromRef<ConsumerHttpState> for Arc<ConsumerRegistry> {
    fn from_ref(state: &ConsumerHttpState) -> Self {
        state.registry.clone()
    }
}

/// Axum adapter for the synchronous Event Dispatch invocation protocol.
pub struct ConsumerHttpAdapter {
    registry: Arc<ConsumerRegistry>,
    config: ConsumerHttpConfig,
}

impl ConsumerHttpAdapter {
    pub fn new(registry: Arc<ConsumerRegistry>) -> Self {
        Self {
            registry,
            config: ConsumerHttpConfig::default(),
        }
    }

    pub fn with_config(mut self, config: ConsumerHttpConfig) -> Self {
        self.config = config;
        self
    }

    /// Returns a route rooted at `/`; applications normally nest it under a
    /// service-authenticated internal path.
    pub fn router(self) -> Router {
        let maximum_request_bytes = self.config.maximum_request_bytes;
        Router::new()
            .route("/", post(consume_event))
            .layer(DefaultBodyLimit::max(maximum_request_bytes))
            .with_state(ConsumerHttpState {
                registry: self.registry,
            })
    }
}

async fn consume_event(
    State(registry): State<Arc<ConsumerRegistry>>,
    headers: HeaderMap,
    invocation: Result<Json<InvocationV1>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(invocation) = match invocation {
        Ok(invocation) => invocation,
        Err(error) => {
            return protocol_error(
                StatusCode::BAD_REQUEST,
                "invalid_invocation_body",
                error.body_text(),
            );
        }
    };
    if let Err(error) = validate_invocation(&registry, &headers, &invocation) {
        return error;
    }
    let Some(consumer) = registry.find(&invocation) else {
        return protocol_error(
            StatusCode::NOT_FOUND,
            "event_consumer_not_registered",
            "no Event consumer is registered for the Event selector",
        );
    };
    let timeout = match (invocation.consumption.invocation_deadline - Utc::now()).to_std() {
        Ok(timeout) if !timeout.is_zero() => timeout,
        _ => return deadline_exceeded(),
    };
    let context = EventContext::from_invocation(&invocation);
    match tokio::time::timeout(timeout, consumer.consume(context, invocation.event.payload)).await {
        Ok(Ok(result)) => Json(json!({ "result": result })).into_response(),
        Ok(Err(error)) => consume_error_response(error),
        Err(_) => deadline_exceeded(),
    }
}

fn validate_invocation(
    registry: &ConsumerRegistry,
    headers: &HeaderMap,
    invocation: &InvocationV1,
) -> Result<(), Response> {
    if invocation.contract_version != INVOCATION_CONTRACT_VERSION {
        return Err(protocol_error(
            StatusCode::BAD_REQUEST,
            "unsupported_contract_version",
            "unsupported Event invocation contract version",
        ));
    }
    if invocation.consumption.mode != DeliveryMode::Sync || invocation.completion.is_some() {
        return Err(protocol_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "unsupported_delivery_mode",
            "the Rust Event consumer SDK supports synchronous delivery only",
        ));
    }
    if invocation.event.app_id != registry.app_id() {
        return Err(protocol_error(
            StatusCode::NOT_FOUND,
            "event_consumer_not_registered",
            "no Event consumer is registered for the Event selector",
        ));
    }
    if invocation.event.event_id == 0
        || invocation.consumption.consumption_id == 0
        || invocation.consumption.subscription_id == 0
        || invocation.consumption.group_id == 0
        || invocation.consumption.invocation_id == 0
        || invocation.consumption.attempt_generation == 0
        || invocation.consumption.idempotency_key.trim().is_empty()
    {
        return Err(protocol_error(
            StatusCode::BAD_REQUEST,
            "invalid_invocation_identity",
            "Event invocation identity is incomplete",
        ));
    }
    let idempotency_key = required_header(headers, IDEMPOTENCY_HEADER)?;
    if idempotency_key != invocation.consumption.idempotency_key {
        return Err(protocol_error(
            StatusCode::BAD_REQUEST,
            "idempotency_key_mismatch",
            "Idempotency-Key does not match the invocation body",
        ));
    }
    let invocation_id = required_header(headers, INVOCATION_ID_HEADER)?;
    if invocation_id != invocation.consumption.invocation_id.to_string() {
        return Err(protocol_error(
            StatusCode::BAD_REQUEST,
            "invocation_id_mismatch",
            "X-Event-Invocation-Id does not match the invocation body",
        ));
    }
    if invocation.consumption.invocation_deadline <= Utc::now() {
        return Err(deadline_exceeded());
    }
    Ok(())
}

fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String, Response> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            protocol_error(
                StatusCode::BAD_REQUEST,
                "missing_invocation_header",
                format!("{name} header is required"),
            )
        })
}

#[derive(Serialize)]
struct ErrorResponse {
    code: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<Value>,
}

fn consume_error_response(error: ConsumerError) -> Response {
    let (status, code, message, retry_after, details) = match error {
        ConsumerError::Retryable {
            code,
            message,
            retry_after,
            details,
        } => (
            StatusCode::SERVICE_UNAVAILABLE,
            code,
            message,
            retry_after,
            details,
        ),
        ConsumerError::Permanent {
            code,
            message,
            details,
        } => (
            StatusCode::UNPROCESSABLE_ENTITY,
            code,
            message,
            None,
            details,
        ),
        ConsumerError::Throttled {
            code,
            message,
            retry_after,
            details,
        } => (
            StatusCode::TOO_MANY_REQUESTS,
            code,
            message,
            retry_after,
            details,
        ),
    };
    error_response(status, code, message, retry_after, details)
}

fn deadline_exceeded() -> Response {
    protocol_error(
        StatusCode::REQUEST_TIMEOUT,
        "event_consumer_deadline_exceeded",
        "Event consumer did not complete before the invocation deadline",
    )
}

fn protocol_error(
    status: StatusCode,
    code: impl Into<String>,
    message: impl Into<String>,
) -> Response {
    error_response(status, code.into(), message.into(), None, None)
}

fn error_response(
    status: StatusCode,
    code: String,
    message: String,
    retry_after: Option<Duration>,
    details: Option<Value>,
) -> Response {
    let mut response = (
        status,
        Json(ErrorResponse {
            code: bounded(code, 64),
            message: bounded(message, 512),
            details,
        }),
    )
        .into_response();
    if let Some(delay) = retry_after {
        if let Ok(value) = HeaderValue::from_str(&delay.as_secs().to_string()) {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
    }
    response
}

fn bounded(value: String, maximum_chars: usize) -> String {
    value.chars().take(maximum_chars).collect()
}
