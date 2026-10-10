//! HTTP middleware over the shared outbound identity/trust connection.
use super::*;
use axum::{
    body::{to_bytes, Body},
    extract::{OriginalUri, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    Router,
};
use kish_lingshu_foundation_contract::service_auth::{verify_callback, SIGNATURE_HEADER};

const MAX_CALLBACK_BYTES: usize = 1024 * 1024;
const MAX_PROOF_BYTES: usize = 16 * 1024;
const MAX_NONCES: usize = 16_384;

pub(super) fn protect(
    connection: &ServiceConnection,
    router: Router,
    invocation_url: &str,
) -> Result<Router, ServiceAuthError> {
    connection.ensure_open()?;
    let target = callback_url(invocation_url)?;
    let path_query = target[url::Position::BeforePath..url::Position::AfterQuery].to_owned();
    let state = CallbackState {
        connection: connection.clone(),
        target: target.to_string(),
        path_query,
    };
    Ok(router.layer(middleware::from_fn_with_state(state, authenticate_callback)))
}

#[derive(Clone)]
struct CallbackState {
    connection: ServiceConnection,
    target: String,
    path_query: String,
}

async fn authenticate_callback(
    State(state): State<CallbackState>,
    request: Request,
    next: Next,
) -> Response {
    if state.connection.ensure_open().is_err() {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let uri = request
        .extensions()
        .get::<OriginalUri>()
        .map(|original| &original.0)
        .unwrap_or_else(|| request.uri());
    if uri.path_and_query().map(|v| v.as_str()) != Some(state.path_query.as_str()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let mut proofs = request.headers().get_all(SIGNATURE_HEADER).iter();
    let proof = match proofs.next().and_then(|v| v.to_str().ok()) {
        Some(proof) if proof.len() <= MAX_PROOF_BYTES && proofs.next().is_none() => {
            proof.to_owned()
        }
        _ => return StatusCode::UNAUTHORIZED.into_response(),
    };
    let (parts, body) = request.into_parts();
    let bytes =
        match tokio::time::timeout(REQUEST_TIMEOUT, to_bytes(body, MAX_CALLBACK_BYTES)).await {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
            Err(_) => return StatusCode::REQUEST_TIMEOUT.into_response(),
        };
    let now = chrono::Utc::now().timestamp();
    let shared = &state.connection.0.shared;
    let verified = {
        let trust = shared.trust.read().unwrap_or_else(|e| e.into_inner());
        verify_callback(
            &trust,
            &proof,
            state.connection.application_id(),
            parts.method.as_str(),
            &state.target,
            &bytes,
            now,
        )
    };
    let claims = match verified {
        Ok(claims) if claims.expires_at > now && shared.closed.borrow().is_none() => claims,
        _ => return StatusCode::UNAUTHORIZED.into_response(),
    };
    {
        let mut nonces = shared.nonces.lock().unwrap_or_else(|e| e.into_inner());
        nonces.retain(|_, expires_at| *expires_at > now);
        let key: [u8; 32] = Sha256::digest(claims.nonce.as_bytes()).into();
        if nonces.contains_key(&key) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        if nonces.len() >= MAX_NONCES {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        nonces.insert(key, claims.expires_at);
    }
    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use kish_lingshu_foundation_contract::service_auth::ServiceSigner;
    use tower::ServiceExt;

    #[tokio::test]
    async fn full_nonce_cache_fails_closed_without_evicting_live_proofs_and_recovers_after_expiry()
    {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let signer = ServiceSigner::new(&[1; 32]).unwrap();
        let now = chrono::Utc::now().timestamp();
        let connection = ServiceConnection(Arc::new(ConnectionOwner {
            shared: Arc::new(Shared {
                client: Client::new(),
                base: Url::parse("http://127.0.0.1/").unwrap(),
                credential: ServiceCredential::new("app", "secret").unwrap(),
                trust: RwLock::new(signer.trust("app", now).unwrap()),
                nonces: Mutex::new(
                    (0..MAX_NONCES)
                        .map(|i| (Sha256::digest(i.to_le_bytes()).into(), now + 60))
                        .collect(),
                ),
                closed: watch::channel(None).0,
                #[cfg(feature = "service-zenoh")]
                channel_sessions: Arc::new(tokio::sync::Semaphore::new(
                    kish_lingshu_foundation_contract::service_transport::MAX_DATA_LANES,
                )),
                #[cfg(feature = "service-zenoh")]
                native_runtime: Arc::default(),
                heartbeats: Arc::default(),
                #[cfg(feature = "service-zenoh")]
                catalog_snapshots: Arc::new(tokio::sync::Semaphore::new(8 * 1024 * 1024)),
                #[cfg(feature = "service-zenoh")]
                catalog_replies: Arc::new(tokio::sync::Semaphore::new(2)),
                #[cfg(feature = "service-call-zenoh")]
                call_reports: Arc::new(tokio::sync::Semaphore::new(16)),
                registration: tokio::sync::Mutex::new(InstanceRegistrationState { request: None }),
            }),
            refresh: Mutex::new(None),
            heartbeat: Mutex::new(None),
        }));
        let target = "https://callback.example/";
        let router = connection
            .protect(
                Router::new().route("/", axum::routing::post(|| async { StatusCode::OK })),
                target,
            )
            .unwrap();
        let proof = signer
            .sign_callback("app", "POST", target, b"", now)
            .unwrap();
        let request = || {
            Request::builder()
                .method("POST")
                .uri("/")
                .header(SIGNATURE_HEADER, &proof)
                .body(Body::empty())
                .unwrap()
        };
        assert_eq!(
            router.clone().oneshot(request()).await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(connection.0.shared.nonces.lock().unwrap().len(), MAX_NONCES);
        connection
            .0
            .shared
            .nonces
            .lock()
            .unwrap()
            .values_mut()
            .for_each(|expiry| *expiry = now - 1);
        assert_eq!(
            router.clone().oneshot(request()).await.unwrap().status(),
            StatusCode::OK
        );
        assert_eq!(
            router.oneshot(request()).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }
}
