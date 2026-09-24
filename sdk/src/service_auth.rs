//! Single application-key bootstrap and authentication of signed callbacks.
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use axum::{
    body::{to_bytes, Body},
    extract::{OriginalUri, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    Router,
};
use kish_lingshu_foundation_contract::service_auth::{
    verify_callback, ServiceTrust, SIGNATURE_HEADER,
};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::{Client, Method, RequestBuilder, Url};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tokio::{sync::watch, task::JoinHandle};

use crate::ServiceCredential;

const API: &str = "api/user/event-dispatch/v1/";
const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_CALLBACK_BYTES: usize = 1024 * 1024;
const MAX_PROOF_BYTES: usize = 16 * 1024;
const MAX_NONCES: usize = 16_384;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Errors deliberately exclude URLs, remote bodies, credentials and proofs.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ServiceAuthError {
    #[error("invalid service URL or insecure bootstrap transport")]
    InvalidUrl,
    #[error("credential cannot be represented safely in HTTP headers")]
    InvalidCredential,
    #[error("invalid service trust or application identity")]
    InvalidTrust,
    #[error("invalid or oversized service response")]
    InvalidResponse,
    #[error("invalid consumer configuration")]
    InvalidNodeConfig,
    #[error("service transport failed")]
    Transport,
    #[error("service returned HTTP {0}")]
    Http(u16),
    #[error("service connection is closed")]
    Closed,
}

pub(crate) struct InstanceRegistrationState {
    request: Option<kish_lingshu_foundation_contract::ServiceInstanceRegistration>,
}
impl InstanceRegistrationState {
    pub(crate) fn request(
        &mut self,
        node: &str,
    ) -> kish_lingshu_foundation_contract::ServiceInstanceRegistration {
        self.request
            .get_or_insert_with(
                || kish_lingshu_foundation_contract::ServiceInstanceRegistration {
                    instance_id: format!("instance-{:x}", Sha256::digest(node.as_bytes())),
                    incarnation_id: uuid::Uuid::new_v4().to_string(),
                    generation: None,
                },
            )
            .clone()
    }
    pub(crate) fn accept(
        &mut self,
        identity: Option<&kish_lingshu_foundation_contract::ServiceInstanceIdentity>,
    ) -> Result<(), ServiceAuthError> {
        let request = self
            .request
            .as_mut()
            .ok_or(ServiceAuthError::InvalidResponse)?;
        let identity = identity.ok_or(ServiceAuthError::InvalidResponse)?;
        if identity.instance_id != request.instance_id
            || identity.generation.is_empty()
            || request
                .generation
                .as_ref()
                .is_some_and(|g| g != &identity.generation)
        {
            return Err(ServiceAuthError::InvalidResponse);
        }
        request.generation = Some(identity.generation.clone());
        Ok(())
    }
}

struct Shared {
    client: Client,
    base: Url,
    credential: ServiceCredential,
    trust: RwLock<ServiceTrust>,
    nonces: Mutex<HashMap<[u8; 32], i64>>,
    registration: tokio::sync::Mutex<InstanceRegistrationState>,
    closed: watch::Sender<Option<ServiceAuthError>>,
}

impl Shared {
    fn close(&self, reason: ServiceAuthError) {
        self.closed.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(reason);
            true
        });
    }
}

// The refresh task holds Shared, never ConnectionOwner. Last-clone drop therefore
// cancels the task even while an HTTP request is pending.
struct ConnectionOwner {
    shared: Arc<Shared>,
    refresh: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for ConnectionOwner {
    fn drop(&mut self) {
        self.shared.close(ServiceAuthError::Closed);
        if let Some(task) = self
            .refresh
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            task.abort();
        }
    }
}

/// A cloneable authenticated connection. The last clone cancels trust refresh.
///
/// Base credential rejection closes all clones. Role-session rejection stops only
/// that role; siblings retain their independently authorized membership. One
/// connection represents one provider instance with a shared registration generation.
#[derive(Clone)]
pub struct ServiceConnection(Arc<ConnectionOwner>);

impl fmt::Debug for ServiceConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceConnection")
            .field("application_id", &self.application_id())
            .finish_non_exhaustive()
    }
}

impl ServiceConnection {
    /// Fetches trust before returning. Only HTTPS and loopback HTTP are accepted.
    pub async fn connect(
        base_url: &str,
        credential: ServiceCredential,
    ) -> Result<Self, ServiceAuthError> {
        let mut base = callback_url(base_url)?;
        if base.query().is_some() {
            return Err(ServiceAuthError::InvalidUrl);
        }
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        reqwest::header::HeaderValue::from_str(&format!("Bearer {}", credential.expose()))
            .map_err(|_| ServiceAuthError::InvalidCredential)?;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|_| ServiceAuthError::Transport)?;
        let trust = fetch_trust(&client, &base, &credential).await?;
        let shared = Arc::new(Shared {
            client,
            base,
            credential,
            trust: RwLock::new(trust),
            nonces: Mutex::new(HashMap::new()),
            registration: tokio::sync::Mutex::new(InstanceRegistrationState { request: None }),
            closed: watch::channel(None).0,
        });
        let refresh = tokio::spawn(refresh_trust(shared.clone()));
        Ok(Self(Arc::new(ConnectionOwner {
            shared,
            refresh: Mutex::new(Some(refresh)),
        })))
    }

    /// A ServiceConnection represents one provider instance; every role shares
    /// its first registered deployment node identity and incarnation. Serialize
    /// role bootstrap so later roles join the issued generation, never replace it.
    pub(crate) async fn registration(
        &self,
    ) -> tokio::sync::MutexGuard<'_, InstanceRegistrationState> {
        self.0.shared.registration.lock().await
    }

    pub fn application_id(&self) -> &str {
        self.0.shared.credential.application_id()
    }

    /// Protects an Event Consumer or User Task adapter before its handlers run.
    /// Configure the external HTTPS URL (HTTP is allowed only on loopback),
    /// including any proxy prefix and exact query. URLs use reqwest's canonical
    /// representation; the original request path/query must match it exactly.
    /// The 1 MiB body and 16,384 live nonce limits fail closed when exceeded.
    /// Business idempotency across processes/restarts remains the caller's duty.
    pub fn protect(
        &self,
        router: Router,
        invocation_url: &str,
    ) -> Result<Router, ServiceAuthError> {
        self.ensure_open()?;
        let target = callback_url(invocation_url)?;
        let path_query = target[url::Position::BeforePath..url::Position::AfterQuery].to_owned();
        let state = CallbackState {
            connection: self.clone(),
            target: target.to_string(),
            path_query,
        };
        Ok(router.layer(middleware::from_fn_with_state(state, authenticate_callback)))
    }

    /// Cancels and joins trust refresh for all clones; protected routes fail closed.
    pub async fn shutdown(&self) {
        self.0.shared.close(ServiceAuthError::Closed);
        let task = self
            .0
            .refresh
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
    }

    pub(crate) fn ensure_open(&self) -> Result<(), ServiceAuthError> {
        match self.0.shared.closed.borrow().clone() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Observe shared credential revocation or shutdown without exposing credentials.
    pub fn subscribe_closed(&self) -> watch::Receiver<Option<ServiceAuthError>> {
        self.0.shared.closed.subscribe()
    }

    pub(crate) fn root_request(
        &self,
        method: Method,
        path: &str,
    ) -> Result<RequestBuilder, ServiceAuthError> {
        self.ensure_open()?;
        Ok(root_request(
            &self.0.shared.client,
            &self.0.shared.base,
            &self.0.shared.credential,
            method,
            path,
        ))
    }

    #[cfg(feature = "service-http")]
    pub(crate) fn service_request(
        &self,
        method: Method,
        path: &'static str,
        token: Option<&str>,
    ) -> Result<RequestBuilder, ServiceAuthError> {
        self.ensure_open()?;
        let url = self
            .0
            .shared
            .base
            .join(&format!(
                "{}{}",
                kish_lingshu_runtime_contract::service::SERVICE_API_PATH,
                path
            ))
            .map_err(|_| ServiceAuthError::InvalidUrl)?;
        Ok(self
            .0
            .shared
            .client
            .request(method, url)
            .bearer_auth(token.unwrap_or_else(|| self.0.shared.credential.expose()))
            .header(
                "x-kish-app-id",
                utf8_percent_encode(self.application_id(), NON_ALPHANUMERIC).to_string(),
            ))
    }

    pub(crate) async fn root_response_json<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
    ) -> Result<T, ServiceAuthError> {
        let result = response_json(request).await;
        if let Err(error @ ServiceAuthError::Http(401 | 403)) = &result {
            self.reject_authentication(error.clone());
        }
        result
    }

    pub(crate) fn reject_authentication(&self, reason: ServiceAuthError) {
        self.0.shared.close(reason);
    }

    pub(crate) fn session_request(
        &self,
        path: &str,
        credential: &str,
    ) -> Result<RequestBuilder, ServiceAuthError> {
        self.ensure_open()?;
        Ok(self
            .0
            .shared
            .client
            .post(endpoint(&self.0.shared.base, path))
            .bearer_auth(credential))
    }
}

pub(crate) fn callback_url(value: &str) -> Result<Url, ServiceAuthError> {
    Url::parse(value)
        .ok()
        .filter(|url| {
            matches!(url.scheme(), "http" | "https")
                && url.host().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none()
                && trusted_transport(url)
        })
        .ok_or(ServiceAuthError::InvalidUrl)
}

fn trusted_transport(url: &Url) -> bool {
    url.scheme() == "https"
        || match url.host() {
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            Some(url::Host::Domain(name)) => name == "localhost",
            _ => false,
        }
}

fn endpoint(base: &Url, path: &str) -> Url {
    base.join(&format!("{API}{path}")).expect("static API path")
}

fn root_request(
    client: &Client,
    base: &Url,
    credential: &ServiceCredential,
    method: Method,
    path: &str,
) -> RequestBuilder {
    client
        .request(method, endpoint(base, path))
        .bearer_auth(credential.expose())
        .header(
            "x-kish-app-id",
            utf8_percent_encode(credential.application_id(), NON_ALPHANUMERIC).to_string(),
        )
}

pub(crate) async fn response_bytes(request: RequestBuilder) -> Result<Vec<u8>, ServiceAuthError> {
    let mut response = request
        .send()
        .await
        .map_err(|_| ServiceAuthError::Transport)?;
    if !response.status().is_success() {
        return Err(ServiceAuthError::Http(response.status().as_u16()));
    }
    if response
        .content_length()
        .is_some_and(|size| size > MAX_RESPONSE_BYTES as u64)
    {
        return Err(ServiceAuthError::InvalidResponse);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| ServiceAuthError::Transport)?
    {
        if chunk.len() > MAX_RESPONSE_BYTES - bytes.len() {
            return Err(ServiceAuthError::InvalidResponse);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub(crate) async fn response_json<T: DeserializeOwned>(
    request: RequestBuilder,
) -> Result<T, ServiceAuthError> {
    serde_json::from_slice(&response_bytes(request).await?)
        .map_err(|_| ServiceAuthError::InvalidResponse)
}

async fn fetch_trust(
    client: &Client,
    base: &Url,
    credential: &ServiceCredential,
) -> Result<ServiceTrust, ServiceAuthError> {
    // Native trust works when Dispatch is disabled. A missing route alone allows
    // compatibility with older platforms; authentication errors never downgrade.
    let native = client
        .get(
            base.join("api/user/services/v1/service-auth/trust")
                .map_err(|_| ServiceAuthError::InvalidUrl)?,
        )
        .bearer_auth(credential.expose())
        .header(
            "x-kish-app-id",
            utf8_percent_encode(credential.application_id(), NON_ALPHANUMERIC).to_string(),
        );
    let trust: ServiceTrust = match response_json(native).await {
        Err(ServiceAuthError::Http(404)) => {
            response_json(root_request(
                client,
                base,
                credential,
                Method::GET,
                "service-auth/trust",
            ))
            .await?
        }
        result => result?,
    };
    let now = chrono::Utc::now().timestamp();
    if trust.app_id != credential.application_id()
        || trust.expires_at <= now
        || trust.keys.is_empty()
        || trust.keys.len() > 8
        || !trust
            .keys
            .iter()
            .any(|key| key.not_before <= now && key.not_after > now)
        || trust.keys.iter().any(|key| {
            key.not_after <= key.not_before || key.key_id.is_empty() || key.public_key.is_empty()
        })
    {
        return Err(ServiceAuthError::InvalidTrust);
    }
    Ok(trust)
}

async fn refresh_trust(shared: Arc<Shared>) {
    loop {
        let remaining = shared
            .trust
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .expires_at
            .saturating_sub(chrono::Utc::now().timestamp());
        tokio::time::sleep(Duration::from_secs((remaining / 2).clamp(1, 60) as u64)).await;
        if shared.closed.borrow().is_some() {
            return;
        }
        match fetch_trust(&shared.client, &shared.base, &shared.credential).await {
            Ok(trust) => *shared.trust.write().unwrap_or_else(|e| e.into_inner()) = trust,
            Err(error @ ServiceAuthError::Http(401 | 403)) => {
                shared.close(error);
                return;
            }
            // Keep cached trust only until its signed validity expires. Each
            // refresh is one bounded request; this is not publication recovery.
            Err(_) => {}
        }
    }
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
                registration: tokio::sync::Mutex::new(InstanceRegistrationState { request: None }),
            }),
            refresh: Mutex::new(None),
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
