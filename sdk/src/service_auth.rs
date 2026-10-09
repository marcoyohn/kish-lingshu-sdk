//! Single application-key bootstrap and authentication of signed callbacks.
use std::{
    fmt,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

#[cfg(feature = "service-auth")]
use axum::Router;
use kish_lingshu_foundation_contract::service_auth::ServiceTrust;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::{Client, Method, RequestBuilder, Url};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tokio::{sync::watch, task::JoinHandle};

#[cfg(any(feature = "service-auth", feature = "service-zenoh"))]
use std::collections::HashMap;

use crate::ServiceCredential;
#[cfg(feature = "service-auth")]
mod http;

const API: &str = "api/user/event-dispatch/v1/";
const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) mod heartbeat;

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

#[derive(Default)]
pub(crate) struct InstanceRegistrationState {
    request: Option<kish_lingshu_foundation_contract::ServiceInstanceRegistration>,
}
impl InstanceRegistrationState {
    #[cfg(feature = "service-channel")]
    pub(crate) fn bind(
        &mut self,
        request: kish_lingshu_foundation_contract::ServiceInstanceRegistration,
    ) -> Result<kish_lingshu_foundation_contract::ServiceInstanceRegistration, ServiceAuthError>
    {
        match &mut self.request {
            Some(current) => {
                if current.instance_id != request.instance_id
                    || current.incarnation_id != request.incarnation_id
                    || (current.generation.is_some()
                        && request.generation.is_some()
                        && current.generation != request.generation)
                {
                    return Err(ServiceAuthError::InvalidNodeConfig);
                }
                if current.generation.is_none() {
                    current.generation = request.generation;
                }
                // An omitted generation joins the assigned identity, never
                // clears it to silently replace an expired/rejected instance.
                Ok(current.clone())
            }
            None => {
                self.request = Some(request.clone());
                Ok(request)
            }
        }
    }
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
    #[cfg(any(feature = "service-auth", feature = "service-zenoh"))]
    nonces: Mutex<HashMap<[u8; 32], i64>>,
    registration: tokio::sync::Mutex<InstanceRegistrationState>,
    closed: watch::Sender<Option<ServiceAuthError>>,
    #[cfg(feature = "service-zenoh")]
    channel_sessions: Arc<tokio::sync::Semaphore>,
    #[cfg(feature = "service-zenoh")]
    catalog_snapshots: Arc<tokio::sync::Semaphore>,
    #[cfg(feature = "service-zenoh")]
    catalog_replies: Arc<tokio::sync::Semaphore>,
    #[cfg(feature = "service-call-zenoh")]
    call_reports: Arc<tokio::sync::Semaphore>,
    heartbeats: Arc<heartbeat::Heartbeats>,
}

impl Shared {
    fn close(&self, reason: ServiceAuthError) {
        #[cfg(feature = "service-zenoh")]
        self.channel_sessions.close();
        #[cfg(feature = "service-zenoh")]
        self.catalog_snapshots.close();
        #[cfg(feature = "service-zenoh")]
        self.catalog_replies.close();
        #[cfg(feature = "service-call-zenoh")]
        self.call_reports.close();
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
    heartbeat: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for ConnectionOwner {
    fn drop(&mut self) {
        self.shared.close(ServiceAuthError::Closed);
        if let Some(task) = self
            .heartbeat
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            task.abort();
        }
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
            #[cfg(any(feature = "service-auth", feature = "service-zenoh"))]
            nonces: Mutex::new(HashMap::new()),
            registration: tokio::sync::Mutex::new(InstanceRegistrationState { request: None }),
            closed: watch::channel(None).0,
            #[cfg(feature = "service-zenoh")]
            channel_sessions: Arc::new(tokio::sync::Semaphore::new(
                kish_lingshu_foundation_contract::service_transport::MAX_DATA_LANES,
            )),
            #[cfg(feature = "service-zenoh")]
            catalog_snapshots: Arc::new(tokio::sync::Semaphore::new(8 * 1024 * 1024)),
            #[cfg(feature = "service-zenoh")]
            catalog_replies: Arc::new(tokio::sync::Semaphore::new(2)),
            #[cfg(feature = "service-call-zenoh")]
            call_reports: Arc::new(tokio::sync::Semaphore::new(16)),
            heartbeats: Arc::default(),
        });
        let refresh = tokio::spawn(refresh_trust(shared.clone()));
        let heartbeat = tokio::spawn(heartbeat::run(shared.clone()));
        Ok(Self(Arc::new(ConnectionOwner {
            shared,
            refresh: Mutex::new(Some(refresh)),
            heartbeat: Mutex::new(Some(heartbeat)),
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

    /// Wait for catalog-dependent enrollment while discovery HTTP is already
    /// serving. This is control-plane activation, never business-work retry.
    pub async fn wait_for_catalog<T, F, Fut>(&self, mut enroll: F) -> Result<T, ServiceAuthError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, ServiceAuthError>>,
    {
        let mut closed = self.subscribe_closed();
        loop {
            self.ensure_open()?;
            let result = tokio::select! { _=closed.changed()=>return Err(ServiceAuthError::Closed),result=enroll()=>result };
            match result {
                Ok(value) => return Ok(value),
                Err(ServiceAuthError::Http(404 | 422 | 429 | 503))
                | Err(ServiceAuthError::Transport) => {}
                Err(error) => return Err(error),
            }
            tokio::select! { _=closed.changed()=>return Err(ServiceAuthError::Closed),_=tokio::time::sleep(std::time::Duration::from_secs(3))=>{} }
        }
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
    #[cfg(feature = "service-auth")]
    pub fn protect(
        &self,
        router: Router,
        invocation_url: &str,
    ) -> Result<Router, ServiceAuthError> {
        http::protect(self, router, invocation_url)
    }

    /// Cancels and joins trust refresh and heartbeats for all clones; routes fail closed.
    pub async fn shutdown(&self) {
        self.0.shared.close(ServiceAuthError::Closed);
        let heartbeat = self
            .0
            .heartbeat
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(task) = heartbeat {
            task.abort();
            let _ = task.await;
        }
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

    #[cfg(feature = "service-zenoh")]
    pub(crate) fn channel_session_budget(&self) -> Arc<tokio::sync::Semaphore> {
        self.0.shared.channel_sessions.clone()
    }

    #[cfg(feature = "service-call-zenoh")]
    pub(crate) fn call_report_budget(&self) -> Arc<tokio::sync::Semaphore> {
        self.0.shared.call_reports.clone()
    }

    #[cfg(feature = "service-zenoh")]
    pub(crate) fn catalog_budgets(
        &self,
    ) -> (Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>) {
        (
            self.0.shared.catalog_snapshots.clone(),
            self.0.shared.catalog_replies.clone(),
        )
    }

    #[cfg(feature = "service-zenoh")]
    pub(crate) fn verify_channel_message(
        &self,
        trust: &ServiceTrust,
        envelope: &kish_lingshu_foundation_contract::service_transport::TransportEnvelope,
    ) -> Result<
        kish_lingshu_foundation_contract::service_auth::TransportMessageClaims,
        ServiceAuthError,
    > {
        self.ensure_open()?;
        let now = chrono::Utc::now().timestamp_millis();
        let claims = kish_lingshu_foundation_contract::service_auth::verify_transport_message(
            trust,
            &envelope.proof,
            self.application_id(),
            envelope.kind,
            &envelope.target,
            &envelope.request_id,
            envelope.payload.get().as_bytes(),
            now,
        )
        .map_err(|_| ServiceAuthError::InvalidResponse)?;
        if claims.deadline_unix_ms != envelope.deadline_unix_ms {
            return Err(ServiceAuthError::InvalidResponse);
        }
        let key: [u8; 32] = Sha256::digest(
            serde_json::to_vec(&("zenoh-message", self.application_id(), &claims.nonce))
                .map_err(|_| ServiceAuthError::InvalidResponse)?,
        )
        .into();
        let mut nonces = self
            .0
            .shared
            .nonces
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        nonces.retain(|_, expiry| *expiry > now / 1000);
        if nonces.contains_key(&key) || nonces.len() >= 16_384 {
            return Err(ServiceAuthError::InvalidResponse);
        }
        nonces.insert(key, claims.expires_at);
        Ok(claims)
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

    #[cfg(any(feature = "service-http", feature = "service-channel"))]
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
    response_bytes_limited(request, MAX_RESPONSE_BYTES).await
}

async fn response_bytes_limited(
    request: RequestBuilder,
    maximum: usize,
) -> Result<Vec<u8>, ServiceAuthError> {
    let mut response = request
        .send()
        .await
        .map_err(|_| ServiceAuthError::Transport)?;
    if !response.status().is_success() {
        return Err(ServiceAuthError::Http(response.status().as_u16()));
    }
    if response
        .content_length()
        .is_some_and(|size| size > maximum as u64)
    {
        return Err(ServiceAuthError::InvalidResponse);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| ServiceAuthError::Transport)?
    {
        if chunk.len() > maximum - bytes.len() {
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
