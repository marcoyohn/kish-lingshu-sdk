//! Versioned, transport-independent proofs. No configuration, storage or network I/O.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use ring::{
    digest, hmac,
    rand::{SecureRandom, SystemRandom},
    signature::{self, Ed25519KeyPair, KeyPair},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub const SIGNATURE_HEADER: &str = "x-lingshu-signature";
const EPOCH_SECONDS: i64 = 3600;
const CLOCK_SKEW: i64 = 30;
const CALLBACK_LIFETIME: i64 = 60;
const SESSION_LIFETIME: i64 = 300;
const MAX_PROOF_BYTES: usize = 16 * 1024;

#[derive(Debug, thiserror::Error)]
#[error("service authentication proof is invalid or expired")]
pub struct ServiceAuthError;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServicePublicKey {
    pub key_id: String,
    pub public_key: String,
    pub not_before: i64,
    pub not_after: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceTrust {
    pub app_id: String,
    pub keys: Vec<ServicePublicKey>,
    pub expires_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallbackClaims {
    pub method: String,
    pub target: String,
    pub body_sha256: String,
    pub nonce: String,
    pub expires_at: i64,
}

#[cfg(feature = "service-transport")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransportMessageClaims {
    pub kind: crate::service_transport::MessageKind,
    pub target: crate::service_transport::ExactRouteKey,
    pub request_id: crate::service_transport::RouteIdentity,
    pub body_sha256: String,
    pub nonce: String,
    pub expires_at: i64,
    pub issued_at_unix_ms: i64,
    pub deadline_unix_ms: i64,
}

#[cfg(feature = "service-transport")]
impl TransportMessageClaims {
    /// Apply a request-specific lifetime cap to already verified claims.
    /// Clock skew is independent of sender lifetime; absolute expiration and
    /// caller-owned role/phase deadlines must never be extended.
    pub fn validate_request_time(
        &self,
        now_unix_ms: i64,
        maximum_lifetime_ms: i64,
    ) -> Result<(), ServiceAuthError> {
        use crate::service_transport::bootstrap::MAX_BOOTSTRAP_CLOCK_SKEW_MS;
        if now_unix_ms < 0
            || maximum_lifetime_ms <= 0
            || self.issued_at_unix_ms < 0
            || self.issued_at_unix_ms > now_unix_ms.saturating_add(MAX_BOOTSTRAP_CLOCK_SKEW_MS)
            || self.deadline_unix_ms <= now_unix_ms
            || self.deadline_unix_ms <= self.issued_at_unix_ms
            || self.deadline_unix_ms.saturating_sub(self.issued_at_unix_ms) > maximum_lifetime_ms
        {
            return Err(ServiceAuthError);
        }
        Ok(())
    }
}

/// Compared against the host's native admission receipt, never payload identity.
#[cfg(feature = "service-transport")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientChannelIdentity {
    pub application_id: crate::service_transport::RouteIdentity,
    pub instance_id: crate::service_transport::RouteIdentity,
    pub base_generation: crate::service_transport::RouteIdentity,
    pub certificate_identity: crate::service_transport::RouteIdentity,
}

#[cfg(feature = "service-transport")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientTransportClaims {
    identity: ClientChannelIdentity,
    message: TransportMessageClaims,
}

/// Uses the local Ed25519 CSR key; no root API Key or platform signing key.
#[cfg(feature = "service-transport")]
pub struct ChannelMessageSigner(Ed25519KeyPair);

#[cfg(feature = "service-transport")]
impl std::fmt::Debug for ChannelMessageSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ChannelMessageSigner([REDACTED])")
    }
}

#[cfg(feature = "service-transport")]
impl ChannelMessageSigner {
    pub fn from_pkcs8(bytes: &[u8]) -> Result<Self, ServiceAuthError> {
        Ed25519KeyPair::from_pkcs8(bytes)
            .map(Self)
            .map_err(|_| ServiceAuthError)
    }

    pub fn public_key(&self) -> &[u8] {
        self.0.public_key().as_ref()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sign_message(
        &self,
        identity: &ClientChannelIdentity,
        kind: crate::service_transport::MessageKind,
        target: &crate::service_transport::ExactRouteKey,
        request_id: &crate::service_transport::RouteIdentity,
        body: &[u8],
        now_unix_ms: i64,
        deadline_unix_ms: i64,
    ) -> Result<String, ServiceAuthError> {
        let message = new_transport_claims(
            kind,
            target,
            request_id,
            body,
            now_unix_ms,
            deadline_unix_ms,
        )?;
        let envelope = Envelope {
            version: 1,
            purpose: "client-zenoh-message".to_owned(),
            app_id: identity.application_id.as_str().to_owned(),
            key_id: identity.certificate_identity.as_str().to_owned(),
            issued_at: now_unix_ms / 1000,
            expires_at: message.expires_at,
            claims: ClientTransportClaims {
                identity: identity.clone(),
                message,
            },
        };
        let encoded =
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&envelope).map_err(|_| ServiceAuthError)?);
        let proof = format!(
            "{}.{}",
            encoded,
            URL_SAFE_NO_PAD.encode(self.0.sign(encoded.as_bytes()).as_ref())
        );
        if proof.len() > MAX_PROOF_BYTES {
            return Err(ServiceAuthError);
        }
        Ok(proof)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<T> {
    version: u8,
    purpose: String,
    app_id: String,
    key_id: String,
    issued_at: i64,
    expires_at: i64,
    claims: T,
}

/// Root stays exclusively in the server; derived keys are separated by app, epoch and purpose.
pub struct ServiceSigner(hmac::Key);

impl std::fmt::Debug for ServiceSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ServiceSigner([REDACTED])")
    }
}

impl ServiceSigner {
    pub fn new(root: &[u8]) -> Result<Self, ServiceAuthError> {
        if root.len() < 32 {
            return Err(ServiceAuthError);
        }
        Ok(Self(hmac::Key::new(hmac::HMAC_SHA256, root)))
    }

    fn key(
        &self,
        app: &str,
        epoch: i64,
        purpose: &str,
    ) -> Result<Ed25519KeyPair, ServiceAuthError> {
        if app.is_empty() || app.len() > 255 || app.chars().any(char::is_control) || epoch < 0 {
            return Err(ServiceAuthError);
        }
        let context = serde_json::to_vec(&("kish-lingshu/service-auth/v1", app, epoch, purpose))
            .map_err(|_| ServiceAuthError)?;
        let seed = hmac::sign(&self.0, &context);
        Ed25519KeyPair::from_seed_unchecked(seed.as_ref()).map_err(|_| ServiceAuthError)
    }

    pub fn trust(&self, app: &str, now: i64) -> Result<ServiceTrust, ServiceAuthError> {
        self.trust_for_purpose(app, now, "callback")
    }

    fn trust_for_purpose(
        &self,
        app: &str,
        now: i64,
        purpose: &str,
    ) -> Result<ServiceTrust, ServiceAuthError> {
        if now < EPOCH_SECONDS {
            return Err(ServiceAuthError);
        }
        let epoch = now / EPOCH_SECONDS;
        let keys = (epoch - 1..=epoch + 1)
            .map(|id| {
                let key = self.key(app, id, purpose)?;
                Ok(ServicePublicKey {
                    key_id: id.to_string(),
                    public_key: URL_SAFE_NO_PAD.encode(key.public_key().as_ref()),
                    not_before: id * EPOCH_SECONDS,
                    not_after: (id + 1) * EPOCH_SECONDS + CALLBACK_LIFETIME + CLOCK_SKEW,
                })
            })
            .collect::<Result<Vec<_>, ServiceAuthError>>()?;
        Ok(ServiceTrust {
            app_id: app.into(),
            keys,
            expires_at: (epoch + 2) * EPOCH_SECONDS,
        })
    }

    /// Distinct derived signing keys: HTTP trust/proofs cannot authorize Zenoh messages.
    #[cfg(feature = "service-transport")]
    pub fn transport_trust(&self, app: &str, now: i64) -> Result<ServiceTrust, ServiceAuthError> {
        self.trust_for_purpose(app, now, "zenoh-message")
    }

    #[cfg(feature = "service-transport")]
    #[allow(clippy::too_many_arguments)]
    pub fn sign_transport_message(
        &self,
        app: &str,
        kind: crate::service_transport::MessageKind,
        target: &crate::service_transport::ExactRouteKey,
        request_id: &crate::service_transport::RouteIdentity,
        body: &[u8],
        now_unix_ms: i64,
        deadline_unix_ms: i64,
    ) -> Result<String, ServiceAuthError> {
        let claims = new_transport_claims(
            kind,
            target,
            request_id,
            body,
            now_unix_ms,
            deadline_unix_ms,
        )?;
        let now = now_unix_ms / 1000;
        let expires_at = claims.expires_at;
        self.sign(app, "zenoh-message", &claims, now, expires_at)
    }

    fn sign<T: Serialize>(
        &self,
        app: &str,
        purpose: &str,
        claims: &T,
        now: i64,
        expires_at: i64,
    ) -> Result<String, ServiceAuthError> {
        let epoch = now / EPOCH_SECONDS;
        let key = self.key(app, epoch, purpose)?;
        let body = serde_json::to_vec(&Envelope {
            version: 1,
            purpose: purpose.into(),
            app_id: app.into(),
            key_id: epoch.to_string(),
            issued_at: now,
            expires_at,
            claims,
        })
        .map_err(|_| ServiceAuthError)?;
        let encoded = URL_SAFE_NO_PAD.encode(body);
        let result = format!(
            "{}.{}",
            encoded,
            URL_SAFE_NO_PAD.encode(key.sign(encoded.as_bytes()).as_ref())
        );
        if result.len() > MAX_PROOF_BYTES {
            return Err(ServiceAuthError);
        }
        Ok(result)
    }

    pub fn sign_callback(
        &self,
        app: &str,
        method: &str,
        target: &str,
        body: &[u8],
        now: i64,
    ) -> Result<String, ServiceAuthError> {
        let mut nonce = [0; 16];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| ServiceAuthError)?;
        let expires_at = now.checked_add(CALLBACK_LIFETIME).ok_or(ServiceAuthError)?;
        let claims = CallbackClaims {
            method: method.into(),
            target: target.into(),
            body_sha256: body_digest(body),
            nonce: URL_SAFE_NO_PAD.encode(nonce),
            expires_at,
        };
        self.sign(app, "callback", &claims, now, expires_at)
    }

    pub fn sign_service_session<T: Serialize>(
        &self,
        app: &str,
        claims: &T,
        now: i64,
    ) -> Result<String, ServiceAuthError> {
        self.sign(
            app,
            "service-instance",
            claims,
            now,
            now.checked_add(SESSION_LIFETIME).ok_or(ServiceAuthError)?,
        )
    }

    pub fn verify_service_session<T: DeserializeOwned>(
        &self,
        proof: &str,
        now: i64,
    ) -> Result<(String, T), ServiceAuthError> {
        self.verify_service_proof(proof, "service-instance", now, SESSION_LIFETIME)
    }

    pub fn sign_service_completion<T: Serialize>(
        &self,
        app: &str,
        claims: &T,
        now: i64,
        expires_at: i64,
    ) -> Result<String, ServiceAuthError> {
        validate_time(now, expires_at, now, 86_460)?;
        self.sign(app, "service-completion", claims, now, expires_at)
    }

    pub fn verify_service_completion<T: DeserializeOwned>(
        &self,
        proof: &str,
        now: i64,
    ) -> Result<(String, T), ServiceAuthError> {
        self.verify_service_proof(proof, "service-completion", now, 86_460)
    }

    fn verify_service_proof<T: DeserializeOwned>(
        &self,
        proof: &str,
        purpose: &str,
        now: i64,
        lifetime: i64,
    ) -> Result<(String, T), ServiceAuthError> {
        let (encoded, signature, envelope): (_, _, Envelope<T>) = decode(proof)?;
        validate_envelope(&envelope, purpose, now, lifetime)?;
        let epoch = envelope
            .key_id
            .parse::<i64>()
            .map_err(|_| ServiceAuthError)?;
        if epoch != envelope.issued_at / EPOCH_SECONDS {
            return Err(ServiceAuthError);
        }
        let key = self.key(&envelope.app_id, epoch, purpose)?;
        signature::UnparsedPublicKey::new(&signature::ED25519, key.public_key().as_ref())
            .verify(encoded.as_bytes(), &signature)
            .map_err(|_| ServiceAuthError)?;
        Ok((envelope.app_id, envelope.claims))
    }

    /// Platform-only, nonce-bound discovery metadata; never a Consumer credential.
    pub fn sign_consumer_presence<T: Serialize>(
        &self,
        app: &str,
        claims: &T,
        now: i64,
    ) -> Result<String, ServiceAuthError> {
        self.sign(app, "consumer-presence", claims, now, now + 3)
    }
    pub fn verify_consumer_presence<T: DeserializeOwned>(
        &self,
        proof: &str,
        now: i64,
    ) -> Result<(String, T), ServiceAuthError> {
        let (encoded, signature, envelope): (_, _, Envelope<T>) = decode(proof)?;
        validate_envelope(&envelope, "consumer-presence", now, 3)?;
        let epoch = envelope
            .key_id
            .parse::<i64>()
            .map_err(|_| ServiceAuthError)?;
        if epoch != envelope.issued_at / EPOCH_SECONDS {
            return Err(ServiceAuthError);
        }
        let key = self.key(&envelope.app_id, epoch, "consumer-presence")?;
        signature::UnparsedPublicKey::new(&signature::ED25519, key.public_key().as_ref())
            .verify(encoded.as_bytes(), &signature)
            .map_err(|_| ServiceAuthError)?;
        Ok((envelope.app_id, envelope.claims))
    }

    pub fn sign_session<T: Serialize>(
        &self,
        app: &str,
        claims: &T,
        now: i64,
        expires_at: i64,
    ) -> Result<String, ServiceAuthError> {
        validate_time(now, expires_at, now, SESSION_LIFETIME)?;
        self.sign(app, "consumer-session", claims, now, expires_at)
    }

    pub fn verify_session<T: DeserializeOwned>(
        &self,
        proof: &str,
        now: i64,
    ) -> Result<(String, T), ServiceAuthError> {
        let (encoded, signature, envelope): (_, _, Envelope<T>) = decode(proof)?;
        validate_envelope(&envelope, "consumer-session", now, SESSION_LIFETIME)?;
        let epoch = envelope
            .key_id
            .parse::<i64>()
            .map_err(|_| ServiceAuthError)?;
        if epoch != envelope.issued_at / EPOCH_SECONDS {
            return Err(ServiceAuthError);
        }
        let key = self.key(&envelope.app_id, epoch, "consumer-session")?;
        signature::UnparsedPublicKey::new(&signature::ED25519, key.public_key().as_ref())
            .verify(encoded.as_bytes(), &signature)
            .map_err(|_| ServiceAuthError)?;
        Ok((envelope.app_id, envelope.claims))
    }
}

fn body_digest(body: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(digest::digest(&digest::SHA256, body).as_ref())
}

fn decode<T: DeserializeOwned>(
    proof: &str,
) -> Result<(&str, Vec<u8>, Envelope<T>), ServiceAuthError> {
    if proof.len() > MAX_PROOF_BYTES {
        return Err(ServiceAuthError);
    }
    let (encoded, sig) = proof.split_once('.').ok_or(ServiceAuthError)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| ServiceAuthError)?;
    let envelope = serde_json::from_slice(&bytes).map_err(|_| ServiceAuthError)?;
    let sig = URL_SAFE_NO_PAD.decode(sig).map_err(|_| ServiceAuthError)?;
    Ok((encoded, sig, envelope))
}

fn validate_time(
    issued: i64,
    expires: i64,
    now: i64,
    maximum: i64,
) -> Result<(), ServiceAuthError> {
    if issued < 0
        || issued > now.saturating_add(CLOCK_SKEW)
        || expires <= now
        || expires <= issued
        || expires.saturating_sub(issued) > maximum
    {
        return Err(ServiceAuthError);
    }
    Ok(())
}

fn validate_envelope<T>(
    envelope: &Envelope<T>,
    purpose: &str,
    now: i64,
    maximum: i64,
) -> Result<(), ServiceAuthError> {
    if envelope.version != 1 || envelope.purpose != purpose {
        return Err(ServiceAuthError);
    }
    validate_time(envelope.issued_at, envelope.expires_at, now, maximum)
}

pub fn verify_callback(
    trust: &ServiceTrust,
    proof: &str,
    app: &str,
    method: &str,
    target: &str,
    body: &[u8],
    now: i64,
) -> Result<CallbackClaims, ServiceAuthError> {
    let (encoded, sig, envelope): (_, _, Envelope<CallbackClaims>) = decode(proof)?;
    validate_envelope(&envelope, "callback", now, CALLBACK_LIFETIME)?;
    if trust.app_id != app
        || envelope.app_id != app
        || trust.expires_at <= now
        || trust.keys.len() > 8
    {
        return Err(ServiceAuthError);
    }
    let key = trust
        .keys
        .iter()
        .find(|key| key.key_id == envelope.key_id)
        .ok_or(ServiceAuthError)?;
    if envelope.issued_at < key.not_before || envelope.expires_at > key.not_after {
        return Err(ServiceAuthError);
    }
    let public_key = URL_SAFE_NO_PAD
        .decode(&key.public_key)
        .map_err(|_| ServiceAuthError)?;
    signature::UnparsedPublicKey::new(&signature::ED25519, public_key)
        .verify(encoded.as_bytes(), &sig)
        .map_err(|_| ServiceAuthError)?;
    let claims = envelope.claims;
    if claims.method != method
        || claims.target != target
        || claims.body_sha256 != body_digest(body)
        || claims.expires_at != envelope.expires_at
        || claims.nonce.len() != 22
    {
        return Err(ServiceAuthError);
    }
    Ok(claims)
}

#[cfg(feature = "service-transport")]
#[allow(clippy::too_many_arguments)]
fn new_transport_claims(
    kind: crate::service_transport::MessageKind,
    target: &crate::service_transport::ExactRouteKey,
    request_id: &crate::service_transport::RouteIdentity,
    body: &[u8],
    now_unix_ms: i64,
    deadline_unix_ms: i64,
) -> Result<TransportMessageClaims, ServiceAuthError> {
    if now_unix_ms < 0
        || deadline_unix_ms <= now_unix_ms
        || deadline_unix_ms.saturating_sub(now_unix_ms) > CALLBACK_LIFETIME * 1000
        || body.len() > kind.payload_limit()
    {
        return Err(ServiceAuthError);
    }
    let expires_at = deadline_unix_ms.checked_add(999).ok_or(ServiceAuthError)? / 1000;
    validate_time(
        now_unix_ms / 1000,
        expires_at,
        now_unix_ms / 1000,
        CALLBACK_LIFETIME + 1,
    )?;
    let mut nonce = [0; 16];
    SystemRandom::new()
        .fill(&mut nonce)
        .map_err(|_| ServiceAuthError)?;
    Ok(TransportMessageClaims {
        kind,
        target: target.clone(),
        request_id: request_id.clone(),
        body_sha256: body_digest(body),
        nonce: URL_SAFE_NO_PAD.encode(nonce),
        expires_at,
        issued_at_unix_ms: now_unix_ms,
        deadline_unix_ms,
    })
}

#[cfg(feature = "service-transport")]
#[allow(clippy::too_many_arguments)]
fn validate_transport_scope(
    claims: &TransportMessageClaims,
    issued_at: i64,
    expires_at: i64,
    kind: crate::service_transport::MessageKind,
    target: &crate::service_transport::ExactRouteKey,
    request_id: &crate::service_transport::RouteIdentity,
    body: &[u8],
    now_unix_ms: i64,
) -> Result<(), ServiceAuthError> {
    if now_unix_ms < 0
        || body.len() > kind.payload_limit()
        || claims.kind != kind
        || &claims.target != target
        || &claims.request_id != request_id
        || claims.body_sha256 != body_digest(body)
        || claims.expires_at != expires_at
        || claims.issued_at_unix_ms < 0
        || claims.issued_at_unix_ms / 1000 != issued_at
        || claims.issued_at_unix_ms > now_unix_ms.saturating_add(CLOCK_SKEW * 1000)
        || claims.deadline_unix_ms <= now_unix_ms
        || claims.deadline_unix_ms <= claims.issued_at_unix_ms
        || claims
            .deadline_unix_ms
            .saturating_sub(claims.issued_at_unix_ms)
            > CALLBACK_LIFETIME * 1000
        || claims
            .deadline_unix_ms
            .checked_add(999)
            .ok_or(ServiceAuthError)?
            / 1000
            != expires_at
        || claims.nonce.len() != 22
        || URL_SAFE_NO_PAD
            .decode(&claims.nonce)
            .map_err(|_| ServiceAuthError)?
            .len()
            != 16
    {
        return Err(ServiceAuthError);
    }
    Ok(())
}

/// Verify using the public key and identity supplied by native mTLS admission.
/// The caller must consume the nonce atomically and check current business authority.
#[cfg(feature = "service-transport")]
#[allow(clippy::too_many_arguments)]
pub fn verify_client_transport_message(
    public_key: &[u8],
    identity: &ClientChannelIdentity,
    proof: &str,
    kind: crate::service_transport::MessageKind,
    target: &crate::service_transport::ExactRouteKey,
    request_id: &crate::service_transport::RouteIdentity,
    body: &[u8],
    now_unix_ms: i64,
) -> Result<TransportMessageClaims, ServiceAuthError> {
    if now_unix_ms < 0 || public_key.len() != 32 || body.len() > kind.payload_limit() {
        return Err(ServiceAuthError);
    }
    let (encoded, sig, envelope): (_, _, Envelope<ClientTransportClaims>) = decode(proof)?;
    validate_envelope(
        &envelope,
        "client-zenoh-message",
        now_unix_ms / 1000,
        CALLBACK_LIFETIME + 1,
    )?;
    if envelope.app_id != identity.application_id.as_str()
        || envelope.key_id != identity.certificate_identity.as_str()
        || &envelope.claims.identity != identity
    {
        return Err(ServiceAuthError);
    }
    signature::UnparsedPublicKey::new(&signature::ED25519, public_key)
        .verify(encoded.as_bytes(), &sig)
        .map_err(|_| ServiceAuthError)?;
    let claims = envelope.claims.message;
    validate_transport_scope(
        &claims,
        envelope.issued_at,
        envelope.expires_at,
        kind,
        target,
        request_id,
        body,
        now_unix_ms,
    )?;
    Ok(claims)
}

/// Verifies bytes/route/request scope only. Adapters must atomically consume the
/// nonce in a bounded shared replay cache and recheck current role/call authority.
#[cfg(feature = "service-transport")]
#[allow(clippy::too_many_arguments)]
pub fn verify_transport_message(
    trust: &ServiceTrust,
    proof: &str,
    app: &str,
    kind: crate::service_transport::MessageKind,
    target: &crate::service_transport::ExactRouteKey,
    request_id: &crate::service_transport::RouteIdentity,
    body: &[u8],
    now_unix_ms: i64,
) -> Result<TransportMessageClaims, ServiceAuthError> {
    if now_unix_ms < 0 || body.len() > kind.payload_limit() {
        return Err(ServiceAuthError);
    }
    let now = now_unix_ms / 1000;
    let (encoded, sig, envelope): (_, _, Envelope<TransportMessageClaims>) = decode(proof)?;
    validate_envelope(&envelope, "zenoh-message", now, CALLBACK_LIFETIME + 1)?;
    if trust.app_id != app
        || envelope.app_id != app
        || trust.expires_at <= now
        || trust.keys.len() > 8
    {
        return Err(ServiceAuthError);
    }
    let key = trust
        .keys
        .iter()
        .find(|key| key.key_id == envelope.key_id)
        .ok_or(ServiceAuthError)?;
    if envelope.issued_at < key.not_before || envelope.expires_at > key.not_after {
        return Err(ServiceAuthError);
    }
    let public_key = URL_SAFE_NO_PAD
        .decode(&key.public_key)
        .map_err(|_| ServiceAuthError)?;
    signature::UnparsedPublicKey::new(&signature::ED25519, public_key)
        .verify(encoded.as_bytes(), &sig)
        .map_err(|_| ServiceAuthError)?;
    let claims = envelope.claims;
    validate_transport_scope(
        &claims,
        envelope.issued_at,
        envelope.expires_at,
        kind,
        target,
        request_id,
        body,
        now_unix_ms,
    )?;

    Ok(claims)
}

#[cfg(all(test, feature = "service-transport"))]
mod transport_tests {
    use super::*;
    use crate::service_transport::{ExactRouteKey, MessageKind, RouteIdentity};

    const NOW: i64 = 1_800_000_001_123;
    fn target() -> ExactRouteKey {
        ExactRouteKey::new("ls/v1/646576/platform/6e6f6465/626f6f74/control").unwrap()
    }
    fn request() -> RouteIdentity {
        RouteIdentity::new("req-1").unwrap()
    }

    #[test]
    fn signed_request_lifetime_is_independent_of_receiver_clock_and_never_extends_expiry() {
        let platform = ServiceSigner::new(&[7; 32]).unwrap();
        let trust = platform.transport_trust("app-a", NOW / 1000).unwrap();
        let bytes = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let client = ChannelMessageSigner::from_pkcs8(bytes.as_ref()).unwrap();
        let identity = ClientChannelIdentity {
            application_id: RouteIdentity::new("app-a").unwrap(),
            instance_id: RouteIdentity::new("instance").unwrap(),
            base_generation: RouteIdentity::new("base").unwrap(),
            certificate_identity: RouteIdentity::new("certificate").unwrap(),
        };
        // Both request directions and every native receiver lifetime policy.
        for (kind, maximum) in [
            (MessageKind::Register, 10_000),
            (MessageKind::CatalogRead, 10_000),
            (MessageKind::InvokeCall, 10_000),
            (MessageKind::InvokeEvent, 30_000),
            (MessageKind::PublishEvent, 10_000),
            (MessageKind::CompleteCall, 3_000),
            (MessageKind::CallHeartbeat, 3_000),
            (MessageKind::BindLane, 5_000),
        ] {
            for (offset, ttl, accepted) in [
                (2, maximum, true),
                (5_000, maximum, true),
                (-2, maximum, true),
                (0, maximum + 1, false),
                (-2, maximum + 1, false),
                (5_001, 1_000, false),
            ] {
                let issued = NOW + offset;
                let deadline = issued + ttl;
                let proof = platform
                    .sign_transport_message(
                        "app-a",
                        kind,
                        &target(),
                        &request(),
                        b"payload",
                        issued,
                        deadline,
                    )
                    .unwrap();
                let platform_claims = verify_transport_message(
                    &trust,
                    &proof,
                    "app-a",
                    kind,
                    &target(),
                    &request(),
                    b"payload",
                    NOW,
                )
                .unwrap();
                let proof = client
                    .sign_message(
                        &identity,
                        kind,
                        &target(),
                        &request(),
                        b"payload",
                        issued,
                        deadline,
                    )
                    .unwrap();
                let client_claims = verify_client_transport_message(
                    client.public_key(),
                    &identity,
                    &proof,
                    kind,
                    &target(),
                    &request(),
                    b"payload",
                    NOW,
                )
                .unwrap();
                for claims in [platform_claims, client_claims] {
                    assert_eq!(
                        claims.validate_request_time(NOW, maximum).is_ok(),
                        accepted,
                        "kind={kind:?} offset={offset} ttl={ttl}"
                    );
                    assert!(claims.validate_request_time(deadline, maximum).is_err());
                    assert!(claims.validate_request_time(NOW, 0).is_err());
                    assert!(claims.validate_request_time(-1, maximum).is_err());
                }
            }
        }
    }

    #[test]
    fn client_proof_uses_the_admitted_csr_key_and_binds_the_complete_principal() {
        let bytes = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let signer = ChannelMessageSigner::from_pkcs8(bytes.as_ref()).unwrap();
        let identity = ClientChannelIdentity {
            application_id: RouteIdentity::new("app-a").unwrap(),
            instance_id: RouteIdentity::new("instance-a").unwrap(),
            base_generation: RouteIdentity::new("generation-1").unwrap(),
            certificate_identity: RouteIdentity::new("channel-a").unwrap(),
        };
        let proof = signer
            .sign_message(
                &identity,
                MessageKind::Register,
                &target(),
                &request(),
                b"payload",
                NOW,
                NOW + 1000,
            )
            .unwrap();
        let check = |identity: &ClientChannelIdentity, key: &[u8], body: &[u8], now| {
            verify_client_transport_message(
                key,
                identity,
                &proof,
                MessageKind::Register,
                &target(),
                &request(),
                body,
                now,
            )
        };
        assert!(check(&identity, signer.public_key(), b"payload", NOW).is_ok());
        assert!(check(&identity, &[0; 32], b"payload", NOW).is_err());
        assert!(check(&identity, signer.public_key(), b"changed", NOW).is_err());
        assert!(check(&identity, signer.public_key(), b"payload", NOW + 999).is_ok());
        assert!(check(&identity, signer.public_key(), b"payload", NOW + 1000).is_err());
        for field in 0..4 {
            let mut other = identity.clone();
            let replaced = match field {
                0 => &mut other.application_id,
                1 => &mut other.instance_id,
                2 => &mut other.base_generation,
                _ => &mut other.certificate_identity,
            };
            *replaced = RouteIdentity::new("other").unwrap();
            assert!(check(&other, signer.public_key(), b"payload", NOW).is_err());
        }
        assert!(verify_client_transport_message(
            signer.public_key(),
            &identity,
            &proof,
            MessageKind::RenewRoles,
            &target(),
            &request(),
            b"payload",
            NOW
        )
        .is_err());
        assert!(verify_client_transport_message(
            signer.public_key(),
            &identity,
            &proof,
            MessageKind::Register,
            &target(),
            &RouteIdentity::new("req-other").unwrap(),
            b"payload",
            NOW
        )
        .is_err());
        assert!(verify_client_transport_message(
            signer.public_key(),
            &identity,
            &proof,
            MessageKind::Register,
            &ExactRouteKey::new("ls/v1/other/control").unwrap(),
            &request(),
            b"payload",
            NOW
        )
        .is_err());
        let platform = ServiceSigner::new(&[7; 32]).unwrap();
        let trust = platform.transport_trust("app-a", NOW / 1000).unwrap();
        assert!(verify_transport_message(
            &trust,
            &proof,
            "app-a",
            MessageKind::Register,
            &target(),
            &request(),
            b"payload",
            NOW
        )
        .is_err());
        let platform_proof = platform
            .sign_transport_message(
                "app-a",
                MessageKind::Register,
                &target(),
                &request(),
                b"payload",
                NOW,
                NOW + 1000,
            )
            .unwrap();
        assert!(check(&identity, signer.public_key(), b"payload", NOW).is_ok());
        assert!(verify_client_transport_message(
            signer.public_key(),
            &identity,
            &platform_proof,
            MessageKind::Register,
            &target(),
            &request(),
            b"payload",
            NOW
        )
        .is_err());
        assert_eq!(format!("{signer:?}"), "ChannelMessageSigner([REDACTED])");
    }

    #[test]
    fn catalog_wait_rejection_is_authenticated_and_bound_to_register_request() {
        use crate::service_transport::channel::{
            ChannelEnrollmentError, ChannelEnrollmentRejection,
        };
        let signer = ServiceSigner::new(&[7; 32]).unwrap();
        let trust = signer.transport_trust("app-a", NOW / 1000).unwrap();
        let payload = serde_json::to_vec(&ChannelEnrollmentRejection {
            enrollment_error: ChannelEnrollmentError::CatalogNotReady,
        })
        .unwrap();
        let proof = signer
            .sign_transport_message(
                "app-a",
                MessageKind::Register,
                &target(),
                &request(),
                &payload,
                NOW,
                NOW + 10_000,
            )
            .unwrap();
        assert!(verify_transport_message(
            &trust,
            &proof,
            "app-a",
            MessageKind::Register,
            &target(),
            &request(),
            &payload,
            NOW
        )
        .is_ok());
        for (app, kind, id, bytes) in [
            ("other", MessageKind::Register, request(), payload.clone()),
            ("app-a", MessageKind::BindLane, request(), payload.clone()),
            (
                "app-a",
                MessageKind::Register,
                RouteIdentity::new("another").unwrap(),
                payload.clone(),
            ),
            (
                "app-a",
                MessageKind::Register,
                request(),
                b"{\"enrollment_error\":\"rejected\"}".to_vec(),
            ),
        ] {
            assert!(verify_transport_message(
                &trust,
                &proof,
                app,
                kind,
                &target(),
                &id,
                &bytes,
                NOW
            )
            .is_err());
        }
        assert!(verify_transport_message(
            &trust,
            "unsigned",
            "app-a",
            MessageKind::Register,
            &target(),
            &request(),
            &payload,
            NOW
        )
        .is_err());
        assert!(serde_json::from_str::<ChannelEnrollmentRejection>(
            r#"{"enrollment_error":"unknown"}"#
        )
        .is_err());
        assert!(serde_json::from_str::<ChannelEnrollmentRejection>(
            r#"{"enrollment_error":"catalog_not_ready","extra":true}"#
        )
        .is_err());
    }

    #[test]
    fn proof_binds_message_target_application_request_bytes_and_exact_deadline() {
        let signer = ServiceSigner::new(&[7; 32]).unwrap();
        let trust = signer.transport_trust("app-a", NOW / 1000).unwrap();
        let proof = signer
            .sign_transport_message(
                "app-a",
                MessageKind::CompleteCall,
                &target(),
                &request(),
                b"payload",
                NOW,
                NOW + 60_000,
            )
            .unwrap();
        let check = |app, kind, target: &ExactRouteKey, req: &RouteIdentity, bytes, time| {
            verify_transport_message(&trust, &proof, app, kind, target, req, bytes, time)
        };
        assert!(check(
            "app-a",
            MessageKind::CompleteCall,
            &target(),
            &request(),
            b"payload",
            NOW
        )
        .is_ok());
        assert!(check(
            "app-b",
            MessageKind::CompleteCall,
            &target(),
            &request(),
            b"payload",
            NOW
        )
        .is_err());
        assert!(check(
            "app-a",
            MessageKind::PublishEvent,
            &target(),
            &request(),
            b"payload",
            NOW
        )
        .is_err());
        assert!(check(
            "app-a",
            MessageKind::CompleteCall,
            &ExactRouteKey::new("ls/v1/other/control").unwrap(),
            &request(),
            b"payload",
            NOW
        )
        .is_err());
        assert!(check(
            "app-a",
            MessageKind::CompleteCall,
            &target(),
            &RouteIdentity::new("req-2").unwrap(),
            b"payload",
            NOW
        )
        .is_err());
        assert!(check(
            "app-a",
            MessageKind::CompleteCall,
            &target(),
            &request(),
            b"changed",
            NOW
        )
        .is_err());
        assert!(check(
            "app-a",
            MessageKind::CompleteCall,
            &target(),
            &request(),
            b"payload",
            NOW + 59_999
        )
        .is_ok());
        assert!(check(
            "app-a",
            MessageKind::CompleteCall,
            &target(),
            &request(),
            b"payload",
            NOW + 60_000
        )
        .is_err());
        assert!(signer
            .sign_transport_message(
                "app-a",
                MessageKind::CompleteCall,
                &target(),
                &request(),
                b"payload",
                NOW,
                NOW + 60_001
            )
            .is_err());
    }

    #[test]
    fn http_and_zenoh_proofs_and_trust_are_not_interchangeable() {
        let signer = ServiceSigner::new(&[7; 32]).unwrap();
        let http_trust = signer.trust("app-a", NOW / 1000).unwrap();
        let transport_trust = signer.transport_trust("app-a", NOW / 1000).unwrap();
        let proof = signer
            .sign_transport_message(
                "app-a",
                MessageKind::InvokeCall,
                &target(),
                &request(),
                b"payload",
                NOW,
                NOW + 5000,
            )
            .unwrap();
        assert!(verify_transport_message(
            &http_trust,
            &proof,
            "app-a",
            MessageKind::InvokeCall,
            &target(),
            &request(),
            b"payload",
            NOW
        )
        .is_err());
        assert!(verify_callback(
            &transport_trust,
            &proof,
            "app-a",
            "POST",
            target().as_str(),
            b"payload",
            NOW / 1000
        )
        .is_err());
        let callback = signer
            .sign_callback("app-a", "POST", target().as_str(), b"payload", NOW / 1000)
            .unwrap();
        assert!(verify_transport_message(
            &transport_trust,
            &callback,
            "app-a",
            MessageKind::InvokeCall,
            &target(),
            &request(),
            b"payload",
            NOW
        )
        .is_err());
        assert!(verify_callback(
            &http_trust,
            &callback,
            "app-a",
            "POST",
            target().as_str(),
            b"payload",
            NOW / 1000
        )
        .is_ok());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const NOW: i64 = 1_800_000_001;

    #[test]
    fn consumer_presence_proofs_are_short_lived_and_not_session_credentials() {
        let signer = ServiceSigner::new(&[7; 32]).unwrap();
        let proof = signer.sign_consumer_presence("app", &42u64, 1000).unwrap();
        assert_eq!(
            signer
                .verify_consumer_presence::<u64>(&proof, 1001)
                .unwrap(),
            ("app".into(), 42)
        );
        assert!(signer
            .verify_consumer_presence::<u64>(&proof, 1003)
            .is_err());
        assert!(signer.verify_session::<u64>(&proof, 1001).is_err());
        let session = signer.sign_session("app", &42u64, 1000, 1100).unwrap();
        assert!(signer
            .verify_consumer_presence::<u64>(&session, 1001)
            .is_err());
    }
    #[test]
    fn callback_binds_application_method_target_body_and_time() {
        let signer = ServiceSigner::new(&[7; 32]).unwrap();
        let trust = signer.trust("app-a", NOW).unwrap();
        let proof = signer
            .sign_callback("app-a", "POST", "https://app/events", b"payload", NOW)
            .unwrap();
        assert!(verify_callback(
            &trust,
            &proof,
            "app-a",
            "POST",
            "https://app/events",
            b"payload",
            NOW
        )
        .is_ok());
        for (app, method, url, body, time) in [
            (
                "app-b",
                "POST",
                "https://app/events",
                b"payload".as_slice(),
                NOW,
            ),
            (
                "app-a",
                "GET",
                "https://app/events",
                b"payload".as_slice(),
                NOW,
            ),
            (
                "app-a",
                "POST",
                "https://other/events",
                b"payload".as_slice(),
                NOW,
            ),
            (
                "app-a",
                "POST",
                "https://app/events",
                b"tampered".as_slice(),
                NOW,
            ),
            (
                "app-a",
                "POST",
                "https://app/events",
                b"payload".as_slice(),
                NOW + 60,
            ),
            (
                "app-a",
                "POST",
                "https://app/events",
                b"payload".as_slice(),
                NOW - 31,
            ),
        ] {
            assert!(verify_callback(&trust, &proof, app, method, url, body, time).is_err());
        }
        let other = ServiceSigner::new(&[8; 32])
            .unwrap()
            .trust("app-a", NOW)
            .unwrap();
        assert!(verify_callback(
            &other,
            &proof,
            "app-a",
            "POST",
            "https://app/events",
            b"payload",
            NOW
        )
        .is_err());
    }

    #[test]
    fn overlapping_public_keys_cover_epoch_rotation() {
        let signer = ServiceSigner::new(&[7; 32]).unwrap();
        let now = NOW / 3600 * 3600 + 3599;
        let trust = signer.trust("a", now).unwrap();
        let proof = signer
            .sign_callback("a", "POST", "https://a/cb", b"", now + 2)
            .unwrap();
        assert!(verify_callback(&trust, &proof, "a", "POST", "https://a/cb", b"", now + 2).is_ok());
        assert!(signer.trust("b", now).unwrap().keys[0].public_key != trust.keys[0].public_key);
    }

    #[test]
    fn session_proofs_expire_and_cannot_be_used_as_callbacks() {
        let signer = ServiceSigner::new(&[7; 32]).unwrap();
        let proof = signer.sign_session("a", &42u64, NOW, NOW + 300).unwrap();
        assert_eq!(
            signer.verify_session::<u64>(&proof, NOW).unwrap(),
            ("a".into(), 42)
        );
        assert!(signer.verify_session::<u64>(&proof, NOW + 300).is_err());
        assert!(signer.sign_session("a", &42u64, NOW, NOW + 301).is_err());
        let callback = signer
            .sign_callback("a", "POST", "https://a", b"", NOW)
            .unwrap();
        assert!(signer
            .verify_session::<CallbackClaims>(&callback, NOW)
            .is_err());
        assert!(ServiceSigner::new(b"short").is_err());
        assert!(!format!("{:?}", signer).contains("777"));
    }
}

#[cfg(test)]
mod native_service_tests {
    use super::*;
    use serde_json::{json, Value};
    #[test]
    fn service_proofs_are_scoped_by_purpose_application_and_time() {
        let signer = ServiceSigner::new(&[71; 32]).unwrap();
        let other = ServiceSigner::new(&[72; 32]).unwrap();
        let now = 1_800_000_000;
        let token = signer
            .sign_service_session("app-a", &json!({"node":"one","generation":"v1"}), now)
            .unwrap();
        let (app, _) = signer
            .verify_service_session::<Value>(&token, now + 1)
            .unwrap();
        assert_eq!(app, "app-a");
        assert!(signer
            .verify_service_completion::<Value>(&token, now + 1)
            .is_err());
        assert!(signer.verify_session::<Value>(&token, now + 1).is_err());
        assert!(signer
            .verify_service_session::<Value>(&token, now + 301)
            .is_err());
        assert!(other
            .verify_service_session::<Value>(&token, now + 1)
            .is_err());
        let completion = signer
            .sign_service_completion(
                "app-a",
                &json!({"call":"one","attempt":1}),
                now,
                now + 86_400,
            )
            .unwrap();
        assert!(signer
            .verify_service_completion::<Value>(&completion, now + 86_399)
            .is_ok());
        assert!(signer
            .verify_service_session::<Value>(&completion, now + 1)
            .is_err());
        assert!(signer
            .verify_service_completion::<Value>(&completion, now + 86_461)
            .is_err());
        assert!(signer
            .sign_service_completion("app-a", &json!({}), now, now + 90_000)
            .is_err());
    }
}
