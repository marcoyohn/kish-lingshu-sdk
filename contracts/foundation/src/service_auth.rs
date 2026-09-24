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
        if now < EPOCH_SECONDS {
            return Err(ServiceAuthError);
        }
        let epoch = now / EPOCH_SECONDS;
        let keys = (epoch - 1..=epoch + 1)
            .map(|id| {
                let key = self.key(app, id, "callback")?;
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

#[cfg(test)]
mod tests {
    use super::*;
    const NOW: i64 = 1_800_000_001;

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
