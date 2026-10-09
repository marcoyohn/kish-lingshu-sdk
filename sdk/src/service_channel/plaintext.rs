//! A transport possession key bound by the existing signed CSR and HTTPS bootstrap.
//! Payloads remain unencrypted. Neither private key is sent to the platform.
use crate::ServiceAuthError;

pub(super) struct PlaintextKey {
    #[cfg(feature = "service-plaintext")]
    key: zenss_client_sdk::credentials::PossessionKey,
}

#[cfg(all(test, feature = "service-plaintext"))]
mod tests {
    use super::*;
    #[test]
    fn transport_key_has_matching_native_keys_and_a_public_only_csr_binding() {
        use rsa::pkcs1::{DecodeRsaPrivateKey, DecodeRsaPublicKey, EncodeRsaPublicKey};
        use sha2::{Digest, Sha256};
        let key = PlaintextKey::generate().unwrap();
        let config = key.native_config().unwrap();
        let private = rsa::RsaPrivateKey::from_pkcs1_pem(
            config["pubkey"]["private_key_pem"].as_str().unwrap(),
        )
        .unwrap();
        let public =
            rsa::RsaPublicKey::from_pkcs1_pem(config["pubkey"]["public_key_pem"].as_str().unwrap())
                .unwrap();
        assert_eq!(private.to_public_key(), public);
        let rcgen::SanType::URI(name) = key.csr_name().unwrap() else {
            panic!("URI SAN required")
        };
        assert_eq!(name.as_str(), format!("{}{:x}",
            kish_lingshu_foundation_contract::service_transport::bootstrap::TCP_PUBLIC_KEY_SAN_PREFIX,
            Sha256::digest(public.to_pkcs1_der().unwrap().as_bytes())));
        assert!(!name.as_str().contains("PRIVATE"));
    }
}

impl PlaintextKey {
    pub(super) fn generate() -> Result<Self, ServiceAuthError> {
        #[cfg(feature = "service-plaintext")]
        {
            zenss_client_sdk::credentials::PossessionKey::generate()
                .map(|key| Self { key })
                .map_err(|_| ServiceAuthError::InvalidCredential)
        }
        #[cfg(not(feature = "service-plaintext"))]
        Err(ServiceAuthError::InvalidCredential)
    }

    pub(super) fn csr_name(&self) -> Result<rcgen::SanType, ServiceAuthError> {
        #[cfg(feature = "service-plaintext")]
        {
            let fingerprint = self
                .key
                .fingerprint()
                .map_err(|_| ServiceAuthError::InvalidCredential)?;
            let name = format!("{}{fingerprint}",
                kish_lingshu_foundation_contract::service_transport::bootstrap::TCP_PUBLIC_KEY_SAN_PREFIX);
            Ok(rcgen::SanType::URI(
                name.try_into()
                    .map_err(|_| ServiceAuthError::InvalidCredential)?,
            ))
        }
        #[cfg(not(feature = "service-plaintext"))]
        Err(ServiceAuthError::InvalidCredential)
    }

    #[cfg(feature = "service-plaintext")]
    pub(super) fn native_config(&self) -> Result<serde_json::Value, ServiceAuthError> {
        self.key
            .native_config()
            .map_err(|_| ServiceAuthError::InvalidCredential)
    }
    #[cfg(feature = "service-plaintext")]
    pub(super) fn platform_key(&self) -> &zenss_client_sdk::credentials::PossessionKey {
        &self.key
    }
}
