use std::{fmt, sync::Arc};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_CREDENTIAL_BYTES: usize = 8 * 1024;
const REDACTED: &str = "[REDACTED]";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthenticatedUser {
    pub user_type: String,
    pub user_id: String,
    pub user_name: String,
    pub nick_name: String,
    pub real_name: String,
    #[serde(rename = "app_id")]
    pub application_id: String,
    pub is_admin: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_authority_id: Option<String>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum CredentialError {
    #[error("Application identity must be 1..=255 non-control characters without surrounding whitespace")]
    InvalidApplication,
    #[error("credential must not be empty")]
    Empty,
    #[error("credential exceeds 8192 bytes")]
    TooLong,
    #[error("credential contains control characters")]
    ControlCharacter,
}

pub struct ServiceCredential {
    application_id: String,
    #[cfg_attr(not(feature = "http-client"), allow(dead_code))]
    secret: Secret,
}

impl ServiceCredential {
    pub fn new(
        application_id: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, CredentialError> {
        let application_id = application_id.into();
        if application_id.trim().is_empty()
            || application_id.trim() != application_id
            || application_id.len() > 255
            || application_id.chars().any(char::is_control)
        {
            return Err(CredentialError::InvalidApplication);
        }
        Ok(Self {
            application_id,
            secret: Secret::new(value)?,
        })
    }

    pub fn application_id(&self) -> &str {
        &self.application_id
    }

    #[cfg(feature = "http-client")]
    pub(crate) fn expose(&self) -> &str {
        self.secret.expose()
    }
}

impl fmt::Debug for ServiceCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceCredential")
            .field("application_id", &self.application_id)
            .field("secret", &REDACTED)
            .finish()
    }
}

impl fmt::Display for ServiceCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(REDACTED)
    }
}

struct Secret(#[allow(dead_code)] Arc<str>);

impl Secret {
    fn new(value: impl Into<String>) -> Result<Self, CredentialError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(CredentialError::Empty);
        }
        if value.len() > MAX_CREDENTIAL_BYTES {
            return Err(CredentialError::TooLong);
        }
        if value.chars().any(char::is_control) {
            return Err(CredentialError::ControlCharacter);
        }
        Ok(Self(Arc::from(value)))
    }

    #[cfg(any(feature = "http-client", feature = "event-consumer-http"))]
    fn expose(&self) -> &str {
        &self.0
    }
}

macro_rules! credential_type {
    ($name:ident, $label:literal) => {
        pub struct $name(#[allow(dead_code)] Secret);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, CredentialError> {
                Secret::new(value).map(Self)
            }

            #[cfg(feature = "event-consumer-http")]
            #[allow(dead_code)]
            pub(crate) fn expose(&self) -> &str {
                self.0.expose()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.debug_tuple($label).field(&REDACTED).finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(REDACTED)
            }
        }
    };
}

credential_type!(
    ConsumerGroupRegistrationCredential,
    "ConsumerGroupRegistrationCredential"
);
credential_type!(CallbackCredential, "CallbackCredential");

pub struct UserCredential {
    secret: Option<Secret>,
}

impl UserCredential {
    pub fn new(value: impl Into<String>) -> Result<Self, CredentialError> {
        Secret::new(value).map(|secret| Self {
            secret: Some(secret),
        })
    }

    /// Select the Server's explicitly configured local-deployment identity.
    pub fn local_deployment() -> Self {
        Self { secret: None }
    }

    #[cfg(feature = "http-client")]
    pub(crate) fn expose(&self) -> Option<&str> {
        self.secret.as_ref().map(Secret::expose)
    }
}

impl fmt::Debug for UserCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UserCredential")
            .field(
                "mode",
                &self.secret.as_ref().map(|_| REDACTED).unwrap_or("local"),
            )
            .finish()
    }
}

impl fmt::Display for UserCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(REDACTED)
    }
}

pub(crate) enum ClientCredential {
    Service(ServiceCredential),
    User(#[cfg_attr(not(feature = "http-client"), allow(dead_code))] UserCredential),
}

impl ClientCredential {
    #[cfg(feature = "http-client")]
    pub(crate) fn expose(&self) -> Option<&str> {
        match self {
            Self::Service(credential) => Some(credential.expose()),
            Self::User(credential) => credential.expose(),
        }
    }

    pub(crate) fn service_application_id(&self) -> Option<&str> {
        match self {
            Self::Service(credential) => Some(credential.application_id()),
            Self::User(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_credential_kinds_redact_debug_and_display() {
        let secret = "super-secret-value";
        let service = ServiceCredential::new("contract-app", secret).unwrap();
        let user = UserCredential::new(secret).unwrap();
        let registration = ConsumerGroupRegistrationCredential::new(secret).unwrap();
        let callback = CallbackCredential::new(secret).unwrap();
        let rendered = [
            format!("{service:?} {service}"),
            format!("{user:?} {user}"),
            format!("{registration:?} {registration}"),
            format!("{callback:?} {callback}"),
        ];
        for value in rendered {
            assert!(value.contains(REDACTED));
            assert!(!value.contains(secret));
        }
    }

    #[test]
    fn credential_validation_rejects_empty_control_and_oversized_values() {
        assert!(matches!(
            ServiceCredential::new("contract-app", ""),
            Err(CredentialError::Empty)
        ));
        assert!(matches!(
            ServiceCredential::new("contract-app", "   "),
            Err(CredentialError::Empty)
        ));
        assert!(matches!(
            ServiceCredential::new(" app ", "secret"),
            Err(CredentialError::InvalidApplication)
        ));
        assert!(matches!(
            UserCredential::new("line\nbreak"),
            Err(CredentialError::ControlCharacter)
        ));
        assert!(matches!(
            CallbackCredential::new("x".repeat(MAX_CREDENTIAL_BYTES + 1)),
            Err(CredentialError::TooLong)
        ));
    }
}
