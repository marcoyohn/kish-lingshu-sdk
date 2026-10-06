use std::{fmt, time::Duration};

use kish_lingshu_runtime_contract::PrincipalKind;

use crate::Error;

pub(crate) const MAX_RETRY_LIMIT: u32 = 10;
const MAX_METADATA_BYTES: usize = 128;
const MAX_APPLICATION_ID_BYTES: usize = 255;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientMetadata {
    pub name: String,
    pub version: String,
}

impl ClientMetadata {
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
        }
    }

    pub fn user_agent(&self) -> String {
        format!("{}/{}", self.name, self.version)
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ClientConfig {
    endpoint: Option<String>,
    timeout: Duration,
    retry_limit: u32,
    metadata: ClientMetadata,
    selected_application: Option<String>,
    auth_application: Option<String>,
    auth_brand: Option<String>,
}

impl ClientConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: Some(endpoint.into()),
            ..Self::in_process()
        }
    }

    pub fn in_process() -> Self {
        Self {
            endpoint: None,
            timeout: Duration::from_secs(30),
            retry_limit: 3,
            metadata: ClientMetadata::new("kish-lingshu-sdk", env!("CARGO_PKG_VERSION")),
            selected_application: None,
            auth_application: None,
            auth_brand: None,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_retry_limit(mut self, retry_limit: u32) -> Self {
        self.retry_limit = retry_limit;
        self
    }

    pub fn with_metadata(mut self, metadata: ClientMetadata) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_selected_application(mut self, application_id: impl Into<String>) -> Self {
        self.selected_application = Some(application_id.into());
        self
    }

    pub fn with_account_context(
        mut self,
        auth_application: Option<String>,
        auth_brand: Option<String>,
    ) -> Self {
        self.auth_application = auth_application;
        self.auth_brand = auth_brand;
        self
    }

    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn retry_limit(&self) -> u32 {
        self.retry_limit
    }

    pub fn metadata(&self) -> &ClientMetadata {
        &self.metadata
    }

    pub fn selected_application(&self) -> Option<&str> {
        self.selected_application.as_deref()
    }

    #[cfg(feature = "http-client")]
    pub(crate) fn auth_application(&self) -> Option<&str> {
        self.auth_application.as_deref()
    }

    #[cfg(feature = "http-client")]
    pub(crate) fn auth_brand(&self) -> Option<&str> {
        self.auth_brand.as_deref()
    }

    pub(crate) fn validate(
        &self,
        principal: PrincipalKind,
        endpoint_required: bool,
    ) -> Result<(), Error> {
        match &self.endpoint {
            Some(endpoint) => validate_endpoint(endpoint)?,
            None if endpoint_required => {
                return Err(Error::configuration(
                    "endpoint",
                    "an HTTP endpoint is required",
                ));
            }
            None => {}
        }
        if self.timeout.is_zero() {
            return Err(Error::configuration("timeout", "value must be positive"));
        }
        if self.retry_limit > MAX_RETRY_LIMIT {
            return Err(Error::configuration(
                "retry_limit",
                format!("value must not exceed {MAX_RETRY_LIMIT}"),
            ));
        }
        validate_metadata("client_name", &self.metadata.name)?;
        validate_metadata("client_version", &self.metadata.version)?;
        if let Some(application_id) = &self.selected_application {
            validate_application_id(application_id)?;
            if principal == PrincipalKind::Service {
                return Err(Error::configuration(
                    "selected_application",
                    "service credentials derive Application scope and cannot override it",
                ));
            }
        }
        for (field, value) in [
            ("auth_application", self.auth_application.as_deref()),
            ("auth_brand", self.auth_brand.as_deref()),
        ] {
            if value
                .is_some_and(|value| value.trim().is_empty() || value.chars().any(char::is_control))
            {
                return Err(Error::configuration(
                    field,
                    "value must be non-empty and contain no control characters",
                ));
            }
        }
        Ok(())
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self::in_process()
    }
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientConfig")
            .field("endpoint", &self.endpoint)
            .field("timeout", &self.timeout)
            .field("retry_limit", &self.retry_limit)
            .field("metadata", &self.metadata)
            .field("selected_application", &self.selected_application)
            .field("auth_application", &self.auth_application)
            .field("auth_brand", &self.auth_brand)
            .finish()
    }
}

fn validate_endpoint(endpoint: &str) -> Result<(), Error> {
    let parsed = url::Url::parse(endpoint)
        .map_err(|_| Error::configuration("endpoint", "value must be an absolute HTTP URL"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(Error::configuration(
            "endpoint",
            "scheme must be http or https",
        ));
    }
    if parsed.host_str().is_none() {
        return Err(Error::configuration(
            "endpoint",
            "URL must contain an authority",
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(Error::configuration(
            "endpoint",
            "URL user information is not allowed",
        ));
    }
    Ok(())
}

fn validate_metadata(field: &'static str, value: &str) -> Result<(), Error> {
    if value.is_empty()
        || value.len() > MAX_METADATA_BYTES
        || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(Error::configuration(
            field,
            "value must be 1..=128 visible ASCII bytes",
        ));
    }
    Ok(())
}

fn validate_application_id(value: &str) -> Result<(), Error> {
    if value.trim().is_empty()
        || value.trim() != value
        || value.len() > MAX_APPLICATION_ID_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(Error::configuration(
            "selected_application",
            "value must be 1..=255 non-control characters",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_invalid(config: ClientConfig, principal: PrincipalKind, field: &str) {
        let error = config.validate(principal, false).unwrap_err();
        match error {
            Error::Configuration(error) => assert_eq!(error.field, field),
            other => panic!("expected configuration error, got {other:?}"),
        }
    }

    #[test]
    fn endpoint_requires_supported_absolute_authority_without_user_info() {
        for endpoint in [
            "relative/path",
            "ftp://example.test",
            "http://",
            "https://user:secret@example.test",
        ] {
            assert_invalid(ClientConfig::new(endpoint), PrincipalKind::User, "endpoint");
        }
        assert!(ClientConfig::new("https://example.test/api")
            .validate(PrincipalKind::User, true)
            .is_ok());
        let missing = ClientConfig::in_process()
            .validate(PrincipalKind::User, true)
            .unwrap_err();
        assert!(matches!(missing, Error::Configuration(error) if error.field == "endpoint"));
    }

    #[test]
    fn timeout_retry_metadata_and_application_scope_validate_independently() {
        assert_invalid(
            ClientConfig::in_process().with_timeout(Duration::ZERO),
            PrincipalKind::User,
            "timeout",
        );
        assert_invalid(
            ClientConfig::in_process().with_retry_limit(MAX_RETRY_LIMIT + 1),
            PrincipalKind::User,
            "retry_limit",
        );
        assert_invalid(
            ClientConfig::in_process().with_metadata(ClientMetadata::new("bad name", "1")),
            PrincipalKind::User,
            "client_name",
        );
        assert_invalid(
            ClientConfig::in_process().with_selected_application(" app "),
            PrincipalKind::User,
            "selected_application",
        );
        assert_invalid(
            ClientConfig::in_process().with_selected_application("app"),
            PrincipalKind::Service,
            "selected_application",
        );
        assert!(ClientConfig::in_process()
            .with_selected_application("app")
            .validate(PrincipalKind::User, false)
            .is_ok());
    }
}
