//! A channel authorization observation is not an instance or role renewal.
use super::{bootstrap::*, RouteIdentity, TransportContractError};
use crate::ServiceInstanceIdentity;
use serde::{Deserialize, Serialize};

pub const MAX_CHANNEL_CONTROL_BYTES: usize = 32 * 1024;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelAuthorization {
    pub application_id: RouteIdentity,
    pub instance: ServiceInstanceIdentity,
    pub certificate_identity: RouteIdentity,
    pub authorization_issued_unix_ms: i64,
    pub authorization_expires_unix_ms: i64,
}
impl std::fmt::Debug for ChannelAuthorization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelAuthorization")
            .field("application_id", &self.application_id)
            .field("instance", &self.instance)
            .field(
                "authorization_expires_unix_ms",
                &self.authorization_expires_unix_ms,
            )
            .finish_non_exhaustive()
    }
}
impl ChannelAuthorization {
    pub fn validate(
        &self,
        initial: &ChannelBootstrapResponse,
        now: i64,
    ) -> Result<(), TransportContractError> {
        if self.application_id != initial.application_id
            || self.instance != initial.instance
            || self.certificate_identity != initial.certificate.certificate_identity
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        let duration = self
            .authorization_expires_unix_ms
            .checked_sub(self.authorization_issued_unix_ms)
            .ok_or(TransportContractError::Expired)?;
        if self.authorization_issued_unix_ms < 0
            || self.authorization_issued_unix_ms > now.saturating_add(MAX_BOOTSTRAP_CLOCK_SKEW_MS)
            || !(1..=CHANNEL_AUTHORIZATION_MS).contains(&duration)
            || self.authorization_expires_unix_ms <= now
            || self.authorization_expires_unix_ms > initial.certificate.expires_unix_ms
        {
            return Err(TransportContractError::Expired);
        }
        Ok(())
    }
}

/// Explicit retirement receipt. No client-supplied predecessor or role list is
/// accepted; lineage and the complete role census belong to the platform.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelRotationFinalization {
    pub previous_certificate_identity: RouteIdentity,
    pub authorization: ChannelAuthorization,
}
impl ChannelRotationFinalization {
    pub fn validate(
        &self,
        initial: &ChannelBootstrapResponse,
        predecessor: &RouteIdentity,
        now: i64,
    ) -> Result<(), TransportContractError> {
        if &self.previous_certificate_identity != predecessor
            || self.previous_certificate_identity == self.authorization.certificate_identity
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        self.authorization.validate(initial, now)
    }
}

/// Only accepted inside a verified Register reply bound to the original request.
/// CatalogNotReady guarantees rejection before any enrollment side effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelEnrollmentRejection {
    pub enrollment_error: ChannelEnrollmentError,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelEnrollmentError {
    CatalogNotReady,
    Rejected,
}
