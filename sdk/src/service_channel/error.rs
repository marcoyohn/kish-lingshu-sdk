//! Lingshu protocol errors remain separate from product-neutral transport errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChannelSessionError {
    #[error("invalid channel session configuration")]
    InvalidConfig,
    #[error("channel sessions require a Tokio multi-thread runtime")]
    UnsupportedRuntime,
    #[error("logical service connection channel capacity exhausted")]
    CapacityExceeded,
    #[error("channel authority expired")]
    AuthorityExpired,
    #[error("logical service connection closed")]
    Closed,
    #[error("channel connection failed")]
    Transport,
    #[error("invalid channel control response")]
    InvalidResponse,
    #[error("managed certificate rotation failed")]
    RotationFailed,
    #[error("channel cleanup failed")]
    CleanupFailed,
    /// Authenticated rejection before any role mutation. Safe to reconcile later.
    #[error("role catalog is not imported or enabled")]
    CatalogNotReady,
    #[error("platform rejected channel control; registration outcome is not retryable")]
    ControlRejected,
}

impl From<zenss_client_sdk::TransportError> for ChannelSessionError {
    fn from(error: zenss_client_sdk::TransportError) -> Self {
        match error {
            zenss_client_sdk::TransportError::InvalidConfig => Self::InvalidConfig,
            zenss_client_sdk::TransportError::UnsupportedRuntime => Self::UnsupportedRuntime,
            zenss_client_sdk::TransportError::CapacityExceeded => Self::CapacityExceeded,
            zenss_client_sdk::TransportError::AuthorityExpired => Self::AuthorityExpired,
            zenss_client_sdk::TransportError::Closed => Self::Closed,
            zenss_client_sdk::TransportError::Transport => Self::Transport,
            zenss_client_sdk::TransportError::InvalidResponse => Self::InvalidResponse,
            zenss_client_sdk::TransportError::RotationFailed => Self::RotationFailed,
            zenss_client_sdk::TransportError::CleanupFailed => Self::CleanupFailed,
        }
    }
}
