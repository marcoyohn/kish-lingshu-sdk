//! Bounded native declaration control. Authentication and connection identity
//! are supplied by the transport; the caller cannot choose an application/epoch.
use crate::{
    validate_registration_key, ConsumerActivationVersion, ConsumerDeclarationUpdate,
    ConsumerRegistrationError, ConsumerRegistrationProtocol, ConsumerRegistrationReceipt,
};
use serde::{Deserialize, Serialize};

pub const MAX_CONSUMER_CONTROL_BYTES: usize = 24 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerRegistrationControl {
    pub consumer_registration: ConsumerRegistrationProtocol,
    pub command: ConsumerRegistrationCommand,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConsumerRegistrationCommand {
    Negotiate,
    Declare {
        /// CAS fence for replacement across Host boots. Never supplies the new
        /// connection identity; that always comes from authenticated Host proof.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        previous_connection_epoch: Option<String>,
        update: ConsumerDeclarationUpdate,
    },
    Lookup {
        registration_key: String,
        operation_id: String,
    },
    Status {
        registration_key: String,
    },
    #[cfg(feature = "service-transport")]
    Prepare {
        version: ConsumerActivationVersion,
        enrollment: crate::ConsumerEnrollmentRequestV2,
    },
    PreparedStatus {
        version: ConsumerActivationVersion,
    },
    Acknowledge {
        version: ConsumerActivationVersion,
        role_generation: String,
    },
    Remove {
        version: ConsumerActivationVersion,
    },
}

impl ConsumerRegistrationControl {
    pub fn validate(&self) -> Result<(), ConsumerRegistrationError> {
        if serde_json::to_vec(self)
            .map_err(|_| ConsumerRegistrationError::InvalidDeclaration)?
            .len()
            > MAX_CONSUMER_CONTROL_BYTES
        {
            return Err(ConsumerRegistrationError::CapacityExceeded);
        }
        match &self.command {
            ConsumerRegistrationCommand::Negotiate => Ok(()),
            ConsumerRegistrationCommand::Declare {
                update,
                previous_connection_epoch,
            } => {
                if let Some(epoch) = previous_connection_epoch {
                    validate_registration_key(epoch)?;
                }
                update.validate()
            }
            ConsumerRegistrationCommand::Lookup {
                registration_key,
                operation_id,
            } => {
                validate_registration_key(registration_key)?;
                validate_registration_key(operation_id)
            }
            ConsumerRegistrationCommand::Status { registration_key } => {
                validate_registration_key(registration_key)
            }
            #[cfg(feature = "service-transport")]
            ConsumerRegistrationCommand::Prepare {
                version,
                enrollment,
            } => {
                validate_version(version)?;
                enrollment
                    .validate()
                    .map_err(|_| ConsumerRegistrationError::InvalidDeclaration)
            }
            ConsumerRegistrationCommand::Acknowledge {
                version,
                role_generation,
            } => {
                validate_registration_key(role_generation)?;
                validate_version(version)
            }
            ConsumerRegistrationCommand::Remove { version }
            | ConsumerRegistrationCommand::PreparedStatus { version } => validate_version(version),
        }
    }
}
fn validate_version(version: &ConsumerActivationVersion) -> Result<(), ConsumerRegistrationError> {
    for value in [
        &version.owner_boot,
        &version.connection_epoch,
        &version.registration_key,
    ] {
        validate_registration_key(value)?;
    }
    if version.declaration_revision == 0 {
        return Err(ConsumerRegistrationError::RevisionConflict);
    }
    Ok(())
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConsumerRegistrationControlResponse {
    Supported {
        protocol: ConsumerRegistrationProtocol,
        owner_boot: String,
        /// Authenticated physical control connection, supplied by the Host.
        connection_epoch: String,
        /// Declaration negotiation does not imply perpetual role authority.
        connection_lifecycle: bool,
    },
    #[cfg(feature = "service-transport")]
    Prepared {
        version: ConsumerActivationVersion,
        enrollment: crate::ConsumerEnrollmentResponseV2,
    },
    Registered {
        receipt: ConsumerRegistrationReceipt,
    },
    Removed {
        version: ConsumerActivationVersion,
    },
    Rejected {
        code: ConsumerRegistrationRejection,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerRegistrationRejection {
    InvalidDeclaration,
    CapacityExceeded,
    RevisionConflict,
    OperationConflict,
    StaleConnection,
    NotFound,
    StaleActivation,
    SourceConflict,
    Unauthorized,
    Unavailable,
}
impl From<ConsumerRegistrationError> for ConsumerRegistrationRejection {
    fn from(value: ConsumerRegistrationError) -> Self {
        match value {
            ConsumerRegistrationError::InvalidDeclaration => Self::InvalidDeclaration,
            ConsumerRegistrationError::CapacityExceeded => Self::CapacityExceeded,
            ConsumerRegistrationError::RevisionConflict => Self::RevisionConflict,
            ConsumerRegistrationError::OperationConflict => Self::OperationConflict,
            ConsumerRegistrationError::StaleConnection => Self::StaleConnection,
            ConsumerRegistrationError::NotFound => Self::NotFound,
            ConsumerRegistrationError::StaleActivation => Self::StaleActivation,
            ConsumerRegistrationError::SourceConflict => Self::SourceConflict,
        }
    }
}

/// Advisory wakeup. The SDK fetches a request-bound current receipt before
/// changing gates; a hint alone never grants or extends authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerRegistrationHint {
    pub owner_boot: String,
    pub directory_revision: u64,
}
