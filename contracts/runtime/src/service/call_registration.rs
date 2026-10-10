//! Ephemeral Call declarations. Publication is an independent governance action.
use super::{InstanceCapability, ServiceEnrollmentResponseV2};
use kish_lingshu_foundation_contract::service_transport::{RouteIdentity, TransportContractError};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallDeclaration {
    pub node_id: String,
    pub maximum_in_flight: u32,
    pub lane_count: u8,
    pub capabilities: Vec<InstanceCapability>,
}
impl CallDeclaration {
    pub fn validate(&self) -> Result<(), TransportContractError> {
        RouteIdentity::new(&self.node_id)?;
        if !(1..=65_536).contains(&self.maximum_in_flight)
            || !(1..=4).contains(&self.lane_count)
            || self.capabilities.len() != 1
            || !self.capabilities[0].call
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        let op = &self.capabilities[0].operation;
        if !super::valid_key(&op.service_key)
            || !super::valid_key(&op.operation_key)
            || !super::valid_key(&op.version)
            || op.contract_digest.len() != 64
            || !op
                .contract_digest
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallActivationVersion {
    pub owner_boot: String,
    pub connection_epoch: String,
    pub node_id: String,
    pub revision: u64,
    pub activation_revision: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallActivationState {
    WaitingForCatalog,
    Activating,
    Active,
    Disabled,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallRegistrationReceipt {
    pub version: CallActivationVersion,
    pub declaration: CallDeclaration,
    pub state: CallActivationState,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallRegistrationControl {
    pub call_registration: CallRegistrationProtocol,
    pub command: CallRegistrationCommand,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallRegistrationProtocol {
    #[serde(rename = "1")]
    V1,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum CallRegistrationCommand {
    Negotiate,
    Declare {
        operation_id: String,
        previous_connection_epoch: Option<String>,
        expected_revision: Option<u64>,
        declaration: CallDeclaration,
    },
    Status {
        node_id: String,
    },
    Lookup {
        node_id: String,
        operation_id: String,
    },
    Prepare {
        version: CallActivationVersion,
    },
    PreparedStatus {
        version: CallActivationVersion,
    },
    Acknowledge {
        version: CallActivationVersion,
        role_generation: String,
    },
    Remove {
        version: CallActivationVersion,
    },
}
impl CallRegistrationControl {
    pub fn validate(&self) -> Result<(), TransportContractError> {
        if serde_json::to_vec(self).map_or(true, |v| v.len() > 16 * 1024) {
            return Err(TransportContractError::InvalidIdentity);
        }
        if let CallRegistrationCommand::Acknowledge {
            role_generation, ..
        } = &self.command
        {
            RouteIdentity::new(role_generation)?;
        }
        match &self.command {
            CallRegistrationCommand::Negotiate => (),
            CallRegistrationCommand::Declare {
                operation_id,
                previous_connection_epoch,
                expected_revision,
                declaration,
            } => {
                RouteIdentity::new(operation_id)?;
                if let Some(epoch) = previous_connection_epoch {
                    RouteIdentity::new(epoch)?;
                }
                if *expected_revision == Some(0) {
                    return Err(TransportContractError::InvalidIdentity);
                }
                declaration.validate()?;
            }
            CallRegistrationCommand::Status { node_id } => {
                RouteIdentity::new(node_id)?;
            }
            CallRegistrationCommand::Lookup {
                node_id,
                operation_id,
            } => {
                RouteIdentity::new(node_id)?;
                RouteIdentity::new(operation_id)?;
            }
            CallRegistrationCommand::Acknowledge { version, .. }
            | CallRegistrationCommand::Prepare { version }
            | CallRegistrationCommand::PreparedStatus { version }
            | CallRegistrationCommand::Remove { version } => {
                RouteIdentity::new(&version.owner_boot)?;
                RouteIdentity::new(&version.connection_epoch)?;
                RouteIdentity::new(&version.node_id)?;
                if version.revision == 0 || version.activation_revision == 0 {
                    return Err(TransportContractError::InvalidIdentity);
                }
            }
        }
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case", deny_unknown_fields)]
pub enum CallRegistrationResponse {
    Supported {
        owner_boot: String,
        connection_epoch: String,
        connection_lifecycle: bool,
    },
    Registered {
        receipt: CallRegistrationReceipt,
    },
    Prepared {
        version: CallActivationVersion,
        enrollment: ServiceEnrollmentResponseV2,
    },
    Removed {
        version: CallActivationVersion,
    },
    Rejected {
        code: CallRegistrationRejection,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallRegistrationRejection {
    InvalidDeclaration,
    Unauthorized,
    CapacityExceeded,
    RevisionConflict,
    StaleConnection,
    NotFound,
    Unavailable,
    OutcomeUnknown,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn declaration() -> CallDeclaration {
        CallDeclaration { node_id: "call".into(), maximum_in_flight: 8, lane_count: 1,
            capabilities: vec![InstanceCapability { operation: super::super::OperationRef {service_key:"orders".into(),operation_key:"complete".into(),version:"v1".into(),contract_digest:"a".repeat(64)},call:true,events:Default::default()}] }
    }
    #[test]
    fn declaration_is_bounded_and_cannot_select_authority_or_route_destinations() {
        let initial = declaration(); initial.validate().unwrap();
        let request = CallRegistrationControl {call_registration:CallRegistrationProtocol::V1,command:CallRegistrationCommand::Declare {operation_id:"once".into(),previous_connection_epoch:None,expected_revision:None,declaration:initial}};
        request.validate().unwrap();
        for field in ["application_id","connection_epoch","certificate_identity","endpoint","generation"] {
            let mut value=serde_json::to_value(&request).unwrap();value["command"][field]=serde_json::json!("forged");
            assert!(serde_json::from_value::<CallRegistrationControl>(value).is_err());
        }
        for lanes in [0,5] {let mut d=declaration();d.lane_count=lanes;assert!(d.validate().is_err());}
        let mut d=declaration();d.capabilities.push(d.capabilities[0].clone());assert!(d.validate().is_err());
        let mut d=declaration();d.capabilities[0].operation.contract_digest="invalid".into();assert!(d.validate().is_err());
    }
}
