//! Authenticated lane attestation. Requests name slots, never physical epochs.
//! Attestation alone neither extends a lease nor grants executable capability.
use super::{RouteIdentity, TransportContractError, MAX_DATA_LANES};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionProtocol {
    #[serde(rename = "lingshu-connection/1")]
    V1,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionControl {
    pub connection_registration: ConnectionProtocol,
    pub command: ConnectionCommand,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConnectionCommand {
    /// Read the authenticated owner's lifecycle capabilities before enrollment.
    Negotiate,
    Begin {
        binding_id: RouteIdentity,
        data_lanes: u8,
        dedicated_control: bool,
    },
    Attest {
        binding_id: RouteIdentity,
        lane: u8,
    },
    Inspect {
        binding_id: RouteIdentity,
    },
    /// Requests activation of the server-retained proof; no proof is accepted
    /// from the request body.
    Activate {
        binding_id: RouteIdentity,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionCapabilities {
    pub protocol: ConnectionProtocol,
    pub owner_id: RouteIdentity,
    pub host_boot: RouteIdentity,
    pub connection_lifecycle: bool,
    /// A pool binds all lanes to one authoritative edge. Other platform nodes
    /// may dispatch through that edge; they cannot borrow its SDK grants.
    pub single_host_pool: bool,
}
impl ConnectionControl {
    pub fn validate(&self) -> Result<(), TransportContractError> {
        match &self.command {
            ConnectionCommand::Begin {
                data_lanes,
                dedicated_control,
                ..
            } if *data_lanes == 0
                || *data_lanes as usize > MAX_DATA_LANES
                || *data_lanes as usize + usize::from(*dedicated_control) > 4 =>
            {
                Err(TransportContractError::InvalidEnvelope)
            }
            ConnectionCommand::Attest { lane, .. } if *lane as usize >= MAX_DATA_LANES => {
                Err(TransportContractError::InvalidEnvelope)
            }
            _ => Ok(()),
        }
    }
}
/// Host evidence returned in a signed response, never accepted as client input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalConnectionIdentity {
    pub host_boot: RouteIdentity,
    pub transport_epoch: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionAttestation {
    pub protocol: ConnectionProtocol,
    pub binding_id: RouteIdentity,
    pub owner_id: RouteIdentity,
    pub control: PhysicalConnectionIdentity,
    pub data: Vec<Option<PhysicalConnectionIdentity>>,
    pub dedicated_control: bool,
    /// Original handshake deadline; retrying cannot refresh this window.
    pub expires_unix_ms: i64,
}
impl ConnectionAttestation {
    pub fn validate(&self, now: i64) -> Result<(), TransportContractError> {
        if self.control.transport_epoch == 0
            || self.data.is_empty()
            || self.data.len() > MAX_DATA_LANES
            || self.data.len() + usize::from(self.dedicated_control) > 4
            || self.expires_unix_ms <= now
        {
            return Err(TransportContractError::InvalidEnvelope);
        }
        let mut epochs = std::collections::HashSet::new();
        for (index, lane) in self.data.iter().enumerate() {
            if let Some(lane) = lane {
                if lane.host_boot != self.control.host_boot
                    || lane.transport_epoch == 0
                    || !epochs.insert(lane.transport_epoch)
                    || (lane == &self.control) != (!self.dedicated_control && index == 0)
                {
                    return Err(TransportContractError::InvalidIdentity);
                }
            }
        }
        Ok(())
    }
    pub fn complete(&self) -> bool {
        self.data.iter().all(Option::is_some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn requests_cannot_supply_epoch_evidence_or_exceed_the_physical_pool() {
        assert!(serde_json::from_str::<ConnectionControl>(r#"{"connection_registration":"lingshu-connection/1","command":{"action":"attest","binding_id":"binding","lane":0,"transport_epoch":8}}"#).is_err());
        for (lanes, dedicated, valid) in [
            (0, false, false),
            (4, true, false),
            (3, true, true),
            (4, false, true),
        ] {
            let request = ConnectionControl {
                connection_registration: ConnectionProtocol::V1,
                command: ConnectionCommand::Begin {
                    binding_id: RouteIdentity::new("binding").unwrap(),
                    data_lanes: lanes,
                    dedicated_control: dedicated,
                },
            };
            assert_eq!(request.validate().is_ok(), valid);
        }
    }
    #[test]
    fn attestation_rejects_foreign_duplicate_and_wrong_control_lanes() {
        let control = PhysicalConnectionIdentity {
            host_boot: RouteIdentity::new("host").unwrap(),
            transport_epoch: 1,
        };
        let mut attestation = ConnectionAttestation {
            protocol: ConnectionProtocol::V1,
            binding_id: RouteIdentity::new("binding").unwrap(),
            owner_id: RouteIdentity::new("owner").unwrap(),
            control: control.clone(),
            data: vec![Some(control.clone()), None],
            dedicated_control: false,
            expires_unix_ms: 100,
        };
        assert!(attestation.validate(1).is_ok());
        assert!(!attestation.complete());
        attestation.data[1] = Some(control.clone());
        assert!(attestation.validate(1).is_err());
        attestation.data[1].as_mut().unwrap().transport_epoch = 2;
        assert!(attestation.validate(1).is_ok());
        assert!(attestation.complete());
        attestation.dedicated_control = true;
        assert!(attestation.validate(1).is_err());
        attestation.dedicated_control = false;
        attestation.data[1].as_mut().unwrap().host_boot = RouteIdentity::new("other").unwrap();
        assert!(attestation.validate(1).is_err());
        assert!(attestation.validate(100).is_err());
    }
}
