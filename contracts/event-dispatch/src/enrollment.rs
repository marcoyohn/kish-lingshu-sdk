//! V2 Consumer requests preserve authoritative group selection and member leases.
use super::{ConsumerEnrollmentRequest, ConsumerSession};
use kish_lingshu_foundation_contract::{
    service_transport::{
        enrollment::{validate_registration, EnrollmentVersion, RequestedServiceEndpoint},
        ServiceEndpoint, TransportContractError,
    },
    ServiceInstanceRegistration,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerEnrollmentRequestV2 {
    pub enrollment_version: EnrollmentVersion,
    pub instance: ServiceInstanceRegistration,
    pub group_key: String,
    pub node_id: String,
    pub endpoint: RequestedServiceEndpoint,
    pub maximum_in_flight: u32,
}
impl ConsumerEnrollmentRequestV2 {
    pub fn validate(&self) -> Result<(), TransportContractError> {
        validate_registration(&self.instance)?;
        self.endpoint.validate()?;
        for key in [&self.group_key, &self.node_id] {
            if key.is_empty()
                || key.len() > 128
                || !key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
            {
                return Err(TransportContractError::InvalidIdentity);
            }
        }
        if !(1..=65_536).contains(&self.maximum_in_flight)
            || matches!(self.endpoint, RequestedServiceEndpoint::Zenoh { .. })
                && self.instance.generation.is_none()
        {
            return Err(TransportContractError::InvalidIdentity);
        }
        Ok(())
    }
}
impl TryFrom<ConsumerEnrollmentRequestV2> for ConsumerEnrollmentRequest {
    type Error = TransportContractError;
    fn try_from(value: ConsumerEnrollmentRequestV2) -> Result<Self, Self::Error> {
        value.validate()?;
        let RequestedServiceEndpoint::Http { invocation_url } = value.endpoint else {
            return Err(TransportContractError::UnsupportedProtocol);
        };
        Ok(Self {
            instance: Some(value.instance),
            group_key: value.group_key,
            node_id: value.node_id,
            invocation_url,
            maximum_in_flight: value.maximum_in_flight,
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerEnrollmentResponseV2 {
    pub enrollment_version: EnrollmentVersion,
    pub session: ConsumerSession,
    pub endpoint: ServiceEndpoint,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn event_v2_cannot_choose_group_id_or_smuggle_a_v1_url() {
        let value = json!({"enrollment_version":2,"instance":{"instance_id":"sdk","incarnation_id":"boot","generation":"base"},"group_key":"orders","node_id":"replica","maximum_in_flight":8,"endpoint":{"transport":"zenoh","protocol_version":"lingshu-zenoh/1"}});
        let request: ConsumerEnrollmentRequestV2 = serde_json::from_value(value.clone()).unwrap();
        assert!(request.validate().is_ok());
        assert!(ConsumerEnrollmentRequest::try_from(request).is_err());
        for field in ["group_id", "invocation_url"] {
            let mut extra = value.clone();
            extra[field] = json!("caller-selected");
            assert!(serde_json::from_value::<ConsumerEnrollmentRequestV2>(extra).is_err());
        }
        let mut http = value;
        http["endpoint"] =
            json!({"transport":"http","invocation_url":"https://sdk.example/events"});
        let request: ConsumerEnrollmentRequestV2 = serde_json::from_value(http).unwrap();
        let legacy = ConsumerEnrollmentRequest::try_from(request).unwrap();
        assert_eq!(legacy.group_key, "orders");
        assert!(serde_json::from_value::<ConsumerEnrollmentRequest>(
            serde_json::to_value(legacy).unwrap()
        )
        .is_ok());
    }
}
