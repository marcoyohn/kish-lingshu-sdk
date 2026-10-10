//! Signed pending Call declarations; hints wake current-state reconciliation.
use super::{ChannelSessionError, ServiceChannelSessions};
use kish_lingshu_runtime_contract::service::{
    CallActivationState as State, CallActivationVersion, CallRegistrationCommand as Command,
    CallRegistrationControl as Control, CallRegistrationProtocol as Protocol,
    CallRegistrationResponse as Response,
};

pub(super) fn validate_response(
    request: &Control,
    response: &Response,
) -> Result<(), ChannelSessionError> {
    use kish_lingshu_foundation_contract::service_transport::RouteIdentity;
    let valid = match (&request.command, response) {
        (_, Response::Rejected { .. }) => true,
        (
            Command::Negotiate,
            Response::Supported {
                owner_boot,
                connection_epoch,
                ..
            },
        ) => RouteIdentity::new(owner_boot).is_ok() && RouteIdentity::new(connection_epoch).is_ok(),
        (Command::Declare { declaration, .. }, Response::Registered { receipt }) => {
            declaration == &receipt.declaration
        }
        (
            Command::Status { node_id } | Command::Lookup { node_id, .. },
            Response::Registered { receipt },
        ) => node_id == &receipt.version.node_id,
        (
            Command::Prepare { version } | Command::PreparedStatus { version },
            Response::Prepared {
                version: actual, ..
            },
        ) => version == actual,
        (Command::Acknowledge { version, .. }, Response::Registered { receipt }) => {
            version == &receipt.version && receipt.state == State::Active
        }
        (Command::Remove { version }, Response::Removed { version: actual }) => version == actual,
        _ => false,
    };
    if !valid {
        return Err(ChannelSessionError::InvalidResponse);
    }
    if let Response::Registered { receipt } = response {
        let version = &receipt.version;
        if receipt.declaration.validate().is_err()
            || version.revision == 0
            || version.activation_revision == 0
            || receipt.declaration.node_id != version.node_id
            || RouteIdentity::new(&version.owner_boot).is_err()
            || RouteIdentity::new(&version.connection_epoch).is_err()
        {
            return Err(ChannelSessionError::InvalidResponse);
        }
    }
    Ok(())
}
#[cfg(feature = "service-call-zenoh")]
impl ServiceChannelSessions {
    pub(super) async fn prepare_declared_call(
        &self,
        version: &CallActivationVersion,
        node_id: &str,
        maximum_in_flight: u32,
        registry: &crate::services::ServiceRegistry,
    ) -> Result<super::RegisteredChannelRole, ChannelSessionError> {
        use kish_lingshu_foundation_contract::service_transport::{
            enrollment::{EnrollmentVersion, RequestedServiceEndpoint},
            MessageKind, ProtocolVersion,
        };
        use kish_lingshu_runtime_contract::service::{
            ChannelRoleEnrollment, ChannelRoleEnrollmentResponse, ServiceEnrollmentV2,
        };
        if registry.manifest().application_id != self.identity.connection.application_id()
            || version.node_id != node_id
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let initial = self.identity.bootstrap_response();
        let instance = self
            .identity
            .connection
            .registration()
            .await
            .request(&initial.instance.instance_id);
        let request = Control {
            call_registration: Protocol::V1,
            command: Command::Prepare {
                version: version.clone(),
            },
        };
        let started = tokio::time::Instant::now();
        let result = self
            .verified_role_control(MessageKind::Register, &request)
            .await;
        let verified = match result {
            Err(ChannelSessionError::Transport) => {
                self.verified_role_control(
                    MessageKind::Register,
                    &Control {
                        call_registration: Protocol::V1,
                        command: Command::PreparedStatus {
                            version: version.clone(),
                        },
                    },
                )
                .await?
            }
            other => other?,
        };
        let response = serde_json::from_str(verified.envelope.payload.get())
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        validate_response(&request, &response)?;
        let Response::Prepared { enrollment, .. } = response else {
            return Err(ChannelSessionError::ControlRejected);
        };
        self.install_registered_role(
            ChannelRoleEnrollment::Call(ServiceEnrollmentV2 {
                enrollment_version: EnrollmentVersion::V2,
                instance,
                node_id: node_id.into(),
                maximum_in_flight,
                capabilities: registry.capabilities(),
                endpoint: RequestedServiceEndpoint::Zenoh {
                    protocol_version: ProtocolVersion::V1,
                    lane_count: (self.lane_count() as u8)
                        .try_into()
                        .map_err(|_| ChannelSessionError::InvalidConfig)?,
                },
            }),
            None,
            Some(version.clone()),
            None,
            ChannelRoleEnrollmentResponse::Call(enrollment),
            started,
            verified.issued_at_unix_ms,
        )
        .await
    }
    pub(super) async fn acknowledge_declared_call(
        &self,
        version: &CallActivationVersion,
        generation: &str,
    ) -> Result<(), ChannelSessionError> {
        let request = Control {
            call_registration: Protocol::V1,
            command: Command::Acknowledge {
                version: version.clone(),
                role_generation: generation.into(),
            },
        };
        let response = self.call_registration_control(&request).await;
        let response = match response {
            Err(ChannelSessionError::Transport) => {
                self.call_registration_control(&Control {
                    call_registration: Protocol::V1,
                    command: Command::Status {
                        node_id: version.node_id.clone(),
                    },
                })
                .await?
            }
            other => other?,
        };
        if matches!(response, Response::Registered {receipt} if receipt.version == *version && receipt.state == State::Active)
        {
            Ok(())
        } else {
            Err(ChannelSessionError::ControlRejected)
        }
    }
}
#[cfg(feature = "service-call-zenoh")]
#[path = "registered_calls/plan.rs"]
mod plan;
#[cfg(feature = "service-call-zenoh")]
pub use plan::{RegisteredCallPlan, RegisteredCallUpdates};
