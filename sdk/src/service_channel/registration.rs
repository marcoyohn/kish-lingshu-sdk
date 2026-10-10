//! Native declaration control is serialized with the existing role owner.
use super::{ChannelSessionError, ManagedRoleChannel, ServiceChannelSessions};
use kish_lingshu_event_dispatch_contract::{
    ConsumerRegistrationControl, ConsumerRegistrationControlResponse,
};
use kish_lingshu_foundation_contract::service_transport::MessageKind;
use tokio::{sync::oneshot, time::Instant};

pub(super) enum RegistrationRequest {
    Consumer(ConsumerRegistrationControl),
    Call(kish_lingshu_runtime_contract::service::CallRegistrationControl),
}
pub(super) enum RegistrationResponse {
    Consumer(ConsumerRegistrationControlResponse),
    Call(kish_lingshu_runtime_contract::service::CallRegistrationResponse),
}
pub(super) struct RegistrationCommand {
    pub request: RegistrationRequest,
    pub deadline: Instant,
    pub result: oneshot::Sender<Result<RegistrationResponse, ChannelSessionError>>,
}
impl ManagedRoleChannel {
    /// A transport error has an unknown mutation outcome. Recover using Lookup
    /// with the original operation ID; never turn it into a fresh registration.
    pub async fn consumer_registration_control(
        &self,
        request: ConsumerRegistrationControl,
    ) -> Result<ConsumerRegistrationControlResponse, ChannelSessionError> {
        request
            .validate()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        match self
            .registration_request(RegistrationRequest::Consumer(request))
            .await?
        {
            RegistrationResponse::Consumer(response) => Ok(response),
            _ => Err(ChannelSessionError::InvalidResponse),
        }
    }
    pub async fn call_registration_control(
        &self,
        request: kish_lingshu_runtime_contract::service::CallRegistrationControl,
    ) -> Result<kish_lingshu_runtime_contract::service::CallRegistrationResponse, ChannelSessionError>
    {
        request
            .validate()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        match self
            .registration_request(RegistrationRequest::Call(request))
            .await?
        {
            RegistrationResponse::Call(response) => Ok(response),
            _ => Err(ChannelSessionError::InvalidResponse),
        }
    }
    async fn registration_request(
        &self,
        request: RegistrationRequest,
    ) -> Result<RegistrationResponse, ChannelSessionError> {
        let (result, received) = oneshot::channel();
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        self.registration_commands
            .try_send(RegistrationCommand {
                request,
                deadline,
                result,
            })
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    ChannelSessionError::CapacityExceeded
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => ChannelSessionError::Closed,
            })?;
        tokio::time::timeout_at(deadline, received)
            .await
            .map_err(|_| ChannelSessionError::Transport)?
            .map_err(|_| ChannelSessionError::Closed)?
    }
}
impl ServiceChannelSessions {
    pub(super) async fn registration_request(
        &self,
        request: &RegistrationRequest,
    ) -> Result<RegistrationResponse, ChannelSessionError> {
        match request {
            RegistrationRequest::Consumer(request) => self
                .registration_control(request)
                .await
                .map(RegistrationResponse::Consumer),
            RegistrationRequest::Call(request) => self
                .call_registration_control(request)
                .await
                .map(RegistrationResponse::Call),
        }
    }
    pub(super) async fn call_registration_control(
        &self,
        request: &kish_lingshu_runtime_contract::service::CallRegistrationControl,
    ) -> Result<kish_lingshu_runtime_contract::service::CallRegistrationResponse, ChannelSessionError>
    {
        request
            .validate()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let response = self.role_control(MessageKind::Register, request).await?;
        let response = serde_json::from_str(response.payload.get())
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        super::registered_calls::validate_response(request, &response)?;
        Ok(response)
    }
    pub(super) async fn registration_control(
        &self,
        request: &ConsumerRegistrationControl,
    ) -> Result<ConsumerRegistrationControlResponse, ChannelSessionError> {
        request
            .validate()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let response = self.role_control(MessageKind::Register, request).await?;
        let response: ConsumerRegistrationControlResponse =
            serde_json::from_str(response.payload.get())
                .map_err(|_| ChannelSessionError::InvalidResponse)?;
        validate_response(request, &response)?;
        Ok(response)
    }
}
fn validate_response(
    request: &ConsumerRegistrationControl,
    response: &ConsumerRegistrationControlResponse,
) -> Result<(), ChannelSessionError> {
    use kish_lingshu_event_dispatch_contract::{
        ConsumerRegistrationCommand as Command, ConsumerRegistrationControlResponse as Response,
    };
    let valid = match (&request.command, response) {
        (_, Response::Rejected { .. }) => true,
        (
            Command::Negotiate,
            Response::Supported {
                protocol,
                owner_boot,
                connection_epoch,
                ..
            },
        ) => {
            protocol == &request.consumer_registration
                && kish_lingshu_event_dispatch_contract::validate_registration_key(owner_boot)
                    .is_ok()
                && kish_lingshu_event_dispatch_contract::validate_registration_key(connection_epoch)
                    .is_ok()
        }
        (Command::Declare { update, .. }, Response::Registered { receipt }) => {
            receipt.version.registration_key == update.registration_key
                && receipt.declaration_digest
                    == update
                        .declaration
                        .digest()
                        .map_err(|_| ChannelSessionError::InvalidConfig)?
                && update.expected_revision.map_or(
                    receipt.version.declaration_revision > 0,
                    |revision| {
                        revision.checked_add(1) == Some(receipt.version.declaration_revision)
                    },
                )
        }
        (
            Command::Lookup {
                registration_key, ..
            }
            | Command::Status { registration_key },
            Response::Registered { receipt },
        ) => {
            &receipt.version.registration_key == registration_key
                && receipt.version.declaration_revision > 0
        }
        (
            Command::Prepare { version, .. } | Command::PreparedStatus { version },
            Response::Prepared {
                version: offered, ..
            },
        ) => version == offered,
        (Command::Acknowledge { version, .. }, Response::Registered { receipt }) => {
            version == &receipt.version
                && receipt.state
                    == kish_lingshu_event_dispatch_contract::ConsumerActivationState::Active
        }
        (Command::Remove { version }, Response::Removed { version: removed }) => version == removed,
        _ => false,
    };
    if !valid {
        return Err(ChannelSessionError::InvalidResponse);
    }
    if let Response::Registered { receipt } = response {
        if receipt.protocol != request.consumer_registration
            || receipt.directory_revision == 0
            || kish_lingshu_event_dispatch_contract::validate_registration_key(
                &receipt.version.owner_boot,
            )
            .is_err()
            || kish_lingshu_event_dispatch_contract::validate_registration_key(
                &receipt.version.connection_epoch,
            )
            .is_err()
            || (matches!(
                receipt.state,
                kish_lingshu_event_dispatch_contract::ConsumerActivationState::Active
                    | kish_lingshu_event_dispatch_contract::ConsumerActivationState::Activating
            ) && (receipt.version.catalog_revision == 0
                || receipt.version.activation_revision == 0))
        {
            return Err(ChannelSessionError::InvalidResponse);
        }
    }
    Ok(())
}

impl ManagedRoleChannel {
    pub fn subscribe_consumer_registration_changes(
        &self,
    ) -> tokio::sync::broadcast::Receiver<
        kish_lingshu_event_dispatch_contract::ConsumerRegistrationHint,
    > {
        self.registration_hints.subscribe()
    }
}

pub(super) fn hint_reply(
    request: kish_lingshu_foundation_contract::service_transport::TransportEnvelope,
    endpoint: &kish_lingshu_foundation_contract::service_transport::ServiceEndpoint,
    expires: i64,
    connection: &crate::ServiceConnection,
    initial: &kish_lingshu_foundation_contract::service_transport::bootstrap::ChannelBootstrapResponse,
    signer: &kish_lingshu_foundation_contract::service_auth::ChannelMessageSigner,
    hints: &tokio::sync::broadcast::Sender<
        kish_lingshu_event_dispatch_contract::ConsumerRegistrationHint,
    >,
) -> Result<Vec<u8>, ChannelSessionError> {
    use kish_lingshu_foundation_contract::{
        service_auth::ClientChannelIdentity,
        service_transport::{RouteIdentity, ServiceEndpoint, TransportEnvelope},
    };
    let now = chrono::Utc::now().timestamp_millis();
    let ServiceEndpoint::Zenoh { route, lanes, .. } = endpoint else {
        return Err(ChannelSessionError::InvalidResponse);
    };
    let hint: kish_lingshu_event_dispatch_contract::ConsumerRegistrationHint =
        serde_json::from_str(request.payload.get())
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
    let target = route
        .invoke_key(lanes.first().ok_or(ChannelSessionError::InvalidResponse)?)
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
    if request.kind != MessageKind::ConsumerRegistrationChanged
        || request.target != target
        || request.deadline_unix_ms > expires
        || request.payload.get().len() > 1024
        || hint.directory_revision == 0
        || kish_lingshu_event_dispatch_contract::validate_registration_key(&hint.owner_boot)
            .is_err()
    {
        return Err(ChannelSessionError::InvalidResponse);
    }
    let claims = connection
        .verify_channel_message(&initial.transport_trust, &request)
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
    claims
        .validate_request_time(now, 2_000)
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
    let _ = hints.send(hint);
    let subject = ClientChannelIdentity {
        application_id: initial.application_id.clone(),
        instance_id: RouteIdentity::new(&initial.instance.instance_id)
            .map_err(|_| ChannelSessionError::InvalidResponse)?,
        base_generation: RouteIdentity::new(&initial.instance.generation)
            .map_err(|_| ChannelSessionError::InvalidResponse)?,
        certificate_identity: initial.certificate.certificate_identity.clone(),
    };
    let proof = signer
        .sign_message(
            &subject,
            request.kind,
            &target,
            &request.request_id,
            request.payload.get().as_bytes(),
            now,
            request.deadline_unix_ms,
        )
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
    TransportEnvelope {
        proof,
        trace_parent: super::trace::current_trace_parent(),
        ..request
    }
    .encode(now)
    .map_err(|_| ChannelSessionError::InvalidResponse)
}

#[cfg(feature = "event-consumer-zenoh")]
impl ServiceChannelSessions {
    pub(super) async fn prepare_declared_consumer(
        &self,
        version: &kish_lingshu_event_dispatch_contract::ConsumerActivationVersion,
        group_key: &str,
        node_id: &str,
        maximum_in_flight: u32,
    ) -> Result<super::RegisteredChannelRole, ChannelSessionError> {
        use kish_lingshu_event_dispatch_contract::{
            ConsumerRegistrationCommand as Command, ConsumerRegistrationControlResponse as Response,
        };
        use kish_lingshu_foundation_contract::service_transport::{
            enrollment::{EnrollmentVersion, RequestedServiceEndpoint},
            ProtocolVersion,
        };
        use kish_lingshu_runtime_contract::service::{
            ChannelRoleEnrollment, ChannelRoleEnrollmentResponse,
        };
        let initial = self.identity.bootstrap_response();
        let instance = self
            .identity
            .connection
            .registration()
            .await
            .request(&initial.instance.instance_id);
        let enrollment = kish_lingshu_event_dispatch_contract::ConsumerEnrollmentRequestV2 {
            enrollment_version: EnrollmentVersion::V2,
            instance,
            group_key: group_key.into(),
            node_id: node_id.into(),
            maximum_in_flight,
            endpoint: RequestedServiceEndpoint::Zenoh {
                protocol_version: ProtocolVersion::V1,
                lane_count: (self.lane_count() as u8)
                    .try_into()
                    .map_err(|_| ChannelSessionError::InvalidConfig)?,
            },
        };
        let request = ConsumerRegistrationControl {
            consumer_registration:
                kish_lingshu_event_dispatch_contract::ConsumerRegistrationProtocol::V1,
            command: Command::Prepare {
                version: version.clone(),
                enrollment: enrollment.clone(),
            },
        };
        request
            .validate()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let started = Instant::now();
        let verified = match self
            .verified_role_control(MessageKind::Register, &request)
            .await
        {
            Ok(verified) => verified,
            Err(ChannelSessionError::Transport) => {
                // Read the original outcome. No second enrollment mutation and
                // no interpretation of an unknown result as catalog absence.
                let lookup = ConsumerRegistrationControl {
                    consumer_registration: request.consumer_registration,
                    command: Command::PreparedStatus {
                        version: version.clone(),
                    },
                };
                self.verified_role_control(MessageKind::Register, &lookup)
                    .await?
            }
            Err(error) => return Err(error),
        };
        let response: Response = serde_json::from_str(verified.envelope.payload.get())
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        validate_response(&request, &response)?;
        let Response::Prepared {
            enrollment: response,
            ..
        } = response
        else {
            return Err(ChannelSessionError::ControlRejected);
        };
        let mut role = self
            .install_registered_role(
                ChannelRoleEnrollment::Consumer(enrollment),
                Some(version.clone()),
                None,
                None,
                ChannelRoleEnrollmentResponse::Consumer(response),
                started,
                verified.issued_at_unix_ms,
            )
            .await?;
        role.consumer_registration = Some(version.clone());
        role.logical_key = format!("consumer:{group_key}:{}", version.registration_key);
        Ok(role)
    }
}

#[cfg(feature = "event-consumer-zenoh")]
impl ManagedRoleChannel {
    pub async fn activate_registered_consumer(
        &self,
        version: kish_lingshu_event_dispatch_contract::ConsumerActivationVersion,
        group_key: String,
        node_id: String,
        maximum_in_flight: u32,
        registry: std::sync::Arc<crate::event_dispatch::ConsumerRegistry>,
        budget: crate::ServiceExecutionBudget,
    ) -> Result<super::ChannelRoleStatus, ChannelSessionError> {
        if registry.app_id() != self.application_id
            || !registry.supports_group(&group_key)
            || maximum_in_flight == 0
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let bytes = serde_json::to_vec(&version)
            .map_err(|_| ChannelSessionError::InvalidConfig)?
            .len()
            + group_key.len()
            + node_id.len()
            + 256;
        self.commands
            .send(
                super::role_changes::RoleMutation::DeclaredConsumer {
                    version,
                    group_key,
                    node_id,
                    maximum_in_flight,
                    registry,
                    budget,
                },
                bytes,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kish_lingshu_event_dispatch_contract::*;
    fn receipt() -> ConsumerRegistrationReceipt {
        ConsumerRegistrationReceipt {
            protocol: ConsumerRegistrationProtocol::V1,
            version: ConsumerActivationVersion {
                owner_boot: "owner".into(),
                connection_epoch: "epoch".into(),
                registration_key: "orders".into(),
                declaration_revision: 1,
                catalog_revision: 2,
                activation_revision: 3,
            },
            declaration_digest: ManifestDigest::sha256(b"test declaration"),
            state: ConsumerActivationState::Active,
            directory_revision: 4,
        }
    }
    #[test]
    fn acknowledgement_rejects_old_owner_connection_and_activation_even_for_same_declaration() {
        let accepted = receipt();
        let request = ConsumerRegistrationControl {
            consumer_registration: ConsumerRegistrationProtocol::V1,
            command: ConsumerRegistrationCommand::Acknowledge {
                version: accepted.version.clone(),
                role_generation: "role".into(),
            },
        };
        assert!(validate_response(
            &request,
            &ConsumerRegistrationControlResponse::Registered {
                receipt: accepted.clone()
            }
        )
        .is_ok());
        for field in 0..5 {
            let mut old = accepted.clone();
            match field {
                0 => old.version.owner_boot = "old-owner".into(),
                1 => old.version.connection_epoch = "old-epoch".into(),
                2 => old.version.activation_revision -= 1,
                3 => old.state = ConsumerActivationState::Activating,
                _ => old.version.declaration_revision += 1,
            }
            assert_eq!(
                validate_response(
                    &request,
                    &ConsumerRegistrationControlResponse::Registered { receipt: old }
                ),
                Err(ChannelSessionError::InvalidResponse)
            );
        }
    }
    #[test]
    fn status_rejects_foreign_registration_and_impossible_active_version() {
        let request = ConsumerRegistrationControl {
            consumer_registration: ConsumerRegistrationProtocol::V1,
            command: ConsumerRegistrationCommand::Status {
                registration_key: "orders".into(),
            },
        };
        for field in 0..4 {
            let mut bad = receipt();
            match field {
                0 => bad.version.registration_key = "foreign".into(),
                1 => bad.version.activation_revision = 0,
                2 => bad.version.connection_epoch = "*".into(),
                _ => bad.directory_revision = 0,
            }
            assert!(validate_response(
                &request,
                &ConsumerRegistrationControlResponse::Registered { receipt: bad }
            )
            .is_err());
        }
    }
}
