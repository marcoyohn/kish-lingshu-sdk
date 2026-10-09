//! Outbound HTTPS bootstrap. The SDK generates and retains the private CSR key;
//! no inbound listener, native Plugin SDK, Router or local execution scheduler.
#[cfg(feature = "service-zenoh")]
pub mod trace;
use crate::{ServiceAuthError, ServiceConnection};
pub use kish_lingshu_foundation_contract::service_transport::bootstrap::ChannelTransport;
mod plaintext;
pub use kish_lingshu_foundation_contract::service_transport::channel::{
    ChannelAuthorization, ChannelRotationFinalization,
};
use kish_lingshu_foundation_contract::{
    service_auth::ChannelMessageSigner,
    service_transport::{
        bootstrap::{ChannelBootstrapRequest, ChannelBootstrapResponse},
        ProtocolVersion, RouteIdentity,
    },
    ServiceInstanceRegistration,
};
use rcgen::KeyPair;
use std::{fmt, time::Duration};
use tokio::time::Instant;

#[cfg(feature = "service-call-zenoh")]
mod call;
#[cfg(feature = "service-call-zenoh")]
mod call_report;
#[cfg(feature = "event-consumer-zenoh")]
mod consumer;
#[cfg(feature = "event-publication-zenoh")]
pub(crate) mod publication;
#[cfg(feature = "service-call-zenoh")]
pub use call_report::NativeCallReportClient;
#[cfg(feature = "service-zenoh")]
mod catalog;
#[cfg(feature = "service-zenoh")]
mod connectivity;
#[cfg(feature = "service-zenoh")]
mod observation;
#[cfg(feature = "service-zenoh")]
pub use connectivity::ChannelConnectivityStatus;
#[cfg(feature = "service-zenoh")]
mod activation;
#[cfg(feature = "service-zenoh")]
mod role_changes;
#[cfg(feature = "service-zenoh")]
pub use activation::{
    CapabilityActivationState, CapabilityActivationStatus, CatalogActivation, CatalogActivationPlan,
};
#[cfg(feature = "service-zenoh")]
mod role_supervisor;
#[cfg(feature = "service-zenoh")]
mod roles;
#[cfg(feature = "service-zenoh")]
pub use role_supervisor::{
    CertificateRotationPhase, CertificateRotationStatus, ChannelCertificateRotationConfig,
    ChannelRoleStatus, ManagedRoleChannel, RoleLifecycleState,
};
#[cfg(feature = "service-zenoh")]
mod route_probe;
#[cfg(feature = "service-zenoh")]
pub use roles::RegisteredChannelRole;
#[cfg(feature = "service-zenoh")]
mod error;
#[cfg(feature = "service-zenoh")]
mod sessions;
#[cfg(feature = "service-zenoh")]
mod supervisor;
#[cfg(feature = "service-zenoh")]
pub use sessions::{
    ChannelCloseReason, ChannelSessionConfig, ChannelSessionError, ServiceChannelSessions,
};
#[cfg(feature = "service-zenoh")]
pub use supervisor::{ChannelSupervisorStatus, ManagedServiceChannel};
#[cfg(all(test, feature = "service-zenoh"))]
mod reconnect_host_tests;

pub struct ServiceChannelIdentity {
    credential: PreparedIdentity,
    predecessor: Option<RouteIdentity>,
    #[cfg_attr(not(feature = "service-zenoh"), allow(dead_code))]
    connection: ServiceConnection,
    #[cfg_attr(not(feature = "service-zenoh"), allow(dead_code))]
    authorization_deadline: Instant,
}
impl fmt::Debug for ServiceChannelIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.credential.fmt(f)
    }
}
impl ServiceChannelIdentity {
    pub fn bootstrap_response(&self) -> &ChannelBootstrapResponse {
        &self.credential.response
    }
    pub fn message_signer(&self) -> &ChannelMessageSigner {
        &self.credential.signer
    }
    /// Informational lineage, not evidence of role transfer or predecessor retirement.
    pub fn rotation_predecessor(&self) -> Option<&RouteIdentity> {
        self.predecessor.as_ref()
    }
}

struct PreparedIdentity {
    response: ChannelBootstrapResponse,
    signer: ChannelMessageSigner,
    // TLS consumes this material; plaintext keeps it for the signed identity lifecycle.
    #[allow(dead_code)]
    key: KeyPair,
    plaintext: Option<plaintext::PlaintextKey>,
}
impl fmt::Debug for PreparedIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceChannelIdentity")
            .field("application_id", &self.response.application_id)
            .field("instance", &self.response.instance)
            .finish_non_exhaustive()
    }
}
impl PreparedIdentity {
    fn bind_transport(
        mut self,
        transport: ChannelTransport,
        key: Option<plaintext::PlaintextKey>,
    ) -> Result<Self, ServiceAuthError> {
        if self
            .response
            .endpoints
            .iter()
            .any(|e| e.transport() != transport)
            || (transport == ChannelTransport::IntranetPlaintext) != key.is_some()
        {
            return Err(ServiceAuthError::InvalidResponse);
        }
        self.plaintext = key;
        Ok(self)
    }
    fn accept(
        response: ChannelBootstrapResponse,
        request: &ChannelBootstrapRequest,
        app: &str,
        key: KeyPair,
    ) -> Result<Self, ServiceAuthError> {
        response
            .validate(app, request, chrono::Utc::now().timestamp_millis())
            .map_err(|_| ServiceAuthError::InvalidResponse)?;
        let signer = ChannelMessageSigner::from_pkcs8(&key.serialize_der())
            .map_err(|_| ServiceAuthError::InvalidResponse)?;
        let actual = signer
            .public_key()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        if actual != response.certificate.message_public_key {
            return Err(ServiceAuthError::InvalidResponse);
        }
        Ok(Self {
            response,
            signer,
            key,
            plaintext: None,
        })
    }
}

#[cfg(test)]
fn prepare_request(
    instance: ServiceInstanceRegistration,
    expected_deployment: Option<RouteIdentity>,
) -> Result<(ChannelBootstrapRequest, KeyPair), ServiceAuthError> {
    prepare_request_for_transport(instance, expected_deployment, ChannelTransport::Mtls)
        .map(|(request, key, _)| (request, key))
}

fn prepare_request_for_transport(
    instance: ServiceInstanceRegistration,
    expected_deployment: Option<RouteIdentity>,
    transport: ChannelTransport,
) -> Result<
    (
        ChannelBootstrapRequest,
        KeyPair,
        Option<plaintext::PlaintextKey>,
    ),
    ServiceAuthError,
> {
    let mut names = Vec::new();
    let plaintext = if transport == ChannelTransport::IntranetPlaintext {
        let key = plaintext::PlaintextKey::generate()?;
        names.push(key.csr_name()?);
        Some(key)
    } else {
        None
    };
    let (key, csr_pem) = zenss_client_sdk::credentials::signing_request(names)
        .map_err(|_| ServiceAuthError::InvalidCredential)?;
    let request = ChannelBootstrapRequest {
        protocol_version: ProtocolVersion::V1,
        instance,
        csr_pem,
        expected_deployment,
    };
    request
        .validate()
        .map_err(|_| ServiceAuthError::InvalidNodeConfig)?;
    Ok((request, key, plaintext))
}

impl ServiceConnection {
    #[cfg(test)]
    pub(crate) async fn bootstrap_test_channel(
        &self,
        instance: ServiceInstanceRegistration,
        expected_deployment: Option<RouteIdentity>,
    ) -> Result<ServiceChannelIdentity, ServiceAuthError> {
        let transport = match std::env::var("LINGSHU_CHANNEL_TEST_TRANSPORT").as_deref() {
            Ok("intranet_plaintext") => ChannelTransport::IntranetPlaintext,
            Ok("mtls") | Err(_) => ChannelTransport::Mtls,
            _ => return Err(ServiceAuthError::InvalidNodeConfig),
        };
        self.bootstrap_channel_with_transport(instance, expected_deployment, transport)
            .await
    }
    /// Prepare a short-lived TLS identity from a platform with prototype channel
    /// bootstrap explicitly enabled. This does not create or activate a Session.
    /// Keep the returned base generation on subsequent registration/renewal;
    /// a stale generation must never be cleared automatically to retry.
    pub async fn bootstrap_channel(
        &self,
        instance: ServiceInstanceRegistration,
        expected_deployment: Option<RouteIdentity>,
    ) -> Result<ServiceChannelIdentity, ServiceAuthError> {
        self.bootstrap_channel_with_transport(instance, expected_deployment, ChannelTransport::Mtls)
            .await
    }

    /// Explicit unencrypted TCP requires the `service-plaintext` feature and a
    /// matching Host. HTTPS authentication and signed business envelopes remain.
    pub async fn bootstrap_channel_with_transport(
        &self,
        instance: ServiceInstanceRegistration,
        expected_deployment: Option<RouteIdentity>,
        transport: ChannelTransport,
    ) -> Result<ServiceChannelIdentity, ServiceAuthError> {
        self.ensure_open()?;
        let (mut request, key, plaintext) = tokio::task::spawn_blocking(move || {
            prepare_request_for_transport(instance, expected_deployment, transport)
        })
        .await
        .map_err(|_| ServiceAuthError::InvalidCredential)??;
        let mut registration = self.registration().await;
        request.instance = registration.bind(request.instance)?;
        let started = Instant::now();
        let builder = self
            .service_request(reqwest::Method::POST, "channel-bootstrap", None)?
            .json(&request);
        let response = self.root_response_json(builder).await?;
        self.ensure_open()?;
        let credential = PreparedIdentity::accept(response, &request, self.application_id(), key)?
            .bind_transport(transport, plaintext)?;
        let authorization_deadline = authorization_deadline(&credential.response, started)?;
        registration.accept(Some(&credential.response.instance))?;
        Ok(ServiceChannelIdentity {
            credential,
            predecessor: None,
            connection: self.clone(),
            authorization_deadline,
        })
    }
}

#[cfg(feature = "service-zenoh")]
impl ServiceChannelSessions {
    /// Issue one control-only candidate through the original authenticated
    /// channel. The new CSR private key stays local. No role transfer, old
    /// retirement, automatic retry or physical connection occurs here.
    pub async fn prepare_certificate_rotation(
        &self,
    ) -> Result<ServiceChannelIdentity, ChannelSessionError> {
        use kish_lingshu_foundation_contract::service_transport::{
            bootstrap::{ChannelRotationPreparation, ChannelRotationRequest},
            MessageKind,
        };
        let initial = self.identity.bootstrap_response();
        let instance = self
            .identity
            .connection
            .registration()
            .await
            .request(&initial.instance.instance_id);
        let transport = initial.endpoints[0].transport();
        let deployment = Some(initial.deployment.clone());
        let (request, key, plaintext) = tokio::task::spawn_blocking(move || {
            prepare_request_for_transport(instance, deployment, transport)
        })
        .await
        .map_err(|_| ChannelSessionError::InvalidConfig)?
        .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let started = Instant::now();
        let old_deadline = self.authorization_deadline();
        let envelope = self
            .role_control(
                MessageKind::PrepareChannelRotation,
                &ChannelRotationRequest {
                    csr_pem: request.csr_pem.clone(),
                },
            )
            .await?;
        let preparation: ChannelRotationPreparation = serde_json::from_str(envelope.payload.get())
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        preparation
            .validate(initial)
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let deadline = authorization_deadline(&preparation.candidate, started)
            .map_err(|_| ChannelSessionError::InvalidResponse)?
            .min(old_deadline);
        let credential = PreparedIdentity::accept(
            preparation.candidate,
            &request,
            self.identity.connection.application_id(),
            key,
        )
        .and_then(|identity| identity.bind_transport(transport, plaintext))
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
        if Instant::now() >= deadline || self.closed.borrow().is_some() {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        self.identity
            .connection
            .ensure_open()
            .map_err(|_| ChannelSessionError::Closed)?;
        Ok(ServiceChannelIdentity {
            credential,
            predecessor: Some(preparation.previous_certificate_identity),
            connection: self.identity.connection.clone(),
            authorization_deadline: deadline,
        })
    }
}

fn authorization_deadline(
    response: &ChannelBootstrapResponse,
    started: Instant,
) -> Result<Instant, ServiceAuthError> {
    // Bootstrap round-trip time consumes the initial observation window. Clock
    // skew can shorten it but must not grant more than the server's duration.
    let remaining = (response.authorization_expires_unix_ms
        - chrono::Utc::now().timestamp_millis())
    .min(response.authorization_expires_unix_ms - response.authorization_issued_unix_ms);
    let remaining: u64 = remaining
        .try_into()
        .map_err(|_| ServiceAuthError::InvalidResponse)?;
    let deadline = started + Duration::from_millis(remaining);
    if Instant::now() >= deadline {
        return Err(ServiceAuthError::InvalidResponse);
    }
    Ok(deadline)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kish_lingshu_foundation_contract::{
        service_transport::{
            bootstrap::{ChannelCertificate, ChannelEndpoint},
            PlatformControlRoute,
        },
        ServiceInstanceIdentity,
    };
    fn registration() -> ServiceInstanceRegistration {
        ServiceInstanceRegistration {
            instance_id: "sdk".into(),
            incarnation_id: "boot".into(),
            generation: None,
        }
    }
    #[test]
    fn shared_registration_preserves_generation_and_rejects_scope_changes() {
        let mut state = crate::service_auth::InstanceRegistrationState::default();
        let request = registration();
        assert_eq!(state.bind(request.clone()).unwrap(), request);
        state
            .accept(Some(&ServiceInstanceIdentity {
                instance_id: "sdk".into(),
                generation: "base".into(),
            }))
            .unwrap();
        assert_eq!(
            state.bind(request.clone()).unwrap().generation.as_deref(),
            Some("base")
        );
        for changed in [
            ServiceInstanceRegistration {
                instance_id: "other".into(),
                ..request.clone()
            },
            ServiceInstanceRegistration {
                incarnation_id: "other".into(),
                ..request.clone()
            },
            ServiceInstanceRegistration {
                generation: Some("other".into()),
                ..request.clone()
            },
        ] {
            assert!(matches!(
                state.bind(changed),
                Err(ServiceAuthError::InvalidNodeConfig)
            ));
        }
        assert_eq!(
            state.bind(request).unwrap().generation.as_deref(),
            Some("base")
        );
    }
    #[test]
    fn local_ed25519_key_binds_the_certificate_reply_and_never_enters_the_request() {
        let (request, key) = prepare_request(registration(), None).unwrap();
        let public = key
            .public_key_raw()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let private = key.serialize_pem();
        assert!(!serde_json::to_string(&request)
            .unwrap()
            .contains("PRIVATE KEY"));
        let now = chrono::Utc::now().timestamp_millis();
        let response = ChannelBootstrapResponse {
            protocol_version: ProtocolVersion::V1,
            deployment: RouteIdentity::new("dev").unwrap(),
            application_id: RouteIdentity::new("app").unwrap(),
            parent_credential_fingerprint: "b".repeat(64),
            instance: ServiceInstanceIdentity {
                instance_id: "sdk".into(),
                generation: "base".into(),
            },
            endpoints: vec![ChannelEndpoint::new("tls/router.example:7447".into()).unwrap()],
            control_route: PlatformControlRoute {
                deployment: RouteIdentity::new("dev").unwrap(),
                platform_node: RouteIdentity::new("node").unwrap(),
                boot_epoch: RouteIdentity::new("boot").unwrap(),
            },
            certificate: ChannelCertificate {
                certificate_identity: RouteIdentity::new("cn").unwrap(),
                certificate_pem: "certificate".into(),
                root_ca_pem: "root".into(),
                message_public_key: public,
                expires_unix_ms: now + 300_000,
            },
            transport_trust: kish_lingshu_foundation_contract::service_auth::ServiceSigner::new(
                &[7; 32],
            )
            .unwrap()
            .transport_trust("app", now / 1000)
            .unwrap(),
            authorization_expires_unix_ms: now + 30_000,
            authorization_issued_unix_ms: now,
        };
        let mut identity =
            PreparedIdentity::accept(response.clone(), &request, "app", key).unwrap();
        assert!(!format!("{identity:?}").contains(&private));
        assert!(identity.signer.public_key().len() == 32);
        identity.response.endpoints =
            vec![ChannelEndpoint::new("tcp/router.example:7447".into()).unwrap()];
        assert!(matches!(
            identity.bind_transport(ChannelTransport::Mtls, None),
            Err(ServiceAuthError::InvalidResponse)
        ));
        let started = Instant::now();
        let mut ahead = response.clone();
        ahead.authorization_issued_unix_ms = now + 5_000;
        ahead.authorization_expires_unix_ms = now + 35_000;
        assert!(
            authorization_deadline(&ahead, started).unwrap() <= started + Duration::from_secs(30)
        );
        assert!(authorization_deadline(&response, started - Duration::from_secs(31)).is_err());
        let (_, unrelated) = prepare_request(registration(), None).unwrap();
        assert!(matches!(
            PreparedIdentity::accept(response, &request, "app", unrelated),
            Err(ServiceAuthError::InvalidResponse)
        ));
    }
    #[cfg(feature = "service-zenoh")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "isolated native host; run zenss_channel_bootstrap_acceptance.py --role-test"]
    async fn native_rotation_finalization_accounts_for_roles_retires_old_control_and_survives_candidate_cap(
    ) {
        use kish_lingshu_foundation_contract::service_transport::{MessageKind, ServiceEndpoint};
        use kish_lingshu_runtime_contract::service::ChannelRoleAdoption;
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let connection = crate::ServiceConnection::connect(
            &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
            crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
        let identity = connection
            .bootstrap_test_channel(
                ServiceInstanceRegistration {
                    instance_id: "native-finalization".into(),
                    incarnation_id: "finalization-boot".into(),
                    generation: None,
                },
                Some(RouteIdentity::new("dev").unwrap()),
            )
            .await
            .unwrap();
        let mut old = identity
            .open_sessions(ChannelSessionConfig::default())
            .await
            .unwrap();
        assert!(old.finalize_certificate_rotation().await.is_err());
        let catalog = |key: &str| kish_lingshu_runtime_contract::provider::ProviderCatalog {
            format_version: 1,
            application_id: app.clone(),
            provider_key: key.into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![],
        };
        let mut role = old
            .register_provider_role(&catalog("finalization-first"))
            .await
            .unwrap();
        let mut sibling = old
            .register_provider_role(&catalog("finalization-sibling"))
            .await
            .unwrap();
        let original = serde_json::to_value(role.registration()).unwrap();
        let identity = old.prepare_certificate_rotation().await.unwrap();
        let cap = identity.authorization_deadline;
        let mut candidate = identity
            .open_sessions(ChannelSessionConfig::default())
            .await
            .unwrap();
        // Omitting a live predecessor role is rejected by the server's census.
        assert!(candidate.finalize_certificate_rotation().await.is_err());
        assert!(old.refresh_authorization().await.is_ok());
        let ServiceEndpoint::Zenoh { route, .. } = role.endpoint() else {
            panic!()
        };
        let generation = route.role_generation.as_str().to_owned();
        role.close().await.unwrap();
        candidate
            .role_control(
                MessageKind::AdoptChannelRole,
                &ChannelRoleAdoption {
                    role_generation: generation,
                },
            )
            .await
            .unwrap();
        old.deregister_role(&mut sibling).await.unwrap();
        // An applied transfer without new-key route proof cannot be finalized.
        assert!(candidate.finalize_certificate_rotation().await.is_err());
        assert!(candidate.authorization_deadline() <= cap);
        candidate.adopt_role(&mut role).await.unwrap();
        let endpoint = role.endpoint().clone();
        assert!(candidate
            .role_control(
                MessageKind::FinalizeChannelRotation,
                &serde_json::json!({"roles": []})
            )
            .await
            .is_err());
        // Apply finalization but intentionally do not retain the received ACK.
        candidate
            .role_control(MessageKind::FinalizeChannelRotation, &serde_json::json!({}))
            .await
            .unwrap();
        assert!(candidate.authorization_deadline() <= cap);
        assert!(old.refresh_authorization().await.is_err());
        let result = candidate.finalize_certificate_rotation().await.unwrap();
        assert_eq!(
            result.previous_certificate_identity,
            old.identity()
                .bootstrap_response()
                .certificate
                .certificate_identity
        );
        assert_eq!(
            result.authorization.instance,
            old.identity().bootstrap_response().instance
        );
        candidate.finalize_certificate_rotation().await.unwrap();
        let mut additional = candidate
            .register_provider_role(&catalog("finalization-new"))
            .await
            .unwrap();
        candidate.deregister_role(&mut additional).await.unwrap();
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_secs(8)).await;
            let renewed = candidate.renew_role_leases(&mut [&mut role]).await.unwrap();
            assert_eq!(renewed[0].status, 200);
            candidate.refresh_authorization().await.unwrap();
            candidate.confirm_role_route(&mut role).await.unwrap();
            assert_eq!(role.endpoint(), &endpoint);
            assert_eq!(serde_json::to_value(role.registration()).unwrap(), original);
        }
        assert!(Instant::now() > cap);
        assert!(role.route_confirmed());
        assert_eq!(candidate.connected_lanes().await, 1);
        assert!(old.refresh_authorization().await.is_err());
        candidate.deregister_role(&mut role).await.unwrap();
        candidate.close().await.unwrap();
        old.close().await.unwrap();
    }
    #[cfg(feature = "service-zenoh")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "isolated native host; run zenss_channel_bootstrap_acceptance.py --role-test"]
    async fn native_candidate_adopts_only_predecessor_roles_with_fresh_lanes_and_finite_authority()
    {
        use kish_lingshu_foundation_contract::service_transport::{MessageKind, ServiceEndpoint};
        use kish_lingshu_runtime_contract::service::{
            ChannelRoleAdoption, ChannelRoleAdoptionResponse, ChannelRoleDeregistration,
            ChannelRoleRenewal, ChannelRoleRenewalResponse, ChannelRoleRouteConfirmation,
        };
        use sha2::{Digest, Sha256};
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let connection = crate::ServiceConnection::connect(
            &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
            crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
        let registration = || ServiceInstanceRegistration {
            instance_id: "native-adoption".into(),
            incarnation_id: "adoption-boot".into(),
            generation: None,
        };
        let identity = connection
            .bootstrap_test_channel(registration(), Some(RouteIdentity::new("dev").unwrap()))
            .await
            .unwrap();
        let original = identity.bootstrap_response().clone();
        let mut old = identity
            .open_sessions(ChannelSessionConfig::default())
            .await
            .unwrap();
        let catalog = |key: &str| kish_lingshu_runtime_contract::provider::ProviderCatalog {
            format_version: 1,
            application_id: app.clone(),
            provider_key: key.into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![],
        };
        let mut role = old
            .register_provider_role(&catalog("adoption-first"))
            .await
            .unwrap();
        let mut sibling = old
            .register_provider_role(&catalog("adoption-sibling"))
            .await
            .unwrap();
        let app_digest = Sha256::digest(serde_json::to_vec(&app).unwrap())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let role_index = format!("kish:services:registration:{{{app_digest}}}:roles");
        let redis_role = |provider: &str| {
            let output = std::process::Command::new("redis-cli")
                .args([
                    "-h",
                    "127.0.0.1",
                    "-p",
                    &std::env::var("LINGSHU_CHANNEL_TEST_REDIS_PORT").unwrap(),
                    "--raw",
                    "HGET",
                    &role_index,
                    &format!("provider:{provider}:native-adoption"),
                ])
                .output()
                .unwrap();
            assert!(output.status.success());
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        };
        let original_owner = redis_role("adoption-first");
        assert!(!original_owner.is_empty());
        let sibling_owner = redis_role("adoption-sibling");
        assert!(!sibling_owner.is_empty());
        let old_endpoint = role.endpoint().clone();
        let original_registration = serde_json::to_value(role.registration()).unwrap();
        let ServiceEndpoint::Zenoh {
            route,
            route_revision,
            lanes,
            ..
        } = &old_endpoint
        else {
            panic!("native required")
        };
        let request = ChannelRoleAdoption {
            role_generation: route.role_generation.as_str().into(),
        };
        let confirmation = ChannelRoleRouteConfirmation {
            role_generation: request.role_generation.clone(),
            route_revision: *route_revision,
        };
        // Another valid certificate on the same root/base has no issuance lineage.
        let identity = connection
            .bootstrap_test_channel(registration(), Some(RouteIdentity::new("dev").unwrap()))
            .await
            .unwrap();
        let mut foreign = identity
            .open_sessions(ChannelSessionConfig::default())
            .await
            .unwrap();
        let mut foreign_role = foreign
            .register_provider_role(&catalog("adoption-foreign"))
            .await
            .unwrap();
        assert!(foreign
            .role_control(MessageKind::AdoptChannelRole, &request)
            .await
            .is_err());
        let identity = old.prepare_certificate_rotation().await.unwrap();
        let cap = identity.authorization_deadline;
        let mut candidate = identity
            .open_sessions(ChannelSessionConfig::default())
            .await
            .unwrap();
        assert!(candidate.adopt_role(&mut foreign_role).await.is_err());
        let ServiceEndpoint::Zenoh {
            route: foreign_route,
            ..
        } = foreign_role.endpoint()
        else {
            unreachable!()
        };
        assert!(candidate
            .role_control(
                MessageKind::AdoptChannelRole,
                &ChannelRoleAdoption {
                    role_generation: foreign_route.role_generation.as_str().into()
                }
            )
            .await
            .is_err());
        // Simulate an applied transfer whose ACK the application did not retain.
        role.close().await.unwrap();
        let applied = candidate
            .role_control(MessageKind::AdoptChannelRole, &request)
            .await
            .unwrap();
        let applied: ChannelRoleAdoptionResponse =
            serde_json::from_str(applied.payload.get()).unwrap();
        assert!(!role.route_confirmed());
        assert!(old
            .role_control(MessageKind::BindLane, &confirmation)
            .await
            .is_err());
        assert!(old
            .role_control(
                MessageKind::Deregister,
                &ChannelRoleDeregistration {
                    role_generation: request.role_generation.clone()
                }
            )
            .await
            .is_err());
        let rejected = old
            .role_control(
                MessageKind::RenewRoles,
                &ChannelRoleRenewal {
                    role_generations: vec![request.role_generation.clone()],
                },
            )
            .await
            .unwrap();
        let rejected: ChannelRoleRenewalResponse =
            serde_json::from_str(rejected.payload.get()).unwrap();
        assert_eq!(rejected.roles[0].status, 403);
        // Fresh explicit repeat returns the original replacement, not revision 3.
        candidate.adopt_role(&mut role).await.unwrap();
        assert_eq!(redis_role("adoption-first"), original_owner);
        assert_eq!(redis_role("adoption-sibling"), sibling_owner);
        assert_eq!(role.endpoint(), &applied.endpoint);
        assert!(role.route_confirmed());
        assert_eq!(
            serde_json::to_value(role.registration()).unwrap(),
            original_registration
        );
        let ServiceEndpoint::Zenoh {
            route: new_route,
            route_revision: new_revision,
            lanes: new_lanes,
            ..
        } = role.endpoint()
        else {
            unreachable!()
        };
        assert_eq!(new_route, route);
        assert_eq!(*new_revision, route_revision + 1);
        assert_eq!(new_lanes[0].lane, lanes[0].lane);
        assert_ne!(new_lanes[0].epoch, lanes[0].epoch);
        assert!(old.deregister_role(&mut role).await.is_err());
        assert!(candidate
            .register_provider_role(&catalog("adoption-new-denied"))
            .await
            .is_err());
        assert!(candidate.prepare_certificate_rotation().await.is_err());
        tokio::time::sleep(Duration::from_secs(10)).await;
        old.refresh_authorization().await.unwrap();
        assert_eq!(
            old.renew_role_leases(&mut [&mut sibling]).await.unwrap()[0].status,
            200
        );
        old.confirm_role_route(&mut sibling).await.unwrap();
        candidate.refresh_authorization().await.unwrap();
        assert!(candidate.authorization_deadline() <= cap);
        assert_eq!(
            candidate.renew_role_leases(&mut [&mut role]).await.unwrap()[0].status,
            200
        );
        candidate.confirm_role_route(&mut role).await.unwrap();
        assert!(
            role.lifecycle_status(candidate.authorization_deadline())
                .route_deadline
                <= cap
        );
        // Latest transferred grant keeps its original ID through renew/revoke.
        candidate.deregister_role(&mut role).await.unwrap();
        assert!(redis_role("adoption-first").is_empty());
        assert_eq!(redis_role("adoption-sibling"), sibling_owner);
        assert!(candidate
            .role_control(MessageKind::AdoptChannelRole, &request)
            .await
            .is_err());
        assert!(sibling.route_confirmed());
        old.deregister_role(&mut sibling).await.unwrap();
        foreign.deregister_role(&mut foreign_role).await.unwrap();
        candidate.close().await.unwrap();
        old.close().await.unwrap();
        foreign.close().await.unwrap();
        assert!(connection.subscribe_closed().borrow().is_none());
        assert_eq!(
            connection
                .registration()
                .await
                .request(&original.instance.instance_id)
                .generation
                .as_deref(),
            Some(original.instance.generation.as_str())
        );
        connection.shutdown().await;
    }

    #[cfg(feature = "service-zenoh")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "isolated native host; run zenss_channel_bootstrap_acceptance.py --role-test"]
    async fn native_rotation_candidate_is_control_only_and_expires_while_old_roles_renew() {
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let connection = crate::ServiceConnection::connect(
            &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
            crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
        let identity = connection
            .bootstrap_test_channel(
                ServiceInstanceRegistration {
                    instance_id: "native-rotation".into(),
                    incarnation_id: "rotation-boot".into(),
                    generation: None,
                },
                Some(RouteIdentity::new("dev").unwrap()),
            )
            .await
            .unwrap();
        let original = identity.bootstrap_response().clone();
        let mut pool = identity
            .open_sessions(ChannelSessionConfig::default())
            .await
            .unwrap();
        let catalog = kish_lingshu_runtime_contract::provider::ProviderCatalog {
            format_version: 1,
            application_id: app,
            provider_key: "rotation-original".into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![],
        };
        let mut role = pool.register_provider_role(&catalog).await.unwrap();
        let original_endpoint = role.endpoint().clone();
        let identity = pool.prepare_certificate_rotation().await.unwrap();
        assert_eq!(
            identity.rotation_predecessor(),
            Some(&original.certificate.certificate_identity)
        );
        assert_eq!(identity.bootstrap_response().instance, original.instance);
        assert_ne!(
            identity.bootstrap_response().certificate.message_public_key,
            original.certificate.message_public_key
        );
        let candidate_cap = identity.authorization_deadline;
        let mut candidate = identity
            .open_sessions(ChannelSessionConfig::default())
            .await
            .unwrap();
        // Same base and authenticated root are insufficient to claim roles.
        assert!(candidate.register_provider_role(&catalog).await.is_err());
        assert!(candidate.renew_role_leases(&mut [&mut role]).await.is_err());
        assert!(candidate.prepare_certificate_rotation().await.is_err());
        assert!(pool.prepare_certificate_rotation().await.is_err());
        assert!(role.route_confirmed());
        let started = Instant::now();
        for tick in 1..=4 {
            tokio::time::sleep_until(started + Duration::from_secs(tick * 10)).await;
            pool.refresh_authorization().await.unwrap();
            assert_eq!(
                pool.renew_role_leases(&mut [&mut role]).await.unwrap()[0].status,
                200
            );
            pool.confirm_role_route(&mut role).await.unwrap();
            assert_eq!(role.endpoint(), &original_endpoint);
            if tick <= 2 {
                candidate.refresh_authorization().await.unwrap();
                assert!(candidate.authorization_deadline() <= candidate_cap);
            }
        }
        assert!(Instant::now() > candidate_cap);
        assert!(candidate.subscribe_closed().borrow().is_some());
        assert!(candidate.refresh_authorization().await.is_err());
        assert!(role.route_confirmed());
        pool.deregister_role(&mut role).await.unwrap();
        candidate.close().await.unwrap();
        pool.close().await.unwrap();
        assert!(connection.subscribe_closed().borrow().is_none());
        assert_eq!(
            connection
                .registration()
                .await
                .request(&original.instance.instance_id)
                .generation
                .as_deref(),
            Some(original.instance.generation.as_str())
        );
        connection.shutdown().await;
    }
}
