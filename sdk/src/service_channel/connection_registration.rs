//! Establish physical data-lane evidence using queries sent on those Sessions.
use super::{ChannelSessionError, ServiceChannelSessions};
use kish_lingshu_foundation_contract::service_transport::{
    connection::{ConnectionAttestation, ConnectionCommand, ConnectionControl, ConnectionProtocol},
    MessageKind, RouteIdentity,
};
/// Querier matching tracks the exact product worker even when its shared Router
/// stays up. Every loss is sticky; a quick delete/redeclare cannot restore it.
pub(super) struct ControlOwnerObservation {
    _listener: zenoh::matching::MatchingListener<()>,
    _querier: zenoh::query::Querier<'static>,
    topology: tokio::task::JoinHandle<()>,
}
impl Drop for ControlOwnerObservation {
    fn drop(&mut self) {
        self.topology.abort();
    }
}
impl ServiceChannelSessions {
    /// The binding ID is stable across outcome lookup. This protocol proves
    /// lanes only; it must not suppress finite authorization or role renewal.
    pub async fn attest_connections(
        &self,
        binding_id: RouteIdentity,
    ) -> Result<ConnectionAttestation, ChannelSessionError> {
        let begin = ConnectionCommand::Begin {
            binding_id: binding_id.clone(),
            data_lanes: self.lane_count() as u8,
            dedicated_control: self.has_control_lane(),
        };
        let mut proof = self.attest_on(self.control_session()?, begin).await?;
        if proof.binding_id != binding_id
            || proof.data.len() != self.lane_count()
            || proof.dedicated_control != self.has_control_lane()
        {
            return Err(ChannelSessionError::InvalidResponse);
        }
        for lane in 0..self.lane_count() {
            let next = self
                .attest_on(
                    &self.sessions[lane],
                    ConnectionCommand::Attest {
                        binding_id: binding_id.clone(),
                        lane: lane as u8,
                    },
                )
                .await?;
            validate_transition(&proof, &next, Some(lane))?;
            proof = next;
        }
        let current = self
            .attest_on(
                self.control_session()?,
                ConnectionCommand::Inspect { binding_id },
            )
            .await?;
        validate_transition(&proof, &current, None)?;
        if !current.complete() {
            return Err(ChannelSessionError::InvalidResponse);
        }
        Ok(current)
    }
    /// Negotiate and bind this pool before attaching roles. Unsupported peers
    /// fail explicitly; a lost response never silently selects finite renewal.
    pub async fn activate_connection_authority(
        &mut self,
        binding_id: RouteIdentity,
    ) -> Result<(), ChannelSessionError> {
        if self.connection_authority.is_some() {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let negotiated = self
            .role_control(
                MessageKind::Register,
                &ConnectionControl {
                    connection_registration: ConnectionProtocol::V1,
                    command: ConnectionCommand::Negotiate,
                },
            )
            .await?;
        let capabilities: kish_lingshu_foundation_contract::service_transport::connection::ConnectionCapabilities =
            serde_json::from_str(negotiated.payload.get()).map_err(|_| ChannelSessionError::InvalidResponse)?;
        if capabilities.protocol != ConnectionProtocol::V1
            || !capabilities.connection_lifecycle
            || !capabilities.single_host_pool
        {
            return Err(ChannelSessionError::InvalidResponse);
        }
        let baseline = self.connectivity.borrow().clone();
        if !baseline.connected(self.lane_count()) || baseline.control_connected == Some(false) {
            return Err(ChannelSessionError::Transport);
        }
        let proof = self.attest_connections(binding_id.clone()).await?;
        if proof.owner_id != capabilities.owner_id
            || proof.control.host_boot != capabilities.host_boot
        {
            return Err(ChannelSessionError::InvalidResponse);
        }
        let key = self
            .identity
            .bootstrap_response()
            .control_route
            .key()
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let querier = self
            .control_session()?
            .declare_querier(key.as_str().to_owned())
            .await
            .map_err(|_| ChannelSessionError::Transport)?;
        let stop = self.connection_stop.clone();
        let (matching, mut observed) = tokio::sync::watch::channel(false);
        let seen = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let listener = querier
            .matching_listener()
            .callback(move |status| {
                if status.matching() {
                    seen.store(true, std::sync::atomic::Ordering::Release);
                    matching.send_replace(true);
                } else if seen.load(std::sync::atomic::Ordering::Acquire) {
                    stop.send_replace(true);
                    matching.send_replace(false);
                }
            })
            .await
            .map_err(|_| ChannelSessionError::Transport)?;
        // Matching discovery is asynchronous. An initial false is not an
        // observed owner loss; after the first true, any loss remains sticky.
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if *self.connection_stop.borrow() {
                    return Err(ChannelSessionError::Transport);
                }
                if *observed.borrow_and_update() {
                    return Ok(());
                }
                observed
                    .changed()
                    .await
                    .map_err(|_| ChannelSessionError::Transport)?;
            }
        })
        .await
        .map_err(|_| ChannelSessionError::Transport)??;
        let mut connectivity = self.connectivity.clone();
        let stop = self.connection_stop.clone();
        let topology = tokio::spawn(async move {
            loop {
                if *connectivity.borrow_and_update() != baseline {
                    stop.send_replace(true);
                    return;
                }
                if connectivity.changed().await.is_err() {
                    stop.send_replace(true);
                    return;
                }
            }
        });
        self.connection_observer = Some(ControlOwnerObservation {
            _listener: listener,
            _querier: querier,
            topology,
        });
        let started = tokio::time::Instant::now();
        let response = self
            .role_control(
                MessageKind::Register,
                &ConnectionControl {
                    connection_registration: ConnectionProtocol::V1,
                    command: ConnectionCommand::Activate { binding_id },
                },
            )
            .await?;
        let authorization: super::ChannelAuthorization =
            serde_json::from_str(response.payload.get())
                .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let now = chrono::Utc::now().timestamp_millis();
        authorization
            .validate(self.identity.bootstrap_response(), now)
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let mut expected = proof;
        expected.expires_unix_ms = self
            .identity
            .bootstrap_response()
            .certificate
            .expires_unix_ms;
        if authorization.connection.as_ref() != Some(&expected) || *self.connection_stop.borrow() {
            return Err(ChannelSessionError::InvalidResponse);
        }
        let remaining = (authorization.authorization_expires_unix_ms - now).min(
            authorization.authorization_expires_unix_ms
                - authorization.authorization_issued_unix_ms,
        );
        let remaining =
            u64::try_from(remaining).map_err(|_| ChannelSessionError::AuthorityExpired)?;
        self.transport
            .update_deadline(started + std::time::Duration::from_millis(remaining))?;
        self.connection_authority = Some(expected);
        Ok(())
    }
    async fn attest_on(
        &self,
        session: &zenoh::Session,
        command: ConnectionCommand,
    ) -> Result<ConnectionAttestation, ChannelSessionError> {
        let request = ConnectionControl {
            connection_registration: ConnectionProtocol::V1,
            command,
        };
        request
            .validate()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let response = self
            .role_control_scoped(session, MessageKind::Register, &request)
            .await?;
        let proof: ConnectionAttestation = serde_json::from_str(response.envelope.payload.get())
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        proof
            .validate(chrono::Utc::now().timestamp_millis())
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        if proof.protocol != request.connection_registration
            || proof.expires_unix_ms
                > self
                    .identity
                    .bootstrap_response()
                    .certificate
                    .expires_unix_ms
        {
            return Err(ChannelSessionError::InvalidResponse);
        }
        Ok(proof)
    }
}
fn validate_transition(
    old: &ConnectionAttestation,
    new: &ConnectionAttestation,
    lane: Option<usize>,
) -> Result<(), ChannelSessionError> {
    if old.protocol != new.protocol
        || old.binding_id != new.binding_id
        || old.owner_id != new.owner_id
        || old.control != new.control
        || old.data.len() != new.data.len()
        || old.dedicated_control != new.dedicated_control
        || old.expires_unix_ms != new.expires_unix_ms
        || old
            .data
            .iter()
            .zip(&new.data)
            .enumerate()
            .any(|(index, (before, after))| {
                if lane == Some(index) {
                    after.is_none()
                        || before
                            .as_ref()
                            .is_some_and(|before| Some(before) != after.as_ref())
                } else {
                    before != after
                }
            })
    {
        Err(ChannelSessionError::InvalidResponse)
    } else {
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use kish_lingshu_foundation_contract::service_transport::connection::PhysicalConnectionIdentity;
    #[test]
    fn a_signed_reply_cannot_replace_other_lanes_owner_or_original_deadline() {
        let control = PhysicalConnectionIdentity {
            host_boot: RouteIdentity::new("host").unwrap(),
            transport_epoch: 1,
        };
        let old = ConnectionAttestation {
            protocol: ConnectionProtocol::V1,
            binding_id: RouteIdentity::new("binding").unwrap(),
            owner_id: RouteIdentity::new("owner").unwrap(),
            control: control.clone(),
            data: vec![None, None],
            dedicated_control: true,
            expires_unix_ms: 30_000,
        };
        let mut next = old.clone();
        next.data[0] = Some(PhysicalConnectionIdentity {
            transport_epoch: 2,
            ..control
        });
        assert!(validate_transition(&old, &next, Some(0)).is_ok());
        assert!(validate_transition(&old, &next, None).is_err());
        assert!(validate_transition(&old, &next, Some(1)).is_err());
        let mut changed = next.clone();
        changed.expires_unix_ms += 1;
        assert!(validate_transition(&old, &changed, Some(0)).is_err());
        changed = next.clone();
        changed.owner_id = RouteIdentity::new("new-owner").unwrap();
        assert!(validate_transition(&old, &changed, Some(0)).is_err());
        changed = next.clone();
        changed.data[0].as_mut().unwrap().transport_epoch += 1;
        assert!(validate_transition(&next, &changed, Some(0)).is_err());
        assert!(validate_transition(&next, &next, None).is_ok());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "actual matched native connection authority; run acceptance --registration-test"]
    async fn native_control_connection_remains_authorized_without_renewal_and_rejects_another_transport(
    ) {
        use kish_lingshu_foundation_contract::ServiceInstanceRegistration;
        let connection = crate::ServiceConnection::connect(
            &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
            crate::ServiceCredential::new(
                std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap(),
                std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let identity = connection
            .bootstrap_test_channel(
                ServiceInstanceRegistration {
                    instance_id: "connected-control".into(),
                    incarnation_id: "connected-control-boot".into(),
                    generation: None,
                },
                None,
            )
            .await
            .unwrap();
        let initial = identity.authorization_deadline;
        let mut pool = identity
            .open_sessions(
                super::super::ChannelSessionConfig::new(1)
                    .unwrap()
                    .with_control_lane()
                    .unwrap(),
            )
            .await
            .unwrap();
        pool.activate_connection_authority(RouteIdentity::new("connected-control-proof").unwrap())
            .await
            .unwrap();
        let deadline = pool.authorization_deadline();
        assert!(deadline > initial + std::time::Duration::from_secs(200));
        // No managed supervisor or role exists, hence no client renewal/probe.
        tokio::time::sleep_until(initial + std::time::Duration::from_secs(2)).await;
        let observed = pool.refresh_authorization().await.unwrap();
        assert!(observed.connection.is_some());
        assert!(pool.authorization_deadline() <= deadline);
        let rogue = zenoh::open(super::super::sessions::client_config(&pool.identity, 0).unwrap())
            .await
            .unwrap();
        let denied = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            pool.role_control_scoped(
                &rogue,
                MessageKind::ChannelAuthorization,
                &serde_json::json!({}),
            ),
        )
        .await;
        assert!(
            !matches!(denied, Ok(Ok(_))),
            "same certificate on another physical transport borrowed the control binding"
        );
        rogue.close().await.unwrap();
        assert!(pool.refresh_authorization().await.is_ok());
        pool.close().await.unwrap();
        assert_eq!(connection.channel_session_budget().available_permits(), 4);
        connection.shutdown().await;
        println!("PASS actual connected control survives original finite deadline without renewal; same-certificate foreign transport denied; all Session permits released");
    }
}

#[cfg(all(test, feature = "service-call-zenoh"))]
mod fault_tests {
    use super::*;
    use crate::service_channel::ChannelSessionConfig;
    use kish_lingshu_foundation_contract::ServiceInstanceRegistration;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "actual connected control traffic measurement in its own process"]
    async fn connected_idle_control_traffic_does_not_grow_with_role_count() {
        let capture = super::super::observation::tests::Capture::default();
        metrics::set_global_recorder(capture.clone()).unwrap();
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        for count in [1, 16] {
            let connection = crate::ServiceConnection::connect(
                &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
                crate::ServiceCredential::new(
                    &app,
                    std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
            let identity = connection
                .bootstrap_test_channel(
                    ServiceInstanceRegistration {
                        instance_id: format!("traffic-{count}"),
                        incarnation_id: format!("traffic-{count}-boot"),
                        generation: None,
                    },
                    None,
                )
                .await
                .unwrap();
            let mut pool = identity
                .open_sessions(ChannelSessionConfig::new(1).unwrap())
                .await
                .unwrap();
            pool.activate_connection_authority(
                RouteIdentity::new(format!("traffic-{count}-binding")).unwrap(),
            )
            .await
            .unwrap();
            let before_registration =
                capture.sum("lingshu_sdk_channel_exchanges_total", Some("control"), None);
            let mut roles = Vec::new();
            for index in 0..count {
                roles.push(
                    pool.register_provider_role(
                        &kish_lingshu_runtime_contract::provider::ProviderCatalog {
                            format_version: 1,
                            application_id: app.clone(),
                            provider_key: format!("traffic-{count}-{index}"),
                            release: "1".into(),
                            services: None,
                            events: None,
                            workflows: vec![],
                        },
                    )
                    .await
                    .unwrap(),
                );
            }
            let after_registration =
                capture.sum("lingshu_sdk_channel_exchanges_total", Some("control"), None);
            assert!(after_registration - before_registration >= 2.0 * count as f64);
            let mut channel = pool.manage_roles(roles).unwrap();
            let until = tokio::time::Instant::now() + Duration::from_secs(35);
            while tokio::time::Instant::now() < until {
                assert_eq!(channel.role_statuses().len(), count);
                assert!(channel
                    .role_statuses()
                    .iter()
                    .all(|role| role.route_confirmed() && role.last_renewal_status.is_none()));
                assert!(matches!(
                    channel.status(),
                    super::super::ChannelSupervisorStatus::Active {
                        observations: 0,
                        ..
                    }
                ));
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let idle = capture.sum("lingshu_sdk_channel_exchanges_total", Some("control"), None)
                - after_registration;
            assert_eq!(idle, 0.0);
            println!("MEASURE connected roles={count} idle_seconds=35 startup_control_exchanges={} idle_control_exchanges={idle} readiness_gaps=0", after_registration - before_registration);
            channel.close().await.unwrap();
            assert_eq!(connection.channel_session_budget().available_permits(), 4);
            connection.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "actual dependency loss and shared root revocation"]
    async fn connected_authority_cannot_survive_dependency_loss_or_shared_root_revocation() {
        let markers =
            std::path::PathBuf::from(std::env::var("LINGSHU_AUTHORITY_MARKER_DIR").unwrap());
        async fn marker(dir: &std::path::Path, name: &str) {
            tokio::time::timeout(Duration::from_secs(20), async {
                while !dir.join(name).exists() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
        }
        async fn pool(instance: &str) -> (crate::ServiceConnection, ServiceChannelSessions) {
            let connection = crate::ServiceConnection::connect(
                &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
                crate::ServiceCredential::new(
                    std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap(),
                    std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
            let identity = connection
                .bootstrap_test_channel(
                    ServiceInstanceRegistration {
                        instance_id: instance.into(),
                        incarnation_id: format!("{instance}-boot"),
                        generation: None,
                    },
                    None,
                )
                .await
                .unwrap();
            let mut pool = identity
                .open_sessions(
                    ChannelSessionConfig::new(1)
                        .unwrap()
                        .with_control_lane()
                        .unwrap(),
                )
                .await
                .unwrap();
            pool.activate_connection_authority(
                RouteIdentity::new(format!("{instance}-binding")).unwrap(),
            )
            .await
            .unwrap();
            (connection, pool)
        }
        let (connection, mut unreadable) = pool("connected-unreadable").await;
        let deadline = unreadable.authorization_deadline();
        assert!(deadline > tokio::time::Instant::now() + Duration::from_secs(200));
        std::fs::write(
            markers.join("unreadable-ready"),
            "connected authority installed",
        )
        .unwrap();
        marker(&markers, "unreadable").await;
        assert!(unreadable.refresh_authorization().await.is_err());
        assert_eq!(unreadable.authorization_deadline(), deadline);
        std::fs::write(
            markers.join("unreadable-expired"),
            "admission denied before certificate expiry",
        )
        .unwrap();
        marker(&markers, "recovered").await;
        assert!(
            unreadable.refresh_authorization().await.is_err(),
            "dependency recovery must not restore retired connected authority"
        );
        unreadable.close().await.unwrap();
        assert_eq!(connection.channel_session_budget().available_permits(), 4);
        connection.shutdown().await;
        let (first_connection, mut first) = pool("connected-root-first").await;
        let (second_connection, mut second) = pool("connected-root-second").await;
        std::fs::write(markers.join("shared-root-ready"), "two connected instances").unwrap();
        marker(&markers, "root-revoked").await;
        assert!(first.refresh_authorization().await.is_err());
        assert!(second.refresh_authorization().await.is_err());
        first.close().await.unwrap();
        second.close().await.unwrap();
        for connection in [&first_connection, &second_connection] {
            assert_eq!(connection.channel_session_budget().available_permits(), 4);
            connection.shutdown().await;
        }
        std::fs::write(markers.join("authority-facts.json"), serde_json::to_vec(&serde_json::json!({
            "connected_authority":true, "denied_before_certificate_expiry":true,
            "dependency_recovery_did_not_restore_authority":true, "shared_root_instances_denied":2,
            "all_session_permits_released":true,
        })).unwrap()).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "matched event-driven SDK and controlled TLS relay"]
    async fn connected_runtime_rebuilds_after_physical_loss_without_role_renewal() {
        use super::super::{NativeRuntimeStatus, RegisteredServiceRuntime};
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let connection = crate::ServiceConnection::connect(
            &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
            crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
        let mut runtime = RegisteredServiceRuntime::new(
            connection.clone(),
            ServiceInstanceRegistration {
                instance_id: "connected-runtime".into(),
                incarnation_id: "connected-runtime-first".into(),
                generation: None,
            },
            Some(RouteIdentity::new("dev").unwrap()),
            kish_lingshu_runtime_contract::provider::ProviderCatalog {
                format_version: 1,
                application_id: app,
                provider_key: "connected-runtime-source".into(),
                release: "1".into(),
                services: None,
                events: None,
                workflows: vec![],
            },
            ChannelSessionConfig::new(1)
                .unwrap()
                .with_control_lane()
                .unwrap(),
        )
        .unwrap()
        .with_connection_lifecycle(true)
        .start()
        .unwrap();
        let wait = |mut status: tokio::sync::watch::Receiver<NativeRuntimeStatus>,
                    previous: Option<String>| async move {
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    let current = status.borrow_and_update().clone();
                    match current {
                        NativeRuntimeStatus::Active {
                            instance_generation,
                        } if previous.as_ref() != Some(&instance_generation) => {
                            return instance_generation
                        }
                        NativeRuntimeStatus::Failed { error } => {
                            panic!("runtime failed: {error:?}")
                        }
                        _ => {}
                    }
                    status.changed().await.unwrap();
                }
            })
            .await
            .unwrap()
        };
        let generation = wait(runtime.subscribe_status(), None).await;
        let control = std::env::var("LINGSHU_GATEWAY_CONTROL_URL").unwrap();
        let client = reqwest::Client::new();
        for path in ["/all/down", "/second/up"] {
            client
                .post(format!("{control}{path}"))
                .timeout(Duration::from_secs(2))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap();
        }
        let replacement = wait(runtime.subscribe_status(), Some(generation.clone())).await;
        assert_ne!(generation, replacement);
        assert_eq!(connection.channel_session_budget().available_permits(), 2);
        runtime.close().await.unwrap();
        assert_eq!(connection.channel_session_budget().available_permits(), 4);
        connection.shutdown().await;
        println!("PASS negotiated runtime joined old physical pool, replaced instance and Provider after rapid loss, and released all permits");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "matched event-driven ZenSS SDK and controlled TLS relay"]
    async fn connected_transport_loss_is_sticky_across_immediate_reopen() {
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
                    instance_id: "connected-fault".into(),
                    incarnation_id: "connected-fault-first".into(),
                    generation: None,
                },
                None,
            )
            .await
            .unwrap();
        let mut pool = identity
            .open_sessions(
                ChannelSessionConfig::new(1)
                    .unwrap()
                    .with_control_lane()
                    .unwrap(),
            )
            .await
            .unwrap();
        pool.activate_connection_authority(RouteIdentity::new("connected-fault-binding").unwrap())
            .await
            .unwrap();
        let mut provider = pool
            .register_provider_role(&kish_lingshu_runtime_contract::provider::ProviderCatalog {
                format_version: 1,
                application_id: app,
                provider_key: "connected-fault-source".into(),
                release: "1".into(),
                services: None,
                events: None,
                workflows: vec![],
            })
            .await
            .unwrap();
        assert!(provider.route_confirmed());
        let client = reqwest::Client::new();
        let control = std::env::var("LINGSHU_GATEWAY_CONTROL_URL").unwrap();
        let started = tokio::time::Instant::now();
        // Reopen immediately. The test does not wait for a sampled offline state.
        for path in ["/all/down", "/second/up"] {
            client
                .post(format!("{control}{path}"))
                .timeout(Duration::from_secs(2))
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap();
        }
        let mut closed = pool.subscribe_closed();
        tokio::time::timeout(Duration::from_secs(2), async {
            while closed.borrow_and_update().is_none() {
                closed.changed().await.unwrap();
            }
        })
        .await
        .expect("event-driven physical loss must close the old pool");
        assert!(!provider.route_confirmed());
        assert!(pool.refresh_authorization().await.is_err());
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            closed.borrow().is_some(),
            "reconnect cannot revive the old physical owner"
        );
        provider.close().await.unwrap();
        pool.close().await.unwrap();
        assert_eq!(connection.channel_session_budget().available_permits(), 4);
        connection.shutdown().await;
        println!("PASS connected TLS loss/reopen fenced old role and pool; observed cleanup in {} ms; all permits released", started.elapsed().as_millis());
    }
}
