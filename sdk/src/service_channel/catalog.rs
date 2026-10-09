//! Immutable, bounded Provider snapshot on the role's existing native owner.
use super::ChannelSessionError;
use kish_lingshu_foundation_contract::{
    service_auth::{ChannelMessageSigner, ClientChannelIdentity, TransportMessageClaims},
    service_transport::{
        bootstrap::{ChannelBootstrapResponse, MAX_BOOTSTRAP_CLOCK_SKEW_MS},
        ExactRouteKey, MessageKind, ServiceEndpoint, TransportEnvelope,
    },
};
use kish_lingshu_runtime_contract::provider::{ProviderCatalog, ProviderCatalogRead};
use std::sync::Arc;

const CATALOG_READ_TIMEOUT_MS: i64 = 10_000;

pub(super) struct CatalogSnapshot {
    pub digest: String,
    provider: String,
    release: String,
    payload: Box<serde_json::value::RawValue>,
    _bytes: tokio::sync::OwnedSemaphorePermit,
}
impl CatalogSnapshot {
    pub(super) fn new(
        catalog: &ProviderCatalog,
        connection: &crate::ServiceConnection,
    ) -> Result<Self, ChannelSessionError> {
        if catalog.application_id != connection.application_id() {
            return Err(ChannelSessionError::InvalidConfig);
        }
        let mut permit = connection
            .catalog_budgets()
            .0
            .try_acquire_many_owned(
                kish_lingshu_runtime_contract::provider::MAX_PROVIDER_CATALOG_BYTES as u32,
            )
            .map_err(|_| ChannelSessionError::CapacityExceeded)?;
        let digest = catalog
            .digest()
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let payload = serde_json::value::to_raw_value(catalog)
            .map_err(|_| ChannelSessionError::InvalidConfig)?;
        // Reserve before validation/serialization; only two maximum-size
        // constructions can allocate on the same logical connection at once.
        drop(permit.split(
            kish_lingshu_runtime_contract::provider::MAX_PROVIDER_CATALOG_BYTES
                - payload.get().len().max(1),
        ));
        Ok(Self {
            digest,
            provider: catalog.provider_key.clone(),
            release: catalog.release.clone(),
            payload,
            _bytes: permit,
        })
    }
}
fn validate_read(
    snapshot: &CatalogSnapshot,
    endpoint: &ServiceEndpoint,
    target: &ExactRouteKey,
    bytes: &[u8],
    role_expires_at_ms: i64,
    now: i64,
) -> Result<TransportEnvelope, ChannelSessionError> {
    use kish_lingshu_foundation_contract::service_transport::channel::MAX_CHANNEL_CONTROL_BYTES;
    if bytes.len() > MAX_CHANNEL_CONTROL_BYTES {
        return Err(ChannelSessionError::InvalidResponse);
    }
    let ServiceEndpoint::Zenoh {
        route,
        route_revision,
        lanes,
        ..
    } = endpoint
    else {
        return Err(ChannelSessionError::InvalidConfig);
    };
    let request = TransportEnvelope::decode(bytes, target, &route.application_id, now)
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
    let read: ProviderCatalogRead = serde_json::from_str(request.payload.get())
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
    if request.kind != MessageKind::CatalogRead
        || request.deadline_unix_ms
            > now.saturating_add(CATALOG_READ_TIMEOUT_MS + MAX_BOOTSTRAP_CLOCK_SKEW_MS)
        || request.deadline_unix_ms > role_expires_at_ms
        || read.provider_key != snapshot.provider
        || read.release != snapshot.release
        || read.catalog_digest != snapshot.digest
        || read.route_revision != *route_revision
        || lanes.first() != Some(&read.lane)
        || route.catalog_key(&snapshot.digest).as_ref() != Ok(target)
    {
        return Err(ChannelSessionError::InvalidResponse);
    }
    Ok(request)
}

// Validate the authenticated sender's duration separately from receiver clock
// skew, as for BindLane. Role expiry and the absolute deadline remain strict.
fn validate_read_time(
    claims: &TransportMessageClaims,
    now: i64,
) -> Result<(), ChannelSessionError> {
    claims
        .validate_request_time(now, CATALOG_READ_TIMEOUT_MS)
        .map_err(|_| ChannelSessionError::InvalidResponse)
}

pub(super) fn reply(
    snapshot: &CatalogSnapshot,
    endpoint: &ServiceEndpoint,
    target: &ExactRouteKey,
    bytes: &[u8],
    role_expires_at_ms: i64,
    connection: &crate::ServiceConnection,
    initial: &ChannelBootstrapResponse,
    signer: &Arc<ChannelMessageSigner>,
    now: i64,
) -> Result<Vec<u8>, ChannelSessionError> {
    let request = validate_read(snapshot, endpoint, target, bytes, role_expires_at_ms, now)?;
    let ServiceEndpoint::Zenoh { route, .. } = endpoint else {
        return Err(ChannelSessionError::InvalidConfig);
    };
    let claims = connection
        .verify_channel_message(&initial.transport_trust, &request)
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
    validate_read_time(&claims, now)?;
    let subject = ClientChannelIdentity {
        application_id: initial.application_id.clone(),
        instance_id: route.instance_id.clone(),
        base_generation: route.base_generation.clone(),
        certificate_identity: initial.certificate.certificate_identity.clone(),
    };
    sign_reply(snapshot, &subject, signer, request, now)
}

fn sign_reply(
    snapshot: &CatalogSnapshot,
    subject: &ClientChannelIdentity,
    signer: &ChannelMessageSigner,
    request: TransportEnvelope,
    now: i64,
) -> Result<Vec<u8>, ChannelSessionError> {
    let parent = request.trace_parent.clone();
    super::trace::scope_sync(parent.as_deref(), || {
        let proof = signer
            .sign_message(
                subject,
                MessageKind::CatalogRead,
                &request.target,
                &request.request_id,
                snapshot.payload.get().as_bytes(),
                now,
                request.deadline_unix_ms,
            )
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        TransportEnvelope {
            protocol_version: request.protocol_version,
            kind: MessageKind::CatalogRead,
            request_id: request.request_id,
            application_id: subject.application_id.clone(),
            target: request.target,
            deadline_unix_ms: request.deadline_unix_ms,
            proof,
            trace_parent: super::trace::current_trace_parent(),
            payload: snapshot.payload.clone(),
        }
        .encode(now)
        .map_err(|_| ChannelSessionError::InvalidResponse)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use kish_lingshu_foundation_contract::ServiceInstanceRegistration;
    use kish_lingshu_runtime_contract::provider::{
        ProviderPlan, ProviderPreviewRequest, ProviderWorkflow,
    };
    use std::{collections::BTreeMap, time::Duration};

    #[tokio::test]
    async fn signed_catalog_reply_preserves_snapshot_and_correlates_without_changing_proof_authority(
    ) {
        use kish_lingshu_foundation_contract::service_transport::{
            DataLaneId, InstanceRoute, LaneIdentity, ProtocolVersion, RouteIdentity,
        };
        let id = |v: &str| RouteIdentity::new(v).unwrap();
        use kish_lingshu_foundation_contract::{
            service_auth::{
                verify_client_transport_message, verify_transport_message, ServiceSigner,
            },
            trace::TraceParent,
        };
        use rcgen::{KeyPair, PKCS_ED25519};
        let platform = ServiceSigner::new(&[42; 32]).unwrap();
        let key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
        let signer = Arc::new(ChannelMessageSigner::from_pkcs8(&key.serialize_der()).unwrap());
        let trust = platform
            .transport_trust("app", chrono::Utc::now().timestamp())
            .unwrap();
        let endpoint = ServiceEndpoint::Zenoh {
            protocol_version: ProtocolVersion::V1,
            route: InstanceRoute {
                deployment: id("dev"),
                application_id: id("app"),
                instance_id: id("sdk"),
                base_generation: id("base"),
                role_generation: id("role"),
            },
            route_revision: 1,
            lanes: vec![LaneIdentity {
                lane: DataLaneId::new(0).unwrap(),
                epoch: id("epoch"),
            }],
        };
        let ServiceEndpoint::Zenoh {
            route,
            route_revision,
            lanes,
            ..
        } = &endpoint
        else {
            unreachable!()
        };
        let catalog = ProviderCatalog {
            format_version: 1,
            application_id: "app".into(),
            provider_key: "provider".into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![],
        };
        let snapshot = CatalogSnapshot {
            digest: catalog.digest().unwrap(),
            provider: "provider".into(),
            release: "1".into(),
            payload: serde_json::value::to_raw_value(&catalog).unwrap(),
            _bytes: Arc::new(tokio::sync::Semaphore::new(1))
                .try_acquire_owned()
                .unwrap(),
        };
        let target = route.catalog_key(&snapshot.digest).unwrap();
        let now = chrono::Utc::now().timestamp_millis();
        let payload = serde_json::value::to_raw_value(&ProviderCatalogRead {
            provider_key: "provider".into(),
            release: "1".into(),
            catalog_digest: snapshot.digest.clone(),
            route_revision: *route_revision,
            lane: lanes[0].clone(),
        })
        .unwrap();
        let request_id = RouteIdentity::new("read").unwrap();
        let parent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
        let request = TransportEnvelope {
            protocol_version: ProtocolVersion::V1,
            kind: MessageKind::CatalogRead,
            request_id: request_id.clone(),
            application_id: id("app"),
            target: target.clone(),
            deadline_unix_ms: now + CATALOG_READ_TIMEOUT_MS + 1_000,
            proof: platform
                .sign_transport_message(
                    "app",
                    MessageKind::CatalogRead,
                    &target,
                    &request_id,
                    payload.get().as_bytes(),
                    now + 1_000,
                    now + CATALOG_READ_TIMEOUT_MS + 1_000,
                )
                .unwrap(),
            trace_parent: Some(parent.into()),
            payload,
        };
        let checked = validate_read(
            &snapshot,
            &endpoint,
            &target,
            &request.encode(now).unwrap(),
            now + 20_000,
            now,
        )
        .unwrap();
        verify_transport_message(
            &trust,
            &checked.proof,
            "app",
            checked.kind,
            &checked.target,
            &checked.request_id,
            checked.payload.get().as_bytes(),
            now,
        )
        .unwrap();
        let subject = ClientChannelIdentity {
            application_id: id("app"),
            instance_id: route.instance_id.clone(),
            base_generation: route.base_generation.clone(),
            certificate_identity: id("certificate"),
        };
        #[cfg(feature = "service-call-zenoh")]
        let bytes = {
            let f = crate::services::execution::tests::fixture(1).await;
            let initial = super::super::call::tests::authority(&platform, &signer);
            let request_bytes = request.encode(now).unwrap();
            let bytes = reply(
                &snapshot,
                &endpoint,
                &target,
                &request_bytes,
                now + 20_000,
                &f.core.connection,
                &initial,
                &signer,
                now,
            )
            .unwrap();
            assert!(
                reply(
                    &snapshot,
                    &endpoint,
                    &target,
                    &request_bytes,
                    now + 20_000,
                    &f.core.connection,
                    &initial,
                    &signer,
                    now
                )
                .is_err(),
                "catalog replay remains rejected"
            );
            f.core.connection.shutdown().await;
            bytes
        };
        #[cfg(not(feature = "service-call-zenoh"))]
        let bytes = sign_reply(&snapshot, &subject, &signer, checked, now).unwrap();
        let response = TransportEnvelope::decode(&bytes, &target, &id("app"), now).unwrap();
        assert_eq!(response.payload.get(), snapshot.payload.get());
        let actual: ProviderCatalog = serde_json::from_str(response.payload.get()).unwrap();
        assert_eq!(actual.digest().unwrap(), snapshot.digest);
        verify_client_transport_message(
            signer.public_key(),
            &subject,
            &response.proof,
            response.kind,
            &response.target,
            &response.request_id,
            response.payload.get().as_bytes(),
            now,
        )
        .unwrap();
        let trace = TraceParent::parse(response.trace_parent.as_deref().unwrap()).unwrap();
        let parent = TraceParent::parse(parent).unwrap();
        assert_eq!(trace.trace_id(), parent.trace_id());
        assert_eq!(trace.flags(), parent.flags());
        assert_ne!(trace.span_id(), parent.span_id());
        assert!(super::super::trace::current_trace_parent().is_none());
    }

    #[tokio::test]
    async fn catalog_request_rejects_wrong_lane_revision_identity_target_and_expiry() {
        use kish_lingshu_foundation_contract::service_transport::{
            DataLaneId, InstanceRoute, LaneIdentity, ProtocolVersion, RouteIdentity,
        };
        let id = |v: &str| RouteIdentity::new(v).unwrap();
        let catalog = ProviderCatalog {
            format_version: 1,
            application_id: "app".into(),
            provider_key: "provider".into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![],
        };
        let snapshot = CatalogSnapshot {
            digest: catalog.digest().unwrap(),
            provider: "provider".into(),
            release: "1".into(),
            payload: serde_json::value::to_raw_value(&catalog).unwrap(),
            _bytes: Arc::new(tokio::sync::Semaphore::new(1))
                .try_acquire_owned()
                .unwrap(),
        };
        let lane = LaneIdentity {
            lane: DataLaneId::new(0).unwrap(),
            epoch: id("epoch"),
        };
        let route = InstanceRoute {
            deployment: id("dev"),
            application_id: id("app"),
            instance_id: id("sdk"),
            base_generation: id("base"),
            role_generation: id("role"),
        };
        let target = route.catalog_key(&snapshot.digest).unwrap();
        let endpoint = ServiceEndpoint::Zenoh {
            protocol_version: ProtocolVersion::V1,
            route,
            route_revision: 2,
            lanes: vec![lane.clone()],
        };
        let now = chrono::Utc::now().timestamp_millis();
        let read = ProviderCatalogRead {
            provider_key: "provider".into(),
            release: "1".into(),
            catalog_digest: snapshot.digest.clone(),
            route_revision: 2,
            lane,
        };
        let mut request = TransportEnvelope {
            protocol_version: ProtocolVersion::V1,
            kind: MessageKind::CatalogRead,
            request_id: id("request"),
            application_id: id("app"),
            target: target.clone(),
            deadline_unix_ms: now + 1_000,
            proof: "checked-separately-by-trust".into(),
            trace_parent: None,
            payload: serde_json::value::to_raw_value(&read).unwrap(),
        };
        assert!(validate_read(
            &snapshot,
            &endpoint,
            &target,
            &request.encode(now).unwrap(),
            now + 2_000,
            now
        )
        .is_ok());
        for mode in 0..5 {
            let mut changed = read.clone();
            match mode {
                0 => changed.route_revision = 1,
                1 => changed.lane.epoch = id("old-epoch"),
                2 => changed.provider_key = "other".into(),
                3 => changed.release = "2".into(),
                _ => changed.catalog_digest = "a".repeat(64),
            }
            request.payload = serde_json::value::to_raw_value(&changed).unwrap();
            assert!(validate_read(
                &snapshot,
                &endpoint,
                &target,
                &request.encode(now).unwrap(),
                now + 2_000,
                now
            )
            .is_err());
        }
        request.payload = serde_json::value::to_raw_value(&read).unwrap();
        assert!(validate_read(
            &snapshot,
            &endpoint,
            &target,
            &request.encode(now).unwrap(),
            now + 999,
            now
        )
        .is_err());
        assert!(validate_read(
            &snapshot,
            &endpoint,
            &target,
            &request.encode(now).unwrap(),
            now + 2_000,
            now + 1_001
        )
        .is_err());
        let wrong_target = ExactRouteKey::new("ls/v1/other/catalog/key").unwrap();
        assert!(validate_read(
            &snapshot,
            &endpoint,
            &wrong_target,
            &request.encode(now).unwrap(),
            now + 2_000,
            now
        )
        .is_err());
        // A two-millisecond clock offset must not reject the platform's
        // ten-second request before checking its signed sender timestamp.
        request.deadline_unix_ms = now + CATALOG_READ_TIMEOUT_MS + 2;
        assert!(validate_read(
            &snapshot,
            &endpoint,
            &target,
            &request.encode(now).unwrap(),
            now + 20_000,
            now
        )
        .is_ok());
        request.deadline_unix_ms = now + CATALOG_READ_TIMEOUT_MS + MAX_BOOTSTRAP_CLOCK_SKEW_MS + 1;
        assert!(validate_read(
            &snapshot,
            &endpoint,
            &target,
            &request.encode(now).unwrap(),
            now + 20_000,
            now
        )
        .is_err());
        assert!(validate_read(
            &snapshot,
            &endpoint,
            &target,
            &vec![b' '; 32 * 1024 + 1],
            now + 20_000,
            now
        )
        .is_err());
    }

    #[test]
    fn signed_catalog_time_accepts_bounded_skew_without_extending_sender_ttl() {
        use kish_lingshu_foundation_contract::{
            service_auth::{verify_transport_message, ServiceSigner},
            service_transport::RouteIdentity,
        };
        let now = 1_800_000_000_000;
        let signer = ServiceSigner::new(&[42; 32]).unwrap();
        let trust = signer.transport_trust("app", now / 1000).unwrap();
        let target = ExactRouteKey::new("ls/v1/test/catalog/snapshot").unwrap();
        let request_id = RouteIdentity::new("catalog-clock-regression").unwrap();
        for (offset, ttl, expected) in [
            (2, 10_000, true),
            (5_000, 10_000, true),
            (-2, 10_000, true),
            (0, 10_001, false),
            (-2, 10_001, false),
            (5_001, 1_000, false),
        ] {
            let issued = now + offset;
            let proof = signer
                .sign_transport_message(
                    "app",
                    MessageKind::CatalogRead,
                    &target,
                    &request_id,
                    b"{}",
                    issued,
                    issued + ttl,
                )
                .unwrap();
            let claims = verify_transport_message(
                &trust,
                &proof,
                "app",
                MessageKind::CatalogRead,
                &target,
                &request_id,
                b"{}",
                now,
            )
            .unwrap();
            assert_eq!(
                validate_read_time(&claims, now).is_ok(),
                expected,
                "offset={offset} ttl={ttl}"
            );
            assert!(validate_read_time(&claims, claims.deadline_unix_ms).is_err());
        }
    }

    async fn assert_failed(response: reqwest::Response) {
        if response.status().is_success() {
            let value: serde_json::Value = response.json().await.unwrap();
            assert_eq!(value["status"], false, "{value:?}");
        }
    }

    async fn admin_result<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> T {
        let value: serde_json::Value = response.error_for_status().unwrap().json().await.unwrap();
        assert_eq!(value["status"], true, "{value:?}");
        serde_json::from_value(value["data"].clone()).unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "isolated native host; run zenss_channel_bootstrap_acceptance.py --role-test"]
    async fn native_discovery_preserves_http_compatibility_and_projects_only_finite_public_observations(
    ) {
        use kish_lingshu_foundation_contract::{
            service_transport::enrollment::*, ServiceInstanceTransport,
        };
        use kish_lingshu_runtime_contract::{provider::*, service::*};
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
        let key = std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap();
        let token = std::env::var("LINGSHU_CHANNEL_TEST_PREVIEW_TOKEN").unwrap();
        let connection = crate::ServiceConnection::connect(
            &url,
            crate::ServiceCredential::new(&app, &key).unwrap(),
        )
        .await
        .unwrap();
        let identity = connection
            .bootstrap_test_channel(
                ServiceInstanceRegistration {
                    instance_id: "native-discovery".into(),
                    incarnation_id: "discovery-boot".into(),
                    generation: None,
                },
                None,
            )
            .await
            .unwrap();
        let mut pool = identity
            .open_sessions(super::super::ChannelSessionConfig::default())
            .await
            .unwrap();
        let catalog = ProviderCatalog {
            format_version: 1,
            application_id: app.clone(),
            provider_key: "native-discovery-source".into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![],
        };
        let enrollment = ChannelRoleEnrollment::Provider(ProviderEnrollmentV2 {
            enrollment_version: EnrollmentVersion::V2,
            instance: connection.registration().await.request("native-discovery"),
            provider_key: "native-discovery-pending".into(),
            release: catalog.release.clone(),
            // A metadata-only sibling uses a distinct catalog key; exact
            // declaration ownership rejects two grants for the same digest.
            catalog_digest: "d".repeat(64),
            endpoint: RequestedProviderEndpoint::Zenoh {
                protocol_version:
                    kish_lingshu_foundation_contract::service_transport::ProtocolVersion::V1,
            },
        });
        // Deliberately register without declaring or proving a route.
        let pending = pool
            .role_control(MessageKind::Register, &enrollment)
            .await
            .unwrap();
        let ChannelRoleEnrollmentResponse::Provider(pending) =
            serde_json::from_str(pending.payload.get()).unwrap()
        else {
            panic!("Provider response required")
        };
        let client = reqwest::Client::new();
        let discovery_url = format!("{url}/api/admin/apps/{app}/providers/discovery");
        let get = || {
            client
                .get(&discovery_url)
                .header("x-token", &token)
                .header("x-kish-app-id", &app)
                .timeout(Duration::from_secs(12))
                .send()
        };
        let rows: Vec<ProviderDiscoveryInstance> = admin_result(get().await.unwrap()).await;
        let source = rows
            .iter()
            .find(|i| i.enrollment.provider_key == "native-discovery-pending")
            .unwrap();
        assert_eq!(source.generation, pending.session.generation);
        assert_eq!(source.connectivity, ProviderConnectivity::Unconfirmed);
        assert_eq!(
            source.role_readiness,
            ProviderRoleReadiness::NativeRouteUnconfirmed
        );
        assert_eq!(source.transport, ServiceInstanceTransport::Zenoh);
        assert!(source.enrollment.catalog_url.is_none());
        // Keep the pending sibling until the configured source is registered:
        // removing the last role retires common-base control authority.
        let mut role = pool.register_provider_role(&catalog).await.unwrap();
        pool.role_control(
            MessageKind::Deregister,
            &ChannelRoleDeregistration {
                role_generation: pending.session.generation,
            },
        )
        .await
        .unwrap();
        let rows: Vec<ProviderDiscoveryInstance> = admin_result(get().await.unwrap()).await;
        let ready = rows
            .iter()
            .find(|i| i.enrollment.provider_key == catalog.provider_key)
            .unwrap()
            .clone();
        assert_eq!(ready.connectivity, ProviderConnectivity::RecentlyConfirmed);
        assert_eq!(
            ready.role_readiness,
            ProviderRoleReadiness::NativeReadConfigured
        );
        // Discovery observations use the server's clock, which can be ahead
        // of this client's clock within the existing cross-host skew bound.
        assert!(ready.observed_at_ms > 0);
        assert!(
            ready.observed_at_ms
                <= chrono::Utc::now().timestamp_millis() + MAX_BOOTSTRAP_CLOCK_SKEW_MS
        );
        let wire: serde_json::Value = get()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(wire["status"], true);
        let wire_source = wire["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|i| i["enrollment"]["provider_key"] == catalog.provider_key)
            .unwrap();
        let public_fields = [
            "enrollment",
            "generation",
            "lease_expires_at_ms",
            "transport",
            "connectivity",
            "role_readiness",
            "observed_at_ms",
        ];
        assert!(wire_source
            .as_object()
            .unwrap()
            .keys()
            .all(|k| public_fields.contains(&k.as_str())));
        let source_fields = ["provider_key", "release", "catalog_digest", "instance"];
        assert_eq!(
            wire_source["enrollment"].as_object().unwrap().len(),
            source_fields.len()
        );
        assert!(wire_source["enrollment"]
            .as_object()
            .unwrap()
            .keys()
            .all(|k| source_fields.contains(&k.as_str())));
        assert_eq!(
            wire_source["enrollment"]["instance"],
            serde_json::json!({"instance_id":"native-discovery"})
        );
        assert_eq!(
            wire_source["lease_expires_at_ms"],
            ready.lease_expires_at_ms
        );
        assert_failed(
            client
                .get(&discovery_url)
                .bearer_auth(&key)
                .header("x-kish-app-id", &app)
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_failed(
            client
                .get(format!(
                    "{url}/api/admin/apps/other-app/providers/discovery"
                ))
                .header("x-token", &token)
                .send()
                .await
                .unwrap(),
        )
        .await;
        // HTTP discovery retains its callback semantics without probing it.
        let http: ProviderSession = client
            .post(format!("{url}/api/user/services/v1/provider-enrollments"))
            .bearer_auth(&key)
            .header("x-kish-app-id", &app)
            .json(&ProviderEnrollment {
                instance: ServiceInstanceRegistration {
                    instance_id: "discovery-http".into(),
                    incarnation_id: "http-boot".into(),
                    generation: None,
                },
                provider_key: "discovery-http-source".into(),
                release: "1".into(),
                catalog_digest: "a".repeat(64),
                catalog_url: "https://unreachable.fixture.invalid/catalog".into(),
            })
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let rows: Vec<ProviderDiscoveryInstance> = admin_result(get().await.unwrap()).await;
        let callback = rows
            .iter()
            .find(|i| i.enrollment.provider_key == "discovery-http-source")
            .unwrap();
        assert_eq!(callback.transport, ServiceInstanceTransport::Http);
        assert_eq!(callback.connectivity, ProviderConnectivity::NotObserved);
        assert_eq!(
            callback.role_readiness,
            ProviderRoleReadiness::HttpCallbackConfigured
        );
        assert_eq!(
            callback.enrollment.catalog_url.as_deref(),
            Some("https://unreachable.fixture.invalid/catalog")
        );
        let legacy: Vec<ProviderInstance> = admin_result(
            client
                .get(format!("{url}/api/admin/apps/{app}/providers"))
                .header("x-token", &token)
                .header("x-kish-app-id", &app)
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert!(legacy
            .iter()
            .any(|i| i.enrollment.provider_key == "discovery-http-source"));
        assert!(legacy
            .iter()
            .all(|i| i.enrollment.provider_key != catalog.provider_key));
        // Local declaration closure is not immediately reflected as a socket
        // observation: the finite prior route proof remains until authority TTL.
        role.close().await.unwrap();
        let rows: Vec<ProviderDiscoveryInstance> = admin_result(get().await.unwrap()).await;
        let closed = rows
            .iter()
            .find(|i| i.enrollment.provider_key == catalog.provider_key)
            .unwrap();
        assert_eq!(closed.connectivity, ProviderConnectivity::RecentlyConfirmed);
        assert_eq!(closed.lease_expires_at_ms, ready.lease_expires_at_ms);
        pool.deregister_role(&mut role).await.unwrap();
        let rows: Vec<ProviderDiscoveryInstance> = admin_result(get().await.unwrap()).await;
        assert!(rows
            .iter()
            .all(|i| i.enrollment.provider_key != catalog.provider_key));
        client
            .post(format!(
                "{url}/api/user/services/v1/provider-sessions/deregister"
            ))
            .bearer_auth(&http.credential)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        pool.close().await.unwrap();
        assert_eq!(connection.channel_session_budget().available_permits(), 4);
    }

    #[cfg(feature = "service-manifest")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "isolated native host; run zenss_channel_bootstrap_acceptance.py --role-test"]
    async fn native_catalog_import_preserves_reviewed_snapshot_and_receipts_without_execution() {
        use kish_lingshu_runtime_contract::provider::{ProviderApplyRequest, ProviderReceipt};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
        let token = std::env::var("LINGSHU_CHANNEL_TEST_PREVIEW_TOKEN").unwrap();
        let connection = crate::ServiceConnection::connect(
            &url,
            crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
        let identity = connection
            .bootstrap_test_channel(
                ServiceInstanceRegistration {
                    instance_id: "native-import".into(),
                    incarnation_id: "import-boot".into(),
                    generation: None,
                },
                None,
            )
            .await
            .unwrap();
        let mut pool = identity
            .open_sessions(super::super::ChannelSessionConfig::default())
            .await
            .unwrap();
        let mut manifest: crate::services::ServiceManifest =
            serde_json::from_str(&std::env::var("LINGSHU_CHANNEL_TEST_MANIFEST").unwrap()).unwrap();
        manifest.services[0].service_key = "native-import-fixture".into();
        let mut builder = crate::services::ServiceRegistryBuilder::new(manifest.clone()).unwrap();
        let executions = Arc::new(AtomicUsize::new(0));
        let observed = executions.clone();
        builder
            .bind::<serde_json::Value, serde_json::Value, _, _>(
                "native-import-fixture",
                "echo",
                "1",
                move |_, value| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    async move { Ok(value) }
                },
            )
            .unwrap();
        let registry = builder.build().unwrap();
        let catalog = ProviderCatalog {
            format_version: 1,
            application_id: app.clone(),
            provider_key: "native-import-source".into(),
            release: "1".into(),
            services: Some(manifest),
            events: None,
            workflows: vec![ProviderWorkflow {
                key: "import-workflow".into(),
                name: "Native reviewed import".into(),
                description: None,
                define_schema: serde_json::from_value(serde_json::json!({
                    "type": "ReactFlow",
                    "reactflow": {
                        "nodes": [
                            {"id":"start","type":"startEvent","data":{"name":"Start","type":"startEvent"}},
                            {"id":"end","type":"endEvent","data":{"name":"End","type":"endEvent"}}
                        ],
                        "edges": [{"id":"start-to-end","source":"start","target":"end"}]
                    }
                }))
                .unwrap(),
            }],
        };
        let mut provider = pool.register_provider_role(&catalog).await.unwrap();
        // Catalog route readiness alone does not publish an Operation contract.
        assert!(pool
            .register_service_role("native-import-call", 1, &registry)
            .await
            .is_err());
        let kish_lingshu_runtime_contract::service::ChannelRoleEnrollmentResponse::Provider(
            response,
        ) = provider.registration()
        else {
            unreachable!()
        };
        let request = ProviderPreviewRequest {
            instance_id: response.session.instance.instance_id.clone(),
            generation: response.session.generation.clone(),
            catalog_digest: catalog.digest().unwrap(),
            environment_bindings: BTreeMap::new(),
            workflow_bindings: BTreeMap::new(),
        };
        let base = format!(
            "{url}/api/admin/apps/{app}/providers/{}",
            catalog.provider_key
        );
        let client = reqwest::Client::new();
        let post = |path: &str| {
            client
                .post(format!("{base}/{path}"))
                .header("x-token", &token)
                .header("x-kish-app-id", &app)
                .timeout(Duration::from_secs(15))
        };
        let plan: ProviderPlan =
            admin_result(post("preview").json(&request).send().await.unwrap()).await;
        assert_eq!(plan.changes.len(), 2);
        assert!(plan.changes.iter().all(
            |c| c.change == kish_lingshu_runtime_contract::provider::ProviderChangeKind::Create
        ));
        let imported_id = plan
            .changes
            .iter()
            .find(|c| c.kind == "workflow")
            .unwrap()
            .target_id
            .clone()
            .unwrap();
        // Reading/previewing still cannot activate an unpublished Operation.
        assert!(pool
            .register_service_role("native-import-call", 1, &registry)
            .await
            .is_err());
        let apply = ProviderApplyRequest {
            plan_id: plan.id.clone(),
            publish: false,
            event_retirement: Default::default(),
        };
        let receipt: ProviderReceipt =
            admin_result(post("apply").json(&apply).send().await.unwrap()).await;
        assert!(receipt.complete, "{receipt:?}");
        assert_eq!(
            receipt.items["workflow:import-workflow"]["workflow_id"],
            imported_id
        );
        assert!(receipt.items.contains_key("services"));
        let repeated: ProviderReceipt =
            admin_result(post("apply").json(&apply).send().await.unwrap()).await;
        assert!(repeated.complete);
        assert_eq!(repeated.items, receipt.items);
        // Explicit import publishes Service contracts; Workflow publication is
        // a separate reviewed choice. Registration only installs signed probes.
        let mut call = pool
            .register_service_role("native-import-call", 1, &registry)
            .await
            .unwrap();
        assert!(call.route_confirmed());
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        let publish_plan: ProviderPlan =
            admin_result(post("preview").json(&request).send().await.unwrap()).await;
        assert!(publish_plan
            .changes
            .iter()
            .all(|c| c.change
                == kish_lingshu_runtime_contract::provider::ProviderChangeKind::Unchanged));
        assert_eq!(
            publish_plan
                .changes
                .iter()
                .find(|c| c.kind == "workflow")
                .unwrap()
                .target_id
                .as_ref(),
            Some(&imported_id)
        );
        provider.close().await.unwrap();
        assert_failed(post("preview").json(&request).send().await.unwrap()).await;
        // The reviewed durable snapshot remains usable when its source goes
        // offline; apply never silently rereads a different source release.
        let publish = ProviderApplyRequest {
            plan_id: publish_plan.id.clone(),
            publish: true,
            event_retirement: Default::default(),
        };
        let published: ProviderReceipt =
            admin_result(post("apply").json(&publish).send().await.unwrap()).await;
        assert!(published.complete, "{published:?}");
        assert_eq!(
            published.items["workflow:import-workflow"],
            receipt.items["workflow:import-workflow"]
        );
        let replay: ProviderReceipt =
            admin_result(post("apply").json(&publish).send().await.unwrap()).await;
        assert!(replay.complete);
        assert_eq!(replay.items, published.items);
        let mut wrong_mode = publish;
        wrong_mode.publish = false;
        assert_failed(post("apply").json(&wrong_mode).send().await.unwrap()).await;
        let receipts: Vec<ProviderReceipt> = admin_result(
            client
                .get(format!("{base}/receipts"))
                .header("x-token", &token)
                .header("x-kish-app-id", &app)
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert!(receipts
            .iter()
            .any(|r| r.plan_id == receipt.plan_id && r.complete && r.items == receipt.items));
        assert!(receipts
            .iter()
            .any(|r| r.plan_id == published.plan_id && r.complete && r.items == published.items));
        assert_eq!(executions.load(Ordering::SeqCst), 0);
        pool.deregister_role(&mut call).await.unwrap();
        pool.deregister_role(&mut provider).await.unwrap();
        pool.close().await.unwrap();
        assert_eq!(connection.channel_session_budget().available_permits(), 4);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "isolated native host; run zenss_channel_bootstrap_acceptance.py --role-test"]
    async fn native_catalog_preview_uses_pinned_snapshot_survives_adoption_and_rejects_stale_or_closed_roles(
    ) {
        let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
        let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
        let token = std::env::var("LINGSHU_CHANNEL_TEST_PREVIEW_TOKEN").unwrap();
        let connection = crate::ServiceConnection::connect(
            &url,
            crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
                .unwrap(),
        )
        .await
        .unwrap();
        let identity = connection
            .bootstrap_test_channel(
                ServiceInstanceRegistration {
                    instance_id: "native-catalog".into(),
                    incarnation_id: "catalog-boot".into(),
                    generation: None,
                },
                None,
            )
            .await
            .unwrap();
        let mut pool = identity
            .open_sessions(super::super::ChannelSessionConfig::default())
            .await
            .unwrap();
        let catalog = ProviderCatalog {
            format_version: 1,
            application_id: app.clone(),
            provider_key: "native-catalog-source".into(),
            release: "1".into(),
            services: None,
            events: None,
            workflows: vec![ProviderWorkflow {
                key: "catalog-workflow".into(),
                name: "Native immutable snapshot".into(),
                description: None,
                define_schema: BTreeMap::from([
                    ("type".into(), serde_json::json!("ReactFlow")),
                    ("release".into(), serde_json::json!("1")),
                ]),
            }],
        };
        let digest = catalog.digest().unwrap();
        let occupied = connection
            .catalog_budgets()
            .0
            .try_acquire_many_owned(8 * 1024 * 1024)
            .unwrap();
        assert!(matches!(
            pool.register_provider_role(&catalog).await,
            Err(ChannelSessionError::CapacityExceeded)
        ));
        drop(occupied);
        let mut role = pool.register_provider_role(&catalog).await.unwrap();
        let kish_lingshu_runtime_contract::service::ChannelRoleEnrollmentResponse::Provider(
            response,
        ) = role.registration()
        else {
            unreachable!()
        };
        let request = ProviderPreviewRequest {
            instance_id: response.session.instance.instance_id.clone(),
            generation: response.session.generation.clone(),
            catalog_digest: digest.clone(),
            environment_bindings: BTreeMap::new(),
            workflow_bindings: BTreeMap::new(),
        };
        let preview_url = format!(
            "{url}/api/admin/apps/{app}/providers/{}/preview",
            catalog.provider_key
        );
        let client = reqwest::Client::new();
        let preview = |request: ProviderPreviewRequest| {
            client
                .post(&preview_url)
                .header("x-token", &token)
                .header("x-kish-app-id", &app)
                .json(&request)
                .timeout(Duration::from_secs(12))
                .send()
        };
        let occupied = connection
            .catalog_budgets()
            .1
            .try_acquire_many_owned(2)
            .unwrap();
        assert_failed(preview(request.clone()).await.unwrap()).await;
        drop(occupied);
        let first = preview(request.clone()).await.unwrap();
        assert!(
            first.status().is_success(),
            "{}",
            first.text().await.unwrap()
        );
        let value: serde_json::Value = first.json().await.unwrap();
        assert_eq!(value["status"], true, "{value:?}");
        let plan: ProviderPlan = serde_json::from_value(value["data"].clone()).unwrap();
        assert_eq!(plan.catalog_digest, digest);
        assert_eq!(plan.release, "1");
        assert_eq!(plan.changes.len(), 1);
        // Read readiness never imports or publishes: another preview still sees Create.
        let value: serde_json::Value = preview(request.clone())
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(value["status"], true, "{value:?}");
        let second: ProviderPlan = serde_json::from_value(value["data"].clone()).unwrap();
        assert_eq!(
            second.changes[0].change,
            kish_lingshu_runtime_contract::provider::ProviderChangeKind::Create
        );
        assert_eq!(second.changes[0].key, plan.changes[0].key);
        assert_eq!(second.changes[0].after, plan.changes[0].after);
        let mut bad = request.clone();
        bad.catalog_digest = "a".repeat(64);
        assert_failed(preview(bad).await.unwrap()).await;
        let mut bad = request.clone();
        bad.generation = "stale".into();
        assert_failed(preview(bad).await.unwrap()).await;
        let unauthorized = client
            .post(&preview_url)
            .bearer_auth(std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
            .header("x-kish-app-id", &app)
            .json(&request)
            .send()
            .await
            .unwrap();
        assert!(!unauthorized.status().is_success());
        let identity = pool.prepare_certificate_rotation().await.unwrap();
        let mut candidate = identity
            .open_sessions(super::super::ChannelSessionConfig::default())
            .await
            .unwrap();
        candidate.adopt_role(&mut role).await.unwrap();
        candidate.finalize_certificate_rotation().await.unwrap();
        pool.close().await.unwrap();
        let value: serde_json::Value = preview(request.clone())
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(value["status"], true, "{value:?}");
        let after: ProviderPlan = serde_json::from_value(value["data"].clone()).unwrap();
        assert_eq!(after.catalog_digest, digest);
        assert_eq!(
            after.changes[0].change,
            kish_lingshu_runtime_contract::provider::ProviderChangeKind::Create
        );
        assert_eq!(after.changes[0].after, plan.changes[0].after);
        // Local close does not counterfeit remote deregistration or an empty catalog.
        role.close().await.unwrap();
        assert_failed(preview(request.clone()).await.unwrap()).await;
        candidate.deregister_role(&mut role).await.unwrap();
        assert_failed(preview(request).await.unwrap()).await;
        candidate.close().await.unwrap();
        assert_eq!(connection.channel_session_budget().available_permits(), 4);
        drop(role);
        assert_eq!(
            connection.catalog_budgets().0.available_permits(),
            8 * 1024 * 1024
        );
        connection.ensure_open().unwrap();
    }
}
