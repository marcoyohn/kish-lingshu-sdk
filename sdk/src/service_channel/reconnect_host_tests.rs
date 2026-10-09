//! Real TLS passthrough faults. Native Zenoh alone reconnects the same Sessions.
use super::*;
use std::time::Duration;
use tokio::time::Instant;

async fn control(path: &str) {
    reqwest::Client::new()
        .post(format!(
            "{}{path}",
            std::env::var("LINGSHU_GATEWAY_CONTROL_URL").unwrap()
        ))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}
pub(super) async fn observe(pool: &ServiceChannelSessions, connected: bool) {
    let mut observations = pool.subscribe_connectivity();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let current = observations.borrow_and_update().clone();
            assert!(!current.closed);
            if current.connected_data_lanes.iter().all(|v| *v == connected)
                && current.control_connected.is_none_or(|v| v == connected)
            {
                break;
            }
            observations.changed().await.unwrap();
        }
    })
    .await
    .expect("finite physical failover observation");
}
pub(super) async fn interrupt_and_reprove(
    pool: &ServiceChannelSessions,
    roles: &mut [&mut RegisteredChannelRole],
) {
    let original_ids = pool.session_ids();
    let endpoints = roles
        .iter()
        .map(|r| r.endpoint().clone())
        .collect::<Vec<_>>();
    let before = pool.subscribe_connectivity().borrow().data_revision;
    control("/all/down").await;
    observe(pool, false).await;
    for role in roles.iter() {
        assert!(!role.route_confirmed());
    }
    pool.identity.connection.ensure_open().unwrap();
    control("/first/up").await;
    observe(pool, true).await;
    assert!(pool.subscribe_connectivity().borrow().data_revision > before);
    assert_eq!(pool.session_ids(), original_ids);
    for (role, endpoint) in roles.iter_mut().zip(endpoints) {
        assert!(
            !role.route_confirmed(),
            "a new physical link must not restore readiness"
        );
        assert_eq!(role.endpoint(), &endpoint);
        pool.confirm_role_route(role).await.unwrap();
        assert!(role.route_confirmed());
        assert_eq!(role.endpoint(), &endpoint);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real controlled TLS entrypoints; run acceptance --reconnect-test"]
async fn native_managed_reconnect_reproves_same_role_then_expiry_stops_all_retries() {
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let connection = crate::ServiceConnection::connect(
        &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
        crate::ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap())
            .unwrap(),
    )
    .await
    .unwrap();
    let identity = connection
        .bootstrap_channel(
            kish_lingshu_foundation_contract::ServiceInstanceRegistration {
                instance_id: "native-reconnect-managed".into(),
                incarnation_id: "reconnect-boot".into(),
                generation: None,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(identity.bootstrap_response().endpoints.len(), 2);
    let base = identity.bootstrap_response().instance.clone();
    let pool = identity
        .open_sessions(ChannelSessionConfig::host_test())
        .await
        .unwrap();
    let ids = pool.session_ids();
    let catalog = kish_lingshu_runtime_contract::provider::ProviderCatalog {
        format_version: 1,
        application_id: app,
        provider_key: "reconnect-source".into(),
        release: "1".into(),
        services: None,
        events: None,
        workflows: vec![],
    };
    let role = pool.register_provider_role(&catalog).await.unwrap();
    let generation = role
        .lifecycle_status(pool.authorization_deadline())
        .role_generation;
    let mut managed = pool.manage_roles(vec![role]).unwrap();
    let mut connectivity = managed.subscribe_connectivity();
    let mut roles = managed.subscribe_roles();
    control("/all/down").await;
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if managed
                .connectivity_status()
                .connected_data_lanes
                .iter()
                .all(|v| !*v)
                && managed.role_statuses().iter().all(|s| !s.route_confirmed())
            {
                break;
            }
            tokio::select! { _ = connectivity.changed() => {}, _ = roles.changed() => {} }
        }
    })
    .await
    .unwrap();
    control("/first/up").await;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if managed
                .connectivity_status()
                .connected_data_lanes
                .iter()
                .all(|v| *v)
                && managed.role_statuses().iter().all(|s| s.route_confirmed())
            {
                break;
            }
            tokio::select! { _ = connectivity.changed() => {}, _ = roles.changed() => {} }
        }
    })
    .await
    .unwrap();
    assert_eq!(managed.rotation_status().session_ids, ids);
    assert_eq!(managed.role_statuses()[0].role_generation, generation);
    assert!(managed.role_statuses()[0].last_renewal_status == Some(200));
    control("/all/down").await;
    let deadline = managed.role_statuses()[0].authorization_deadline;
    let mut status = managed.subscribe_status();
    tokio::time::timeout_at(deadline + Duration::from_secs(14), async {
        loop {
            if matches!(
                *status.borrow_and_update(),
                ChannelSupervisorStatus::Closed {
                    reason: ChannelCloseReason::AuthorityExpired
                }
            ) {
                break;
            }
            status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    managed.close().await.unwrap();
    assert!(Instant::now() >= deadline);
    assert!(managed.connectivity_status().closed);
    assert_eq!(connection.channel_session_budget().available_permits(), 4);
    assert_eq!(managed.role_statuses()[0].role_generation, generation);
    assert!(!managed.role_statuses()[0].remote_deregistered);
    assert_eq!(
        connection
            .registration()
            .await
            .request(&base.instance_id)
            .generation
            .as_deref(),
        Some(base.generation.as_str())
    );
    connection.ensure_open().unwrap();
    control("/second/up").await;
    connection.shutdown().await;
}
