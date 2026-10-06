#![cfg(feature = "service-channel")]

#[cfg(feature = "service-zenoh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "isolated dependency outage; run acceptance --dependency-test"]
async fn native_existing_session_rejects_control_when_degraded_and_recovers_without_reenrollment() {
    use kish_lingshu_foundation_contract::ServiceInstanceRegistration;
    use kish_lingshu_sdk::{
        service_channel::ChannelSessionConfig, ServiceConnection, ServiceCredential,
    };
    use std::{path::PathBuf, time::Duration};
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let key = std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap();
    let markers = PathBuf::from(std::env::var("LINGSHU_DEPENDENCY_MARKER_DIR").unwrap());
    let connection = ServiceConnection::connect(&url, ServiceCredential::new(&app, key).unwrap())
        .await
        .unwrap();
    let identity = connection
        .bootstrap_channel(
            ServiceInstanceRegistration {
                instance_id: "dependency-outage".into(),
                incarnation_id: "same-boot".into(),
                generation: None,
            },
            None,
        )
        .await
        .unwrap();
    let instance = identity.bootstrap_response().instance.clone();
    let mut pool = identity
        .open_sessions(ChannelSessionConfig::default())
        .await
        .unwrap();
    pool.refresh_authorization().await.unwrap();
    let ids = pool.session_ids();
    std::fs::write(markers.join("connected"), "original session").unwrap();
    for (marker, available) in [("degraded", false), ("recovered", true)] {
        tokio::time::timeout(Duration::from_secs(12), async {
            while !markers.join(marker).exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            pool.refresh_authorization().await.is_ok(),
            available,
            "{marker}"
        );
        assert_eq!(pool.session_ids(), ids);
        assert_eq!(pool.identity().bootstrap_response().instance, instance);
        assert_eq!(pool.connected_lanes().await, 1);
        std::fs::write(
            markers.join(format!("{marker}-checked")),
            "same session and identity",
        )
        .unwrap();
    }
    pool.close().await.unwrap();
    connection.shutdown().await;
}

/// Run against the isolated native product fixture; never a deployed service.
#[tokio::test]
#[ignore = "requires isolated native host; run zenss_channel_bootstrap_acceptance.py --sdk-test"]
async fn native_bootstrap_retains_the_local_key_and_server_assigned_base() {
    use kish_lingshu_foundation_contract::{
        service_transport::RouteIdentity, ServiceInstanceRegistration,
    };
    use kish_lingshu_sdk::{ServiceConnection, ServiceCredential};
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let key = std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap();
    let connection = ServiceConnection::connect(&url, ServiceCredential::new(&app, key).unwrap())
        .await
        .unwrap();
    let request = ServiceInstanceRegistration {
        instance_id: "sdk-client".into(),
        incarnation_id: "boot-client".into(),
        generation: None,
    };
    let identity = connection
        .bootstrap_channel(request.clone(), Some(RouteIdentity::new("dev").unwrap()))
        .await
        .unwrap();
    let response = identity.bootstrap_response();
    assert_eq!(response.application_id.as_str(), app);
    assert_eq!(response.instance.instance_id, request.instance_id);
    let public = identity
        .message_signer()
        .public_key()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    assert_eq!(public, response.certificate.message_public_key);
    let mut request = request;
    request.generation = Some(response.instance.generation.clone());
    let renewed = connection
        .bootstrap_channel(request, Some(RouteIdentity::new("dev").unwrap()))
        .await
        .unwrap();
    assert_eq!(renewed.bootstrap_response().instance, response.instance);
    assert_ne!(
        renewed
            .bootstrap_response()
            .certificate
            .certificate_identity,
        response.certificate.certificate_identity
    );
    #[cfg(feature = "service-zenoh")]
    {
        use kish_lingshu_sdk::service_channel::{ChannelSessionConfig, ChannelSessionError};
        // This test intentionally uses current-thread: the public method must
        // reject it before stock Zenoh can panic or create a network resource.
        assert!(matches!(
            renewed.open_sessions(ChannelSessionConfig::default()).await,
            Err(ChannelSessionError::UnsupportedRuntime)
        ));
        assert!(connection.subscribe_closed().borrow().is_none());
    }
    connection.shutdown().await;
}

#[cfg(feature = "service-zenoh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated native host; run zenss_channel_bootstrap_acceptance.py --sdk-test"]
async fn native_sessions_are_independent_and_close_on_authority_or_connection_end() {
    use kish_lingshu_foundation_contract::{
        service_transport::RouteIdentity, ServiceInstanceRegistration,
    };
    use kish_lingshu_sdk::{
        service_channel::{ChannelCloseReason, ChannelSessionConfig, ChannelSessionError},
        ServiceConnection, ServiceCredential,
    };
    use std::{collections::HashSet, time::Duration};
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let key = std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap();
    let connection =
        ServiceConnection::connect(&url, ServiceCredential::new(&app, key.clone()).unwrap())
            .await
            .unwrap();
    for count in [1, 2, 4] {
        let identity = connection
            .bootstrap_channel(
                ServiceInstanceRegistration {
                    instance_id: "pool-shared".into(),
                    incarnation_id: "boot-pool".into(),
                    generation: None,
                },
                Some(RouteIdentity::new("dev").unwrap()),
            )
            .await
            .unwrap();
        let mut pool = identity
            .open_sessions(ChannelSessionConfig::new(count).unwrap())
            .await
            .unwrap();
        assert_eq!(pool.lane_count(), count);
        assert_eq!(
            pool.session_ids().into_iter().collect::<HashSet<_>>().len(),
            count
        );
        assert!(!format!("{pool:?}").contains("PRIVATE KEY"));
        assert!(!format!("{:?}", pool.identity()).contains("PRIVATE KEY"));
        tokio::time::timeout(Duration::from_secs(5), async {
            while pool.connected_lanes().await != count {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        pool.close().await.unwrap();
        pool.close().await.unwrap();
        assert_eq!(
            *pool.subscribe_closed().borrow(),
            Some(ChannelCloseReason::Explicit)
        );
        assert_eq!(pool.connected_lanes().await, 0);
    }
    let identity = connection
        .bootstrap_channel(
            ServiceInstanceRegistration {
                instance_id: "pool-shared".into(),
                incarnation_id: "boot-pool".into(),
                generation: None,
            },
            None,
        )
        .await
        .unwrap();
    let unopened = connection
        .bootstrap_channel(
            ServiceInstanceRegistration {
                instance_id: "pool-shared".into(),
                incarnation_id: "boot-pool".into(),
                generation: None,
            },
            None,
        )
        .await
        .unwrap();
    let mut pool = identity
        .open_sessions(ChannelSessionConfig::new(2).unwrap())
        .await
        .unwrap();
    let extra = connection
        .bootstrap_channel(
            ServiceInstanceRegistration {
                instance_id: "pool-shared".into(),
                incarnation_id: "boot-pool".into(),
                generation: None,
            },
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        extra
            .open_sessions(ChannelSessionConfig::new(3).unwrap())
            .await,
        Err(ChannelSessionError::CapacityExceeded)
    ));
    let extra = connection
        .bootstrap_channel(
            ServiceInstanceRegistration {
                instance_id: "pool-shared".into(),
                incarnation_id: "boot-pool".into(),
                generation: None,
            },
            None,
        )
        .await
        .unwrap();
    let mut second_pool = extra
        .open_sessions(ChannelSessionConfig::new(2).unwrap())
        .await
        .unwrap();
    assert_eq!(
        pool.connected_lanes().await + second_pool.connected_lanes().await,
        4
    );
    let mut closed = pool.subscribe_closed();
    connection.shutdown().await;
    assert!(matches!(
        unopened
            .open_sessions(ChannelSessionConfig::default())
            .await,
        Err(ChannelSessionError::Closed)
    ));
    tokio::time::timeout(Duration::from_secs(5), async {
        while closed.borrow().is_none() {
            closed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(*closed.borrow(), Some(ChannelCloseReason::ConnectionClosed));
    pool.close().await.unwrap();
    second_pool.close().await.unwrap();
    assert_eq!(pool.connected_lanes().await, 0);

    let connection = ServiceConnection::connect(&url, ServiceCredential::new(&app, key).unwrap())
        .await
        .unwrap();
    let identity = connection
        .bootstrap_channel(
            ServiceInstanceRegistration {
                instance_id: "expiry-pool".into(),
                incarnation_id: "boot-expiry".into(),
                generation: None,
            },
            None,
        )
        .await
        .unwrap();
    // Physical authority starts before the HTTPS round trip and can end before
    // the common base lease. Observe the full thirty-second lease after receipt
    // before asserting stale-generation rejection; do not equate those clocks.
    let base_lease_end = tokio::time::Instant::now() + Duration::from_millis(30_250);
    let mut pool = identity
        .open_sessions(ChannelSessionConfig::default())
        .await
        .unwrap();
    let mut closed = pool.subscribe_closed();
    tokio::time::timeout(Duration::from_secs(32), async {
        while closed.borrow().is_none() {
            closed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(*closed.borrow(), Some(ChannelCloseReason::AuthorityExpired));
    pool.close().await.unwrap();
    assert_eq!(pool.connected_lanes().await, 0);
    // An omitted generation cannot reset the expired shared base. Root authority
    // remains open, but deliberate new enrollment needs a new logical connection.
    assert!(connection.subscribe_closed().borrow().is_none());
    tokio::time::sleep_until(base_lease_end).await;
    let rejected = connection
        .bootstrap_channel(
            ServiceInstanceRegistration {
                instance_id: "expiry-pool".into(),
                incarnation_id: "boot-expiry".into(),
                generation: None,
            },
            None,
        )
        .await;
    assert!(matches!(
        rejected,
        Err(kish_lingshu_sdk::ServiceAuthError::Http(409))
    ));
    connection.shutdown().await;
}

#[cfg(feature = "service-zenoh")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated TLS failure fixture; run zenss_channel_bootstrap_acceptance.py --sdk-test"]
async fn native_untrusted_router_is_rejected_without_closing_logical_authority() {
    use kish_lingshu_foundation_contract::ServiceInstanceRegistration;
    use kish_lingshu_sdk::{
        service_channel::{ChannelSessionConfig, ChannelSessionError},
        ServiceConnection, ServiceCredential,
    };
    let failure = std::env::var("LINGSHU_CHANNEL_TLS_FAILURE").unwrap();
    assert!(matches!(failure.as_str(), "name" | "ca"));
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let key = std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap();
    let connection = ServiceConnection::connect(&url, ServiceCredential::new(&app, key).unwrap())
        .await
        .unwrap();
    let mut request = ServiceInstanceRegistration {
        instance_id: "untrusted-router".into(),
        incarnation_id: "boot-negative".into(),
        generation: None,
    };
    let identity = connection
        .bootstrap_channel(request.clone(), None)
        .await
        .unwrap();
    let base = identity.bootstrap_response().instance.clone();
    assert!(matches!(
        identity
            .open_sessions(ChannelSessionConfig::default())
            .await,
        Err(ChannelSessionError::Transport)
    ));
    assert!(connection.subscribe_closed().borrow().is_none());
    request.generation = Some(base.generation.clone());
    let next = connection.bootstrap_channel(request, None).await.unwrap();
    assert_eq!(next.bootstrap_response().instance, base);
    connection.shutdown().await;
}
