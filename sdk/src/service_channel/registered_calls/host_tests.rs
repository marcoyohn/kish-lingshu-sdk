//! Actual native Host acceptance; no substitute transport or fabricated receipt.
use super::*;
use crate::service_channel::{CapabilityActivationState, ChannelSessionConfig};
use kish_lingshu_foundation_contract::{
    service_transport::RouteIdentity, ServiceInstanceRegistration,
};
use kish_lingshu_runtime_contract::{provider::*, service::*};
use std::{collections::BTreeMap, time::Duration};

async fn admin<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> T {
    let body: serde_json::Value = response.error_for_status().unwrap().json().await.unwrap();
    assert_eq!(body["status"], true, "{body:?}");
    serde_json::from_value(body["data"].clone()).unwrap()
}
async fn wait_state(status: &mut watch::Receiver<Vec<Status>>, state: CapabilityActivationState) {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if status.borrow_and_update().iter().all(|s| s.state == state) {
                return;
            }
            status.changed().await.unwrap();
        }
    })
    .await
    .unwrap_or_else(|_| panic!("activation did not reach {state:?}: {:?}", *status.borrow()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run zenss_channel_bootstrap_acceptance.py --registration-test with matched native artifacts"]
async fn native_call_registers_before_import_activates_and_removes_without_provider_loss() {
    call_registration_scenario(false, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "actual matched native connection authority"]
async fn native_connected_call_stays_ready_without_renewal_and_observes_governance() {
    call_registration_scenario(true, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "actual catalog-only Provider import against matched Host"]
async fn native_connected_catalog_only_does_not_activate_handlers_after_import() {
    call_registration_scenario(true, true).await;
}
async fn call_registration_scenario(connected: bool, catalog_only: bool) {
    let name = if catalog_only {
        "catalog-only-native-call"
    } else {
        "pending-native-call"
    };
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
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
                instance_id: name.into(),
                incarnation_id: "pending-call-boot".into(),
                generation: None,
            },
            Some(RouteIdentity::new("dev").unwrap()),
        )
        .await
        .unwrap();
    let config = ChannelSessionConfig::new(1)
        .unwrap()
        .with_control_lane()
        .unwrap();
    let mut pool = identity.open_sessions(config).await.unwrap();
    if connected {
        pool.activate_connection_authority(RouteIdentity::new("connected-call-binding").unwrap())
            .await
            .unwrap();
    }
    let mut manifest: ServiceManifest =
        serde_json::from_str(&std::env::var("LINGSHU_CHANNEL_TEST_MANIFEST").unwrap()).unwrap();
    manifest.services[0].service_key = name.into();
    let mut builder = crate::services::ServiceRegistryBuilder::new(manifest.clone()).unwrap();
    builder
        .bind::<serde_json::Value, serde_json::Value, _, _>(
            name,
            "echo",
            "1",
            |_, value| async move { Ok(value) },
        )
        .unwrap();
    let registry = Arc::new(builder.build().unwrap());
    let catalog = ProviderCatalog {
        format_version: 1,
        application_id: app.clone(),
        provider_key: name.into(),
        release: "1".into(),
        services: Some(manifest),
        events: None,
        workflows: vec![],
    };
    let provider = pool.register_provider_role(&catalog).await.unwrap();
    let ChannelRoleEnrollmentResponse::Provider(enrolled) = provider.registration() else {
        unreachable!()
    };
    let provider_session = enrolled.session.clone();
    let mut channel = pool.manage_roles(vec![provider]).unwrap();
    let plan = RegisteredCallPlan::new(
        "pending-node",
        registry,
        crate::ServiceExecutionBudget::new(1).unwrap(),
        false,
        1,
    )
    .unwrap();
    let mut status = plan.subscribe();
    let updates = plan.updates();
    {
        let run = async {
            if catalog_only {
                std::future::pending::<Result<(), ChannelSessionError>>().await
            } else {
                plan.run(&channel).await
            }
        };
        tokio::pin!(run);
        let scenario = async {
            if !catalog_only {
                wait_state(&mut status, CapabilityActivationState::WaitingForCatalog).await;
            }
            assert_eq!(
                channel.role_statuses().len(),
                1,
                "pending capability must not enroll an executable role"
            );
            assert!(channel.role_statuses()[0].route_confirmed());
            let client = reqwest::Client::new();
            let base = format!(
                "{url}/api/admin/apps/{app}/providers/{}",
                catalog.provider_key
            );
            let post = |action: &str| {
                client
                    .post(format!("{base}/{action}"))
                    .header(
                        "x-token",
                        std::env::var("LINGSHU_CHANNEL_TEST_PREVIEW_TOKEN").unwrap(),
                    )
                    .header("x-kish-app-id", &app)
                    .timeout(Duration::from_secs(15))
            };
            let preview: ProviderPlan = admin(
                post("preview")
                    .json(&ProviderPreviewRequest {
                        instance_id: provider_session.instance.instance_id,
                        generation: provider_session.generation,
                        catalog_digest: catalog.digest().unwrap(),
                        environment_bindings: BTreeMap::new(),
                        workflow_bindings: BTreeMap::new(),
                    })
                    .send()
                    .await
                    .unwrap(),
            )
            .await;
            let receipt: ProviderReceipt = admin(
                post("apply")
                    .json(&ProviderApplyRequest {
                        plan_id: preview.id,
                        publish: true,
                        event_retirement: Default::default(),
                    })
                    .send()
                    .await
                    .unwrap(),
            )
            .await;
            assert!(receipt.complete, "{receipt:?}");
            if catalog_only {
                tokio::time::sleep(Duration::from_secs(2)).await;
                assert_eq!(channel.role_statuses().len(), 1);
                assert!(channel.role_statuses()[0].route_confirmed());
                assert!(status
                    .borrow()
                    .iter()
                    .all(|s| s.state == CapabilityActivationState::Pending));
                return;
            }
            wait_state(&mut status, CapabilityActivationState::Active).await;
            assert_eq!(
                channel
                    .role_statuses()
                    .iter()
                    .filter(|r| r.route_confirmed())
                    .count(),
                2
            );
            let first_generation = status.borrow()[0].role_generation.clone();
            // The connected case crosses the former lease deadline with zero
            // supervisor observations. The finite case exercises compatibility.
            tokio::time::sleep(Duration::from_secs(if connected { 35 } else { 12 })).await;
            if connected {
                assert!(channel
                    .role_statuses()
                    .iter()
                    .all(|role| role.route_confirmed() && role.last_renewal_status.is_none()));
                assert!(matches!(
                    channel.status(),
                    super::super::super::ChannelSupervisorStatus::Active {
                        observations: 0,
                        ..
                    }
                ));
            }
            assert!(status.borrow()[0].ready());
            assert_eq!(status.borrow()[0].role_generation, first_generation);
            let old_role = channel
                .role_statuses()
                .into_iter()
                .find(|r| r.call_registration.is_some())
                .unwrap();
            let operation = catalog.services.as_ref().unwrap().services[0].operations[0]
                .reference(name)
                .unwrap();
            let policy_url = format!("{url}/api/admin/apps/{app}/services/call-policies");
            let token = std::env::var("LINGSHU_CHANNEL_TEST_PREVIEW_TOKEN").unwrap();
            let policies: serde_json::Value = admin(
                client
                    .get(&policy_url)
                    .header("x-token", &token)
                    .header("x-kish-app-id", &app)
                    .send()
                    .await
                    .unwrap(),
            )
            .await;
            let disabled: serde_json::Value = admin(
                client
                    .put(&policy_url)
                    .header("x-token", &token)
                    .header("x-kish-app-id", &app)
                    .json(&UpdateServiceCallPolicy {
                        expected_revision: policies["revision"].as_i64().unwrap(),
                        policy: ServiceCallPolicy {
                            operation: operation.clone(),
                            enabled: false,
                            maximum_concurrency: 1,
                        },
                    })
                    .send()
                    .await
                    .unwrap(),
            )
            .await;
            wait_state(&mut status, CapabilityActivationState::WaitingForCatalog).await;
            assert!(status.borrow()[0].role_generation.is_none());
            let stale = channel
                .call_registration_control(Control {
                    call_registration: Protocol::V1,
                    command: Command::Acknowledge {
                        version: old_role.call_registration.unwrap(),
                        role_generation: old_role.role_generation,
                    },
                })
                .await
                .unwrap();
            assert!(
                matches!(stale, Response::Rejected { .. }),
                "a delayed ACK must not undo governance disable"
            );
            let _: serde_json::Value = admin(
                client
                    .put(&policy_url)
                    .header("x-token", &token)
                    .header("x-kish-app-id", &app)
                    .json(&UpdateServiceCallPolicy {
                        expected_revision: disabled["revision"].as_i64().unwrap(),
                        policy: ServiceCallPolicy {
                            operation,
                            enabled: true,
                            maximum_concurrency: 1,
                        },
                    })
                    .send()
                    .await
                    .unwrap(),
            )
            .await;
            wait_state(&mut status, CapabilityActivationState::Active).await;
            assert_ne!(status.borrow()[0].role_generation, first_generation);
            let empty = Arc::new(
                crate::services::ServiceRegistryBuilder::empty(&app)
                    .unwrap()
                    .build()
                    .unwrap(),
            );
            updates
                .replace(
                    RegisteredCallPlan::new(
                        "pending-node",
                        empty,
                        crate::ServiceExecutionBudget::new(1).unwrap(),
                        false,
                        1,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(15), async {
                while !status.borrow_and_update().is_empty() {
                    status.changed().await.unwrap();
                }
            })
            .await
            .unwrap();
            assert_eq!(
                channel
                    .role_statuses()
                    .iter()
                    .filter(|r| r.route_confirmed())
                    .count(),
                1
            );
            eprintln!("PASS actual native pending Call -> committed Provider import -> ACK activation -> finite refresh -> disable/stale ACK rejection/re-enable -> exact removal; Provider retained");
        };
        tokio::select! { result = &mut run => panic!("plan ended: {result:?}"), _ = scenario => {} }
    }
    channel.close().await.unwrap();
    connection.shutdown().await;
}
