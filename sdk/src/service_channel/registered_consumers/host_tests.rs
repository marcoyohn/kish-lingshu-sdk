//! Pending registration exercised through the real native Host and Dispatch.
use super::*;
use crate::event_dispatch::{ConsumerError, ConsumerSelector, EventConsumer, EventContext};
use kish_lingshu_foundation_contract::{
    service_transport::RouteIdentity, ServiceInstanceRegistration,
};
use kish_lingshu_runtime_contract::{provider::*, service::ChannelRoleEnrollmentResponse};
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

#[derive(
    serde::Serialize, serde::Deserialize, schemars::JsonSchema, crate::event_dispatch::EventPayload,
)]
#[event(
    key = "orders.created",
    topic = "orders",
    event_type = "created",
    schema_version = "1",
    topic_name = "Orders"
)]
struct Order {
    sequence: u64,
}
struct Handler(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl EventConsumer for Handler {
    type Event = Order;
    type Output = ();
    fn selector(&self) -> ConsumerSelector {
        ConsumerSelector::new("orders", "created")
            .unwrap()
            .with_consumer_group("order-workers")
            .unwrap()
    }
    async fn consume(&self, _: EventContext, order: Order) -> Result<(), ConsumerError> {
        assert_eq!(order.sequence, 1);
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
async fn admin<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> T {
    let body: serde_json::Value = response.error_for_status().unwrap().json().await.unwrap();
    assert_eq!(body["status"], true, "{body:?}");
    serde_json::from_value(body["data"].clone()).unwrap()
}
async fn wait_state(status: &mut watch::Receiver<Vec<Status>>, state: State) {
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
async fn native_pending_consumer_activates_after_import_and_delivers_to_bound_handler() {
    consumer_registration_scenario(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "actual matched native connection authority"]
async fn native_connected_consumer_delivers_after_idle_without_renewal() {
    consumer_registration_scenario(true).await;
}
async fn consumer_registration_scenario(connected: bool) {
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    let key = std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap();
    let connection =
        crate::ServiceConnection::connect(&url, crate::ServiceCredential::new(&app, &key).unwrap())
            .await
            .unwrap();
    let identity = connection
        .bootstrap_test_channel(
            ServiceInstanceRegistration {
                instance_id: "pending-native-consumer".into(),
                incarnation_id: "pending-consumer-boot".into(),
                generation: None,
            },
            Some(RouteIdentity::new("dev").unwrap()),
        )
        .await
        .unwrap();
    let mut pool = identity
        .open_sessions(
            super::super::ChannelSessionConfig::new(1)
                .unwrap()
                .with_control_lane()
                .unwrap(),
        )
        .await
        .unwrap();
    let binding = pool
        .attest_connections(RouteIdentity::new("pending-native-consumer-binding").unwrap())
        .await
        .unwrap();
    assert!(binding.complete());
    assert!(binding.dedicated_control);
    assert_ne!(binding.control, *binding.data[0].as_ref().unwrap());
    let repeated = pool
        .attest_connections(binding.binding_id.clone())
        .await
        .unwrap();
    assert_eq!(
        binding, repeated,
        "attestation retry must not extend its deadline"
    );
    if connected {
        pool.activate_connection_authority(binding.binding_id.clone())
            .await
            .unwrap();
    }
    let declaration = super::tests::declaration();
    let catalog = ProviderCatalog {
        format_version: 1,
        application_id: app.clone(),
        provider_key: declaration.provider_key.clone(),
        release: "1".into(),
        services: None,
        workflows: vec![],
        events: Some(
            kish_lingshu_event_dispatch_contract::EventDispatchManifestV1::new(
                declaration.producer,
                vec![],
                declaration.events,
                vec![declaration.consumer],
                vec![],
            )
            .unwrap(),
        ),
    };
    let provider = pool.register_provider_role(&catalog).await.unwrap();
    let ChannelRoleEnrollmentResponse::Provider(enrolled) = provider.registration() else {
        unreachable!()
    };
    let provider_session = enrolled.session.clone();
    let mut channel = pool.manage_roles(vec![provider]).unwrap();
    let executions = Arc::new(AtomicUsize::new(0));
    let mut registry = crate::event_dispatch::ConsumerRegistry::new(&app).unwrap();
    registry.register(Handler(executions.clone())).unwrap();
    let plan = RegisteredConsumerPlan::new(
        &catalog,
        "pending-consumer",
        Arc::new(registry),
        crate::ServiceExecutionBudget::new(8).unwrap(),
    )
    .unwrap();
    let mut status = plan.subscribe();
    {
        let run = plan.run(&channel);
        tokio::pin!(run);
        let scenario = async {
            wait_state(&mut status, State::WaitingForCatalog).await;
            assert_eq!(channel.role_statuses().len(), 1);
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
                        publish: false,
                        event_retirement: Default::default(),
                    })
                    .send()
                    .await
                    .unwrap(),
            )
            .await;
            assert!(receipt.complete, "{receipt:?}");
            wait_state(&mut status, State::Active).await;
            if connected {
                tokio::time::sleep(Duration::from_secs(35)).await;
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
            }

            assert_eq!(executions.load(Ordering::SeqCst), 0);
            let client =
                crate::ClientBuilder::new(crate::ClientConfig::new(&url).with_retry_limit(0))
                    .service_credential(crate::ServiceCredential::new(&app, &key).unwrap())
                    .connect()
                    .unwrap();
            let mut event = crate::event_dispatch::PublishEvent::typed(
                "pending-registration",
                &Order { sequence: 1 },
            )
            .unwrap();
            event.partition_key = Some("order-one".into());
            client
                .event_dispatch()
                .with_managed_channel(&channel)
                .unwrap()
                .publish(
                    event,
                    crate::MutationOptions::new("pending-registration/order-one").unwrap(),
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(15), async {
                while executions.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(executions.load(Ordering::SeqCst), 1);
            assert!(channel.role_statuses()[0].route_confirmed());
            if let Some(markers) = std::env::var_os("LINGSHU_CONNECTED_PEER_PARTITION_MARKERS") {
                let markers = std::path::PathBuf::from(markers);
                let marker = |name: &'static str| {
                    let markers = &markers;
                    async move {
                        tokio::time::timeout(Duration::from_secs(30), async {
                            while !markers.join(name).exists() {
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                        })
                        .await
                        .unwrap();
                    }
                };
                std::fs::write(markers.join("ready"), "original delivery committed").unwrap();
                marker("blocked").await;
                let mut event = crate::event_dispatch::PublishEvent::typed(
                    "partition-registration",
                    &Order { sequence: 1 },
                )
                .unwrap();
                event.partition_key = Some("order-one".into());
                client
                    .event_dispatch()
                    .with_managed_channel(&channel)
                    .unwrap()
                    .publish(
                        event,
                        crate::MutationOptions::new("partition-registration/order-one").unwrap(),
                    )
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_secs(2)).await;
                assert_eq!(
                    executions.load(Ordering::SeqCst),
                    1,
                    "partitioned peer must not borrow cached admission"
                );
                assert!(channel
                    .role_statuses()
                    .iter()
                    .all(|role| role.route_confirmed()));
                std::fs::write(
                    markers.join("blocked-checked"),
                    "durable second event has not invoked handler",
                )
                .unwrap();
                marker("resumed").await;
                tokio::time::timeout(Duration::from_secs(30), async {
                    while executions.load(Ordering::SeqCst) != 2 {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .unwrap();
                assert!(channel
                    .role_statuses()
                    .iter()
                    .all(|role| role.last_renewal_status.is_none()));
                let original_role = status.borrow()[0].role_generation.clone();
                std::fs::write(
                    markers.join("restart-ready"),
                    "same Consumer resumed after peer reconnect",
                )
                .unwrap();
                marker("restarted").await;
                let mut event = crate::event_dispatch::PublishEvent::typed(
                    "restart-registration",
                    &Order { sequence: 1 },
                )
                .unwrap();
                event.partition_key = Some("order-one".into());
                client
                    .event_dispatch()
                    .with_managed_channel(&channel)
                    .unwrap()
                    .publish(
                        event,
                        crate::MutationOptions::new("restart-registration/order-one").unwrap(),
                    )
                    .await
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(30), async {
                    while executions.load(Ordering::SeqCst) != 3 {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .unwrap();
                assert_eq!(status.borrow()[0].role_generation, original_role);
                eprintln!("PASS peer partition fenced remote admission; after reconnect Dispatch delivered the same persisted event to the original connected Consumer");
                eprintln!("PASS restarted Dispatch node reconstructed the registered Consumer directory and delivered without replacing its role generation");
            }
            eprintln!("PASS actual native pending Consumer -> Event-only import without Workflow publish -> activation ACK -> Dispatch delivery to bound Handler");
        };
        tokio::select! { result = &mut run => panic!("plan ended: {result:?}"), _ = scenario => {} }
    }
    channel.close().await.unwrap();
    connection.shutdown().await;

    if connected {
        return;
    }

    // Keep one application-level publisher and the same desired Consumer across
    // an actual loss of this instance's isolated Redis common registration.
    use crate::service_channel::{NativeRuntimeStatus, RegisteredServiceRuntime};
    let connection =
        crate::ServiceConnection::connect(&url, crate::ServiceCredential::new(&app, &key).unwrap())
            .await
            .unwrap();
    let mut registry = crate::event_dispatch::ConsumerRegistry::new(&app).unwrap();
    registry.register(Handler(executions.clone())).unwrap();
    let plan = RegisteredConsumerPlan::new(
        &catalog,
        "logical-consumer",
        Arc::new(registry),
        crate::ServiceExecutionBudget::new(8).unwrap(),
    )
    .unwrap();
    let mut capability = plan.subscribe();
    let mut runtime = RegisteredServiceRuntime::new(
        connection.clone(),
        ServiceInstanceRegistration {
            instance_id: "logical-recovery".into(),
            incarnation_id: "logical-first".into(),
            generation: None,
        },
        Some(RouteIdentity::new("dev").unwrap()),
        catalog.clone(),
        super::super::ChannelSessionConfig::new(1)
            .unwrap()
            .with_control_lane()
            .unwrap(),
    )
    .unwrap()
    .with_consumers(plan)
    .unwrap()
    .start()
    .unwrap();
    wait_state(&mut capability, State::Active).await;
    let NativeRuntimeStatus::Active {
        instance_generation: original_generation,
    } = runtime.status()
    else {
        panic!("{:?}", runtime.status())
    };
    let original_role = capability.borrow()[0].role_generation.clone();
    let original_client = runtime.publication.current.borrow().clone().unwrap();
    let client = crate::ClientBuilder::new(crate::ClientConfig::new(&url).with_retry_limit(0))
        .service_credential(crate::ServiceCredential::new(&app, &key).unwrap())
        .connect()
        .unwrap();
    let publisher = client
        .event_dispatch()
        .with_registered_service(&runtime)
        .unwrap();
    let publish = |id: &'static str| {
        let publisher = &publisher;
        async move {
            let mut event =
                crate::event_dispatch::PublishEvent::typed(id, &Order { sequence: 1 }).unwrap();
            event.partition_key = Some("order-one".into());
            publisher
                .publish(event, crate::MutationOptions::new(id).unwrap())
                .await
                .unwrap();
        }
    };
    publish("logical-before-loss").await;
    tokio::time::timeout(Duration::from_secs(20), async {
        while executions.load(Ordering::SeqCst) != 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let markers = std::path::PathBuf::from(std::env::var("LINGSHU_REGISTRATION_MARKERS").unwrap());
    std::fs::write(
        markers.join("logical-ready"),
        "registration may now be removed",
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if matches!(runtime.status(), NativeRuntimeStatus::Active { instance_generation } if instance_generation != original_generation)
                && capability.borrow()[0].ready() && capability.borrow()[0].role_generation != original_role { break; }
            if let NativeRuntimeStatus::Failed { error } = runtime.status() { panic!("runtime recovery failed: {error:?}"); }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.unwrap_or_else(|_| panic!("logical recovery timed out: {:?}, {:?}", runtime.status(), *capability.borrow()));
    assert_ne!(
        original_client
            .test_bootstrap()
            .certificate
            .certificate_identity,
        runtime
            .publication
            .current
            .borrow()
            .as_ref()
            .unwrap()
            .test_bootstrap()
            .certificate
            .certificate_identity
    );
    publish("logical-after-loss").await;
    tokio::time::timeout(Duration::from_secs(20), async {
        while executions.load(Ordering::SeqCst) != 3 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // The retired publisher cannot follow a new identity or replay old work.
    let mut retired = client.event_dispatch();
    retired.native_publication = Some(super::super::publication::NativePublicationSource {
        app_id: app.clone(),
        current: watch::channel(Some(original_client)).1,
    });
    let mut event =
        crate::event_dispatch::PublishEvent::typed("retired-publisher", &Order { sequence: 1 })
            .unwrap();
    event.partition_key = Some("order-one".into());
    assert!(retired
        .publish(
            event,
            crate::MutationOptions::new("retired-publisher").unwrap()
        )
        .await
        .is_err());
    assert_eq!(executions.load(Ordering::SeqCst), 3);
    runtime.close().await.unwrap();
    assert!(matches!(runtime.status(), NativeRuntimeStatus::Closed));
    assert_eq!(connection.channel_session_budget().available_permits(), 4);
    connection.shutdown().await;
    eprintln!("PASS actual common registration loss -> joined old pool -> new instance and Consumer activation -> existing publisher adopts new channel; retired publisher denied");
}
