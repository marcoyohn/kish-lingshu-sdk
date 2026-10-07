//! Real TLS sockets and production role workers; fixture authority replaces
//! product SQL/Redis/Host receipt admission. No business retries are run here.
use super::*;
use crate::event_dispatch::{
    ConsumerError, ConsumerRegistry, ConsumerSelector, EventConsumer, EventContext,
};
use crate::service_channel::{call, consumer, sessions, PreparedIdentity, ServiceChannelIdentity};
use crate::services::execution::tests::{fixture, invocation, wait_until};
use crate::services::{
    CallMode, InvocationRole, NativeCallAction, NativeCallResponse, ServiceCancellation,
    ServiceInstanceTarget,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use kish_lingshu_event_dispatch_contract::NativeConsumerResponse;
use kish_lingshu_foundation_contract::{
    service_auth::ServiceSigner,
    service_transport::{DataLaneId, LaneIdentity},
};
use kish_lingshu_runtime_contract::provider::{
    ProviderCatalog, ProviderCatalogRead, ProviderWorkflow, MAX_PROVIDER_CATALOG_BYTES,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, PKCS_ED25519,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashSet},
    sync::atomic::AtomicUsize,
};
use tokio::sync::Semaphore;

struct SlowConsumer {
    calls: Arc<AtomicUsize>,
    hold: Arc<Semaphore>,
}
#[async_trait::async_trait]
impl EventConsumer for SlowConsumer {
    type Event = Value;
    type Output = Value;
    fn selector(&self) -> ConsumerSelector {
        ConsumerSelector::new("consumer-security", "order.created")
            .unwrap()
            .with_consumer_group("chosen")
            .unwrap()
    }
    async fn consume(&self, context: EventContext, _: Value) -> Result<Value, ConsumerError> {
        assert_eq!(context.idempotency_key(), "original-business-key");
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.hold.acquire().await.unwrap().forget();
        Ok(json!({"applied":true}))
    }
}

fn lanes(count: usize, generation: &str) -> ServiceEndpoint {
    let mut ep = call::tests::endpoint();
    let ServiceEndpoint::Zenoh { lanes, route, .. } = &mut ep else {
        unreachable!()
    };
    route.role_generation = RouteIdentity::new(generation).unwrap();
    *lanes = (0..count)
        .map(|index| LaneIdentity {
            lane: DataLaneId::new(index as u8).unwrap(),
            epoch: RouteIdentity::new(format!("epoch-{index}")).unwrap(),
        })
        .collect();
    ep
}
fn selected(ep: &ServiceEndpoint, lane: usize) -> ServiceEndpoint {
    let mut ep = ep.clone();
    let ServiceEndpoint::Zenoh { lanes, .. } = &mut ep else {
        unreachable!()
    };
    lanes.rotate_left(lane);
    ep
}
async fn wire(router: &zenoh::Session, request: &TransportEnvelope) -> Vec<u8> {
    let replies = router
        .get(request.target.as_str().to_owned())
        .target(zenoh::query::QueryTarget::All)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .timeout(Duration::from_secs(9))
        .payload(
            request
                .encode(chrono::Utc::now().timestamp_millis())
                .unwrap(),
        )
        .await
        .unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(4), replies.recv_async())
        .await
        .unwrap()
        .unwrap();
    let result = reply.result();
    let sample = result.as_ref().unwrap();
    assert_eq!(sample.key_expr().as_str(), request.target.as_str());
    let bytes = sample.payload().to_bytes().into_owned();
    assert!(
        replies.recv_async().await.is_err(),
        "exact route must have one responder"
    );
    bytes
}
async fn wire_no_reply(router: &zenoh::Session, request: &TransportEnvelope) {
    let replies = router
        .get(request.target.as_str().to_owned())
        .target(zenoh::query::QueryTarget::All)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .timeout(Duration::from_millis(500))
        .payload(
            request
                .encode(chrono::Utc::now().timestamp_millis())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), replies.recv_async())
            .await
            .unwrap()
            .is_err(),
        "ingress exhaustion must never create a success or custody reply"
    );
}

async fn install(
    pool: &ServiceChannelSessions,
    endpoint: &ServiceEndpoint,
    catalog: Option<Arc<super::super::catalog::CatalogSnapshot>>,
) -> RoleDeclarations {
    let ServiceEndpoint::Zenoh { route, .. } = endpoint else {
        unreachable!()
    };
    let declarations = pool
        .install_role_declarations(
            endpoint,
            route.role_generation.as_str(),
            chrono::Utc::now().timestamp_millis() + 30_000,
            Instant::now() + Duration::from_secs(30),
            catalog,
        )
        .await
        .unwrap();
    assert!(declarations.connectivity_gate.confirm(0));
    declarations
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_one_two_four_lanes_share_execution_and_keep_cancel_capacity_under_pressure() {
    if std::env::var_os("LINGSHU_PRESSURE_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
    }
    let selected = std::env::var("LINGSHU_PRESSURE_LANES").ok();
    let counts = selected.map_or_else(|| vec![1, 2, 4], |v| vec![v.parse::<usize>().unwrap()]);
    assert!(counts.iter().all(|n| [1, 2, 4].contains(n)));
    for count in counts {
        pressure(count).await;
    }
}
async fn pressure(count: usize) {
    let started = Instant::now();
    let f = fixture(16).await;
    let platform = ServiceSigner::new(&[73; 32]).unwrap();
    let mut ca = CertificateParams::new(vec![]).unwrap();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
    let root = ca.self_signed(&ca_key).unwrap().pem();
    let issuer = Issuer::new(ca, ca_key);
    let mut leaf = CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
    leaf.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    leaf.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
    let certificate = leaf.signed_by(&key, &issuer).unwrap().pem();
    let signer = ChannelMessageSigner::from_pkcs8(&key.serialize_der()).unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let router_config = zenoh::Config::from_json5(&json!({
        "mode":"router", "listen":{"endpoints":[format!("tls/127.0.0.1:{port}")]},
        "scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}}, "adminspace":{"enabled":false},
        "transport":{"unicast":{"max_sessions":16,"max_links":1,"qos":{"enabled":false}},"link":{"protocols":["tls"],
            "tls":{"root_ca_certificate_base64":STANDARD.encode(&root),"listen_certificate_base64":STANDARD.encode(&certificate),
                "listen_private_key_base64":STANDARD.encode(key.serialize_pem()),"enable_mtls":true}}}
    }).to_string()).unwrap();
    let router = zenoh::open(router_config).await.unwrap();
    let mut initial = call::tests::authority(&platform, &signer);
    initial.endpoints = vec![
        kish_lingshu_foundation_contract::service_transport::bootstrap::ChannelEndpoint::new(
            format!("tls/127.0.0.1:{port}"),
        )
        .unwrap(),
    ];
    initial.certificate.certificate_pem = certificate;
    initial.certificate.root_ca_pem = root;
    let identity = ServiceChannelIdentity {
        credential: PreparedIdentity {
            response: initial.clone(),
            signer,
            key,
            plaintext: None,
        },
        predecessor: None,
        connection: f.core.connection.clone(),
        authorization_deadline: Instant::now() + Duration::from_secs(30),
    };
    let mut physical = Vec::new();
    for lane in 0..count {
        physical.push(
            zenoh::open(sessions::client_config(&identity, lane).unwrap())
                .await
                .unwrap(),
        );
    }
    assert_eq!(
        physical
            .iter()
            .map(|s| s.zid().to_string())
            .collect::<HashSet<_>>()
            .len(),
        count
    );
    for session in &physical {
        assert_eq!(
            session.info().routers_zid().await.collect::<Vec<_>>(),
            vec![router.zid()]
        );
    }
    let mut pool = sessions::test_sync_pool_lanes(
        f.core.connection.clone(),
        initial.clone(),
        identity.credential.key,
        physical,
    );
    let ep = lanes(count, "generation");
    let cp = lanes(count, "consumer-generation");
    let calls = install(&pool, &ep, None).await;
    let binding = call::NativeCallExecution::test_binding(&f);
    *calls.calls.lock().unwrap() = Some(binding.clone());
    let consumer_calls = Arc::new(AtomicUsize::new(0));
    let consumer_hold = Arc::new(Semaphore::new(0));
    let mut registry = ConsumerRegistry::new("app").unwrap();
    registry
        .register(SlowConsumer {
            calls: consumer_calls.clone(),
            hold: consumer_hold.clone(),
        })
        .unwrap();
    let consumers = install(&pool, &cp, None).await;
    *consumers.consumers.lock().unwrap() = Some(consumer::NativeConsumerExecution::test_binding(
        Arc::new(registry),
        f.budget.clone(),
        2,
    ));
    // Near-4MiB immutable catalog is shared, independent of the lane count.
    let catalog = ProviderCatalog {
        format_version: 1,
        application_id: "app".into(),
        provider_key: "pressure".into(),
        release: "1".into(),
        services: None,
        events: None,
        workflows: vec![ProviderWorkflow {
            key: "workflow".into(),
            name: "pressure".into(),
            description: None,
            define_schema: BTreeMap::from([
                ("type".into(), json!("ReactFlow")),
                (
                    "payload".into(),
                    json!("x".repeat(MAX_PROVIDER_CATALOG_BYTES - 2048)),
                ),
            ]),
        }],
    };
    let snapshot = Arc::new(
        super::super::catalog::CatalogSnapshot::new(&catalog, &f.core.connection).unwrap(),
    );
    let provider_ep = lanes(1, "provider-generation");
    let provider = install(&pool, &provider_ep, Some(snapshot.clone())).await;
    let rounds =
        std::env::var("LINGSHU_PRESSURE_ROUNDS").map_or(2, |v| v.parse::<usize>().unwrap());
    assert!((2..=20).contains(&rounds));
    let mut catalog_bytes = 0;
    for round in 0..rounds {
        let mut active_calls = Vec::new();
        for index in 0..14 {
            let mut invocation = invocation(&f, CallMode::Sync, json!({"hold":true}));
            let InvocationRole::Call(context) = &mut invocation.context.invocation else {
                unreachable!()
            };
            context.call_id = format!("pressure-{round}-{index}");
            let request = call::tests::request(
                &platform,
                &selected(&ep, index % count),
                NativeCallAction::Invoke {
                    invocation: invocation.into(),
                },
            );
            let router = router.clone();
            active_calls.push(tokio::spawn(async move {
                let bytes = wire(&router, &request).await;
                (request, bytes)
            }));
        }
        wait_until(|| f.calls.load(Ordering::SeqCst) == round * 14 + 14).await;
        let mut active_consumers = Vec::new();
        for index in 0..2 {
            let lane = selected(&cp, index % count);
            let mut input = consumer::tests::input(&lane);
            input.invocation.consumption.invocation_id += (round * 2 + index) as u64;
            let request = consumer::tests::signed(&platform, &lane, &input);
            let router = router.clone();
            active_consumers.push(tokio::spawn(async move {
                let bytes = wire(&router, &request).await;
                (request, bytes)
            }));
        }
        wait_until(|| consumer_calls.load(Ordering::SeqCst) == round * 2 + 2).await;
        assert_eq!(
            f.budget.semaphore.available_permits(),
            0,
            "Call and Consumer share sixteen executions across {count} physical lanes"
        );
        let request = call::tests::request(
            &platform,
            &selected(&ep, count - 1),
            NativeCallAction::Invoke {
                invocation: invocation(&f, CallMode::Sync, json!({})).into(),
            },
        );
        let bytes = wire(&router, &request).await;
        assert!(matches!(
            call::tests::output(&bytes, &request, pool.identity.message_signer()),
            NativeCallResponse::Rejected { .. }
        ));
        assert_eq!(f.calls.load(Ordering::SeqCst), round * 14 + 14);
        let input = consumer::tests::input(&selected(&cp, count - 1));
        let request = consumer::tests::signed(&platform, &selected(&cp, count - 1), &input);
        let bytes = wire(&router, &request).await;
        assert!(matches!(
            consumer::tests::decode(&bytes, &request, pool.identity.message_signer(), &initial),
            NativeConsumerResponse::Throttled { .. }
        ));
        catalog_bytes = read_catalog(&router, &platform, &provider_ep, &snapshot).await;
        // Explicit reservation exhaustion fault, in addition to sixteen real
        // retained Queries/Handlers. This does not pretend to fill native TX.
        let occupied_count = pool
            .role_query_slots
            .clone()
            .try_acquire_many_owned(pool.role_query_slots.available_permits() as u32)
            .unwrap();
        let refused = call::tests::request(
            &platform,
            &selected(&ep, count - 1),
            NativeCallAction::Readiness {
                target_instance: ServiceInstanceTarget {
                    node_id: "node".into(),
                    generation: "generation".into(),
                },
            },
        );
        wire_no_reply(&router, &refused).await;
        drop(occupied_count);
        let occupied_bytes = pool
            .role_query_bytes
            .clone()
            .try_acquire_many_owned(pool.role_query_bytes.available_permits() as u32)
            .unwrap();
        let refused = call::tests::request(
            &platform,
            &selected(&ep, count - 1),
            NativeCallAction::Readiness {
                target_instance: ServiceInstanceTarget {
                    node_id: "node".into(),
                    generation: "generation".into(),
                },
            },
        );
        wire_no_reply(&router, &refused).await;
        let occupied_count = pool
            .role_query_slots
            .clone()
            .try_acquire_many_owned(pool.role_query_slots.available_permits() as u32)
            .unwrap();
        assert_eq!(pool.role_query_slots.available_permits(), 0);
        assert_eq!(pool.role_query_bytes.available_permits(), 0);
        let request = call::tests::request(
            &platform,
            &selected(&ep, count - 1),
            NativeCallAction::Cancel {
                cancellation: ServiceCancellation {
                    contract_version: 1,
                    target_instance: ServiceInstanceTarget {
                        node_id: "node".into(),
                        generation: "generation".into(),
                    },
                    operation: f.core.registry.capabilities()[0].operation.clone(),
                    call_id: format!("pressure-{round}-0"),
                    attempt: 1,
                },
            },
        );
        let cancel_started = Instant::now();
        let bytes = wire(&router, &request).await;
        assert_eq!(
            call::tests::output(&bytes, &request, pool.identity.message_signer()),
            NativeCallResponse::Cancelled
        );
        let cancel_ms = cancel_started.elapsed().as_millis();
        assert!(cancel_ms < 2000);
        drop((occupied_count, occupied_bytes));
        // A free total slot cannot bypass this group's own capacity of two.
        wait_until(|| f.budget.semaphore.available_permits() == 1).await;
        let input = consumer::tests::input(&selected(&cp, count - 1));
        let request = consumer::tests::signed(&platform, &selected(&cp, count - 1), &input);
        let bytes = wire(&router, &request).await;
        assert!(matches!(
            consumer::tests::decode(&bytes, &request, pool.identity.message_signer(), &initial),
            NativeConsumerResponse::Throttled { .. }
        ));
        f.hold.add_permits(13);
        consumer_hold.add_permits(2);
        let mut unknown = 0;
        for task in active_calls {
            let (request, bytes) = task.await.unwrap();
            match call::tests::output(&bytes, &request, pool.identity.message_signer()) {
                NativeCallResponse::OutcomeUnknown => unknown += 1,
                NativeCallResponse::Completed { .. } => {}
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(unknown, 1, "only exact original attempt was cancelled");
        for task in active_consumers {
            let (request, bytes) = task.await.unwrap();
            assert!(matches!(
                consumer::tests::decode(&bytes, &request, pool.identity.message_signer(), &initial),
                NativeConsumerResponse::Completed { .. }
            ));
        }
        wait_until(|| {
            pool.role_query_slots.available_permits() == 64
                && pool.role_control_slots.available_permits() == 8
        })
        .await;
        assert_eq!(pool.role_query_bytes.available_permits(), 8 * 1024 * 1024);
        assert_eq!(pool.role_control_bytes.available_permits(), 512 * 1024);
        assert_eq!(f.budget.semaphore.available_permits(), 16);
        eprintln!(
            "PRESSURE_ROUND {}",
            json!({"lanes":count,"round":round+1,"catalog_bytes":catalog_bytes,"cancel_ms":cancel_ms})
        );
    }
    assert!(binding.received_lanes[..count]
        .iter()
        .all(|v| v.load(Ordering::SeqCst) > 0));
    for declarations in [calls, consumers, provider] {
        declarations.stop.send_replace(true);
        declarations.task.await.unwrap().unwrap();
    }
    drop(snapshot);
    assert_eq!(pool.role_slots.available_permits(), 1024);
    assert!(pool.role_keys.lock().unwrap().is_empty());
    assert_eq!(
        f.core.connection.catalog_budgets().0.available_permits(),
        8 * 1024 * 1024
    );
    pool.close().await.unwrap();
    router.close().await.unwrap();
    f.core.connection.shutdown().await;
    eprintln!("pressure lanes={count} rounds={rounds} calls={} consumers={} shared_capacity=16 catalog_bytes={catalog_bytes} elapsed_ms={}",rounds*14,rounds*2,started.elapsed().as_millis());
}

async fn read_catalog(
    router: &zenoh::Session,
    platform: &ServiceSigner,
    provider_ep: &ServiceEndpoint,
    snapshot: &super::super::catalog::CatalogSnapshot,
) -> usize {
    let ServiceEndpoint::Zenoh { route, lanes, .. } = provider_ep else {
        unreachable!()
    };
    let target = route.catalog_key(&snapshot.digest).unwrap();
    let payload = serde_json::value::to_raw_value(&ProviderCatalogRead {
        provider_key: "pressure".into(),
        release: "1".into(),
        catalog_digest: snapshot.digest.clone(),
        route_revision: 1,
        lane: lanes[0].clone(),
    })
    .unwrap();
    let now = chrono::Utc::now().timestamp_millis();
    let request_id = RouteIdentity::new(uuid::Uuid::new_v4().to_string()).unwrap();
    let request = TransportEnvelope {
        protocol_version: ProtocolVersion::V1,
        kind: MessageKind::CatalogRead,
        application_id: RouteIdentity::new("app").unwrap(),
        proof: platform
            .sign_transport_message(
                "app",
                MessageKind::CatalogRead,
                &target,
                &request_id,
                payload.get().as_bytes(),
                now,
                now + 9000,
            )
            .unwrap(),
        target,
        request_id,
        deadline_unix_ms: now + 9000,
        trace_parent: None,
        payload,
    };
    let bytes = wire(&router, &request).await;
    assert!(
        bytes.len() > MAX_PROVIDER_CATALOG_BYTES - 2048
            && bytes.len() < MAX_PROVIDER_CATALOG_BYTES + 32 * 1024
    );
    let reply = TransportEnvelope::decode(
        &bytes,
        &request.target,
        &request.application_id,
        chrono::Utc::now().timestamp_millis(),
    )
    .unwrap();
    let received: ProviderCatalog = serde_json::from_str(reply.payload.get()).unwrap();
    assert_eq!(received.digest().unwrap(), snapshot.digest);
    bytes.len()
}
