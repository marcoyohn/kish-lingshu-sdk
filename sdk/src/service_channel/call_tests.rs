use super::*;
use crate::services::execution::tests::{fixture, invocation, wait_until};
use kish_lingshu_foundation_contract::{
    service_auth::{verify_client_transport_message, ServiceSigner},
    service_transport::{
        bootstrap::{ChannelCertificate, TlsEndpoint},
        DataLaneId, InstanceRoute, LaneIdentity, PlatformControlRoute, RouteIdentity,
    },
    ServiceInstanceIdentity,
};
use rcgen::{KeyPair, PKCS_ED25519};
use serde_json::json;
use std::sync::atomic::Ordering;
fn id(v: &str) -> RouteIdentity {
    RouteIdentity::new(v).unwrap()
}
pub(crate) fn authority(
    platform: &ServiceSigner,
    signer: &ChannelMessageSigner,
) -> ChannelBootstrapResponse {
    let now = chrono::Utc::now().timestamp_millis();
    ChannelBootstrapResponse {
        protocol_version: ProtocolVersion::V1,
        deployment: id("dev"),
        application_id: id("app"),
        parent_credential_fingerprint: "f".repeat(64),
        instance: ServiceInstanceIdentity {
            instance_id: "sdk".into(),
            generation: "base".into(),
        },
        endpoints: vec![TlsEndpoint::new("tls/localhost:7447".into()).unwrap()],
        control_route: PlatformControlRoute {
            deployment: id("dev"),
            platform_node: id("platform"),
            boot_epoch: id("boot"),
        },
        certificate: ChannelCertificate {
            certificate_identity: id("certificate"),
            certificate_pem: "test-only".into(),
            root_ca_pem: "test-only".into(),
            message_public_key: signer
                .public_key()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect(),
            expires_unix_ms: now + 30000,
        },
        transport_trust: platform.transport_trust("app", now / 1000).unwrap(),
        authorization_issued_unix_ms: now,
        authorization_expires_unix_ms: now + 30000,
    }
}
pub(crate) fn endpoint() -> ServiceEndpoint {
    ServiceEndpoint::Zenoh {
        protocol_version: ProtocolVersion::V1,
        route: InstanceRoute {
            deployment: id("dev"),
            application_id: id("app"),
            instance_id: id("sdk"),
            base_generation: id("base"),
            role_generation: id("generation"),
        },
        route_revision: 1,
        lanes: vec![LaneIdentity {
            lane: DataLaneId::new(0).unwrap(),
            epoch: id("epoch"),
        }],
    }
}
pub(crate) fn request(
    platform: &ServiceSigner,
    endpoint: &ServiceEndpoint,
    action: NativeCallAction,
) -> TransportEnvelope {
    let ServiceEndpoint::Zenoh { route, lanes, .. } = endpoint else {
        unreachable!()
    };
    let target = route.invoke_key(&lanes[0]).unwrap();
    let now = chrono::Utc::now().timestamp_millis();
    let kind = match action {
        NativeCallAction::Invoke { .. }
        | NativeCallAction::Readiness { .. }
        | NativeCallAction::AsyncReadiness { .. } => MessageKind::InvokeCall,
        _ => MessageKind::CancelCall,
    };
    let payload = serde_json::value::to_raw_value(&NativeCallRequest {
        route_revision: 1,
        lane: lanes[0].clone(),
        action,
    })
    .unwrap();
    let request_id = id(&uuid::Uuid::new_v4().to_string());
    let deadline = now + 9000;
    let proof = platform
        .sign_transport_message(
            "app",
            kind,
            &target,
            &request_id,
            payload.get().as_bytes(),
            now,
            deadline,
        )
        .unwrap();
    TransportEnvelope {
        protocol_version: ProtocolVersion::V1,
        kind,
        request_id,
        application_id: id("app"),
        target,
        deadline_unix_ms: deadline,
        proof,
        trace_parent: None,
        payload,
    }
}
pub(crate) fn output(
    bytes: &[u8],
    req: &TransportEnvelope,
    signer: &ChannelMessageSigner,
) -> NativeCallResponse {
    let now = chrono::Utc::now().timestamp_millis();
    let env = TransportEnvelope::decode(bytes, &req.target, &req.application_id, now).unwrap();
    verify_client_transport_message(
        signer.public_key(),
        &ClientChannelIdentity {
            application_id: id("app"),
            instance_id: id("sdk"),
            base_generation: id("base"),
            certificate_identity: id("certificate"),
        },
        &env.proof,
        env.kind,
        &env.target,
        &env.request_id,
        env.payload.get().as_bytes(),
        now,
    )
    .unwrap();
    assert_eq!(env.request_id, req.request_id);
    assert_eq!(env.kind, req.kind);
    assert_eq!(env.deadline_unix_ms, req.deadline_unix_ms);
    serde_json::from_str(env.payload.get()).unwrap()
}
#[tokio::test]
async fn signed_sync_call_executes_once_rejects_replay_and_keeps_async_closed() {
    let capture = super::super::observation::tests::Capture::default();
    capture.observe(signed_sync_call_fixture()).await;
    for (outcome, count) in [
        ("succeeded", 1.0),
        ("failed", 1.0),
        ("rejected", 1.0),
        ("unknown", 0.0),
    ] {
        assert_eq!(
            capture.sum(
                "lingshu_sdk_channel_business_results_total",
                Some("call"),
                Some(outcome)
            ),
            count
        );
    }
    assert_eq!(
        capture.count("lingshu_sdk_channel_business_seconds"),
        3,
        "readiness and replay never create business execution"
    );
    for (outcome, count) in [
        ("ready", 1.0),
        ("rejected", 2.0),
        ("succeeded", 1.0),
        ("failed", 1.0),
    ] {
        assert_eq!(
            capture.sum(
                "lingshu_sdk_channel_prepared_responses_total",
                Some("call"),
                Some(outcome)
            ),
            count
        );
    }
    capture.assert_balanced_resource_gauges();
    capture.assert_bounded_labels();
}
async fn signed_sync_call_fixture() {
    let f = fixture(2).await;
    let platform = ServiceSigner::new(&[73; 32]).unwrap();
    let key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
    let signer = ChannelMessageSigner::from_pkcs8(&key.serialize_der()).unwrap();
    let initial = authority(&platform, &signer);
    let ep = endpoint();
    let binding = NativeCallExecution {
        received_lanes: Default::default(),
        core: f.core.clone(),
        faults: Default::default(),
        stop: f.stop.clone(),
        enrollment: f.registration.clone(),
        target: ServiceInstanceTarget {
            node_id: "node".into(),
            generation: "generation".into(),
        },
        async_reports: None,
        handoff: AtomicBool::new(false),
    };
    for (generation, ready) in [("generation", true), ("foreign", false)] {
        let req = request(
            &platform,
            &ep,
            NativeCallAction::Readiness {
                target_instance: ServiceInstanceTarget {
                    node_id: "node".into(),
                    generation: generation.into(),
                },
            },
        );
        let result = binding
            .reply(
                &ep,
                &req.target,
                &req.encode(chrono::Utc::now().timestamp_millis()).unwrap(),
                initial.authorization_expires_unix_ms,
                &f.core.connection,
                &initial,
                &signer,
            )
            .await
            .unwrap();
        assert_eq!(
            matches!(output(&result, &req, &signer), NativeCallResponse::Ready),
            ready
        );
        assert_eq!(
            f.calls.load(Ordering::SeqCst),
            0,
            "inspection cannot execute the Handler"
        );
    }
    let req = request(
        &platform,
        &ep,
        NativeCallAction::Invoke {
            invocation: invocation(&f, CallMode::Sync, json!({})).into(),
        },
    );
    let bytes = req.encode(chrono::Utc::now().timestamp_millis()).unwrap();
    let result = binding
        .reply(
            &ep,
            &req.target,
            &bytes,
            initial.authorization_expires_unix_ms,
            &f.core.connection,
            &initial,
            &signer,
        )
        .await
        .unwrap();
    assert!(matches!(
        output(&result, &req, &signer),
        NativeCallResponse::Completed {
            outcome: ServiceOutcome::Succeeded { .. }
        }
    ));
    assert!(binding
        .reply(
            &ep,
            &req.target,
            &bytes,
            initial.authorization_expires_unix_ms,
            &f.core.connection,
            &initial,
            &signer
        )
        .await
        .is_err());
    let req = request(
        &platform,
        &ep,
        NativeCallAction::Invoke {
            invocation: invocation(&f, CallMode::Async, json!({})).into(),
        },
    );
    let result = binding
        .reply(
            &ep,
            &req.target,
            &req.encode(chrono::Utc::now().timestamp_millis()).unwrap(),
            initial.authorization_expires_unix_ms,
            &f.core.connection,
            &initial,
            &signer,
        )
        .await
        .unwrap();
    assert!(
        matches!(output(&result,&req,&signer),NativeCallResponse::Rejected{error} if error.code=="native_async_unavailable")
    );
    let req = request(
        &platform,
        &ep,
        NativeCallAction::Invoke {
            invocation: invocation(&f, CallMode::Sync, json!({"invalid_output":true})).into(),
        },
    );
    let bytes = binding
        .reply(
            &ep,
            &req.target,
            &req.encode(chrono::Utc::now().timestamp_millis()).unwrap(),
            initial.authorization_expires_unix_ms,
            &f.core.connection,
            &initial,
            &signer,
        )
        .await
        .unwrap();
    assert!(matches!(
        output(&bytes, &req, &signer),
        NativeCallResponse::Completed {
            outcome: ServiceOutcome::Failed { .. }
        }
    ));
    assert_eq!(f.calls.load(Ordering::SeqCst), 2);
    f.core.connection.shutdown().await;
}
#[tokio::test]
async fn stale_lane_revision_kind_or_foreign_proof_never_reaches_handler() {
    let capture = super::super::observation::tests::Capture::default();
    capture.observe(stale_lane_fixture()).await;
    assert_eq!(capture.count("lingshu_sdk_channel_business_seconds"), 0);
    assert_eq!(
        capture.sum("lingshu_sdk_channel_prepared_responses_total", None, None),
        0.0
    );
    capture.assert_bounded_labels();
}
async fn stale_lane_fixture() {
    let f = fixture(1).await;
    let platform = ServiceSigner::new(&[73; 32]).unwrap();
    let key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
    let signer = ChannelMessageSigner::from_pkcs8(&key.serialize_der()).unwrap();
    let initial = authority(&platform, &signer);
    let ep = endpoint();
    let binding = NativeCallExecution {
        received_lanes: Default::default(),
        core: f.core.clone(),
        faults: Default::default(),
        stop: f.stop.clone(),
        enrollment: f.registration.clone(),
        target: ServiceInstanceTarget {
            node_id: "node".into(),
            generation: "generation".into(),
        },
        async_reports: None,
        handoff: AtomicBool::new(false),
    };
    for i in 0..5 {
        let mut req = request(
            &platform,
            &ep,
            NativeCallAction::Invoke {
                invocation: invocation(&f, CallMode::Sync, json!({})).into(),
            },
        );
        match i {
            0 | 1 => {
                let mut body: NativeCallRequest = serde_json::from_str(req.payload.get()).unwrap();
                if i == 0 {
                    body.route_revision += 1
                } else {
                    body.lane.epoch = id("stale")
                };
                req.payload = serde_json::value::to_raw_value(&body).unwrap();
            }
            2 => req.kind = MessageKind::CancelCall,
            3 => {
                req.proof = ServiceSigner::new(&[74; 32])
                    .unwrap()
                    .sign_transport_message(
                        "app",
                        req.kind,
                        &req.target,
                        &req.request_id,
                        req.payload.get().as_bytes(),
                        chrono::Utc::now().timestamp_millis(),
                        req.deadline_unix_ms,
                    )
                    .unwrap()
            }
            _ => req.deadline_unix_ms += 1,
        }
        assert!(binding
            .reply(
                &ep,
                &req.target,
                &req.encode(chrono::Utc::now().timestamp_millis()).unwrap(),
                initial.authorization_expires_unix_ms,
                &f.core.connection,
                &initial,
                &signer
            )
            .await
            .is_err());
    }
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    f.core.connection.shutdown().await;
}
#[tokio::test]
async fn signed_cancel_matches_only_exact_attempt_and_stops_sync_as_unknown() {
    let f = fixture(1).await;
    let platform = Arc::new(ServiceSigner::new(&[73; 32]).unwrap());
    let key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
    let signer = Arc::new(ChannelMessageSigner::from_pkcs8(&key.serialize_der()).unwrap());
    let initial = Arc::new(authority(&platform, &signer));
    let ep = endpoint();
    let binding = Arc::new(NativeCallExecution {
        received_lanes: Default::default(),
        core: f.core.clone(),
        faults: Default::default(),
        stop: f.stop.clone(),
        enrollment: f.registration.clone(),
        target: ServiceInstanceTarget {
            node_id: "node".into(),
            generation: "generation".into(),
        },
        async_reports: None,
        handoff: AtomicBool::new(false),
    });
    let req = request(
        &platform,
        &ep,
        NativeCallAction::Invoke {
            invocation: invocation(&f, CallMode::Sync, json!({"hold":true})).into(),
        },
    );
    let task = {
        let binding = binding.clone();
        let initial = initial.clone();
        let signer = signer.clone();
        let ep = ep.clone();
        let connection = f.core.connection.clone();
        tokio::spawn(async move {
            let bytes = binding
                .reply(
                    &ep,
                    &req.target,
                    &req.encode(chrono::Utc::now().timestamp_millis()).unwrap(),
                    initial.authorization_expires_unix_ms,
                    &connection,
                    &initial,
                    &signer,
                )
                .await
                .unwrap();
            output(&bytes, &req, &signer)
        })
    };
    wait_until(|| f.calls.load(Ordering::SeqCst) == 1).await;
    for attempt in [2, 1] {
        let req = request(
            &platform,
            &ep,
            NativeCallAction::Cancel {
                cancellation: ServiceCancellation {
                    contract_version: 1,
                    target_instance: binding.target.clone(),
                    operation: f.core.registry.capabilities()[0].operation.clone(),
                    call_id: "call-one".into(),
                    attempt,
                },
            },
        );
        let bytes = binding
            .reply(
                &ep,
                &req.target,
                &req.encode(chrono::Utc::now().timestamp_millis()).unwrap(),
                initial.authorization_expires_unix_ms,
                &f.core.connection,
                &initial,
                &signer,
            )
            .await
            .unwrap();
        assert_eq!(output(&bytes, &req, &signer), NativeCallResponse::Cancelled);
        if attempt == 2 {
            assert!(!task.is_finished());
        }
    }
    assert_eq!(task.await.unwrap(), NativeCallResponse::OutcomeUnknown);
    assert_eq!(f.budget.semaphore.available_permits(), 1);
    f.core.connection.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_readiness_requires_explicit_binding_current_target_and_running_role() {
    let platform = ServiceSigner::new(&[73; 32]).unwrap();
    let key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
    let signer = Arc::new(ChannelMessageSigner::from_pkcs8(&key.serialize_der()).unwrap());
    let initial = authority(&platform, &signer);
    let ep = endpoint();
    let session = zenoh::open(zenoh::Config::from_json5(r#"{"mode":"router","listen":{"endpoints":[]},"scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}}}"#).unwrap()).await.unwrap();
    for (enabled, generation, stopped, ready) in [
        (false, "generation", false, false),
        (true, "generation", false, true),
        (true, "foreign", false, false),
        (true, "generation", true, false),
    ] {
        let f = fixture(1).await;
        f.stop.send_replace(stopped);
        let binding = NativeCallExecution {
            received_lanes: Default::default(),
            core: f.core.clone(),
            faults: Default::default(),
            stop: f.stop.clone(),
            enrollment: f.registration.clone(),
            target: ServiceInstanceTarget {
                node_id: "node".into(),
                generation: "generation".into(),
            },
            handoff: AtomicBool::new(false),
            async_reports: enabled.then(|| {
                Arc::new(AsyncCallReports {
                    session: session.clone(),
                    authority: initial.clone(),
                    signer: signer.clone(),
                    active: AtomicUsize::new(0),
                    physical: watch::channel(None).1,
                    deadline: std::sync::Mutex::new(tokio::time::Instant::now()),
                })
            }),
        };
        let req = request(
            &platform,
            &ep,
            NativeCallAction::AsyncReadiness {
                target_instance: ServiceInstanceTarget {
                    node_id: "node".into(),
                    generation: generation.into(),
                },
            },
        );
        let bytes = binding
            .reply(
                &ep,
                &req.target,
                &req.encode(chrono::Utc::now().timestamp_millis()).unwrap(),
                initial.authorization_expires_unix_ms,
                &f.core.connection,
                &initial,
                &signer,
            )
            .await
            .unwrap();
        assert_eq!(
            matches!(
                output(&bytes, &req, &signer),
                NativeCallResponse::AsyncReady
            ),
            ready
        );
        assert_eq!(f.calls.load(Ordering::SeqCst), 0);
        f.core.connection.shutdown().await;
    }
    session.close().await.unwrap();
}
