//! Loopback wire/shared-core checks. Fabricated metadata is not a product
//! mTLS, report-route grant or durable Workflow checkpoint acceptance test.
use super::*;
use crate::service_channel::call::tests::authority;
use crate::services::execution::tests::{fixture, invocation, wait_until};
use kish_lingshu_foundation_contract::{
    service_auth::{verify_client_transport_message, ServiceSigner},
    service_transport::CallReportRoute,
};
use rcgen::{KeyPair, PKCS_ED25519};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
fn id(v: &str) -> RouteIdentity {
    RouteIdentity::new(v).unwrap()
}
async fn sessions() -> (Session, Session) {
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let locator = format!("tcp/{}", reserve.local_addr().unwrap());
    drop(reserve);
    let router = zenoh::open(zenoh::Config::from_json5(&json!({"mode":"router",
        "listen":{"endpoints":[locator]},"scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}}}).to_string()).unwrap()).await.unwrap();
    let client = zenoh::open(zenoh::Config::from_json5(&json!({"mode":"client",
        "listen":{"endpoints":[]},"connect":{"endpoints":[locator]},"scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}}}).to_string()).unwrap()).await.unwrap();
    (router, client)
}
fn target(now: i64) -> NativeCallReportTarget {
    NativeCallReportTarget {
        route: CallReportRoute {
            deployment: id("dev"),
            application_id: id("app"),
            call_id: id("call-one"),
            attempt: 1,
            epoch: id("report-epoch"),
        },
        instance: ServiceInstanceTarget {
            node_id: "node".into(),
            generation: "generation".into(),
        },
        heartbeat: CallHeartbeatPolicy {
            version: 1,
            epoch: "report-epoch".into(),
            interval_ms: CALL_HEARTBEAT_INTERVAL_MS,
            execution_deadline_ms: now + 5000,
            delivery_deadline_ms: now + 25000,
        },
    }
}
pub(crate) async fn propagated(session: &Session, key: &ExactRouteKey) {
    let querier = session
        .declare_querier(key.as_str().to_owned())
        .await
        .unwrap();
    let matching = querier.matching_listener().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !querier.matching_status().await.unwrap().matching() {
            matching.recv_async().await.unwrap();
        }
    })
    .await
    .unwrap();
    querier.undeclare().await.unwrap();
}
fn verify(request: &TransportEnvelope, signer: &ChannelMessageSigner) -> NativeCallReportRequest {
    verify_client_transport_message(
        signer.public_key(),
        &ClientChannelIdentity {
            application_id: id("app"),
            instance_id: id("sdk"),
            base_generation: id("base"),
            certificate_identity: id("certificate"),
        },
        &request.proof,
        request.kind,
        &request.target,
        &request.request_id,
        request.payload.get().as_bytes(),
        chrono::Utc::now().timestamp_millis(),
    )
    .unwrap();
    let report: NativeCallReportRequest = serde_json::from_str(request.payload.get()).unwrap();
    assert_eq!(report.token, "result-secret");
    assert!(!format!("{report:?}").contains("result-secret"));
    assert_eq!(report.instance.node_id, "node");
    report
}
pub(crate) fn reply(
    platform: &ServiceSigner,
    request: &TransportEnvelope,
    output: &NativeCallReportResponse,
) -> Vec<u8> {
    let now = chrono::Utc::now().timestamp_millis();
    let payload = serde_json::value::to_raw_value(output).unwrap();
    let proof = platform
        .sign_transport_message(
            "app",
            request.kind,
            &request.target,
            &request.request_id,
            payload.get().as_bytes(),
            now,
            request.deadline_unix_ms,
        )
        .unwrap();
    TransportEnvelope {
        protocol_version: ProtocolVersion::V1,
        kind: request.kind,
        request_id: request.request_id.clone(),
        application_id: request.application_id.clone(),
        target: request.target.clone(),
        deadline_unix_ms: request.deadline_unix_ms,
        proof,
        trace_parent: None,
        payload,
    }
    .encode(now)
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_require_one_scoped_platform_reply_and_do_not_retry_the_query() {
    let capture = super::super::observation::tests::Capture::default();
    let f = fixture(1).await;
    let (router, sdk) = sessions().await;
    let platform = Arc::new(ServiceSigner::new(&[73; 32]).unwrap());
    let key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
    let signer = Arc::new(ChannelMessageSigner::from_pkcs8(&key.serialize_der()).unwrap());
    let initial = authority(&platform, &signer);
    let t = target(chrono::Utc::now().timestamp_millis());
    let completion = CompletionTarget {
        token: "result-secret".into(),
        heartbeat: Some(t.heartbeat.clone()),
    };
    let client = NativeCallReportClient::new(
        sdk.clone(),
        f.core.connection.clone(),
        initial,
        signer.clone(),
        t.clone(),
        completion.clone(),
    )
    .unwrap();
    let result = ServiceCompletion {
        contract_version: 1,
        call_id: "call-one".into(),
        attempt: 1,
        outcome: ServiceOutcome::Succeeded {
            result: json!({"applied":true}),
        },
    };
    let observed = Arc::new(AtomicUsize::new(0));
    // These cases test reply verification, not repeated route declaration.
    // Keep one Queryable so matching cannot refer to an old declaration while
    // the next identical route is still propagating.
    let declared = Arc::new(
        router
            .declare_queryable(client.key.as_str().to_owned())
            .await
            .unwrap(),
    );
    propagated(&sdk, &client.key).await;
    for mode in 0..13 {
        let declared = declared.clone();
        let platform = platform.clone();
        let signer = signer.clone();
        let worker_observed = observed.clone();
        let target = client.key.clone();
        let worker = tokio::spawn(async move {
            let query = tokio::time::timeout(Duration::from_secs(5), declared.recv_async())
                .await
                .unwrap_or_else(|_| panic!("bounded one-shot report delivery in mode {mode}"))
                .unwrap();
            worker_observed.fetch_add(1, Ordering::SeqCst);
            let mut request = TransportEnvelope::decode(
                &query.payload().unwrap().to_bytes(),
                &target,
                &id("app"),
                chrono::Utc::now().timestamp_millis(),
            )
            .unwrap();
            let report = verify(&request, &signer);
            assert!(matches!(
                report.action,
                NativeCallReportAction::Complete { .. }
            ));
            if mode == 9 {
                tokio::time::sleep(Duration::from_millis(600)).await;
                return;
            }
            let output = match mode {
                0 => NativeCallReportResponse::Completed {
                    disposition: CompletionDisposition::Recorded,
                },
                1 => NativeCallReportResponse::Completed {
                    disposition: CompletionDisposition::Duplicate,
                },
                2 => NativeCallReportResponse::Completed {
                    disposition: CompletionDisposition::Invalidated,
                },
                3 => NativeCallReportResponse::Rejected {
                    reason: Error::AuthorityUnavailable,
                },
                4 => NativeCallReportResponse::Rejected {
                    reason: Error::Rejected,
                },
                5 => NativeCallReportResponse::Rejected {
                    reason: Error::Unavailable,
                },
                10 => NativeCallReportResponse::Heartbeat {
                    disposition: HeartbeatDisposition::Invalidated,
                },
                _ => NativeCallReportResponse::Completed {
                    disposition: CompletionDisposition::Recorded,
                },
            };
            if mode == 8 {
                request.request_id = id("wrong-request");
            }
            let foreign = ServiceSigner::new(&[84; 32]).unwrap();
            let bytes = reply(
                if mode == 7 { &foreign } else { &platform },
                &request,
                &output,
            );
            if mode == 12 {
                query
                    .reply(
                        target.as_str().to_owned(),
                        vec![0u8; MAX_NATIVE_CALL_REPORT_REPLY_BYTES + 1],
                    )
                    .await
                    .unwrap();
            } else if mode == 11 {
                query.reply_err("unproved authority denial").await.unwrap();
            } else {
                query
                    .reply(target.as_str().to_owned(), bytes.clone())
                    .await
                    .unwrap();
                if mode == 6 {
                    query
                        .reply(target.as_str().to_owned(), bytes)
                        .await
                        .unwrap();
                }
            }
        });
        let response = capture
            .observe(client.complete(
                &completion,
                &result,
                Some(if mode == 9 {
                    Duration::from_millis(300)
                } else {
                    Duration::from_secs(1)
                }),
            ))
            .await;
        let expected = match mode {
            0 => Ok(CompletionDisposition::Recorded),
            1 => Ok(CompletionDisposition::Duplicate),
            2 => Ok(CompletionDisposition::Invalidated),
            3 => Err(Error::AuthorityUnavailable),
            4 => Err(Error::Rejected),
            _ => Err(Error::Unavailable),
        };
        assert_eq!(response, expected, "mode {mode}");
        worker.await.unwrap();
        assert_eq!(observed.load(Ordering::SeqCst), mode + 1);
    }
    assert_eq!(
        capture.sum(
            "lingshu_sdk_channel_exchanges_total",
            Some("completion"),
            Some("reply_verified")
        ),
        6.0
    );
    assert_eq!(
        capture.sum(
            "lingshu_sdk_channel_exchanges_total",
            Some("completion"),
            Some("unknown")
        ),
        7.0
    );
    for outcome in [
        "recorded",
        "duplicate",
        "invalidated",
        "authority_unavailable",
        "rejected",
        "unavailable",
    ] {
        assert_eq!(
            capture.sum(
                "lingshu_sdk_channel_verified_dispositions_total",
                Some("completion"),
                Some(outcome)
            ),
            1.0
        );
    }
    capture.assert_balanced_resource_gauges();
    capture.assert_bounded_labels();
    Arc::try_unwrap(declared)
        .unwrap_or_else(|_| panic!("reply workers must release the shared Queryable"))
        .undeclare()
        .await
        .unwrap();
    let before = observed.load(Ordering::SeqCst);
    let mut wrong = result.clone();
    wrong.attempt = 2;
    assert_eq!(
        client.complete(&completion, &wrong, None).await,
        Err(Error::Rejected)
    );
    let mut forged = completion.clone();
    forged.token = "foreign".into();
    assert_eq!(
        client.complete(&forged, &result, None).await,
        Err(Error::Rejected)
    );
    let heartbeat = ServiceHeartbeat {
        version: 1,
        call_id: "call-one".into(),
        attempt: 2,
        epoch: t.heartbeat.epoch.clone(),
        instance: t.instance.clone(),
        phase: CallPhase::Running,
    };
    assert_eq!(
        client.heartbeat(&completion, &heartbeat).await,
        Err(Error::Rejected)
    );
    let mut huge = result.clone();
    huge.outcome = ServiceOutcome::Succeeded {
        result: json!({"value":"x".repeat(MAX_SERVICE_PAYLOAD_BYTES)}),
    };
    assert_eq!(
        client.complete(&completion, &huge, None).await,
        Err(Error::Rejected)
    );
    let mut wrong_target = t;
    wrong_target.route.epoch = id("other");
    assert!(wrong_target
        .validate(chrono::Utc::now().timestamp_millis())
        .is_err());
    let _slots = f
        .core
        .connection
        .call_report_budget()
        .try_acquire_many_owned(16)
        .unwrap();
    assert_eq!(
        client.complete(&completion, &result, None).await,
        Err(Error::Unavailable)
    );
    assert_eq!(observed.load(Ordering::SeqCst), before);
    drop(_slots);
    f.core.connection.shutdown().await;
    assert_eq!(
        client.complete(&completion, &result, None).await,
        Err(Error::AuthorityUnavailable)
    );
    sdk.close().await.unwrap();
    router.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reporting_phase_keeps_the_original_execution_and_delivery_windows() {
    let f = fixture(1).await;
    let (router, sdk) = sessions().await;
    let platform = Arc::new(ServiceSigner::new(&[73; 32]).unwrap());
    let key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
    let signer = Arc::new(ChannelMessageSigner::from_pkcs8(&key.serialize_der()).unwrap());
    let mut t = target(chrono::Utc::now().timestamp_millis());
    t.heartbeat.execution_deadline_ms = chrono::Utc::now().timestamp_millis() + 500;
    t.heartbeat.delivery_deadline_ms = t.heartbeat.execution_deadline_ms + 1000;
    let completion = CompletionTarget {
        token: "result-secret".into(),
        heartbeat: Some(t.heartbeat.clone()),
    };
    let initial = authority(&platform, &signer);
    let mut expired = initial.clone();
    expired.certificate.expires_unix_ms = t.heartbeat.delivery_deadline_ms - 1;
    assert!(NativeCallReportClient::new(
        sdk.clone(),
        f.core.connection.clone(),
        expired,
        signer.clone(),
        t.clone(),
        completion.clone()
    )
    .is_err());
    let client = NativeCallReportClient::new(
        sdk.clone(),
        f.core.connection.clone(),
        initial,
        signer.clone(),
        t.clone(),
        completion.clone(),
    )
    .unwrap();
    let declared = router
        .declare_queryable(client.key.as_str().to_owned())
        .await
        .unwrap();
    propagated(&sdk, &client.key).await;
    let key = client.key.clone();
    let worker = tokio::spawn(async move {
        let query = declared.recv_async().await.unwrap();
        let request = TransportEnvelope::decode(
            &query.payload().unwrap().to_bytes(),
            &key,
            &id("app"),
            chrono::Utc::now().timestamp_millis(),
        )
        .unwrap();
        let report = verify(&request, &signer);
        assert!(matches!(
            report.action,
            NativeCallReportAction::Heartbeat {
                heartbeat: ServiceHeartbeat {
                    phase: CallPhase::Completing,
                    ..
                }
            }
        ));
        let now = chrono::Utc::now().timestamp_millis();
        let output = NativeCallReportResponse::Heartbeat {
            disposition: HeartbeatDisposition::Renewed {
                store_now_ms: now,
                liveness_until_ms: now + CALL_LIVENESS_MS,
            },
        };
        query
            .reply(key.as_str().to_owned(), reply(&platform, &request, &output))
            .await
            .unwrap();
    });
    tokio::time::sleep_until(client.execution_deadline + Duration::from_millis(10)).await;
    let mut h = ServiceHeartbeat {
        version: 1,
        call_id: "call-one".into(),
        attempt: 1,
        epoch: t.heartbeat.epoch.clone(),
        instance: t.instance,
        phase: CallPhase::Running,
    };
    assert_eq!(
        client.heartbeat(&completion, &h).await,
        Err(Error::AuthorityUnavailable)
    );
    h.phase = CallPhase::Completing;
    assert!(matches!(
        client.heartbeat(&completion, &h).await.unwrap(),
        HeartbeatDisposition::Renewed { .. }
    ));
    worker.await.unwrap();
    // A valid heartbeat cannot extend the fixed delivery deadline.
    tokio::time::sleep_until(client.delivery_deadline + Duration::from_millis(10)).await;
    assert_eq!(
        client.heartbeat(&completion, &h).await,
        Err(Error::AuthorityUnavailable)
    );
    sdk.close().await.unwrap();
    router.close().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_core_retains_permits_and_retries_only_the_same_completed_result() {
    let f = fixture(1).await;
    let (router, sdk) = sessions().await;
    let platform = Arc::new(ServiceSigner::new(&[73; 32]).unwrap());
    let key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
    let signer = Arc::new(ChannelMessageSigner::from_pkcs8(&key.serialize_der()).unwrap());
    let t = target(chrono::Utc::now().timestamp_millis());
    let completion = CompletionTarget {
        token: "result-secret".into(),
        heartbeat: Some(t.heartbeat.clone()),
    };
    let reporter = Arc::new(
        NativeCallReportClient::new(
            sdk.clone(),
            f.core.connection.clone(),
            authority(&platform, &signer),
            signer.clone(),
            t.clone(),
            completion.clone(),
        )
        .unwrap(),
    );
    let declared = router
        .declare_queryable(reporter.key.as_str().to_owned())
        .await
        .unwrap();
    propagated(&sdk, &reporter.key).await;
    let completions = Arc::new(AtomicUsize::new(0));
    let held = Arc::new(tokio::sync::Semaphore::new(0));
    let worker = {
        let completed = completions.clone();
        let held = held.clone();
        let route = reporter.key.clone();
        tokio::spawn(async move {
            let mut first = None;
            while let Ok(query) = declared.recv_async().await {
                let request = TransportEnvelope::decode(
                    &query.payload().unwrap().to_bytes(),
                    &route,
                    &id("app"),
                    chrono::Utc::now().timestamp_millis(),
                )
                .unwrap();
                let report = verify(&request, &signer);
                let output = match report.action {
                    NativeCallReportAction::Heartbeat { .. } => {
                        let now = chrono::Utc::now().timestamp_millis();
                        NativeCallReportResponse::Heartbeat {
                            disposition: HeartbeatDisposition::Renewed {
                                store_now_ms: now,
                                liveness_until_ms: now + CALL_LIVENESS_MS,
                            },
                        }
                    }
                    NativeCallReportAction::Complete { completion: result } => {
                        let n = completed.fetch_add(1, Ordering::SeqCst);
                        if n == 0 {
                            first = Some(result);
                            held.acquire().await.unwrap().forget();
                            NativeCallReportResponse::Rejected {
                                reason: Error::Unavailable,
                            }
                        } else {
                            assert_eq!(first, Some(result));
                            NativeCallReportResponse::Completed {
                                disposition: CompletionDisposition::Duplicate,
                            }
                        }
                    }
                };
                let bytes = reply(&platform, &request, &output);
                query.reply(route.as_str().to_owned(), bytes).await.unwrap();
                if completed.load(Ordering::SeqCst) == 2 {
                    break;
                }
            }
        })
    };
    let (core, stop) = CallExecutionCore::new(
        f.core.registry.clone(),
        f.core.connection.clone(),
        f.budget.clone(),
        reporter,
    )
    .unwrap();
    *core.enrollment.write().unwrap() = f.core.enrollment.read().unwrap().clone();
    let mut input = invocation(&f, CallMode::Async, json!({}));
    input.context.deadline_ms = t.heartbeat.execution_deadline_ms;
    input.completion = Some(completion);
    assert!(matches!(
        core.invoke(input.clone()).await.unwrap(),
        InvocationResponse::Accepted { .. }
    ));
    wait_until(|| completions.load(Ordering::SeqCst) == 1).await;
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    assert_eq!(core.total.available_permits(), 0);
    assert!(core.invoke(input).await.is_err());
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    held.add_permits(1);
    wait_until(|| core.active.load(Ordering::SeqCst) == 0).await;
    assert_eq!(completions.load(Ordering::SeqCst), 2);
    assert_eq!(core.total.available_permits(), 1);
    assert_eq!(core.unconfirmed.load(Ordering::SeqCst), 0);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    stop.send_replace(true);
    worker.await.unwrap();
    sdk.close().await.unwrap();
    router.close().await.unwrap();
}
