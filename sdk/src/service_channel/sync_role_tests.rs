//! Loopback declaration/execution integration; fabricated role metadata is a
//! fixture, not evidence of platform TLS/ACL/readiness admission.
use super::super::call::tests::{authority, endpoint, output, request};
use super::*;
use crate::services::{
    execution::tests::{fixture, invocation, wait_until},
    *,
};
use kish_lingshu_foundation_contract::service_auth::ServiceSigner;
use rcgen::{KeyPair, PKCS_ED25519};
use std::sync::atomic::Ordering;
use zenoh::query::{ConsolidationMode, QueryTarget};
async fn exchange(
    router: zenoh::Session,
    req: TransportEnvelope,
    signer: Arc<ChannelMessageSigner>,
) -> NativeCallResponse {
    let replies = router
        .get(req.target.as_str().to_owned())
        .target(QueryTarget::All)
        .consolidation(ConsolidationMode::None)
        .payload(req.encode(chrono::Utc::now().timestamp_millis()).unwrap())
        .timeout(Duration::from_secs(10))
        .await
        .unwrap();
    let reply = replies.recv_async().await.unwrap();
    let result = reply.result();
    let sample = result.as_ref().unwrap();
    let result = output(&sample.payload().to_bytes(), &req, &signer);
    assert!(replies.recv_async().await.is_err());
    result
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_call_role_executes_and_cancels_without_blocking_its_control_worker() {
    let (f, platform, signer, _initial, ep, mut pool, mut role, router) = setup(1).await;
    let deadline = role.role_deadline;
    let expires = role.lease_window.borrow().expires_at_ms;
    assert!(pool
        .install_role_declarations(&ep, "generation", expires, deadline, None, false)
        .await
        .is_err());
    assert!(pool
        .enable_sync_calls(
            &mut role,
            f.core.registry.clone(),
            crate::ServiceExecutionBudget::new(2).unwrap()
        )
        .is_err());
    assert!(role.calls.lock().unwrap().is_none());
    pool.enable_sync_calls(&mut role, f.core.registry.clone(), f.budget.clone())
        .unwrap();
    assert!(pool
        .enable_sync_calls(&mut role, f.core.registry.clone(), f.budget.clone())
        .is_err());
    let ServiceEndpoint::Zenoh { route, lanes, .. } = &ep else {
        unreachable!()
    };
    let querier = router
        .declare_querier(route.invoke_key(&lanes[0]).unwrap().as_str().to_owned())
        .await
        .unwrap();
    let matching = querier.matching_listener().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !querier.matching_status().await.unwrap().matching() {
            matching.recv_async().await.unwrap();
        }
    })
    .await
    .expect("bounded declaration propagation before invoking business work");
    querier.undeclare().await.unwrap();
    let req = request(
        &platform,
        &ep,
        NativeCallAction::Invoke {
            invocation: invocation(&f, CallMode::Sync, serde_json::json!({"hold":true})).into(),
        },
    );
    let pending = tokio::spawn(exchange(router.clone(), req, signer.clone()));
    wait_until(|| f.calls.load(Ordering::SeqCst) == 1).await;
    role.withdraw_route_confirmation();
    assert!(!role.route_confirmed());
    let new_work = request(
        &platform,
        &ep,
        NativeCallAction::Invoke {
            invocation: invocation(&f, CallMode::Sync, serde_json::json!({"hold":false})).into(),
        },
    );
    let rejected = router
        .get(new_work.target.as_str().to_owned())
        .payload(
            new_work
                .encode(chrono::Utc::now().timestamp_millis())
                .unwrap(),
        )
        .timeout(Duration::from_millis(100))
        .await
        .unwrap();
    assert!(rejected.recv_async().await.is_err());
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    // Withdrawing discovery admission leaves exact cancellation of the
    // already Accepted call available on its original authenticated route.
    // Keep execution task slots exhausted: cancellation uses its bounded control
    // path, independently of invocation query capacity.
    let _slots = pool
        .call_query_slots
        .clone()
        .try_acquire_many_owned(15)
        .unwrap();
    let _ingress_slots = pool
        .role_query_slots
        .clone()
        .try_acquire_many_owned(pool.role_query_slots.available_permits() as u32)
        .unwrap();
    let _ingress_bytes = pool
        .role_query_bytes
        .clone()
        .try_acquire_many_owned(pool.role_query_bytes.available_permits() as u32)
        .unwrap();
    assert_eq!(pool.role_query_slots.available_permits(), 0);
    assert_eq!(pool.role_query_bytes.available_permits(), 0);
    let req = request(
        &platform,
        &ep,
        NativeCallAction::Cancel {
            cancellation: ServiceCancellation {
                contract_version: 1,
                target_instance: ServiceInstanceTarget {
                    node_id: "node".into(),
                    generation: "generation".into(),
                },
                operation: f.core.registry.capabilities()[0].operation.clone(),
                call_id: "call-one".into(),
                attempt: 1,
            },
        },
    );
    assert_eq!(
        exchange(router.clone(), req, signer.clone()).await,
        NativeCallResponse::Cancelled
    );
    assert_eq!(pending.await.unwrap(), NativeCallResponse::OutcomeUnknown);
    assert_eq!(f.budget.semaphore.available_permits(), 1);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    drop(_slots);
    drop(_ingress_slots);
    drop(_ingress_bytes);
    wait_until(|| pool.role_control_slots.available_permits() == 8).await;
    assert_eq!(pool.role_control_bytes.available_permits(), 512 * 1024);
    role.close().await.unwrap();
    assert!(!role.route_confirmed());
    assert!(pool
        .enable_sync_calls(&mut role, f.core.registry.clone(), f.budget.clone())
        .is_err());
    pool.close().await.unwrap();
    router.close().await.unwrap();
    f.core.connection.shutdown().await;
}

async fn setup(
    capacity: u32,
) -> (
    crate::services::execution::tests::Fixture,
    ServiceSigner,
    Arc<ChannelMessageSigner>,
    kish_lingshu_foundation_contract::service_transport::bootstrap::ChannelBootstrapResponse,
    ServiceEndpoint,
    ServiceChannelSessions,
    RegisteredChannelRole,
    zenoh::Session,
) {
    let f = fixture(capacity).await;
    let platform = ServiceSigner::new(&[73; 32]).unwrap();
    let key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
    let signer = Arc::new(ChannelMessageSigner::from_pkcs8(&key.serialize_der()).unwrap());
    let initial = authority(&platform, &signer);
    let ep = endpoint();
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let locator = format!("tcp/{}", reserve.local_addr().unwrap());
    drop(reserve);
    let router=zenoh::open(zenoh::Config::from_json5(&serde_json::json!({"mode":"router","listen":{"endpoints":[locator]},"scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}}}).to_string()).unwrap()).await.unwrap();
    let session=zenoh::open(zenoh::Config::from_json5(&serde_json::json!({"mode":"client","listen":{"endpoints":[]},"connect":{"endpoints":[locator]},"scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}}}).to_string()).unwrap()).await.unwrap();
    let pool = super::super::sessions::test_sync_pool(
        f.core.connection.clone(),
        initial.clone(),
        key,
        session,
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    let expires = chrono::Utc::now().timestamp_millis() + 30000;
    let listener = pool
        .install_role_declarations(&ep, "generation", expires, deadline, None, false)
        .await
        .unwrap();
    assert!(listener.connectivity_gate.confirm(0));
    let role=RegisteredChannelRole {response:ChannelRoleEnrollmentResponse::Call(ServiceEnrollmentResponseV2{enrollment_version:kish_lingshu_foundation_contract::service_transport::enrollment::EnrollmentVersion::V2,session:ServiceSession{instance:Some(initial.instance.clone()),node_id:"node".into(),generation:"generation".into(),credential:"fixture-only".into(),lease_expires_at_ms:expires,heartbeat_interval_ms:10000},endpoint:ep.clone()}),logical_key:"call:node".into(),catalog:None,endpoint:ep.clone(),stop:listener.stop,draining_handoff:listener.draining_handoff,task:Some(listener.task),role_deadline:deadline,connectivity_gate:listener.connectivity_gate,alive:listener.alive,cleanup_failed:false,channel:ClientChannelIdentity{application_id:initial.application_id.clone(),instance_id:RouteIdentity::new("sdk").unwrap(),base_generation:RouteIdentity::new("base").unwrap(),certificate_identity:initial.certificate.certificate_identity.clone()},remote_deregistered:false,lease_window:listener.lease_window,calls:listener.calls,call_contract:Some((f.core.registry.capabilities(),capacity)),
        #[cfg(feature="event-consumer-zenoh")] consumers:listener.consumers,
        #[cfg(feature="event-consumer-zenoh")] consumer_capacity:None,
    };
    (f, platform, signer, initial, ep, pool, role, router)
}

fn renewable(
    f: &crate::services::execution::tests::Fixture,
    call_id: &str,
    token: &str,
    hold: bool,
) -> ServiceInvocation {
    let mut input = invocation(f, CallMode::Async, serde_json::json!({"hold":hold}));
    // The invocation Query ends before the accepted execution/report windows.
    input.context.deadline_ms = chrono::Utc::now().timestamp_millis() + 15000;
    let InvocationRole::Call(call) = &mut input.context.invocation else {
        unreachable!()
    };
    call.call_id = call_id.into();
    input.completion = Some(CompletionTarget {
        token: token.into(),
        heartbeat: Some(CallHeartbeatPolicy {
            version: CALL_HEARTBEAT_VERSION,
            epoch: format!("epoch-{call_id}"),
            interval_ms: CALL_HEARTBEAT_INTERVAL_MS,
            execution_deadline_ms: input.context.deadline_ms,
            delivery_deadline_ms: input.context.deadline_ms + 10000,
        }),
    });
    input
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_binding_rejects_unreportable_calls_before_execution_and_joins_stop() {
    let (f, platform, signer, initial, ep, mut pool, mut role, router) = setup(1).await;
    assert!(pool
        .enable_async_calls(
            &mut role,
            f.core.registry.clone(),
            crate::ServiceExecutionBudget::new(2).unwrap()
        )
        .is_err());
    assert!(role.calls.lock().unwrap().is_none());
    pool.enable_async_calls(&mut role, f.core.registry.clone(), f.budget.clone())
        .unwrap();
    assert!(pool
        .enable_sync_calls(&mut role, f.core.registry.clone(), f.budget.clone())
        .is_err());
    assert!(pool
        .enable_async_calls(&mut role, f.core.registry.clone(), f.budget.clone())
        .is_err());
    let ServiceEndpoint::Zenoh { route, lanes, .. } = &ep else {
        unreachable!()
    };
    super::super::call_report::tests::propagated(&router, &route.invoke_key(&lanes[0]).unwrap())
        .await;
    for n in 0..7 {
        let mut input = renewable(&f, "call-one", "result-secret", true);
        match n {
            0 => input.completion = None,
            1 => input.completion.as_mut().unwrap().heartbeat = None,
            2 => {
                input
                    .completion
                    .as_mut()
                    .unwrap()
                    .heartbeat
                    .as_mut()
                    .unwrap()
                    .execution_deadline_ms += 1
            }
            3 => {
                input
                    .completion
                    .as_mut()
                    .unwrap()
                    .heartbeat
                    .as_mut()
                    .unwrap()
                    .delivery_deadline_ms = initial.certificate.expires_unix_ms + 1
            }
            4 => input.completion.as_mut().unwrap().token.clear(),
            5 => input.target_instance.as_mut().unwrap().generation = "foreign".into(),
            _ => {
                input
                    .completion
                    .as_mut()
                    .unwrap()
                    .heartbeat
                    .as_mut()
                    .unwrap()
                    .epoch = "*".into()
            }
        }
        let req = request(
            &platform,
            &ep,
            NativeCallAction::Invoke {
                invocation: input.into(),
            },
        );
        assert!(
            matches!(exchange(router.clone(), req, signer.clone()).await,
            NativeCallResponse::Rejected { error } if error.code == "invalid_native_report")
        );
    }
    assert_eq!(f.calls.load(Ordering::SeqCst), 0);
    assert_eq!(f.budget.semaphore.available_permits(), 1);
    let req = request(
        &platform,
        &ep,
        NativeCallAction::Invoke {
            invocation: renewable(&f, "call-one", "result-secret", true).into(),
        },
    );
    assert_eq!(
        exchange(router.clone(), req, signer.clone()).await,
        NativeCallResponse::Accepted {
            call_id: "call-one".into(),
            attempt: 1
        }
    );
    wait_until(|| f.calls.load(Ordering::SeqCst) == 1).await;
    let binding = role.calls.lock().unwrap().clone().unwrap();
    assert_eq!(binding.core.active.load(Ordering::SeqCst), 1);
    role.close().await.unwrap();
    assert_eq!(binding.core.active.load(Ordering::SeqCst), 0);
    assert_eq!(f.budget.semaphore.available_permits(), 1);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    pool.close().await.unwrap();
    router.close().await.unwrap();
    f.core.connection.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_role_pins_each_reporter_and_retains_capacity_until_same_result_ack() {
    use kish_lingshu_foundation_contract::{
        service_auth::verify_client_transport_message, service_transport::CallReportRoute,
    };
    use std::sync::atomic::AtomicUsize;
    let (f, platform, signer, initial, ep, mut pool, mut role, router) = setup(2).await;
    pool.enable_async_calls(&mut role, f.core.registry.clone(), f.budget.clone())
        .unwrap();
    let ServiceEndpoint::Zenoh { route, lanes, .. } = &ep else {
        unreachable!()
    };
    super::super::call_report::tests::propagated(&router, &route.invoke_key(&lanes[0]).unwrap())
        .await;
    let platform = Arc::new(platform);
    let completed = Arc::new(AtomicUsize::new(0));
    let report_gate = Arc::new(tokio::sync::Semaphore::new(0));
    let mut workers = Vec::new();
    for (call_id, token) in [
        ("call-one", "result-secret-A"),
        ("call-two", "result-secret-B"),
    ] {
        let input = renewable(&f, call_id, token, true);
        let heartbeat = input
            .completion
            .as_ref()
            .unwrap()
            .heartbeat
            .as_ref()
            .unwrap();
        let key = CallReportRoute {
            deployment: initial.deployment.clone(),
            application_id: initial.application_id.clone(),
            call_id: RouteIdentity::new(call_id).unwrap(),
            attempt: 1,
            epoch: RouteIdentity::new(heartbeat.epoch.clone()).unwrap(),
        }
        .key()
        .unwrap();
        let declared = router
            .declare_queryable(key.as_str().to_owned())
            .await
            .unwrap();
        super::super::call_report::tests::propagated(&pool.sessions[0], &key).await;
        let report_platform = platform.clone();
        let report_signer = signer.clone();
        let count = completed.clone();
        let gate = report_gate.clone();
        let subject = role.channel.clone();
        workers.push(tokio::spawn(async move {
            let mut first = None;
            while let Ok(query) = declared.recv_async().await {
                let request = TransportEnvelope::decode(
                    &query.payload().unwrap().to_bytes(),
                    &key,
                    &subject.application_id,
                    chrono::Utc::now().timestamp_millis(),
                )
                .unwrap();
                verify_client_transport_message(
                    report_signer.public_key(),
                    &subject,
                    &request.proof,
                    request.kind,
                    &request.target,
                    &request.request_id,
                    request.payload.get().as_bytes(),
                    chrono::Utc::now().timestamp_millis(),
                )
                .unwrap();
                let report: NativeCallReportRequest =
                    serde_json::from_str(request.payload.get()).unwrap();
                assert_eq!(report.token, token);
                assert_eq!(
                    report.instance,
                    ServiceInstanceTarget {
                        node_id: "node".into(),
                        generation: "generation".into()
                    }
                );
                let (output, done) = match report.action {
                    NativeCallReportAction::Heartbeat { heartbeat } => {
                        assert_eq!(heartbeat.call_id, call_id);
                        let now = chrono::Utc::now().timestamp_millis();
                        (
                            NativeCallReportResponse::Heartbeat {
                                disposition: HeartbeatDisposition::Renewed {
                                    store_now_ms: now,
                                    liveness_until_ms: now + CALL_LIVENESS_MS,
                                },
                            },
                            false,
                        )
                    }
                    NativeCallReportAction::Complete { completion } => {
                        assert_eq!(completion.call_id, call_id);
                        count.fetch_add(1, Ordering::SeqCst);
                        if let Some(original) = &first {
                            assert_eq!(original, &completion);
                            (
                                NativeCallReportResponse::Completed {
                                    disposition: CompletionDisposition::Duplicate,
                                },
                                true,
                            )
                        } else {
                            first = Some(completion);
                            gate.acquire().await.unwrap().forget();
                            // No first durable ACK reaches the SDK; retry only the result.
                            (
                                NativeCallReportResponse::Rejected {
                                    reason: NativeCallReportRejection::Unavailable,
                                },
                                false,
                            )
                        }
                    }
                };
                query
                    .reply(
                        key.as_str().to_owned(),
                        super::super::call_report::tests::reply(
                            &report_platform,
                            &request,
                            &output,
                        ),
                    )
                    .await
                    .unwrap();
                if done {
                    break;
                }
            }
        }));
        let req = request(
            &platform,
            &ep,
            NativeCallAction::Invoke {
                invocation: input.into(),
            },
        );
        assert_eq!(
            exchange(router.clone(), req, signer.clone()).await,
            NativeCallResponse::Accepted {
                call_id: call_id.into(),
                attempt: 1
            }
        );
    }
    wait_until(|| f.calls.load(Ordering::SeqCst) == 2).await;
    let binding = role.calls.lock().unwrap().clone().unwrap();
    // Expire only discovery while the independently scoped report session stays
    // authorized. Admission/declarations must disappear before either Handler
    // finishes, without cancelling their original attempts.
    role.lease_window.send_replace(RoleLeaseWindow {
        expires_at_ms: chrono::Utc::now().timestamp_millis(),
        deadline: Instant::now(),
    });
    wait_until(|| !*role.alive.borrow()).await;
    assert!(!role.route_confirmed());
    assert_eq!(binding.core.active.load(Ordering::SeqCst), 2);
    assert!(binding.core.live_instance().is_none());
    f.hold.add_permits(2);
    wait_until(|| completed.load(Ordering::SeqCst) == 2).await;
    assert_eq!(binding.core.active.load(Ordering::SeqCst), 2);
    assert_eq!(f.budget.semaphore.available_permits(), 0);
    // The original generation is no longer admissible even when its execution
    // core remains alive solely to deliver accepted results.
    assert!(binding
        .core
        .invoke(renewable(&f, "call-one", "result-secret-A", false))
        .await
        .is_err());
    assert_eq!(f.calls.load(Ordering::SeqCst), 2);
    report_gate.add_permits(2);
    wait_until(|| binding.core.active.load(Ordering::SeqCst) == 0).await;
    assert_eq!(completed.load(Ordering::SeqCst), 4);
    assert_eq!(binding.core.unconfirmed.load(Ordering::SeqCst), 0);
    assert_eq!(f.budget.semaphore.available_permits(), 2);
    assert_eq!(f.calls.load(Ordering::SeqCst), 2);
    for worker in workers {
        worker.await.unwrap();
    }
    role.close().await.unwrap();
    pool.close().await.unwrap();
    router.close().await.unwrap();
    f.core.connection.shutdown().await;
}
