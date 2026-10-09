//! Real Zenoh query/reply with fixture authority; no product Router/SQL claims.
use super::*;
use crate::service_channel::{call::tests::authority, sessions::test_sync_pool};
use crate::services::execution::tests::fixture;
use kish_lingshu_foundation_contract::{
    service_auth::{ChannelMessageSigner, ServiceSigner},
    service_transport::{
        channel::{ChannelEnrollmentError, ChannelEnrollmentRejection},
        MessageKind, RouteIdentity, TransportEnvelope,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enrollment_wait_requires_verified_reply_and_rejection_keeps_transport_open() {
    let f = fixture(1).await;
    let platform = ServiceSigner::new(&[73; 32]).unwrap();
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
    let signer = ChannelMessageSigner::from_pkcs8(&key.serialize_der()).unwrap();
    let initial = authority(&platform, &signer);
    let control = initial.control_route.key().unwrap();
    let session = zenoh::open(zenoh::Config::from_json5(r#"{"mode":"router","listen":{"endpoints":[]},"scouting":{"multicast":{"enabled":false},"gossip":{"enabled":false}}}"#).unwrap()).await.unwrap();
    let queryable = session
        .declare_queryable(control.as_str().to_owned())
        .await
        .unwrap();
    let mut pool = test_sync_pool(f.core.connection.clone(), initial, key, session);
    let worker = tokio::spawn(async move {
        for case in 0..4 {
            let query = queryable.recv_async().await.unwrap();
            let now = chrono::Utc::now().timestamp_millis();
            let request = TransportEnvelope::decode(
                &query.payload().unwrap().to_bytes(),
                &control,
                &RouteIdentity::new("app").unwrap(),
                now,
            )
            .unwrap();
            assert_eq!(request.kind, MessageKind::Register);
            let payload = serde_json::value::to_raw_value(&ChannelEnrollmentRejection {
                enrollment_error: if case == 3 {
                    ChannelEnrollmentError::Rejected
                } else {
                    ChannelEnrollmentError::CatalogNotReady
                },
            })
            .unwrap();
            let mut response = request;
            response.payload = payload;
            if case == 2 {
                response.request_id = RouteIdentity::new("foreign-request").unwrap();
            }
            response.proof = platform
                .sign_transport_message(
                    "app",
                    response.kind,
                    &control,
                    &response.request_id,
                    response.payload.get().as_bytes(),
                    now,
                    response.deadline_unix_ms,
                )
                .unwrap();
            if case == 1 {
                response.proof = "untrusted".into();
            }
            query
                .reply(control.as_str().to_owned(), response.encode(now).unwrap())
                .await
                .unwrap();
        }
    });
    for expected in [
        ChannelSessionError::CatalogNotReady,
        ChannelSessionError::InvalidResponse,
        ChannelSessionError::InvalidResponse,
        ChannelSessionError::ControlRejected,
    ] {
        assert_eq!(
            pool.register_role(kish_lingshu_runtime_contract::service::ChannelRoleEnrollment::Consumer(
                kish_lingshu_event_dispatch_contract::ConsumerEnrollmentRequestV2 {
                    enrollment_version: kish_lingshu_foundation_contract::service_transport::enrollment::EnrollmentVersion::V2,
                    instance: kish_lingshu_foundation_contract::ServiceInstanceRegistration {
                        instance_id: "sdk".into(), incarnation_id: "boot".into(), generation: Some("base".into()),
                    },
                    group_key: "new-group".into(), node_id: "replica".into(), maximum_in_flight: 1,
                    endpoint: kish_lingshu_foundation_contract::service_transport::enrollment::RequestedServiceEndpoint::Zenoh {
                        protocol_version: kish_lingshu_foundation_contract::service_transport::ProtocolVersion::V1,
                        lane_count: Default::default(),
                    },
                }
            )).await
                .err()
                .unwrap(),
            expected
        );
        assert!(
            pool.active_role_channel().is_ok(),
            "one rejected role must not close sibling transport"
        );
    }
    worker.await.unwrap();
    pool.close().await.unwrap();
    f.core.connection.shutdown().await;
}
