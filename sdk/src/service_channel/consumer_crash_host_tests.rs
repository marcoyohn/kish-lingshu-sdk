//! SIGKILL acceptance driven externally; this module never retries a Handler.
use super::*;
use std::{path::PathBuf, sync::atomic::AtomicBool};

pub(super) struct CrashHandler {
    replacement: bool,
    markers: PathBuf,
    effect_url: String,
    completed: AtomicBool,
}
impl CrashHandler {
    pub(super) async fn consume(
        &self,
        context: EventContext,
        event: Order,
    ) -> Result<Value, ConsumerError> {
        let identity = json!({"event_id":context.event_id(),"idempotency_key":context.idempotency_key(),
            "attempt":context.attempt_generation(),"sequence":event.sequence});
        let effect: Value = reqwest::Client::new()
            .post(&self.effect_url)
            .json(&identity)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(effect["event_id"], identity["event_id"]);
        assert_eq!(effect["idempotency_key"], identity["idempotency_key"]);
        if self.replacement {
            assert_eq!(
                effect["applied"], false,
                "replacement must observe the original committed effect"
            );
            assert!(context.attempt_generation() > 1);
            std::fs::write(
                self.markers.join("replacement-effect.json"),
                serde_json::to_vec(&identity).unwrap(),
            )
            .unwrap();
            self.completed.store(true, Ordering::SeqCst);
            Ok(json!({"sequence":event.sequence}))
        } else {
            assert_eq!(effect["applied"], true);
            assert_eq!(context.attempt_generation(), 1);
            std::fs::write(
                self.markers.join("original-effect.json"),
                serde_json::to_vec(&identity).unwrap(),
            )
            .unwrap();
            // The external driver kills this process before the original Invoke
            // deadline. No acknowledgement is emitted for the committed effect.
            std::future::pending().await
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "real SDK SIGKILL; run multirouter --sdk-crash-test"]
async fn native_sdk_process_crash_after_effect_requires_explicit_incarnation_and_dispatch_recovery()
{
    let _ = rustls::crypto::ring::default_provider().install_default();
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    let phase = std::env::var("LINGSHU_SDK_CRASH_PHASE").unwrap();
    assert!(matches!(phase.as_str(), "original" | "replacement"));
    let replacement = phase == "replacement";
    let markers = PathBuf::from(std::env::var("LINGSHU_SDK_CRASH_MARKERS").unwrap());
    if !replacement {
        governance(&url, 2).await;
    }
    let credential = || {
        ServiceCredential::new(&app, std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap()).unwrap()
    };
    let connection = ServiceConnection::connect(&url, credential())
        .await
        .unwrap();
    let pool = connection
        .bootstrap_channel(
            ServiceInstanceRegistration {
                instance_id: "native-crash-sdk".into(),
                incarnation_id: format!("crash-{phase}"),
                generation: None,
            },
            Some(RouteIdentity::new("dev").unwrap()),
        )
        .await
        .unwrap()
        .open_sessions(ChannelSessionConfig::host_test())
        .await
        .unwrap();
    let base = pool
        .identity()
        .bootstrap_response()
        .instance
        .generation
        .clone();
    let handler = Arc::new(CrashHandler {
        replacement,
        markers: markers.clone(),
        effect_url: std::env::var("LINGSHU_SDK_CRASH_EFFECT_URL").unwrap(),
        completed: AtomicBool::new(false),
    });
    let handlers = Arc::new(Handlers {
        crash: Some(handler.clone()),
        ..Handlers::default()
    });
    let mut role = pool
        .register_consumer_role("native-good", "crash-sdk-good", 1)
        .await
        .unwrap();
    pool.enable_sync_consumers(
        &mut role,
        registry(&app, handlers),
        ServiceExecutionBudget::new(1).unwrap(),
    )
    .unwrap();
    pool.confirm_role_route(&mut role).await.unwrap();
    let generation = role
        .lifecycle_status(pool.authorization_deadline())
        .role_generation;
    let mut managed = pool.manage_roles(vec![role]).unwrap();
    std::fs::write(
        markers.join(format!("{phase}-identity.json")),
        serde_json::to_vec(&json!({
            "instance_id":"native-crash-sdk", "base_generation":base, "role_generation":generation,
            "physical_sessions":managed.rotation_status().session_ids.len(),
        }))
        .unwrap(),
    )
    .unwrap();
    if !replacement {
        let client = ClientBuilder::new(ClientConfig::new(&url).with_retry_limit(1))
            .service_credential(credential())
            .connect()
            .unwrap();
        let dispatch = client
            .event_dispatch()
            .with_managed_channel(&managed)
            .unwrap();
        let event = PublishEvent::typed(
            "native-fixture",
            &Order {
                sequence: 1,
                fail_once: false,
            },
        )
        .unwrap();
        let receipt = dispatch
            .publish(event, MutationOptions::new("native-crash/one").unwrap())
            .await
            .unwrap();
        std::fs::write(
            markers.join("publication.json"),
            serde_json::to_vec(&json!({"event_id":receipt.event_id.get()})).unwrap(),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(30), std::future::pending::<()>())
            .await
            .expect("driver must SIGKILL this SDK");
    } else {
        wait_for(|| handler.completed.load(Ordering::SeqCst)).await;
        tokio::time::timeout(Duration::from_secs(20), async {
            while !markers.join("center-confirmed").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("external driver must verify durable Dispatch confirmation");
        managed.close().await.unwrap();
    }
}
