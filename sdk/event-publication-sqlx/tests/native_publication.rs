#![cfg(feature = "native-acceptance")]
//! Licensed Router + real producer SQL journal. No application inbound listener.
use kish_lingshu_event_publication_sqlx::SqliteEventPublicationJournal;
use kish_lingshu_foundation_contract::ServiceInstanceRegistration;
use kish_lingshu_sdk::service_channel::{ChannelSessionConfig, ServiceChannelSessions};
use kish_lingshu_sdk::{
    event_dispatch::{
        DynamicEvent, EventDispatch, EventPublicationJournal, EventPublicationOutcome,
        EventPublicationPolicyCatalog, EventPublicationReliability, EventPublicationRoute,
        EventPublicationScope, EventRoute, PublishEvent, ReliablePublicationConfig,
        ReliablePublicationError,
    },
    ClientBuilder, ClientConfig, MutationOptions, ServiceConnection, ServiceCredential,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

fn credential() -> ServiceCredential {
    ServiceCredential::new(
        std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap(),
        std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap(),
    )
    .unwrap()
}
fn policies(level: EventPublicationReliability) -> EventPublicationPolicyCatalog {
    EventPublicationPolicyCatalog::new([(
        EventPublicationRoute::new("native.publications", "publication.created").unwrap(),
        level,
    )])
    .unwrap()
}
fn event(valid: bool) -> PublishEvent {
    PublishEvent::dynamic(
        "native-journal-fixture",
        DynamicEvent::new(
            EventRoute::new("native.publications", "publication.created", "1").unwrap(),
            if valid {
                json!({"value": 1})
            } else {
                json!({"value": "invalid"})
            },
        )
        .unwrap(),
    )
}
async fn count(pool: &sqlx::SqlitePool, owner: &str, state: &str) -> i64 {
    sqlx::query_scalar(
        "select count(*) from sdk_event_publication_record where publisher_id=? and state=?",
    )
    .bind(owner)
    .bind(state)
    .fetch_one(pool)
    .await
    .unwrap()
}
async fn pool(connection: &ServiceConnection, instance: &str) -> ServiceChannelSessions {
    connection
        .bootstrap_channel(
            ServiceInstanceRegistration {
                instance_id: instance.into(),
                incarnation_id: format!("{instance}-boot"),
                generation: None,
            },
            Some(
                kish_lingshu_foundation_contract::service_transport::RouteIdentity::new("dev")
                    .unwrap(),
            ),
        )
        .await
        .unwrap()
        .open_sessions(ChannelSessionConfig::default())
        .await
        .unwrap()
}
async fn catalog(url: &str) {
    let client = reqwest::Client::new();
    let post = |path: String, body: serde_json::Value, key: &str| {
        client
            .post(format!("{url}/api/user/event-dispatch/v1{path}"))
            .header(
                "x-token",
                std::env::var("LINGSHU_EVENT_TEST_TOKEN").unwrap(),
            )
            .header(
                "x-kish-app-id",
                std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap(),
            )
            .header("Idempotency-Key", key)
            .json(&body)
            .send()
    };
    let response = post("/topics".into(), json!({"topic":"native.publications", "name":"Native Publications", "partition_count":1,"consumption_order":"unordered","state":"active"}), "native-journal-topic").await.unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "{status}: {body}");
    let topic: serde_json::Value = serde_json::from_str(&body).unwrap();
    let response = post(format!("/topics/{}/events", topic["topic_id"].as_u64().unwrap()),
        json!({"event_type":"publication.created","schema_version":"1","payload_schema":{"type":"object","required":["value"],"properties":{"value":{"type":"integer"}}},"state":"active"}), "native-journal-schema").await.unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "{status}: {body}");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "licensed native host; run zenss_channel_bootstrap_acceptance.py --event-test"]
async fn native_publication_policies_preserve_sql_ownership_and_external_recovery() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let url = std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap();
    let app = std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap();
    catalog(&url).await;
    let connection = ServiceConnection::connect(&url, credential())
        .await
        .unwrap();
    let mut sessions = pool(&connection, "native-journal-first").await;
    let client = ClientBuilder::new(ClientConfig::new(&url).with_retry_limit(0))
        .service_credential(credential())
        .connect()
        .unwrap();
    let dispatch = client.event_dispatch().with_channel(&sessions).unwrap();
    let journal_pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!(
            "sqlite://{}?mode=rwc",
            std::env::var("LINGSHU_EVENT_JOURNAL_PATH").unwrap()
        ))
        .await
        .unwrap();
    let scope = |publisher: &str| EventPublicationScope::new(&app, publisher).unwrap();
    let journal = |publisher: &str| {
        Arc::new(SqliteEventPublicationJournal::new(
            journal_pool.clone(),
            scope(publisher),
        ))
    };
    journal("native-confirmed").migrate().await.unwrap();
    let config = ReliablePublicationConfig::default();
    let cases = [
        ("native-confirmed", EventPublicationReliability::Confirmed),
        (
            "native-send-first",
            EventPublicationReliability::PersistOnFailure,
        ),
        ("native-durable", EventPublicationReliability::Durable),
    ];
    let mut ids = Vec::new();
    for (owner, level) in cases {
        let publisher =
            dispatch.reliable_publisher(policies(level), config.clone(), journal(owner));
        let EventPublicationOutcome::Accepted(receipt) = publisher
            .publish(event(true), MutationOptions::new("shared-key").unwrap())
            .await
            .unwrap()
        else {
            panic!("custody not confirmed")
        };
        ids.push(receipt.event_id);
        assert_eq!(
            count(&journal_pool, owner, "ACCEPTED").await,
            i64::from(level == EventPublicationReliability::Durable)
        );
    }
    assert!(ids[0] != ids[1] && ids[1] != ids[2] && ids[0] != ids[2]);
    let wrong_app = Arc::new(SqliteEventPublicationJournal::new(
        journal_pool.clone(),
        EventPublicationScope::new("foreign-app", "native-durable").unwrap(),
    ));
    assert!(dispatch
        .reliable_publisher(
            policies(EventPublicationReliability::Durable),
            config.clone(),
            wrong_app
        )
        .publish(event(true), MutationOptions::new("foreign").unwrap())
        .await
        .is_err());
    let permanent = dispatch.reliable_publisher(
        policies(EventPublicationReliability::PersistOnFailure),
        config.clone(),
        journal("native-permanent"),
    );
    assert!(matches!(
        permanent
            .publish(
                event(false),
                MutationOptions::new("schema-invalid").unwrap()
            )
            .await
            .unwrap(),
        EventPublicationOutcome::PermanentFailure(_)
    ));
    assert_eq!(
        count(&journal_pool, "native-permanent", "PERMANENT_FAILURE").await,
        1
    );
    assert!(journal("native-permanent")
        .load_due(chrono::Utc::now() + chrono::Duration::hours(1), 10)
        .await
        .unwrap()
        .is_empty());
    sessions.close().await.unwrap();
    for (owner, level) in cases {
        let publisher =
            dispatch.reliable_publisher(policies(level), config.clone(), journal(owner));
        let result = publisher
            .publish(event(true), MutationOptions::new("offline").unwrap())
            .await;
        if level == EventPublicationReliability::Confirmed {
            assert!(result.is_err());
        } else {
            assert!(matches!(
                result.unwrap(),
                EventPublicationOutcome::RetryScheduled(_)
            ));
        }
        assert_eq!(
            count(&journal_pool, owner, "RETRYABLE_FAILURE").await,
            i64::from(level != EventPublicationReliability::Confirmed)
        );
    }
    // A failed failure-registration transaction must never claim durable retry.
    sqlx::query("create trigger reject_fixture_journal before insert on sdk_event_publication_record when new.publisher_id='native-store-failure' begin select raise(ABORT,'fixture journal unavailable'); end")
        .execute(&journal_pool).await.unwrap();
    let failed_store = dispatch.reliable_publisher(
        policies(EventPublicationReliability::PersistOnFailure),
        config.clone(),
        journal("native-store-failure"),
    );
    assert!(matches!(
        failed_store
            .publish(event(true), MutationOptions::new("offline").unwrap())
            .await,
        Err(ReliablePublicationError::FailureNotPersisted { .. })
    ));
    sqlx::query(
        "update sdk_event_publication_record set recover_after=? where state='RETRYABLE_FAILURE'",
    )
    .bind((chrono::Utc::now() - chrono::Duration::seconds(1)).naive_utc())
    .execute(&journal_pool)
    .await
    .unwrap();
    let mut replacement = pool(&connection, "native-journal-first").await;
    let fresh: EventDispatch = client.event_dispatch().with_channel(&replacement).unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    // Opening/reconnecting a channel never scans a journal or resends an Event.
    assert_eq!(
        count(&journal_pool, "native-send-first", "RETRYABLE_FAILURE").await,
        1
    );
    assert_eq!(
        count(&journal_pool, "native-durable", "RETRYABLE_FAILURE").await,
        1
    );
    assert!(journal("independent-publisher")
        .load_due(chrono::Utc::now(), 10)
        .await
        .unwrap()
        .is_empty());
    for (owner, level) in cases.into_iter().skip(1) {
        // Reconstruct from durable SQL; the external call starts the sole pass.
        let publisher = fresh.reliable_publisher(policies(level), config.clone(), journal(owner));
        let recovered = publisher.recover_due().await.unwrap();
        assert_eq!((recovered.scanned, recovered.accepted), (1, 1));
        assert_eq!(publisher.recover_due().await.unwrap().scanned, 0);
        assert_eq!(
            count(&journal_pool, owner, "ACCEPTED").await,
            if level == EventPublicationReliability::Durable {
                2
            } else {
                1
            }
        );
    }
    replacement.close().await.unwrap();
    connection.shutdown().await;
    journal_pool.close().await;
}
