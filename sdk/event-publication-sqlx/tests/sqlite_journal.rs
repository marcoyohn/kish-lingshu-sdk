#![cfg(feature = "sqlite")]

use chrono::{Duration, Utc};
use kish_lingshu_event_publication_sqlx::SqliteEventPublicationJournal;
use kish_lingshu_foundation_contract::MutationReceipt;
use kish_lingshu_sdk::event_dispatch::{
    DurableEventPublication, DynamicEvent, EventId, EventPublicationFailure,
    EventPublicationJournal, EventPublicationJournalState, EventRoute, EventVisibility,
    PublishEvent, PublishReceipt,
};
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;

async fn journal() -> (sqlx::SqlitePool, SqliteEventPublicationJournal) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let journal = SqliteEventPublicationJournal::new(pool.clone());
    journal.migrate().await.unwrap();
    (pool, journal)
}

fn publication(order_id: u64, key: &str) -> DurableEventPublication {
    DurableEventPublication::new(
        PublishEvent::dynamic(
            "checkout",
            DynamicEvent::new(
                EventRoute::new("orders", "order.created", "1").unwrap(),
                json!({"order_id": order_id}),
            )
            .unwrap(),
        ),
        key,
    )
    .unwrap()
}

fn receipt(event_id: u64) -> PublishReceipt {
    let now = Utc::now();
    PublishReceipt {
        mutation: MutationReceipt::accepted(format!("request-{event_id}"), now),
        event_id: EventId::new(event_id).unwrap(),
        published_at: now,
        visible_at: now,
        visibility: EventVisibility::Ready,
    }
}

#[tokio::test]
async fn migration_is_versioned_and_creates_the_recovery_index() {
    let (pool, journal) = journal().await;
    journal.migrate().await.unwrap();

    let version = sqlx::query_scalar::<_, i64>(
        "SELECT version FROM _sqlx_migrations WHERE version = 202609070001",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let index_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pragma_index_list('sdk_event_publication_record') \
         WHERE name = 'idx_sdk_event_publication_recovery'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(version, 202609070001);
    assert_eq!(index_count, 1);
}

#[tokio::test]
async fn transaction_append_rolls_back_and_commits_with_business_state() {
    let (pool, journal) = journal().await;
    let intent = publication(1, "orders/1/transactional");
    let recover_after = Utc::now() + Duration::minutes(1);

    let mut rolled_back = pool.begin().await.unwrap();
    journal
        .append(&mut rolled_back, &intent, recover_after)
        .await
        .unwrap();
    rolled_back.rollback().await.unwrap();
    let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sdk_event_publication_record")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);

    let mut committed = pool.begin().await.unwrap();
    journal
        .append(&mut committed, &intent, recover_after)
        .await
        .unwrap();
    committed.commit().await.unwrap();
    let count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sdk_event_publication_record")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn standalone_journal_recovers_and_transitions_monotonically() {
    let (pool, journal) = journal().await;
    let intent = publication(1, "orders/1/direct");
    let initial_due = Utc::now() + Duration::minutes(1);

    let state = journal
        .append_standalone(&intent, initial_due)
        .await
        .unwrap();
    assert_eq!(state, EventPublicationJournalState::Pending);

    let retry_at = Utc::now() - Duration::seconds(1);
    assert!(journal
        .mark_failed(
            &intent,
            &EventPublicationFailure::retryable("network_unavailable", Some("offline".into())),
            Some(retry_at),
        )
        .await
        .unwrap());
    let due = journal.load_due(Utc::now(), 10).await.unwrap();
    assert_eq!(due, vec![intent.clone()]);

    let accepted = receipt(41);
    assert!(journal.mark_accepted(&intent, &accepted).await.unwrap());
    assert!(journal.load_due(Utc::now(), 10).await.unwrap().is_empty());
    assert!(!journal
        .mark_failed(
            &intent,
            &EventPublicationFailure::retryable("late_failure", None),
            Some(Utc::now()),
        )
        .await
        .unwrap());

    let replay = journal
        .append_standalone(&intent, Utc::now())
        .await
        .unwrap();
    assert_eq!(
        replay,
        EventPublicationJournalState::Accepted(accepted.clone())
    );
    let row = sqlx::query_as::<_, (String, i64, Option<String>)>(
        "SELECT state, attempt_count, recover_after \
         FROM sdk_event_publication_record WHERE idempotency_key = ?",
    )
    .bind(intent.idempotency_key())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row, ("ACCEPTED".to_string(), 2, None));
}

#[tokio::test]
async fn same_key_with_changed_event_is_rejected() {
    let (_pool, journal) = journal().await;
    let first = publication(1, "orders/shared-key");
    let changed = publication(2, "orders/shared-key");
    journal.append_standalone(&first, Utc::now()).await.unwrap();

    let error = journal
        .append_standalone(&changed, Utc::now())
        .await
        .unwrap_err();
    assert_eq!(error.code, "idempotency_conflict");
}
