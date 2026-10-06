#![cfg(feature = "sqlite")]

use chrono::{Duration, Utc};
use kish_lingshu_event_publication_sqlx::SqliteEventPublicationJournal;
use kish_lingshu_foundation_contract::MutationReceipt;
use kish_lingshu_sdk::event_dispatch::{
    DurableEventPublication, DynamicEvent, EventId, EventPublicationFailure,
    EventPublicationJournal, EventPublicationJournalState, EventPublicationScope, EventRoute,
    EventVisibility, PublishEvent, PublishReceipt,
};
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;

async fn journal() -> (sqlx::SqlitePool, SqliteEventPublicationJournal) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let journal = SqliteEventPublicationJournal::new(pool.clone(), scope());
    journal.migrate().await.unwrap();
    (pool, journal)
}

fn scope() -> EventPublicationScope {
    EventPublicationScope::new("orders-app", "checkout").unwrap()
}

fn publication(order_id: u64, key: &str) -> DurableEventPublication {
    DurableEventPublication::new(
        scope(),
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

#[tokio::test]
async fn failure_registration_commits_classification_and_recovers_only_retryable_rows() {
    let (pool, journal) = journal().await;
    let retryable = publication(1, "failed/retryable");
    let permanent = publication(2, "failed/permanent");
    let due = Utc::now() - Duration::seconds(1);
    assert_eq!(
        journal
            .record_failure_standalone(
                &retryable,
                &EventPublicationFailure::retryable("offline", Some("network failure".into())),
                Some(due)
            )
            .await
            .unwrap(),
        EventPublicationJournalState::Pending
    );
    let failure = EventPublicationFailure::permanent("forbidden", Some("access rejected".into()));
    assert_eq!(
        journal
            .record_failure_standalone(&permanent, &failure, Some(due))
            .await
            .unwrap(),
        EventPublicationJournalState::PermanentFailure(failure)
    );

    // Reconstruct the adapter to prove no process-local state drives recovery.
    let restarted = SqliteEventPublicationJournal::new(pool.clone(), scope());
    assert_eq!(
        restarted.load_due(Utc::now(), 10).await.unwrap(),
        vec![retryable]
    );
    let rows = sqlx::query_as::<_, (String, i64, Option<String>, String)>(
        "SELECT state, attempt_count, recover_after, last_failure_code FROM sdk_event_publication_record ORDER BY idempotency_key"
    ).fetch_all(&pool).await.unwrap();
    assert_eq!(
        rows[0],
        ("PERMANENT_FAILURE".into(), 1, None, "forbidden".into())
    );
    assert_eq!(rows[1].0, "RETRYABLE_FAILURE");
    assert_eq!(rows[1].1, 1);
    assert!(rows[1].2.is_some());
}

#[tokio::test]
async fn failure_registration_rolls_back_insert_when_transition_cannot_complete() {
    let (pool, journal) = journal().await;
    // Force an error after insertion, during the classification update.
    sqlx::query("CREATE TRIGGER reject_failure_update BEFORE UPDATE ON sdk_event_publication_record BEGIN SELECT RAISE(ABORT, 'simulated disk failure'); END")
        .execute(&pool).await.unwrap();
    let error = journal
        .record_failure_standalone(
            &publication(1, "atomic-failure"),
            &EventPublicationFailure::permanent("forbidden", None),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "failure_update_failure");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sdk_event_publication_record")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert!(journal
        .load_due(Utc::now() + Duration::days(1), 10)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn failure_registration_preserves_terminal_state_and_rejects_changed_content() {
    let (_pool, journal) = journal().await;
    let intent = publication(1, "same-key");
    let failure = EventPublicationFailure::retryable("offline", None);
    let terminal = EventPublicationFailure::permanent("forbidden", None);
    journal
        .record_failure_standalone(&intent, &failure, Some(Utc::now()))
        .await
        .unwrap();
    let accepted = receipt(42);
    journal.mark_accepted(&intent, &accepted).await.unwrap();
    assert_eq!(
        journal
            .record_failure_standalone(&intent, &terminal, None)
            .await
            .unwrap(),
        EventPublicationJournalState::Accepted(accepted)
    );
    let error = journal
        .record_failure_standalone(&publication(2, "same-key"), &failure, Some(Utc::now()))
        .await
        .unwrap_err();
    assert_eq!(error.code, "idempotency_conflict");
    let rejected = publication(3, "terminal-key");
    journal
        .record_failure_standalone(&rejected, &terminal, None)
        .await
        .unwrap();
    assert_eq!(
        journal
            .record_failure_standalone(&rejected, &failure, Some(Utc::now()))
            .await
            .unwrap(),
        EventPublicationJournalState::PermanentFailure(terminal)
    );
    assert!(journal
        .load_due(Utc::now() + Duration::days(1), 10)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn retryable_registration_requires_a_retry_time_without_leaving_an_intent() {
    let (pool, journal) = journal().await;
    let error = journal
        .record_failure_standalone(
            &publication(1, "missing-time"),
            &EventPublicationFailure::retryable("offline", None),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "missing_retry_time");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sdk_event_publication_record")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn shared_table_isolates_apps_publishers_and_conditional_transitions() {
    let (pool, first) = journal().await;
    let scopes = [
        scope(),
        EventPublicationScope::new("another-app", "checkout").unwrap(),
        EventPublicationScope::new("orders-app", "another-service").unwrap(),
    ];
    let journals = scopes
        .iter()
        .map(|scope| SqliteEventPublicationJournal::new(pool.clone(), scope.clone()))
        .collect::<Vec<_>>();
    let intents = scopes
        .iter()
        .enumerate()
        .map(|(i, scope)| {
            DurableEventPublication::new(
                scope.clone(),
                publication(i as u64, "same-key").event().clone(),
                "same-key",
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let due = Utc::now() - Duration::seconds(1);
    for (journal, intent) in journals.iter().zip(&intents) {
        journal
            .record_failure_standalone(
                intent,
                &EventPublicationFailure::retryable("offline", None),
                Some(due),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sdk_event_publication_record")
            .fetch_one(&pool)
            .await
            .unwrap(),
        3
    );
    for (journal, intent) in journals.iter().zip(&intents) {
        assert_eq!(
            journal.load_due(Utc::now(), 10).await.unwrap(),
            vec![intent.clone()]
        );
    }
    // A publication handle is not transferable between owners, even with a valid key.
    for journal in &journals[1..] {
        assert_eq!(
            journal
                .mark_accepted(&intents[0], &receipt(99))
                .await
                .unwrap_err()
                .code,
            "publication_scope_mismatch"
        );
        assert_eq!(
            journal
                .append_standalone(&intents[0], due)
                .await
                .unwrap_err()
                .code,
            "publication_scope_mismatch"
        );
        assert_eq!(
            journal
                .record_failure_standalone(
                    &intents[0],
                    &EventPublicationFailure::permanent("rejected", None),
                    None
                )
                .await
                .unwrap_err()
                .code,
            "publication_scope_mismatch"
        );
        assert_eq!(
            journal
                .mark_failed(
                    &intents[0],
                    &EventPublicationFailure::permanent("rejected", None),
                    None
                )
                .await
                .unwrap_err()
                .code,
            "publication_scope_mismatch"
        );
    }
    journals[0]
        .mark_accepted(&intents[0], &receipt(42))
        .await
        .unwrap();
    journals[1]
        .mark_failed(
            &intents[1],
            &EventPublicationFailure::permanent("rejected", None),
            None,
        )
        .await
        .unwrap();
    assert!(journals[0]
        .load_due(Utc::now(), 10)
        .await
        .unwrap()
        .is_empty());
    assert!(journals[1]
        .load_due(Utc::now(), 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        journals[2].load_due(Utc::now(), 10).await.unwrap(),
        vec![intents[2].clone()]
    );
    // A new instance of the same publisher observes the original terminal receipt.
    assert!(matches!(
        first.append_standalone(&intents[0], due).await.unwrap(),
        EventPublicationJournalState::Accepted(_)
    ));
}

#[tokio::test]
async fn unscoped_upgrade_requires_draining_and_preserves_terminal_history() {
    use sqlx::migrate::Migrator;
    use std::borrow::Cow;
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let all = sqlx::migrate!("./sql/sqlite");
    let old = Migrator {
        migrations: Cow::Owned(
            all.iter()
                .filter(|m| m.version < 202609200001)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    };
    old.run(&pool).await.unwrap();
    sqlx::query("INSERT INTO sdk_event_publication_record (idempotency_key,request_digest,topic,event_type,schema_version,event_json,state,recover_after,created_at,updated_at) VALUES ('old-key','digest','orders','created','1','{}','PENDING','2026-01-01','2026-01-01','2026-01-01')").execute(&pool).await.unwrap();
    let journal = SqliteEventPublicationJournal::new(pool.clone(), scope());
    assert_eq!(
        journal.migrate().await.unwrap_err().code,
        "unscoped_publications_pending"
    );
    let mut transaction = pool.begin().await.unwrap();
    assert!(sqlx::raw_sql(include_str!(
        "../sql/sqlite/202609200001_scope_publication_journal.sql"
    ))
    .execute(&mut *transaction)
    .await
    .is_err());
    transaction.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM _sqlx_migrations WHERE version=202609200001 OR success=0"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    sqlx::query("UPDATE sdk_event_publication_record SET state='PERMANENT_FAILURE',recover_after=NULL,last_failure_code='legacy-rejected'").execute(&pool).await.unwrap();
    journal.migrate().await.unwrap();
    assert_eq!(sqlx::query_as::<_,(String,String,String)>("SELECT application_id,publisher_id,last_failure_code FROM sdk_event_publication_record").fetch_one(&pool).await.unwrap(),(String::new(),String::new(),"legacy-rejected".into()));
    assert!(journal.load_due(Utc::now(), 10).await.unwrap().is_empty());
    assert!(EventPublicationScope::new("", "").is_err());
    journal
        .append_standalone(&publication(1, "old-key"), Utc::now())
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM sdk_event_publication_record")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn current_sqlite_snapshot_matches_forward_migrations() {
    let (migrated, _) = journal().await;
    let full = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../sql/full/sqlite.sql"))
        .execute(&full)
        .await
        .unwrap();
    let query = "SELECT type,name,sql FROM sqlite_master WHERE tbl_name='sdk_event_publication_record' AND sql IS NOT NULL ORDER BY type,name";
    let actual = sqlx::query_as::<_, (String, String, String)>(query)
        .fetch_all(&migrated)
        .await
        .unwrap();
    let expected = sqlx::query_as::<_, (String, String, String)>(query)
        .fetch_all(&full)
        .await
        .unwrap();
    assert_eq!(actual, expected);
}
