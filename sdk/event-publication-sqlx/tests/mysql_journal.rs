#![cfg(feature = "mysql")]

use chrono::{Duration, Utc};
use kish_lingshu_event_publication_sqlx::MySqlEventPublicationJournal;
use kish_lingshu_sdk::event_dispatch::{
    DurableEventPublication, DynamicEvent, EventPublicationFailure, EventPublicationJournal,
    EventPublicationScope, EventRoute, PublishEvent,
};
use sqlx::{
    migrate::Migrator,
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
};
use std::{borrow::Cow, str::FromStr};

#[tokio::test]
#[ignore = "requires disposable MySQL with EVENT_DISPATCH_TEST_MYSQL_URL and CREATE DATABASE permission"]
async fn mysql_upgrade_and_shared_journal_isolate_publication_owners() {
    let url =
        std::env::var("EVENT_DISPATCH_TEST_MYSQL_URL").expect("disposable MySQL URL required");
    let options = MySqlConnectOptions::from_str(&url).unwrap();
    let admin = MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await
        .unwrap();
    let database = format!(
        "sdk_journal_test_{}_{}",
        std::process::id(),
        Utc::now().timestamp_micros()
    );
    sqlx::query(&format!("CREATE DATABASE `{database}`"))
        .execute(&admin)
        .await
        .unwrap();
    let pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect_with(options.database(&database))
        .await
        .unwrap();
    let all = sqlx::migrate!("./sql/mysql");
    let previous = Migrator {
        migrations: Cow::Owned(
            all.iter()
                .filter(|m| m.version < 202609200001)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    };
    previous.run(&pool).await.unwrap();
    sqlx::query("INSERT INTO sdk_event_publication_record (idempotency_key,request_digest,topic,event_type,schema_version,event_json,state,recover_after,created_at,updated_at) VALUES ('legacy','digest','orders','created','1','{}','PENDING',NOW(6),NOW(6),NOW(6))").execute(&pool).await.unwrap();
    let owner = EventPublicationScope::new("app", "checkout").unwrap();
    let journal = MySqlEventPublicationJournal::new(pool.clone(), owner.clone());
    assert_eq!(
        journal.migrate().await.unwrap_err().code,
        "unscoped_publications_pending"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM _sqlx_migrations WHERE version=202609200001 OR success=0"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    sqlx::query("UPDATE sdk_event_publication_record SET state='PERMANENT_FAILURE',recover_after=NULL,last_failure_code='rejected'").execute(&pool).await.unwrap();
    journal.migrate().await.unwrap();
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT application_id,publisher_id FROM sdk_event_publication_record"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        (String::new(), String::new())
    );
    let mut entries = Vec::new();
    for (i, scope) in [
        owner,
        EventPublicationScope::new("other-app", "checkout").unwrap(),
        EventPublicationScope::new("app", "billing").unwrap(),
    ]
    .into_iter()
    .enumerate()
    {
        let journal = MySqlEventPublicationJournal::new(pool.clone(), scope.clone());
        let event = PublishEvent::dynamic(
            "fixture",
            DynamicEvent::new(
                EventRoute::new("orders", "created", "1").unwrap(),
                serde_json::json!({"order":i}),
            )
            .unwrap(),
        );
        let publication = DurableEventPublication::new(scope, event, "same-key").unwrap();
        journal
            .record_failure_standalone(
                &publication,
                &EventPublicationFailure::retryable("offline", None),
                Some(Utc::now() - Duration::seconds(1)),
            )
            .await
            .unwrap();
        entries.push((journal, publication));
    }
    for (journal, publication) in &entries {
        assert_eq!(
            journal.load_due(Utc::now(), 10).await.unwrap(),
            vec![publication.clone()]
        );
    }
    assert_eq!(
        entries[1]
            .0
            .mark_failed(
                &entries[0].1,
                &EventPublicationFailure::permanent("rejected", None),
                None
            )
            .await
            .unwrap_err()
            .code,
        "publication_scope_mismatch"
    );
    entries[0]
        .0
        .mark_failed(
            &entries[0].1,
            &EventPublicationFailure::permanent("rejected", None),
            None,
        )
        .await
        .unwrap();
    assert!(entries[0]
        .0
        .load_due(Utc::now(), 10)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        entries[1].0.load_due(Utc::now(), 10).await.unwrap().len(),
        1
    );
    assert_eq!(
        entries[2].0.load_due(Utc::now(), 10).await.unwrap().len(),
        1
    );
    pool.close().await;
    sqlx::query(&format!("DROP DATABASE `{database}`"))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
