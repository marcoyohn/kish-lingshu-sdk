use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use kish_lingshu_runtime_contract::{
    test_support::ContractProductRuntimeFixture, ApplicationProblem, ApplicationResult,
    EventPublisher as RuntimeEventPublisher, IdempotencyKey, InvocationSource, MutationDisposition,
    PrincipalKind, ProblemCode, ProductRuntimeFacade, RequestContext, TrustedContextFactory,
    UNAVAILABLE_PROBLEM,
};
use kish_lingshu_sdk::{
    event_dispatch::{
        DurableEventPublication, DynamicEvent, EventPublicationFailure,
        EventPublicationFailureKind, EventPublicationJournal, EventPublicationJournalError,
        EventPublicationJournalState, EventPublicationOutcome, EventPublicationPolicyCatalog,
        EventPublicationReliability, EventPublicationRoute, EventRoute, PublishEvent,
        PublishReceipt, ReliableEventPublisher, ReliablePublicationConfig,
        ReliablePublicationError,
    },
    ClientBuilder, ClientConfig, MutationOptions, ServiceCredential,
};
use serde_json::json;

#[derive(Default)]
struct FakeTransaction {
    staged: Vec<(DurableEventPublication, DateTime<Utc>)>,
}

#[derive(Clone)]
struct FakeRecord {
    publication: DurableEventPublication,
    recover_after: Option<DateTime<Utc>>,
    state: FakeState,
}

#[derive(Clone)]
enum FakeState {
    Pending,
    Accepted(PublishReceipt),
    Retryable(EventPublicationFailure),
    Permanent(EventPublicationFailure),
}

#[derive(Default)]
struct FakeJournal {
    records: Mutex<BTreeMap<String, FakeRecord>>,
    fail_next_accept: AtomicBool,
    load_barrier: Mutex<Option<Arc<tokio::sync::Barrier>>>,
}

impl FakeJournal {
    fn commit(&self, transaction: FakeTransaction) -> Result<(), EventPublicationJournalError> {
        let mut records = self.records.lock().unwrap();
        for (publication, recover_after) in transaction.staged {
            match records.get(publication.idempotency_key()) {
                Some(existing)
                    if existing.publication.request_digest() == publication.request_digest() => {}
                Some(_) => {
                    return Err(EventPublicationJournalError::new(
                        "idempotency_conflict",
                        "idempotency key is bound to another request digest",
                    ));
                }
                None => {
                    records.insert(
                        publication.idempotency_key().to_string(),
                        FakeRecord {
                            publication,
                            recover_after: Some(recover_after),
                            state: FakeState::Pending,
                        },
                    );
                }
            }
        }
        Ok(())
    }

    fn make_all_due(&self) {
        let due = Utc::now() - TimeDelta::seconds(1);
        for record in self.records.lock().unwrap().values_mut() {
            if matches!(record.state, FakeState::Pending | FakeState::Retryable(_)) {
                record.recover_after = Some(due);
            }
        }
    }

    fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }

    fn accepted_receipt(&self, idempotency_key: &str) -> Option<PublishReceipt> {
        self.records
            .lock()
            .unwrap()
            .get(idempotency_key)
            .and_then(|record| match &record.state {
                FakeState::Accepted(receipt) => Some(receipt.clone()),
                _ => None,
            })
    }

    fn has_retryable_failure(&self, idempotency_key: &str) -> bool {
        self.records
            .lock()
            .unwrap()
            .get(idempotency_key)
            .is_some_and(|record| matches!(record.state, FakeState::Retryable(_)))
    }

    fn fail_next_accept(&self) {
        self.fail_next_accept.store(true, Ordering::Release);
    }

    fn synchronize_next_two_loads(&self) {
        *self.load_barrier.lock().unwrap() = Some(Arc::new(tokio::sync::Barrier::new(2)));
    }

    fn state(record: &FakeRecord) -> EventPublicationJournalState {
        match &record.state {
            FakeState::Pending | FakeState::Retryable(_) => EventPublicationJournalState::Pending,
            FakeState::Accepted(receipt) => EventPublicationJournalState::Accepted(receipt.clone()),
            FakeState::Permanent(failure) => {
                EventPublicationJournalState::PermanentFailure(failure.clone())
            }
        }
    }
}

struct RetryableFailingEventPublisher;

#[async_trait]
impl RuntimeEventPublisher for RetryableFailingEventPublisher {
    async fn publish(
        &self,
        context: RequestContext,
        _event: PublishEvent,
        _idempotency_key: IdempotencyKey,
    ) -> ApplicationResult<PublishReceipt> {
        Err(ApplicationProblem::new(
            ProblemCode::known(UNAVAILABLE_PROBLEM),
            "Event Dispatch is temporarily unavailable",
            context.request_id().clone(),
        )
        .with_retryable(true))
    }

    async fn get(
        &self,
        _context: RequestContext,
        _event_id: kish_lingshu_sdk::event_dispatch::EventId,
    ) -> ApplicationResult<Option<kish_lingshu_sdk::event_dispatch::EventRecord>> {
        Ok(None)
    }
}

#[async_trait]
impl EventPublicationJournal for FakeJournal {
    type Transaction<'transaction> = FakeTransaction;

    async fn append_standalone(
        &self,
        publication: &DurableEventPublication,
        recover_after: DateTime<Utc>,
    ) -> Result<EventPublicationJournalState, EventPublicationJournalError> {
        let mut transaction = FakeTransaction::default();
        let state = self
            .append(&mut transaction, publication, recover_after)
            .await?;
        self.commit(transaction)?;
        Ok(state)
    }

    async fn append(
        &self,
        transaction: &mut Self::Transaction<'_>,
        publication: &DurableEventPublication,
        recover_after: DateTime<Utc>,
    ) -> Result<EventPublicationJournalState, EventPublicationJournalError> {
        if let Some(existing) = transaction
            .staged
            .iter()
            .find(|(existing, _)| existing.idempotency_key() == publication.idempotency_key())
            .map(|(existing, _)| existing)
        {
            if existing.request_digest() == publication.request_digest() {
                return Ok(EventPublicationJournalState::Pending);
            }
            return Err(EventPublicationJournalError::new(
                "idempotency_conflict",
                "idempotency key is bound to another staged request digest",
            ));
        }
        if let Some(existing) = self
            .records
            .lock()
            .unwrap()
            .get(publication.idempotency_key())
        {
            if existing.publication.request_digest() == publication.request_digest() {
                return Ok(Self::state(existing));
            }
            return Err(EventPublicationJournalError::new(
                "idempotency_conflict",
                "idempotency key is bound to another committed request digest",
            ));
        }
        transaction
            .staged
            .push((publication.clone(), recover_after));
        Ok(EventPublicationJournalState::Pending)
    }

    async fn load_due(
        &self,
        due_at: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<DurableEventPublication>, EventPublicationJournalError> {
        let mut due = self
            .records
            .lock()
            .unwrap()
            .values()
            .filter_map(|record| {
                record
                    .recover_after
                    .filter(|recover_after| recover_after <= &due_at)
                    .filter(|_| {
                        matches!(record.state, FakeState::Pending | FakeState::Retryable(_))
                    })
                    .map(|recover_after| {
                        (
                            recover_after,
                            record.publication.idempotency_key().to_string(),
                            record.publication.clone(),
                        )
                    })
            })
            .collect::<Vec<_>>();
        due.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
        due.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        let publications = due
            .into_iter()
            .map(|(_, _, publication)| publication)
            .collect();
        let barrier = self.load_barrier.lock().unwrap().clone();
        if let Some(barrier) = barrier {
            barrier.wait().await;
        }
        Ok(publications)
    }

    async fn mark_accepted(
        &self,
        publication: &DurableEventPublication,
        receipt: &PublishReceipt,
    ) -> Result<bool, EventPublicationJournalError> {
        if self.fail_next_accept.swap(false, Ordering::AcqRel) {
            return Err(EventPublicationJournalError::new(
                "simulated_process_exit",
                "acceptance state was not recorded",
            ));
        }
        let mut records = self.records.lock().unwrap();
        let record = exact_record(&mut records, publication)?;
        if !matches!(record.state, FakeState::Pending | FakeState::Retryable(_)) {
            return Ok(false);
        }
        record.state = FakeState::Accepted(receipt.clone());
        record.recover_after = None;
        Ok(true)
    }

    async fn mark_failed(
        &self,
        publication: &DurableEventPublication,
        failure: &EventPublicationFailure,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<bool, EventPublicationJournalError> {
        let mut records = self.records.lock().unwrap();
        let record = exact_record(&mut records, publication)?;
        if !matches!(record.state, FakeState::Pending | FakeState::Retryable(_)) {
            return Ok(false);
        }
        match failure.kind() {
            EventPublicationFailureKind::Retryable => {
                let retry_at = retry_at.ok_or_else(|| {
                    EventPublicationJournalError::new(
                        "missing_retry_time",
                        "retryable failure requires a retry time",
                    )
                })?;
                record.state = FakeState::Retryable(failure.clone());
                record.recover_after = Some(retry_at);
            }
            EventPublicationFailureKind::Permanent => {
                record.state = FakeState::Permanent(failure.clone());
                record.recover_after = None;
            }
        }
        Ok(true)
    }
}

fn exact_record<'a>(
    records: &'a mut BTreeMap<String, FakeRecord>,
    publication: &DurableEventPublication,
) -> Result<&'a mut FakeRecord, EventPublicationJournalError> {
    let record = records
        .get_mut(publication.idempotency_key())
        .ok_or_else(|| EventPublicationJournalError::new("missing_record", "record not found"))?;
    if record.publication.request_digest() != publication.request_digest() {
        return Err(EventPublicationJournalError::new(
            "idempotency_conflict",
            "request digest does not match the stored publication",
        ));
    }
    Ok(record)
}

fn build_publisher(
    fixture: &ContractProductRuntimeFixture,
    policies: impl IntoIterator<Item = (&'static str, EventPublicationReliability)>,
    config: ReliablePublicationConfig,
    journal: Arc<FakeJournal>,
) -> ReliableEventPublisher<FakeJournal> {
    build_publisher_with_runtime(fixture.facade.clone(), policies, config, journal)
}

fn build_publisher_with_runtime(
    runtime: ProductRuntimeFacade,
    policies: impl IntoIterator<Item = (&'static str, EventPublicationReliability)>,
    config: ReliablePublicationConfig,
    journal: Arc<FakeJournal>,
) -> ReliableEventPublisher<FakeJournal> {
    let client = ClientBuilder::new(ClientConfig::in_process())
        .service_credential(ServiceCredential::new("orders-app", "service-secret").unwrap())
        .bind_runtime(
            runtime,
            TrustedContextFactory::new(
                "orders-service",
                PrincipalKind::Service,
                Some("orders-app".to_string()),
                InvocationSource::EmbeddedSdk,
            )
            .unwrap(),
        )
        .unwrap();
    let catalog = EventPublicationPolicyCatalog::new(policies.into_iter().map(
        |(event_type, reliability)| {
            (
                EventPublicationRoute::new("orders", event_type).unwrap(),
                reliability,
            )
        },
    ))
    .unwrap();
    client
        .event_dispatch()
        .reliable_publisher(catalog, config, journal)
}

fn event(event_type: &str, order_id: u64) -> PublishEvent {
    PublishEvent::dynamic(
        "checkout",
        DynamicEvent::new(
            EventRoute::new("orders", event_type, "1").unwrap(),
            json!({"order_id": order_id}),
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn policies_select_best_effort_confirmed_and_direct_durable_paths() {
    let fixture = ContractProductRuntimeFixture::default();
    let journal = Arc::new(FakeJournal::default());
    let publisher = build_publisher(
        &fixture,
        [
            ("order.best-effort", EventPublicationReliability::BestEffort),
            ("order.confirmed", EventPublicationReliability::Confirmed),
            ("order.durable", EventPublicationReliability::Durable),
        ],
        ReliablePublicationConfig::default(),
        journal.clone(),
    );

    publisher
        .publish(
            event("order.best-effort", 1),
            MutationOptions::new("orders/1/best-effort").unwrap(),
        )
        .await
        .unwrap();
    publisher
        .publish(
            event("order.confirmed", 2),
            MutationOptions::new("orders/2/confirmed").unwrap(),
        )
        .await
        .unwrap();
    let outcome = publisher
        .publish(
            event("order.durable", 3),
            MutationOptions::new("orders/3/durable").unwrap(),
        )
        .await
        .unwrap();

    assert!(matches!(outcome, EventPublicationOutcome::Accepted(_)));
    assert!(journal.accepted_receipt("orders/3/durable").is_some());
    assert_eq!(fixture.event_publisher.published_count(), 3);
}

#[tokio::test]
async fn direct_durable_failure_is_journaled_and_recovered_after_lingshu_returns() {
    let fixture = ContractProductRuntimeFixture::default();
    let journal = Arc::new(FakeJournal::default());
    let policies = || [("order.durable", EventPublicationReliability::Durable)];
    let unavailable = fixture
        .facade
        .clone()
        .with_event_publisher(Arc::new(RetryableFailingEventPublisher));
    let failing_publisher = build_publisher_with_runtime(
        unavailable,
        policies(),
        ReliablePublicationConfig::default(),
        journal.clone(),
    );

    let outcome = failing_publisher
        .publish(
            event("order.durable", 4),
            MutationOptions::new("orders/4/direct").unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        EventPublicationOutcome::RetryScheduled(_)
    ));
    assert!(journal.has_retryable_failure("orders/4/direct"));
    assert_eq!(journal.record_count(), 1);

    let recovered_publisher = build_publisher(
        &fixture,
        policies(),
        ReliablePublicationConfig::default(),
        journal.clone(),
    );
    journal.make_all_due();
    let recovered = recovered_publisher.recover_due().await.unwrap();

    assert_eq!(recovered.accepted, 1);
    assert!(journal.accepted_receipt("orders/4/direct").is_some());
    assert_eq!(fixture.event_publisher.published_count(), 1);
}

#[tokio::test]
async fn missing_policy_fails_before_journal_or_runtime_io() {
    let fixture = ContractProductRuntimeFixture::default();
    let journal = Arc::new(FakeJournal::default());
    let publisher = build_publisher(
        &fixture,
        [],
        ReliablePublicationConfig::default(),
        journal.clone(),
    );
    let mut transaction = FakeTransaction::default();

    let error = publisher
        .in_transaction(&mut transaction)
        .publish(
            event("order.unregistered", 1),
            MutationOptions::new("orders/1/unregistered").unwrap(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, ReliablePublicationError::Policy(_)));
    assert!(transaction.staged.is_empty());
    assert_eq!(fixture.event_publisher.published_count(), 0);
}

#[tokio::test]
async fn durable_append_rolls_back_or_survives_a_post_commit_process_exit() {
    let fixture = ContractProductRuntimeFixture::default();
    let journal = Arc::new(FakeJournal::default());
    let publisher = build_publisher(
        &fixture,
        [("order.durable", EventPublicationReliability::Durable)],
        ReliablePublicationConfig::default(),
        journal.clone(),
    );

    let mut rolled_back = FakeTransaction::default();
    let pending = publisher
        .in_transaction(&mut rolled_back)
        .publish(
            event("order.durable", 1),
            MutationOptions::new("orders/1/durable").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pending.reliability(), EventPublicationReliability::Durable);
    assert_eq!(fixture.event_publisher.published_count(), 0);
    drop(pending);
    drop(rolled_back);
    assert_eq!(journal.record_count(), 0);

    let mut committed = FakeTransaction::default();
    let pending = publisher
        .in_transaction(&mut committed)
        .publish(
            event("order.durable", 2),
            MutationOptions::new("orders/2/durable").unwrap(),
        )
        .await
        .unwrap();
    journal.commit(committed).unwrap();
    drop(pending);
    assert_eq!(fixture.event_publisher.published_count(), 0);

    journal.make_all_due();
    let recovered = publisher.recover_due().await.unwrap();
    assert_eq!(recovered.scanned, 1);
    assert_eq!(recovered.accepted, 1);
    assert_eq!(fixture.event_publisher.published_count(), 1);
}

#[tokio::test]
async fn accepted_event_is_replayed_after_the_local_custody_update_is_lost() {
    let fixture = ContractProductRuntimeFixture::default();
    let journal = Arc::new(FakeJournal::default());
    let publisher = build_publisher(
        &fixture,
        [("order.durable", EventPublicationReliability::Durable)],
        ReliablePublicationConfig::default(),
        journal.clone(),
    );
    let mut transaction = FakeTransaction::default();
    let pending = publisher
        .in_transaction(&mut transaction)
        .publish(
            event("order.durable", 1),
            MutationOptions::new("orders/1/replay").unwrap(),
        )
        .await
        .unwrap();
    journal.commit(transaction).unwrap();
    journal.fail_next_accept();

    let error = publisher.dispatch(pending).await.unwrap_err();
    assert!(matches!(error, ReliablePublicationError::Journal(_)));
    assert_eq!(fixture.event_publisher.published_count(), 1);

    journal.make_all_due();
    let recovered = publisher.recover_due().await.unwrap();
    assert_eq!(recovered.accepted, 1);
    assert_eq!(fixture.event_publisher.published_count(), 1);
    assert_eq!(
        journal
            .accepted_receipt("orders/1/replay")
            .unwrap()
            .mutation
            .disposition,
        MutationDisposition::Duplicate
    );
}

#[tokio::test]
async fn concurrent_recovery_uses_server_idempotency_and_conditional_journal_updates() {
    let fixture = ContractProductRuntimeFixture::default();
    let journal = Arc::new(FakeJournal::default());
    let publisher = build_publisher(
        &fixture,
        [("order.durable", EventPublicationReliability::Durable)],
        ReliablePublicationConfig::default(),
        journal.clone(),
    );
    let mut transaction = FakeTransaction::default();
    let pending = publisher
        .in_transaction(&mut transaction)
        .publish(
            event("order.durable", 1),
            MutationOptions::new("orders/1/concurrent").unwrap(),
        )
        .await
        .unwrap();
    journal.commit(transaction).unwrap();
    drop(pending);
    journal.make_all_due();
    journal.synchronize_next_two_loads();

    let (first, second) = tokio::join!(publisher.recover_due(), publisher.recover_due());
    let first = first.unwrap();
    let second = second.unwrap();

    assert_eq!(first.accepted + second.accepted, 1);
    assert_eq!(first.skipped + second.skipped, 1);
    assert_eq!(fixture.event_publisher.published_count(), 1);
}

#[tokio::test]
async fn recovery_obeys_batch_and_time_budgets() {
    let fixture = ContractProductRuntimeFixture::default();
    let config = ReliablePublicationConfig::new(
        Duration::from_secs(60),
        Duration::from_secs(30),
        2,
        Duration::from_secs(1),
    )
    .unwrap();
    let journal = Arc::new(FakeJournal::default());
    let publisher = build_publisher(
        &fixture,
        [("order.durable", EventPublicationReliability::Durable)],
        config,
        journal.clone(),
    );
    for order_id in 1..=3 {
        let mut transaction = FakeTransaction::default();
        let pending = publisher
            .in_transaction(&mut transaction)
            .publish(
                event("order.durable", order_id),
                MutationOptions::new(format!("orders/{order_id}/bounded")).unwrap(),
            )
            .await
            .unwrap();
        journal.commit(transaction).unwrap();
        drop(pending);
    }
    journal.make_all_due();

    let first = publisher.recover_due().await.unwrap();
    assert_eq!(first.scanned, 2);
    assert_eq!(first.accepted, 2);
    assert!(first.saturated);
    assert_eq!(fixture.event_publisher.published_count(), 2);

    let no_time = build_publisher(
        &fixture,
        [("order.durable", EventPublicationReliability::Durable)],
        ReliablePublicationConfig::new(
            Duration::from_secs(60),
            Duration::from_secs(30),
            2,
            Duration::from_nanos(1),
        )
        .unwrap(),
        journal.clone(),
    );
    let second = no_time.recover_due().await.unwrap();
    assert_eq!(second.scanned, 1);
    assert!(second.time_budget_exhausted);
    assert_eq!(second.accepted, 0);
    assert_eq!(fixture.event_publisher.published_count(), 2);
}

#[tokio::test]
async fn changed_content_with_the_same_durable_key_is_rejected_in_the_transaction() {
    let fixture = ContractProductRuntimeFixture::default();
    let journal = Arc::new(FakeJournal::default());
    let publisher = build_publisher(
        &fixture,
        [("order.durable", EventPublicationReliability::Durable)],
        ReliablePublicationConfig::default(),
        journal.clone(),
    );
    let mut transaction = FakeTransaction::default();
    let pending = publisher
        .in_transaction(&mut transaction)
        .publish(
            event("order.durable", 1),
            MutationOptions::new("orders/shared-key").unwrap(),
        )
        .await
        .unwrap();
    journal.commit(transaction).unwrap();
    drop(pending);

    let mut conflicting = FakeTransaction::default();
    let error = publisher
        .in_transaction(&mut conflicting)
        .publish(
            event("order.durable", 2),
            MutationOptions::new("orders/shared-key").unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ReliablePublicationError::Journal(_)));
    assert!(conflicting.staged.is_empty());
    assert_eq!(fixture.event_publisher.published_count(), 0);
}

#[tokio::test]
async fn post_commit_dispatch_records_the_lingshu_receipt() {
    let fixture = ContractProductRuntimeFixture::default();
    let journal = Arc::new(FakeJournal::default());
    let publisher = build_publisher(
        &fixture,
        [("order.durable", EventPublicationReliability::Durable)],
        ReliablePublicationConfig::default(),
        journal.clone(),
    );
    let mut transaction = FakeTransaction::default();
    let pending = publisher
        .in_transaction(&mut transaction)
        .publish(
            event("order.durable", 1),
            MutationOptions::new("orders/1/dispatch").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(pending.idempotency_key(), "orders/1/dispatch");
    journal.commit(transaction).unwrap();

    let outcome = publisher.dispatch(pending).await.unwrap();
    assert!(matches!(outcome, EventPublicationOutcome::Accepted(_)));
    assert!(journal.accepted_receipt("orders/1/dispatch").is_some());
    assert_eq!(fixture.event_publisher.published_count(), 1);
}

#[test]
fn fake_terminal_states_retain_failure_diagnostics() {
    let retryable = FakeState::Retryable(EventPublicationFailure::retryable("temporary", None));
    let permanent = FakeState::Permanent(EventPublicationFailure::permanent("invalid", None));
    assert!(matches!(
        retryable,
        FakeState::Retryable(failure) if failure.code() == "temporary"
    ));
    assert!(matches!(
        permanent,
        FakeState::Permanent(failure) if failure.code() == "invalid"
    ));
}
