use std::{collections::HashMap, sync::Mutex, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use kish_lingshu_event_dispatch_contract::{
    DeliveryTime, EventId, EventRecord, EventVisibility, PublishEvent, PublishReceipt,
};
use serde_json::json;

use super::contract_context_for_application;
use crate::{
    ApplicationProblem, ApplicationResult, EventPublisher, IdempotencyKey, MutationDisposition,
    MutationReceipt, PrincipalKind, ProblemCode, RequestContext, CONFLICT_PROBLEM,
    FORBIDDEN_PROBLEM, INVALID_REQUEST_PROBLEM,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventPublisherContractReport {
    pub event_id: EventId,
    pub visibility: EventVisibility,
    pub published_count: usize,
}

/// Run the Event publication and observation contract against one runtime port.
pub async fn assert_event_publisher_contract(
    publisher: std::sync::Arc<dyn EventPublisher>,
    context: RequestContext,
) -> EventPublisherContractReport {
    assert_eq!(context.principal(), PrincipalKind::Service);
    assert!(context.application_id().is_some());

    let mut event = PublishEvent::dynamic(
        "contract-checkout",
        kish_lingshu_event_dispatch_contract::DynamicEvent::new(
            kish_lingshu_event_dispatch_contract::EventRoute::new(
                "contract.orders",
                "contract.order.created",
                "1",
            )
            .unwrap(),
            json!({"order_id": 42}),
        )
        .unwrap(),
    );
    event.subject = Some("order/42".to_string());
    event.partition_key = Some("customer/7".to_string());
    event.correlation_id = Some(context.correlation_id().clone());
    event.headers.insert("contract".to_string(), json!(true));
    event.delivery = DeliveryTime::after(Duration::from_secs(5));
    let key = IdempotencyKey::new("contract/event/order-42").unwrap();

    let accepted = publisher
        .publish(context.clone(), event.clone(), key.clone())
        .await
        .expect("valid Event publication must be accepted");
    assert_eq!(accepted.mutation.disposition, MutationDisposition::Accepted);
    assert_eq!(accepted.visibility, EventVisibility::Delayed);

    let record = publisher
        .get(context.clone(), accepted.event_id)
        .await
        .expect("accepted Event lookup must succeed")
        .expect("accepted Event must be observable");
    assert_eq!(record.event_id, accepted.event_id);
    assert_eq!(record.topic, event.topic);
    assert_eq!(record.event_type, event.event_type);
    assert_eq!(record.schema_version, event.schema_version);
    assert_eq!(record.payload, event.payload);
    assert_eq!(record.visible_at, accepted.visible_at);

    let duplicate = publisher
        .publish(context.clone(), event.clone(), key.clone())
        .await
        .expect("same Event command must replay its receipt");
    assert_eq!(duplicate.event_id, accepted.event_id);
    assert_eq!(duplicate.published_at, accepted.published_at);
    assert_eq!(
        duplicate.mutation.disposition,
        MutationDisposition::Duplicate
    );

    let mut conflicting_event = event;
    conflicting_event.payload = json!({"order_id": 43});
    let conflict = publisher
        .publish(context.clone(), conflicting_event, key)
        .await
        .expect_err("same key with different Event data must conflict");
    assert_eq!(conflict.code.as_str(), CONFLICT_PROBLEM);
    assert!(!conflict.retryable);

    let absent = publisher
        .get(context.clone(), EventId::new(u64::MAX).unwrap())
        .await
        .expect("missing Event lookup must be a successful observation");
    assert!(absent.is_none());

    let other_application = contract_context_for_application(&context, "contract-other-app");
    let isolated = publisher
        .get(other_application, accepted.event_id)
        .await
        .expect("cross-Application lookup must not disclose an Event");
    assert!(isolated.is_none());

    EventPublisherContractReport {
        event_id: accepted.event_id,
        visibility: accepted.visibility,
        published_count: 1,
    }
}

#[derive(Default)]
struct EventPublisherState {
    next_event_id: u64,
    publications: HashMap<(String, String), ContractPublication>,
    records: HashMap<EventId, ContractEventRecord>,
}

#[derive(Clone)]
struct ContractPublication {
    event: PublishEvent,
    receipt: PublishReceipt,
}

#[derive(Clone)]
struct ContractEventRecord {
    application_id: String,
    record: EventRecord,
}

/// Deterministic in-memory Event publisher for SDK and adapter contract tests.
pub struct ContractEventPublisher {
    state: Mutex<EventPublisherState>,
}

impl Default for ContractEventPublisher {
    fn default() -> Self {
        Self {
            state: Mutex::new(EventPublisherState {
                next_event_id: 8_100,
                ..Default::default()
            }),
        }
    }
}

impl ContractEventPublisher {
    pub fn published_count(&self) -> usize {
        self.state.lock().unwrap().records.len()
    }

    fn application<'a>(context: &'a RequestContext) -> ApplicationResult<&'a str> {
        if context.principal() != PrincipalKind::Service {
            return Err(problem(
                context,
                FORBIDDEN_PROBLEM,
                "Event publication requires a service principal",
            ));
        }
        context.application_id().ok_or_else(|| {
            problem(
                context,
                FORBIDDEN_PROBLEM,
                "Event publication requires an Application scope",
            )
        })
    }
}

#[async_trait]
impl EventPublisher for ContractEventPublisher {
    async fn publish(
        &self,
        context: RequestContext,
        event: PublishEvent,
        idempotency_key: IdempotencyKey,
    ) -> ApplicationResult<PublishReceipt> {
        let application_id = Self::application(&context)?.to_string();
        event.validate().map_err(|error| {
            problem(
                &context,
                INVALID_REQUEST_PROBLEM,
                format!("invalid Event publication: {error}"),
            )
        })?;

        let publication_key = (application_id.clone(), idempotency_key.to_string());
        let mut state = self.state.lock().unwrap();
        if let Some(existing) = state.publications.get(&publication_key) {
            if existing.event != event {
                return Err(problem(
                    &context,
                    CONFLICT_PROBLEM,
                    "idempotency key is already bound to different Event data",
                )
                .with_details(json!({"idempotency_key": idempotency_key})));
            }
            let mut receipt = existing.receipt.clone();
            receipt.mutation.disposition = MutationDisposition::Duplicate;
            return Ok(receipt);
        }

        state.next_event_id += 1;
        let event_id = EventId::new(state.next_event_id).unwrap();
        let published_at = contract_event_time();
        let (visible_at, visibility) = visibility(&event.delivery, published_at);
        let occurred_at = event.occurred_at.unwrap_or(published_at);
        let record = EventRecord {
            event_id,
            topic: event.topic.clone(),
            event_type: event.event_type.clone(),
            schema_version: event.schema_version.clone(),
            payload: event.payload.clone(),
            source: event.source.clone(),
            subject: event.subject.clone(),
            occurred_at,
            published_at,
            visible_at,
            partition_key: event.partition_key.clone(),
            correlation_id: event.correlation_id.clone(),
            causation_id: event.causation_id.clone(),
            headers: event.headers.clone(),
            visibility,
        };
        let receipt = PublishReceipt {
            mutation: MutationReceipt::accepted(context.request_id().clone(), published_at),
            event_id,
            published_at,
            visible_at,
            visibility,
        };
        state.records.insert(
            event_id,
            ContractEventRecord {
                application_id,
                record,
            },
        );
        state.publications.insert(
            publication_key,
            ContractPublication {
                event,
                receipt: receipt.clone(),
            },
        );
        Ok(receipt)
    }

    async fn get(
        &self,
        context: RequestContext,
        event_id: EventId,
    ) -> ApplicationResult<Option<EventRecord>> {
        let application_id = Self::application(&context)?;
        Ok(self
            .state
            .lock()
            .unwrap()
            .records
            .get(&event_id)
            .filter(|record| record.application_id == application_id)
            .map(|record| record.record.clone()))
    }
}

fn visibility(
    delivery: &DeliveryTime,
    published_at: DateTime<Utc>,
) -> (DateTime<Utc>, EventVisibility) {
    match delivery {
        DeliveryTime::Immediate => (published_at, EventVisibility::Ready),
        DeliveryTime::At { timestamp } if timestamp > &published_at => {
            (*timestamp, EventVisibility::Delayed)
        }
        DeliveryTime::At { .. } => (published_at, EventVisibility::Ready),
        DeliveryTime::After { delay } => (
            published_at
                + TimeDelta::from_std(*delay)
                    .expect("contract delivery delay must fit chrono duration"),
            EventVisibility::Delayed,
        ),
    }
}

fn contract_event_time() -> DateTime<Utc> {
    "2026-09-06T08:00:01Z".parse().unwrap()
}

fn problem(
    context: &RequestContext,
    code: &'static str,
    message: impl Into<String>,
) -> ApplicationProblem {
    ApplicationProblem::new(
        ProblemCode::known(code),
        message,
        context.request_id().clone(),
    )
}
