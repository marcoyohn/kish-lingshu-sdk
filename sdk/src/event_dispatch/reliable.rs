use std::{collections::BTreeMap, sync::Arc, time::Duration, time::Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{EventDispatch, EventRoute, PublishEvent, PublishReceipt};
use crate::{Error as SdkError, MutationOptions};

const MAX_RECOVERY_BATCH_SIZE: u32 = 10_000;
const MAX_FAILURE_CODE_BYTES: usize = 64;
const MAX_FAILURE_MESSAGE_CHARS: usize = 512;

/// Producer-side guarantee applied to one Topic/Event type route.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventPublicationReliability {
    /// One network attempt. Loss before Kish Lingshu accepts custody is allowed.
    BestEffort,
    /// Bounded idempotent retry. Success proves Kish Lingshu accepted custody.
    Confirmed,
    /// Intent is journaled with business state before confirmed handoff.
    Durable,
}

impl EventPublicationReliability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BestEffort => "BEST_EFFORT",
            Self::Confirmed => "CONFIRMED",
            Self::Durable => "DURABLE",
        }
    }
}

/// Route key for a producer publication policy. Schema version is deliberately absent.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct EventPublicationRoute {
    topic: String,
    event_type: String,
}

impl EventPublicationRoute {
    pub fn new(
        topic: impl Into<String>,
        event_type: impl Into<String>,
    ) -> Result<Self, EventPublicationPolicyError> {
        let topic = topic.into();
        let event_type = event_type.into();
        EventRoute::new(&topic, &event_type, "1").map_err(|error| {
            EventPublicationPolicyError::InvalidRoute {
                topic: topic.clone(),
                event_type: event_type.clone(),
                message: error.to_string(),
            }
        })?;
        Ok(Self { topic, event_type })
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn event_type(&self) -> &str {
        &self.event_type
    }
}

/// Immutable route-to-reliability catalog used by [`ReliableEventPublisher`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EventPublicationPolicyCatalog {
    routes: BTreeMap<EventPublicationRoute, EventPublicationReliability>,
}

impl EventPublicationPolicyCatalog {
    pub fn new(
        registrations: impl IntoIterator<Item = (EventPublicationRoute, EventPublicationReliability)>,
    ) -> Result<Self, EventPublicationPolicyError> {
        let mut catalog = Self::default();
        for (route, reliability) in registrations {
            catalog.register(route, reliability)?;
        }
        Ok(catalog)
    }

    pub fn register(
        &mut self,
        route: EventPublicationRoute,
        reliability: EventPublicationReliability,
    ) -> Result<(), EventPublicationPolicyError> {
        if let Some(existing) = self.routes.insert(route.clone(), reliability) {
            self.routes.insert(route.clone(), existing);
            return Err(EventPublicationPolicyError::DuplicateRoute {
                topic: route.topic,
                event_type: route.event_type,
                existing,
                duplicate: reliability,
            });
        }
        Ok(())
    }

    pub fn reliability(
        &self,
        topic: &str,
        event_type: &str,
    ) -> Result<EventPublicationReliability, EventPublicationPolicyError> {
        self.routes
            .get(&EventPublicationRoute {
                topic: topic.to_string(),
                event_type: event_type.to_string(),
            })
            .copied()
            .ok_or_else(|| EventPublicationPolicyError::MissingRoute {
                topic: topic.to_string(),
                event_type: event_type.to_string(),
            })
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EventPublicationPolicyError {
    #[error("invalid publication route ({topic}, {event_type}): {message}")]
    InvalidRoute {
        topic: String,
        event_type: String,
        message: String,
    },
    #[error(
        "duplicate publication policy for ({topic}, {event_type}): existing={existing:?}, duplicate={duplicate:?}"
    )]
    DuplicateRoute {
        topic: String,
        event_type: String,
        existing: EventPublicationReliability,
        duplicate: EventPublicationReliability,
    },
    #[error("missing publication policy for ({topic}, {event_type})")]
    MissingRoute { topic: String, event_type: String },
    #[error(
        "recovered durable publication route ({topic}, {event_type}) is now configured as {actual:?}"
    )]
    RecoveredRouteNotDurable {
        topic: String,
        event_type: String,
        actual: EventPublicationReliability,
    },
}

/// Bounds one recovery invocation and pre-custody retry scheduling.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReliablePublicationConfig {
    orphan_timeout: Duration,
    retry_delay: Duration,
    recovery_batch_size: u32,
    recovery_time_budget: Duration,
}

impl ReliablePublicationConfig {
    pub fn new(
        orphan_timeout: Duration,
        retry_delay: Duration,
        recovery_batch_size: u32,
        recovery_time_budget: Duration,
    ) -> Result<Self, ReliablePublicationConfigError> {
        validate_duration("orphan_timeout", orphan_timeout)?;
        validate_duration("retry_delay", retry_delay)?;
        validate_duration("recovery_time_budget", recovery_time_budget)?;
        if recovery_batch_size == 0 || recovery_batch_size > MAX_RECOVERY_BATCH_SIZE {
            return Err(ReliablePublicationConfigError {
                field: "recovery_batch_size",
                message: format!("value must be in 1..={MAX_RECOVERY_BATCH_SIZE}"),
            });
        }
        Ok(Self {
            orphan_timeout,
            retry_delay,
            recovery_batch_size,
            recovery_time_budget,
        })
    }

    pub const fn orphan_timeout(&self) -> Duration {
        self.orphan_timeout
    }

    pub const fn retry_delay(&self) -> Duration {
        self.retry_delay
    }

    pub const fn recovery_batch_size(&self) -> u32 {
        self.recovery_batch_size
    }

    pub const fn recovery_time_budget(&self) -> Duration {
        self.recovery_time_budget
    }
}

impl Default for ReliablePublicationConfig {
    fn default() -> Self {
        Self {
            orphan_timeout: Duration::from_secs(60),
            retry_delay: Duration::from_secs(30),
            recovery_batch_size: 100,
            recovery_time_budget: Duration::from_secs(10),
        }
    }
}

fn validate_duration(
    field: &'static str,
    value: Duration,
) -> Result<(), ReliablePublicationConfigError> {
    if value.is_zero() {
        return Err(ReliablePublicationConfigError {
            field,
            message: "value must be positive".to_string(),
        });
    }
    chrono::Duration::from_std(value).map_err(|_| ReliablePublicationConfigError {
        field,
        message: "value exceeds the supported timestamp range".to_string(),
    })?;
    Ok(())
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("invalid reliable publication configuration for {field}: {message}")]
pub struct ReliablePublicationConfigError {
    pub field: &'static str,
    pub message: String,
}

/// Immutable Event publication intent persisted before a durable network attempt.
#[derive(Clone, Debug, PartialEq)]
pub struct DurableEventPublication {
    event: PublishEvent,
    idempotency_key: String,
    request_digest: String,
}

impl DurableEventPublication {
    pub fn new(
        event: PublishEvent,
        idempotency_key: impl Into<String>,
    ) -> Result<Self, ReliablePublicationError> {
        event
            .validate()
            .map_err(|error| ReliablePublicationError::InvalidPublication(error.to_string()))?;
        let idempotency_key = idempotency_key.into();
        MutationOptions::new(idempotency_key.clone())
            .map_err(|error| ReliablePublicationError::InvalidPublication(error.to_string()))?;
        let mut normalized = serde_json::to_value(&event)
            .map_err(|error| ReliablePublicationError::InvalidPublication(error.to_string()))?;
        normalize_json(&mut normalized);
        let request = serde_json::to_vec(&normalized)
            .map_err(|error| ReliablePublicationError::InvalidPublication(error.to_string()))?;
        Ok(Self {
            event,
            idempotency_key,
            request_digest: format!("{:x}", Sha256::digest(request)),
        })
    }

    pub fn event(&self) -> &PublishEvent {
        &self.event
    }

    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    pub fn request_digest(&self) -> &str {
        &self.request_digest
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventPublicationFailureKind {
    Retryable,
    Permanent,
}

/// Bounded diagnostic for a failed handoff before Kish Lingshu accepts custody.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventPublicationFailure {
    kind: EventPublicationFailureKind,
    code: String,
    message: Option<String>,
}

impl EventPublicationFailure {
    pub fn retryable(code: impl Into<String>, message: Option<String>) -> Self {
        Self::new(EventPublicationFailureKind::Retryable, code, message)
    }

    pub fn permanent(code: impl Into<String>, message: Option<String>) -> Self {
        Self::new(EventPublicationFailureKind::Permanent, code, message)
    }

    fn new(
        kind: EventPublicationFailureKind,
        code: impl Into<String>,
        message: Option<String>,
    ) -> Self {
        let mut code = code.into();
        code.retain(|character| character.is_ascii_alphanumeric() || character == '_');
        code.truncate(MAX_FAILURE_CODE_BYTES);
        if code.is_empty() {
            code = "unspecified".to_string();
        }
        Self {
            kind,
            code,
            message: message.map(|value| value.chars().take(MAX_FAILURE_MESSAGE_CHARS).collect()),
        }
    }

    pub const fn kind(&self) -> EventPublicationFailureKind {
        self.kind
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

/// Storage-adapter error surfaced by the durable publication coordinator.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("Event publication journal failure {code}: {message}")]
pub struct EventPublicationJournalError {
    pub code: String,
    pub message: String,
}

/// Existing state returned when a durable publication intent is appended.
///
/// Retryable entries remain pending because another immediate attempt with the
/// same idempotency key is safe. Accepted and permanent entries are terminal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EventPublicationJournalState {
    Pending,
    Accepted(PublishReceipt),
    PermanentFailure(EventPublicationFailure),
}

impl EventPublicationJournalError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        let mut code = code.into();
        code.retain(|character| character.is_ascii_alphanumeric() || character == '_');
        code.truncate(MAX_FAILURE_CODE_BYTES);
        if code.is_empty() {
            code = "journal_failure".to_string();
        }
        Self {
            code,
            message: message
                .into()
                .chars()
                .take(MAX_FAILURE_MESSAGE_CHARS)
                .collect(),
        }
    }
}

/// Producer-owned durable storage for direct and transaction-bound publication.
///
/// `append_standalone` must commit before returning. `append` must use the
/// caller's supplied transaction without committing it. Implementations bind an
/// idempotency key to `request_digest`, return the original state for an
/// identical replay, and reject changed content. Mark operations must be
/// conditional so accepted or terminal state never regresses.
#[async_trait]
pub trait EventPublicationJournal: Send + Sync {
    type Transaction<'transaction>: Send
    where
        Self: 'transaction;

    /// Atomically commits a standalone publication intent in journal-owned
    /// storage before returning.
    async fn append_standalone(
        &self,
        publication: &DurableEventPublication,
        recover_after: DateTime<Utc>,
    ) -> Result<EventPublicationJournalState, EventPublicationJournalError>;

    async fn append(
        &self,
        transaction: &mut Self::Transaction<'_>,
        publication: &DurableEventPublication,
        recover_after: DateTime<Utc>,
    ) -> Result<EventPublicationJournalState, EventPublicationJournalError>;

    /// Loads at most `limit` recoverable entries due at or before `due_at` in
    /// deterministic due-time order. Implementations should use an indexed
    /// range/keyset query and must not use an unbounded scan or offset.
    async fn load_due(
        &self,
        due_at: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<DurableEventPublication>, EventPublicationJournalError>;

    async fn mark_accepted(
        &self,
        publication: &DurableEventPublication,
        receipt: &PublishReceipt,
    ) -> Result<bool, EventPublicationJournalError>;

    async fn mark_failed(
        &self,
        publication: &DurableEventPublication,
        failure: &EventPublicationFailure,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<bool, EventPublicationJournalError>;
}

/// Policy-bound producer coordinator. It owns a journal handle but starts no
/// storage lifecycle or background task.
pub struct ReliableEventPublisher<J>
where
    J: EventPublicationJournal,
{
    dispatch: EventDispatch,
    policies: Arc<EventPublicationPolicyCatalog>,
    config: ReliablePublicationConfig,
    journal: Arc<J>,
}

impl<J> Clone for ReliableEventPublisher<J>
where
    J: EventPublicationJournal,
{
    fn clone(&self) -> Self {
        Self {
            dispatch: self.dispatch.clone(),
            policies: self.policies.clone(),
            config: self.config.clone(),
            journal: self.journal.clone(),
        }
    }
}

impl EventDispatch {
    pub fn reliable_publisher<J>(
        &self,
        policies: EventPublicationPolicyCatalog,
        config: ReliablePublicationConfig,
        journal: Arc<J>,
    ) -> ReliableEventPublisher<J>
    where
        J: EventPublicationJournal,
    {
        ReliableEventPublisher {
            dispatch: self.clone(),
            policies: Arc::new(policies),
            config,
            journal,
        }
    }
}

impl<J> ReliableEventPublisher<J>
where
    J: EventPublicationJournal,
{
    /// Publishes according to the route policy.
    ///
    /// Durable routes commit a standalone journal intent before network I/O.
    pub async fn publish(
        &self,
        event: PublishEvent,
        options: MutationOptions,
    ) -> Result<EventPublicationOutcome, ReliablePublicationError> {
        validate_event(&event)?;
        let reliability = self.policy_for(&event)?;
        match reliability {
            EventPublicationReliability::BestEffort | EventPublicationReliability::Confirmed => {
                self.publish_direct(reliability, event, options)
                    .await
                    .map(EventPublicationOutcome::Accepted)
            }
            EventPublicationReliability::Durable => {
                let publication = durable_publication(event, &options)?;
                let recover_after = add_duration(Utc::now(), self.config.orphan_timeout)?;
                let state = self
                    .journal
                    .append_standalone(&publication, recover_after)
                    .await?;
                self.dispatch_from_state(publication, options, state).await
            }
        }
    }

    pub fn in_transaction<'publisher, 'transaction, 'connection>(
        &'publisher self,
        transaction: &'transaction mut J::Transaction<'connection>,
    ) -> TransactionalEventPublisher<'publisher, 'transaction, 'connection, J>
    where
        J: 'connection,
    {
        TransactionalEventPublisher {
            publisher: self,
            transaction,
        }
    }

    /// Transfers one transactionally prepared publication after business commit.
    pub async fn dispatch(
        &self,
        pending: PendingEventPublication,
    ) -> Result<EventPublicationOutcome, ReliablePublicationError> {
        match pending.kind {
            PendingEventPublicationKind::Direct {
                reliability,
                event,
                options,
            } => self
                .publish_direct(reliability, event, options)
                .await
                .map(EventPublicationOutcome::Accepted),
            PendingEventPublicationKind::Durable {
                publication,
                options,
                state,
            } => self.dispatch_from_state(publication, options, state).await,
        }
    }

    /// Performs one externally triggered, bounded recovery pass.
    pub async fn recover_due(
        &self,
    ) -> Result<EventPublicationRecoveryResult, ReliablePublicationError> {
        let due_at = Utc::now();
        let publications = self
            .journal
            .load_due(due_at, self.config.recovery_batch_size)
            .await?;
        let mut result = EventPublicationRecoveryResult {
            scanned: u32::try_from(publications.len()).unwrap_or(u32::MAX),
            saturated: publications.len()
                >= usize::try_from(self.config.recovery_batch_size).unwrap_or(usize::MAX),
            ..EventPublicationRecoveryResult::default()
        };
        let started = Instant::now();
        for publication in publications {
            if started.elapsed() >= self.config.recovery_time_budget {
                result.time_budget_exhausted = true;
                break;
            }
            let reliability = self.policy_for(publication.event())?;
            if reliability != EventPublicationReliability::Durable {
                return Err(EventPublicationPolicyError::RecoveredRouteNotDurable {
                    topic: publication.event().topic.clone(),
                    event_type: publication.event().event_type.clone(),
                    actual: reliability,
                }
                .into());
            }
            let options = MutationOptions::new(publication.idempotency_key())
                .map_err(|error| ReliablePublicationError::InvalidPublication(error.to_string()))?;
            match self.dispatch_durable(publication, options).await? {
                EventPublicationOutcome::Accepted(_) => {
                    result.accepted = result.accepted.saturating_add(1)
                }
                EventPublicationOutcome::RetryScheduled(_) => {
                    result.retryable_failures = result.retryable_failures.saturating_add(1)
                }
                EventPublicationOutcome::PermanentFailure(_) => {
                    result.permanent_failures = result.permanent_failures.saturating_add(1)
                }
                EventPublicationOutcome::Skipped => {
                    result.skipped = result.skipped.saturating_add(1)
                }
            }
        }
        Ok(result)
    }

    async fn publish_direct(
        &self,
        reliability: EventPublicationReliability,
        event: PublishEvent,
        options: MutationOptions,
    ) -> Result<PublishReceipt, ReliablePublicationError> {
        let result = match reliability {
            EventPublicationReliability::BestEffort => {
                self.dispatch.publish_best_effort(event, options).await
            }
            EventPublicationReliability::Confirmed => self.dispatch.publish(event, options).await,
            EventPublicationReliability::Durable => {
                unreachable!("durable publication is journaled")
            }
        };
        result.map_err(ReliablePublicationError::from)
    }

    async fn dispatch_from_state(
        &self,
        publication: DurableEventPublication,
        options: MutationOptions,
        state: EventPublicationJournalState,
    ) -> Result<EventPublicationOutcome, ReliablePublicationError> {
        match state {
            EventPublicationJournalState::Pending => {
                self.dispatch_durable(publication, options).await
            }
            EventPublicationJournalState::Accepted(receipt) => {
                Ok(EventPublicationOutcome::Accepted(receipt))
            }
            EventPublicationJournalState::PermanentFailure(failure) => {
                Ok(EventPublicationOutcome::PermanentFailure(failure))
            }
        }
    }

    async fn dispatch_durable(
        &self,
        publication: DurableEventPublication,
        options: MutationOptions,
    ) -> Result<EventPublicationOutcome, ReliablePublicationError> {
        match self
            .dispatch
            .publish(publication.event.clone(), options)
            .await
        {
            Ok(receipt) => {
                if self.journal.mark_accepted(&publication, &receipt).await? {
                    Ok(EventPublicationOutcome::Accepted(receipt))
                } else {
                    Ok(EventPublicationOutcome::Skipped)
                }
            }
            Err(error) => {
                let failure = classify_pre_custody_failure(&error);
                let retry_at = match failure.kind {
                    EventPublicationFailureKind::Retryable => {
                        Some(add_duration(Utc::now(), self.config.retry_delay)?)
                    }
                    EventPublicationFailureKind::Permanent => None,
                };
                if !self
                    .journal
                    .mark_failed(&publication, &failure, retry_at)
                    .await?
                {
                    return Ok(EventPublicationOutcome::Skipped);
                }
                Ok(match failure.kind {
                    EventPublicationFailureKind::Retryable => {
                        EventPublicationOutcome::RetryScheduled(failure)
                    }
                    EventPublicationFailureKind::Permanent => {
                        EventPublicationOutcome::PermanentFailure(failure)
                    }
                })
            }
        }
    }

    fn policy_for(
        &self,
        event: &PublishEvent,
    ) -> Result<EventPublicationReliability, EventPublicationPolicyError> {
        self.policies.reliability(&event.topic, &event.event_type)
    }
}

pub struct TransactionalEventPublisher<'publisher, 'transaction, 'connection, J>
where
    J: EventPublicationJournal + 'connection,
{
    publisher: &'publisher ReliableEventPublisher<J>,
    transaction: &'transaction mut J::Transaction<'connection>,
}

impl<J> TransactionalEventPublisher<'_, '_, '_, J>
where
    J: EventPublicationJournal,
{
    /// Prepares publication inside the supplied transaction without network I/O.
    pub async fn publish(
        self,
        event: PublishEvent,
        options: MutationOptions,
    ) -> Result<PendingEventPublication, ReliablePublicationError> {
        validate_event(&event)?;
        let reliability = self.publisher.policy_for(&event)?;
        let kind = match reliability {
            EventPublicationReliability::BestEffort | EventPublicationReliability::Confirmed => {
                PendingEventPublicationKind::Direct {
                    reliability,
                    event,
                    options,
                }
            }
            EventPublicationReliability::Durable => {
                let publication = durable_publication(event, &options)?;
                let recover_after = add_duration(Utc::now(), self.publisher.config.orphan_timeout)?;
                let state = self
                    .publisher
                    .journal
                    .append(self.transaction, &publication, recover_after)
                    .await?;
                PendingEventPublicationKind::Durable {
                    publication,
                    options,
                    state,
                }
            }
        };
        Ok(PendingEventPublication { reliability, kind })
    }
}

/// Opaque publication token returned before the producer transaction commits.
#[derive(Debug)]
pub struct PendingEventPublication {
    reliability: EventPublicationReliability,
    kind: PendingEventPublicationKind,
}

#[derive(Debug)]
enum PendingEventPublicationKind {
    Direct {
        reliability: EventPublicationReliability,
        event: PublishEvent,
        options: MutationOptions,
    },
    Durable {
        publication: DurableEventPublication,
        options: MutationOptions,
        state: EventPublicationJournalState,
    },
}

impl PendingEventPublication {
    pub const fn reliability(&self) -> EventPublicationReliability {
        self.reliability
    }

    pub fn idempotency_key(&self) -> &str {
        match &self.kind {
            PendingEventPublicationKind::Direct { options, .. }
            | PendingEventPublicationKind::Durable { options, .. } => {
                options.idempotency_key().as_str()
            }
        }
    }
}

#[derive(Debug)]
pub enum EventPublicationOutcome {
    Accepted(PublishReceipt),
    RetryScheduled(EventPublicationFailure),
    PermanentFailure(EventPublicationFailure),
    Skipped,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EventPublicationRecoveryResult {
    pub scanned: u32,
    pub accepted: u32,
    pub retryable_failures: u32,
    pub permanent_failures: u32,
    pub skipped: u32,
    pub saturated: bool,
    pub time_budget_exhausted: bool,
}

#[derive(Debug, Error)]
pub enum ReliablePublicationError {
    #[error(transparent)]
    Policy(#[from] EventPublicationPolicyError),
    #[error(transparent)]
    Configuration(#[from] ReliablePublicationConfigError),
    #[error(transparent)]
    Journal(#[from] EventPublicationJournalError),
    #[error("invalid durable Event publication: {0}")]
    InvalidPublication(String),
    #[error(transparent)]
    Publication(Box<SdkError>),
}

impl From<SdkError> for ReliablePublicationError {
    fn from(error: SdkError) -> Self {
        Self::Publication(Box::new(error))
    }
}

fn validate_event(event: &PublishEvent) -> Result<(), ReliablePublicationError> {
    event
        .validate()
        .map_err(|error| ReliablePublicationError::InvalidPublication(error.to_string()))
}

fn durable_publication(
    event: PublishEvent,
    options: &MutationOptions,
) -> Result<DurableEventPublication, ReliablePublicationError> {
    DurableEventPublication::new(event, options.idempotency_key().as_str().to_string())
}

fn add_duration(
    timestamp: DateTime<Utc>,
    duration: Duration,
) -> Result<DateTime<Utc>, ReliablePublicationError> {
    let duration = chrono::Duration::from_std(duration).map_err(|_| {
        ReliablePublicationError::InvalidPublication(
            "publication recovery duration exceeds the supported timestamp range".to_string(),
        )
    })?;
    timestamp.checked_add_signed(duration).ok_or_else(|| {
        ReliablePublicationError::InvalidPublication(
            "publication recovery timestamp exceeds the supported range".to_string(),
        )
    })
}

fn classify_pre_custody_failure(error: &SdkError) -> EventPublicationFailure {
    let message = Some(error.to_string());
    match error {
        SdkError::Transport(failure) if failure.retryable => {
            EventPublicationFailure::retryable("transport_retryable", message)
        }
        SdkError::Transport(_) => EventPublicationFailure::permanent("transport_rejected", message),
        SdkError::Application(failure)
            if failure.problem.retryable
                || failure.http_status.is_some_and(|status| {
                    status == 408 || status == 425 || status == 429 || status >= 500
                }) =>
        {
            EventPublicationFailure::retryable("application_retryable", message)
        }
        SdkError::Application(failure) => {
            EventPublicationFailure::permanent(failure.problem.code.as_str().to_string(), message)
        }
        SdkError::Configuration(_) => {
            EventPublicationFailure::permanent("client_configuration", message)
        }
        SdkError::Protocol(_) => EventPublicationFailure::permanent("protocol_rejected", message),
        SdkError::ContractViolation(_) => {
            EventPublicationFailure::permanent("contract_violation", message)
        }
    }
}

fn normalize_json(value: &mut Value) {
    match value {
        Value::Object(object) => {
            let mut entries = std::mem::take(object).into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            for (_, value) in &mut entries {
                normalize_json(value);
            }
            object.extend(entries);
        }
        Value::Array(values) => {
            for value in values {
                normalize_json(value);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_route_registration_is_rejected() {
        let route = EventPublicationRoute::new("orders", "order.created").unwrap();
        let error = EventPublicationPolicyCatalog::new([
            (route.clone(), EventPublicationReliability::Confirmed),
            (route, EventPublicationReliability::Confirmed),
        ])
        .unwrap_err();
        assert!(matches!(
            error,
            EventPublicationPolicyError::DuplicateRoute { .. }
        ));
    }

    #[test]
    fn configuration_rejects_unbounded_or_zero_values() {
        assert!(ReliablePublicationConfig::new(
            Duration::ZERO,
            Duration::from_secs(1),
            1,
            Duration::from_secs(1),
        )
        .is_err());
        assert!(ReliablePublicationConfig::new(
            Duration::from_secs(1),
            Duration::from_secs(1),
            MAX_RECOVERY_BATCH_SIZE + 1,
            Duration::from_secs(1),
        )
        .is_err());
    }

    #[test]
    fn durable_publication_digest_changes_with_content() {
        let first = DurableEventPublication::new(
            PublishEvent::dynamic(
                "checkout",
                super::super::DynamicEvent::new(
                    EventRoute::new("orders", "order.created", "1").unwrap(),
                    serde_json::json!({"order_id": 42}),
                )
                .unwrap(),
            ),
            "orders/42/created",
        )
        .unwrap();
        let second = DurableEventPublication::new(
            PublishEvent::dynamic(
                "checkout",
                super::super::DynamicEvent::new(
                    EventRoute::new("orders", "order.created", "1").unwrap(),
                    serde_json::json!({"order_id": 43}),
                )
                .unwrap(),
            ),
            "orders/42/created",
        )
        .unwrap();
        assert_ne!(first.request_digest(), second.request_digest());
    }

    #[test]
    fn failure_classification_and_diagnostics_are_bounded() {
        let retryable =
            classify_pre_custody_failure(&SdkError::Transport(crate::TransportFailure {
                kind: crate::TransportKind::Request,
                message: "temporary".to_string(),
                request_id: None,
                retryable: true,
            }));
        assert_eq!(retryable.kind(), EventPublicationFailureKind::Retryable);

        let permanent =
            classify_pre_custody_failure(&SdkError::Transport(crate::TransportFailure {
                kind: crate::TransportKind::Request,
                message: "rejected".to_string(),
                request_id: None,
                retryable: false,
            }));
        assert_eq!(permanent.kind(), EventPublicationFailureKind::Permanent);

        let retryable_problem = kish_lingshu_runtime_contract::ApplicationProblem::new(
            kish_lingshu_runtime_contract::ProblemCode::new("upstream_unavailable").unwrap(),
            "try again",
            "request-1",
        )
        .with_retryable(true);
        let retryable_application = classify_pre_custody_failure(&SdkError::Application(
            crate::ApplicationFailure::new(retryable_problem),
        ));
        assert_eq!(
            retryable_application.kind(),
            EventPublicationFailureKind::Retryable
        );

        let bounded = EventPublicationFailure::retryable(
            "bad-code!".repeat(20),
            Some("x".repeat(MAX_FAILURE_MESSAGE_CHARS + 50)),
        );
        assert!(bounded.code().len() <= MAX_FAILURE_CODE_BYTES);
        assert_eq!(
            bounded.message().unwrap().chars().count(),
            MAX_FAILURE_MESSAGE_CHARS
        );
    }
}
