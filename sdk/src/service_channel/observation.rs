//! Resource observations, never routing authority or business receipts. All
//! dimensions are static; applications own installation of the metrics recorder.
use metrics::{Gauge, Histogram};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy)]
pub(super) enum Plane {
    RoleBusiness,
    RoleControl,
    Catalog,
    Declaration,
    RoleChange,
    Control,
    #[cfg(feature = "service-call-zenoh")]
    Heartbeat,
    #[cfg(feature = "service-call-zenoh")]
    Completion,
    #[cfg(feature = "event-publication-zenoh")]
    Publication,
    #[cfg(feature = "service-call-zenoh")]
    Call,
    #[cfg(feature = "event-consumer-zenoh")]
    Consumer,
}
impl Plane {
    fn label(self) -> &'static str {
        match self {
            Self::RoleBusiness => "role_business",
            Self::RoleControl => "role_control",
            Self::Catalog => "catalog",
            Self::Declaration => "declaration",
            Self::RoleChange => "role_change",
            Self::Control => "control",
            #[cfg(feature = "service-call-zenoh")]
            Self::Heartbeat => "heartbeat",
            #[cfg(feature = "service-call-zenoh")]
            Self::Completion => "completion",
            #[cfg(feature = "event-publication-zenoh")]
            Self::Publication => "publication",
            #[cfg(feature = "service-call-zenoh")]
            Self::Call => "call",
            #[cfg(feature = "event-consumer-zenoh")]
            Self::Consumer => "consumer",
        }
    }
}
#[derive(Clone, Copy)]
pub(super) enum Rejection {
    Invalid,
    CountExhausted,
    BytesExhausted,
    CatalogExhausted,
    QueueFull,
    QueueClosed,
    DuplicateDeclaration,
    #[cfg(any(feature = "service-call-zenoh", feature = "event-consumer-zenoh"))]
    TaskExhausted,
}
pub(super) fn rejected(plane: Plane, reason: Rejection) {
    let outcome = match reason {
        Rejection::Invalid => "invalid",
        Rejection::CountExhausted => "count_exhausted",
        Rejection::BytesExhausted => "bytes_exhausted",
        Rejection::CatalogExhausted => "catalog_exhausted",
        Rejection::QueueFull => "queue_full",
        Rejection::QueueClosed => "queue_closed",
        Rejection::DuplicateDeclaration => "duplicate_declaration",
        #[cfg(any(feature = "service-call-zenoh", feature = "event-consumer-zenoh"))]
        Rejection::TaskExhausted => "task_exhausted",
    };
    metrics::counter!("lingshu_sdk_channel_admission_rejections_total", "plane" => plane.label(), "outcome" => outcome).increment(1);
    #[cfg(test)]
    if std::env::var("LINGSHU_VERIFY_SDK_RESOURCES").as_deref() == Ok("true") {
        eprintln!(
            "native fixture admission: plane={} outcome={outcome}",
            plane.label()
        );
    }
}

/// Owns exactly the same count/byte permits throughout queueing, processing and
/// reply. Keeping handles also preserves recorder ownership across worker threads.
pub(super) struct QueryReservation {
    _count: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    bytes: u32,
    queries: Gauge,
    byte_gauge: Gauge,
    queued: Gauge,
    queue_wait: Histogram,
    duration: Histogram,
    started: Instant,
    processing: bool,
}
impl QueryReservation {
    pub(super) fn acquire(
        plane: Plane,
        count: &Arc<Semaphore>,
        bytes: &Arc<Semaphore>,
        size: u32,
    ) -> Option<Self> {
        let Ok(slot) = count.clone().try_acquire_owned() else {
            rejected(plane, Rejection::CountExhausted);
            return None;
        };
        let Ok(byte_permit) = bytes.clone().try_acquire_many_owned(size) else {
            rejected(plane, Rejection::BytesExhausted);
            return None;
        };
        let queries =
            metrics::gauge!("lingshu_sdk_channel_reserved_queries", "plane" => plane.label());
        let byte_gauge =
            metrics::gauge!("lingshu_sdk_channel_reserved_bytes", "plane" => plane.label());
        let queued =
            metrics::gauge!("lingshu_sdk_channel_queued_queries", "plane" => plane.label());
        queries.increment(1.0);
        byte_gauge.increment(size as f64);
        queued.increment(1.0);
        Some(Self {
            _count: slot,
            _bytes: byte_permit,
            bytes: size,
            queries,
            byte_gauge,
            queued,
            queue_wait: metrics::histogram!("lingshu_sdk_channel_queue_wait_seconds", "plane" => plane.label()),
            duration: metrics::histogram!("lingshu_sdk_channel_reservation_seconds", "plane" => plane.label()),
            started: Instant::now(),
            processing: false,
        })
    }
    pub(super) fn start_processing(&mut self) {
        if !self.processing {
            self.processing = true;
            self.queued.decrement(1.0);
            self.queue_wait.record(self.started.elapsed().as_secs_f64());
        }
    }
}
impl Drop for QueryReservation {
    fn drop(&mut self) {
        if !self.processing {
            self.queued.decrement(1.0);
        }
        self.queries.decrement(1.0);
        self.byte_gauge.decrement(self.bytes as f64);
        self.duration.record(self.started.elapsed().as_secs_f64());
    }
}
pub(super) fn enqueue<T>(sender: &mpsc::Sender<T>, value: T, plane: Plane) {
    if let Err(error) = sender.try_send(value) {
        rejected(
            plane,
            match error {
                mpsc::error::TrySendError::Full(_) => Rejection::QueueFull,
                mpsc::error::TrySendError::Closed(_) => Rejection::QueueClosed,
            },
        );
    }
}

/// A local lifecycle command owns its original byte permit until the serial
/// owner finishes/drops it. Cancelling the waiting caller does not release it.
pub(super) struct CommandReservation {
    _permit: OwnedSemaphorePermit,
    bytes: u32,
    reserved: Gauge,
    byte_gauge: Gauge,
    queued: Gauge,
    wait: Histogram,
    duration: Histogram,
    started: Instant,
    processing: bool,
}
impl CommandReservation {
    pub(super) fn new(permit: OwnedSemaphorePermit, bytes: u32) -> Self {
        let reserved = metrics::gauge!("lingshu_sdk_channel_reserved_commands");
        let byte_gauge = metrics::gauge!("lingshu_sdk_channel_command_reserved_bytes");
        let queued = metrics::gauge!("lingshu_sdk_channel_queued_commands");
        reserved.increment(1.0);
        byte_gauge.increment(bytes as f64);
        queued.increment(1.0);
        Self {
            _permit: permit,
            bytes,
            reserved,
            byte_gauge,
            queued,
            wait: metrics::histogram!("lingshu_sdk_channel_command_queue_wait_seconds"),
            duration: metrics::histogram!("lingshu_sdk_channel_command_reservation_seconds"),
            started: Instant::now(),
            processing: false,
        }
    }
    pub(super) fn start_processing(&mut self) {
        if !self.processing {
            self.processing = true;
            self.queued.decrement(1.0);
            self.wait.record(self.started.elapsed().as_secs_f64());
        }
    }
}
impl Drop for CommandReservation {
    fn drop(&mut self) {
        if !self.processing {
            self.queued.decrement(1.0);
        }
        self.reserved.decrement(1.0);
        self.byte_gauge.decrement(self.bytes as f64);
        self.duration.record(self.started.elapsed().as_secs_f64());
    }
}

/// One encoded native exchange, separate from any business/domain disposition.
/// Dropped or unproved replies remain unknown, including task cancellation.
pub(super) struct ExchangeObservation {
    inflight: Gauge,
    duration: Histogram,
    unknown: metrics::Counter,
    verified: metrics::Counter,
    started: Instant,
    confirmed: bool,
}
impl ExchangeObservation {
    pub(super) fn new(plane: Plane) -> Self {
        let inflight =
            metrics::gauge!("lingshu_sdk_channel_exchange_inflight", "plane" => plane.label());
        inflight.increment(1.0);
        Self {
            inflight,
            duration: metrics::histogram!("lingshu_sdk_channel_exchange_seconds", "plane" => plane.label()),
            unknown: metrics::counter!("lingshu_sdk_channel_exchanges_total", "plane" => plane.label(), "outcome" => "unknown"),
            verified: metrics::counter!("lingshu_sdk_channel_exchanges_total", "plane" => plane.label(), "outcome" => "reply_verified"),
            started: Instant::now(),
            confirmed: false,
        }
    }
    /// Call only after the path's correlation, signature, finite scope and
    /// response checks. A verified rejection is also a verified exchange;
    /// control envelopes do not prove role readiness or business custody.
    pub(super) fn reply_verified(&mut self) {
        self.confirmed = true;
    }
}
impl Drop for ExchangeObservation {
    fn drop(&mut self) {
        self.inflight.decrement(1.0);
        self.duration.record(self.started.elapsed().as_secs_f64());
        if self.confirmed {
            self.verified.increment(1);
        } else {
            self.unknown.increment(1);
        }
    }
}

#[cfg(any(feature = "service-call-zenoh", feature = "event-consumer-zenoh"))]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum BusinessOutcome {
    #[cfg(feature = "service-call-zenoh")]
    Succeeded,
    #[cfg(feature = "service-call-zenoh")]
    Failed,
    #[cfg(feature = "service-call-zenoh")]
    Rejected,
    #[cfg(feature = "event-consumer-zenoh")]
    Completed,
    #[cfg(feature = "event-consumer-zenoh")]
    Throttled,
    #[cfg(feature = "event-consumer-zenoh")]
    RetryableFailure,
    #[cfg(feature = "event-consumer-zenoh")]
    PermanentFailure,
    #[cfg(feature = "event-consumer-zenoh")]
    TimedOut,
    Unknown,
}
#[cfg(any(feature = "service-call-zenoh", feature = "event-consumer-zenoh"))]
impl BusinessOutcome {
    fn label(self) -> &'static str {
        match self {
            #[cfg(feature = "service-call-zenoh")]
            Self::Succeeded => "succeeded",
            #[cfg(feature = "service-call-zenoh")]
            Self::Failed => "failed",
            #[cfg(feature = "service-call-zenoh")]
            Self::Rejected => "rejected",
            #[cfg(feature = "event-consumer-zenoh")]
            Self::Completed => "completed",
            #[cfg(feature = "event-consumer-zenoh")]
            Self::Throttled => "throttled",
            #[cfg(feature = "event-consumer-zenoh")]
            Self::RetryableFailure => "retryable_failure",
            #[cfg(feature = "event-consumer-zenoh")]
            Self::PermanentFailure => "permanent_failure",
            #[cfg(feature = "event-consumer-zenoh")]
            Self::TimedOut => "timed_out",
            Self::Unknown => "unknown",
        }
    }
}

/// Shared only with the original accepted reporter. The first observed result
/// ends timing; same-result report retries never recount business execution.
#[cfg(any(feature = "service-call-zenoh", feature = "event-consumer-zenoh"))]
#[derive(Clone)]
pub(super) struct BusinessObservation(Arc<Mutex<Option<BusinessTimer>>>);
#[cfg(any(feature = "service-call-zenoh", feature = "event-consumer-zenoh"))]
struct BusinessTimer {
    inflight: Gauge,
    duration: Histogram,
    outcomes: Vec<(BusinessOutcome, metrics::Counter)>,
    started: Instant,
    outcome: BusinessOutcome,
}
#[cfg(any(feature = "service-call-zenoh", feature = "event-consumer-zenoh"))]
impl BusinessObservation {
    pub(super) fn new(plane: Plane) -> Self {
        let inflight =
            metrics::gauge!("lingshu_sdk_channel_business_inflight", "plane" => plane.label());
        inflight.increment(1.0);
        let outcomes = [
            #[cfg(feature = "service-call-zenoh")]
            BusinessOutcome::Succeeded,
            #[cfg(feature = "service-call-zenoh")]
            BusinessOutcome::Failed,
            #[cfg(feature = "service-call-zenoh")]
            BusinessOutcome::Rejected,
            #[cfg(feature = "event-consumer-zenoh")]
            BusinessOutcome::Completed,
            #[cfg(feature = "event-consumer-zenoh")]
            BusinessOutcome::Throttled,
            #[cfg(feature = "event-consumer-zenoh")]
            BusinessOutcome::RetryableFailure,
            #[cfg(feature = "event-consumer-zenoh")]
            BusinessOutcome::PermanentFailure,
            #[cfg(feature = "event-consumer-zenoh")]
            BusinessOutcome::TimedOut,
            BusinessOutcome::Unknown,
        ].into_iter().map(|outcome| (outcome, metrics::counter!("lingshu_sdk_channel_business_results_total", "plane" => plane.label(), "outcome" => outcome.label()))).collect();
        Self(Arc::new(Mutex::new(Some(BusinessTimer {
            inflight,
            duration: metrics::histogram!("lingshu_sdk_channel_business_seconds", "plane" => plane.label()),
            outcomes,
            started: Instant::now(),
            outcome: BusinessOutcome::Unknown,
        }))))
    }
    pub(super) fn finish(&self, outcome: BusinessOutcome) {
        if let Some(mut timer) = self.0.lock().unwrap_or_else(|e| e.into_inner()).take() {
            timer.outcome = outcome;
        }
    }
}
#[cfg(any(feature = "service-call-zenoh", feature = "event-consumer-zenoh"))]
impl Drop for BusinessTimer {
    fn drop(&mut self) {
        self.inflight.decrement(1.0);
        self.duration.record(self.started.elapsed().as_secs_f64());
        if let Some((_, counter)) = self
            .outcomes
            .iter()
            .find(|(outcome, _)| *outcome == self.outcome)
        {
            counter.increment(1);
        }
    }
}

/// A local reply API completion is not a remote receipt or durable acknowledgement.
#[cfg(any(feature = "service-call-zenoh", feature = "event-consumer-zenoh"))]
pub(super) struct InboundExchangeObservation {
    inflight: Gauge,
    duration: Histogram,
    unknown: metrics::Counter,
    submitted: metrics::Counter,
    started: Instant,
    replied: bool,
}
#[cfg(any(feature = "service-call-zenoh", feature = "event-consumer-zenoh"))]
impl InboundExchangeObservation {
    pub(super) fn new(plane: Plane) -> Self {
        let inflight = metrics::gauge!("lingshu_sdk_channel_inbound_exchange_inflight", "plane" => plane.label());
        inflight.increment(1.0);
        Self {
            inflight,
            duration: metrics::histogram!("lingshu_sdk_channel_inbound_exchange_seconds", "plane" => plane.label()),
            unknown: metrics::counter!("lingshu_sdk_channel_inbound_exchanges_total", "plane" => plane.label(), "outcome" => "unknown"),
            submitted: metrics::counter!("lingshu_sdk_channel_inbound_exchanges_total", "plane" => plane.label(), "outcome" => "reply_submitted"),
            started: Instant::now(),
            replied: false,
        }
    }
    pub(super) fn reply_submitted(&mut self) {
        self.replied = true;
    }
}
#[cfg(any(feature = "service-call-zenoh", feature = "event-consumer-zenoh"))]
impl Drop for InboundExchangeObservation {
    fn drop(&mut self) {
        self.inflight.decrement(1.0);
        self.duration.record(self.started.elapsed().as_secs_f64());
        if self.replied {
            self.submitted.increment(1);
        } else {
            self.unknown.increment(1);
        }
    }
}

#[cfg(feature = "service-call-zenoh")]
pub(super) fn prepared_call_response(
    response: &kish_lingshu_runtime_contract::service::NativeCallResponse,
) {
    use kish_lingshu_runtime_contract::service::{NativeCallResponse as R, ServiceOutcome};
    let outcome = match response {
        R::Ready => "ready",
        R::AsyncReady => "async_ready",
        R::Accepted { .. } => "accepted",
        R::Completed {
            outcome: ServiceOutcome::Succeeded { .. },
        } => "succeeded",
        R::Completed {
            outcome: ServiceOutcome::Failed { .. },
        } => "failed",
        R::Cancelled => "cancelled",
        R::Rejected { .. } => "rejected",
        R::OutcomeUnknown => "unknown",
    };
    metrics::counter!("lingshu_sdk_channel_prepared_responses_total", "plane" => "call", "outcome" => outcome).increment(1);
}
#[cfg(feature = "event-consumer-zenoh")]
pub(super) fn consumer_outcome(
    response: &kish_lingshu_event_dispatch_contract::NativeConsumerResponse,
) -> BusinessOutcome {
    use kish_lingshu_event_dispatch_contract::NativeConsumerResponse as R;
    match response {
        R::Completed { .. } => BusinessOutcome::Completed,
        R::Throttled { .. } => BusinessOutcome::Throttled,
        R::RetryableFailure { .. } => BusinessOutcome::RetryableFailure,
        R::PermanentFailure { .. } => BusinessOutcome::PermanentFailure,
        R::TimedOut => BusinessOutcome::TimedOut,
    }
}
#[cfg(feature = "event-consumer-zenoh")]
pub(super) fn prepared_consumer_response(outcome: BusinessOutcome) {
    metrics::counter!("lingshu_sdk_channel_prepared_responses_total", "plane" => "consumer", "outcome" => outcome.label()).increment(1);
}

#[cfg(any(feature = "service-call-zenoh", feature = "event-publication-zenoh"))]
#[derive(Clone, Copy)]
pub(super) enum Disposition {
    #[cfg(feature = "event-publication-zenoh")]
    Accepted,
    #[cfg(feature = "event-publication-zenoh")]
    RetryableFailure,
    #[cfg(feature = "event-publication-zenoh")]
    PermanentFailure,
    #[cfg(feature = "service-call-zenoh")]
    Recorded,
    #[cfg(feature = "service-call-zenoh")]
    Duplicate,
    #[cfg(feature = "service-call-zenoh")]
    Renewed,
    #[cfg(feature = "service-call-zenoh")]
    Recovering,
    #[cfg(feature = "service-call-zenoh")]
    Invalidated,
    #[cfg(feature = "service-call-zenoh")]
    AuthorityUnavailable,
    #[cfg(feature = "service-call-zenoh")]
    Rejected,
    #[cfg(feature = "service-call-zenoh")]
    Unavailable,
}
#[cfg(any(feature = "service-call-zenoh", feature = "event-publication-zenoh"))]
pub(super) fn disposition(plane: Plane, value: Disposition) {
    let label = match value {
        #[cfg(feature = "event-publication-zenoh")]
        Disposition::Accepted => "accepted",
        #[cfg(feature = "event-publication-zenoh")]
        Disposition::RetryableFailure => "retryable_failure",
        #[cfg(feature = "event-publication-zenoh")]
        Disposition::PermanentFailure => "permanent_failure",
        #[cfg(feature = "service-call-zenoh")]
        Disposition::Recorded => "recorded",
        #[cfg(feature = "service-call-zenoh")]
        Disposition::Duplicate => "duplicate",
        #[cfg(feature = "service-call-zenoh")]
        Disposition::Renewed => "renewed",
        #[cfg(feature = "service-call-zenoh")]
        Disposition::Recovering => "recovering",
        #[cfg(feature = "service-call-zenoh")]
        Disposition::Invalidated => "invalidated",
        #[cfg(feature = "service-call-zenoh")]
        Disposition::AuthorityUnavailable => "authority_unavailable",
        #[cfg(feature = "service-call-zenoh")]
        Disposition::Rejected => "rejected",
        #[cfg(feature = "service-call-zenoh")]
        Disposition::Unavailable => "unavailable",
    };
    metrics::counter!("lingshu_sdk_channel_verified_dispositions_total", "plane" => plane.label(), "outcome" => label).increment(1);
}

/// Per-pool accounting for reserved namespaces, including reservations retained
/// after unproven cleanup. Dropping the last owner ends local accounting only;
/// it does not assert that Router declarations were successfully removed.
pub(super) struct DeclarationAccounting {
    totals: Mutex<(usize, usize)>,
    roles: Gauge,
    keys: Gauge,
}
impl DeclarationAccounting {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            totals: Mutex::new((0, 0)),
            roles: metrics::gauge!("lingshu_sdk_channel_reserved_roles"),
            keys: metrics::gauge!("lingshu_sdk_channel_reserved_declaration_keys"),
        })
    }
    pub(super) fn reserve(self: &Arc<Self>, keys: usize) -> DeclarationObservation {
        let mut totals = self.totals.lock().unwrap_or_else(|e| e.into_inner());
        totals.0 += 1;
        totals.1 += keys;
        self.roles.increment(1.0);
        self.keys.increment(keys as f64);
        DeclarationObservation {
            owner: self.clone(),
            keys,
            released: false,
            retained: metrics::counter!(
                "lingshu_sdk_channel_declaration_reservations_retained_total"
            ),
        }
    }
}
impl Drop for DeclarationAccounting {
    fn drop(&mut self) {
        let totals = self.totals.get_mut().unwrap_or_else(|e| e.into_inner());
        self.roles.decrement(totals.0 as f64);
        self.keys.decrement(totals.1 as f64);
    }
}
pub(super) struct DeclarationObservation {
    owner: Arc<DeclarationAccounting>,
    keys: usize,
    released: bool,
    retained: metrics::Counter,
}
impl DeclarationObservation {
    pub(super) fn release(&mut self) {
        if !self.released {
            self.released = true;
            let mut totals = self.owner.totals.lock().unwrap_or_else(|e| e.into_inner());
            totals.0 -= 1;
            totals.1 -= self.keys;
            self.owner.roles.decrement(1.0);
            self.owner.keys.decrement(self.keys as f64);
        }
    }
}
impl Drop for DeclarationObservation {
    fn drop(&mut self) {
        if !self.released {
            self.retained.increment(1);
        }
    }
}

#[cfg(test)]
pub(super) mod tests;
