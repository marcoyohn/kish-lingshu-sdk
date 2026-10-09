use super::*;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

#[derive(Default, Clone)]
pub(crate) struct Capture(Arc<Mutex<Vec<(Key, f64)>>>);
struct Handle(Key, Arc<Mutex<Vec<(Key, f64)>>>);
impl Handle {
    fn record(&self, value: f64) {
        self.1.lock().unwrap().push((self.0.clone(), value));
    }
}
impl metrics::GaugeFn for Handle {
    fn increment(&self, value: f64) {
        self.record(value);
    }
    fn decrement(&self, value: f64) {
        self.record(-value);
    }
    fn set(&self, _: f64) {
        panic!("resource gauges must be additive");
    }
}
impl metrics::CounterFn for Handle {
    fn increment(&self, value: u64) {
        self.record(value as f64);
    }
    fn absolute(&self, _: u64) {
        panic!("resource counters must be additive");
    }
}
impl metrics::HistogramFn for Handle {
    fn record(&self, value: f64) {
        self.record(value);
    }
}
impl Capture {
    pub(crate) fn for_native_fixture() -> Option<Self> {
        (std::env::var("LINGSHU_VERIFY_SDK_RESOURCES").as_deref() == Ok("true")).then(|| {
            let capture = Self::default();
            metrics::set_global_recorder(capture.clone()).unwrap_or_else(|_| {
                panic!("SDK resource acceptance requires its own test process")
            });
            capture
        })
    }
    pub(crate) async fn observe<F: Future>(&self, future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        std::future::poll_fn(|cx| metrics::with_local_recorder(self, || future.as_mut().poll(cx)))
            .await
    }
    fn handle(&self, key: &Key) -> Arc<Handle> {
        Arc::new(Handle(key.clone(), self.0.clone()))
    }
    pub(crate) fn sum(&self, name: &str, plane: Option<&str>, outcome: Option<&str>) -> f64 {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| {
                key.name() == name
                    && plane.map_or(true, |v| {
                        key.labels().any(|l| l.key() == "plane" && l.value() == v)
                    })
                    && outcome.map_or(true, |v| {
                        key.labels().any(|l| l.key() == "outcome" && l.value() == v)
                    })
            })
            .map(|(_, v)| v)
            .sum()
    }
    pub(crate) fn count(&self, name: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.name() == name)
            .count()
    }
    pub(crate) fn values(&self, name: &str, plane: Option<&str>) -> Vec<f64> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| {
                key.name() == name
                    && plane.map_or(true, |value| {
                        key.labels()
                            .any(|label| label.key() == "plane" && label.value() == value)
                    })
            })
            .map(|(_, value)| *value)
            .collect()
    }
    pub(crate) fn assert_bounded_labels(&self) {
        for (key, _) in self.0.lock().unwrap().iter() {
            if !key.name().starts_with("lingshu_sdk_channel_") {
                continue;
            }
            assert!(key.labels().all(|l| matches!(
                (l.key(), l.value()),
                (
                    "plane",
                    "role_business"
                        | "role_control"
                        | "catalog"
                        | "call"
                        | "consumer"
                        | "declaration"
                        | "data"
                        | "control"
                        | "role_change"
                        | "heartbeat"
                        | "completion"
                        | "publication"
                ) | (
                    "outcome",
                    "invalid"
                        | "count_exhausted"
                        | "bytes_exhausted"
                        | "catalog_exhausted"
                        | "queue_full"
                        | "queue_closed"
                        | "task_exhausted"
                        | "duplicate_declaration"
                        | "explicit"
                        | "connection_closed"
                        | "rotation_failed"
                        | "authority_expired"
                        | "cleanup_failed"
                        | "unknown"
                        | "reply_verified"
                        | "accepted"
                        | "recorded"
                        | "duplicate"
                        | "renewed"
                        | "recovering"
                        | "invalidated"
                        | "authority_unavailable"
                        | "rejected"
                        | "unavailable"
                        | "retryable_failure"
                        | "permanent_failure"
                        | "succeeded"
                        | "failed"
                        | "completed"
                        | "throttled"
                        | "timed_out"
                        | "ready"
                        | "async_ready"
                        | "cancelled"
                        | "reply_submitted"
                )
            )));
        }
    }

    pub(crate) fn assert_balanced_resource_gauges(&self) {
        let mut totals = std::collections::HashMap::<Key, f64>::new();
        for (key, value) in self.0.lock().unwrap().iter() {
            if !matches!(
                key.name(),
                "lingshu_sdk_channel_reserved_queries"
                    | "lingshu_sdk_channel_reserved_bytes"
                    | "lingshu_sdk_channel_queued_queries"
                    | "lingshu_sdk_channel_reserved_roles"
                    | "lingshu_sdk_channel_reserved_declaration_keys"
                    | "lingshu_sdk_channel_reserved_commands"
                    | "lingshu_sdk_channel_command_reserved_bytes"
                    | "lingshu_sdk_channel_queued_commands"
                    | "lingshu_sdk_channel_exchange_inflight"
                    | "lingshu_sdk_channel_business_inflight"
                    | "lingshu_sdk_channel_inbound_exchange_inflight"
            ) {
                continue;
            }
            let total = totals.entry(key.clone()).or_default();
            *total += value;
            assert!(
                *total >= 0.0,
                "negative resource observation: {}",
                key.name()
            );
        }
        assert!(!totals.is_empty());
        assert!(totals.values().all(|v| *v == 0.0));
    }
}
impl Recorder for Capture {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(self.handle(key))
    }
    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(self.handle(key))
    }
    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(self.handle(key))
    }
}
fn budgets(count: usize, bytes: usize) -> (Arc<Semaphore>, Arc<Semaphore>) {
    (
        Arc::new(Semaphore::new(count)),
        Arc::new(Semaphore::new(bytes)),
    )
}

#[test]
fn saturated_business_bytes_and_count_leave_control_reserve_available() {
    let capture = Capture::default();
    metrics::with_local_recorder(&capture, || {
        let (count, bytes) = budgets(2, 8);
        let (control_count, control_bytes) = budgets(1, 4);
        let first = QueryReservation::acquire(Plane::RoleBusiness, &count, &bytes, 4).unwrap();
        assert!(QueryReservation::acquire(Plane::RoleBusiness, &count, &bytes, 5).is_none());
        assert_eq!(count.available_permits(), 1);
        let second = QueryReservation::acquire(Plane::RoleBusiness, &count, &bytes, 4).unwrap();
        assert!(QueryReservation::acquire(Plane::RoleBusiness, &count, &bytes, 1).is_none());
        let control =
            QueryReservation::acquire(Plane::RoleControl, &control_count, &control_bytes, 4)
                .unwrap();
        assert_eq!(
            capture.sum(
                "lingshu_sdk_channel_reserved_queries",
                Some("role_business"),
                None
            ),
            2.0
        );
        assert_eq!(
            capture.sum(
                "lingshu_sdk_channel_reserved_queries",
                Some("role_control"),
                None
            ),
            1.0
        );
        drop((first, second, control));
        assert_eq!(count.available_permits(), 2);
        assert_eq!(bytes.available_permits(), 8);
        assert_eq!(control_count.available_permits(), 1);
        assert_eq!(control_bytes.available_permits(), 4);
    });
    assert_eq!(
        capture.sum("lingshu_sdk_channel_reserved_queries", None, None),
        0.0
    );
    assert_eq!(
        capture.sum("lingshu_sdk_channel_reserved_bytes", None, None),
        0.0
    );
    for outcome in ["bytes_exhausted", "count_exhausted"] {
        assert_eq!(
            capture.sum(
                "lingshu_sdk_channel_admission_rejections_total",
                Some("role_business"),
                Some(outcome)
            ),
            1.0
        );
    }
    capture.assert_bounded_labels();
}

#[test]
fn full_closed_and_dropped_queues_return_reservations_without_dequeue_observations() {
    let capture = Capture::default();
    metrics::with_local_recorder(&capture, || {
        let (count, bytes) = budgets(3, 12);
        let (sender, mut receiver) = mpsc::channel(1);
        for _ in 0..2 {
            enqueue(
                &sender,
                QueryReservation::acquire(Plane::RoleBusiness, &count, &bytes, 4).unwrap(),
                Plane::RoleBusiness,
            );
        }
        assert_eq!(count.available_permits(), 2);
        assert_eq!(bytes.available_permits(), 8);
        assert_eq!(
            capture.sum("lingshu_sdk_channel_queued_queries", None, None),
            1.0
        );
        receiver.close();
        enqueue(
            &sender,
            QueryReservation::acquire(Plane::RoleBusiness, &count, &bytes, 4).unwrap(),
            Plane::RoleBusiness,
        );
        drop(receiver);
        assert_eq!(count.available_permits(), 3);
        assert_eq!(bytes.available_permits(), 12);
    });
    assert_eq!(
        capture.sum("lingshu_sdk_channel_queued_queries", None, None),
        0.0
    );
    assert_eq!(
        capture.sum("lingshu_sdk_channel_reserved_queries", None, None),
        0.0
    );
    assert_eq!(
        capture.sum("lingshu_sdk_channel_reserved_bytes", None, None),
        0.0
    );
    assert_eq!(capture.count("lingshu_sdk_channel_queue_wait_seconds"), 0);
    assert_eq!(capture.count("lingshu_sdk_channel_reservation_seconds"), 3);
    for outcome in ["queue_full", "queue_closed"] {
        assert_eq!(
            capture.sum(
                "lingshu_sdk_channel_admission_rejections_total",
                None,
                Some(outcome)
            ),
            1.0
        );
    }
    capture.assert_bounded_labels();
}

#[test]
fn dequeued_worker_cancellation_releases_bytes_and_records_queue_wait_once() {
    let capture = Capture::default();
    metrics::with_local_recorder(&capture, || {
        let (count, bytes) = budgets(1, 4);
        let (sender, mut receiver) = mpsc::channel(1);
        enqueue(
            &sender,
            QueryReservation::acquire(Plane::RoleBusiness, &count, &bytes, 4).unwrap(),
            Plane::RoleBusiness,
        );
        let mut reservation = receiver.try_recv().unwrap();
        reservation.start_processing();
        reservation.start_processing();
        assert_eq!(
            capture.sum("lingshu_sdk_channel_queued_queries", None, None),
            0.0
        );
        assert_eq!(
            capture.sum("lingshu_sdk_channel_reserved_queries", None, None),
            1.0
        );
        assert_eq!(
            capture.sum("lingshu_sdk_channel_reserved_bytes", None, None),
            4.0
        );
        let mut worker = Box::pin(async move {
            let _reservation = reservation;
            std::future::pending::<()>().await;
        });
        assert_eq!(
            worker
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        );
        drop(worker);
        assert_eq!(count.available_permits(), 1);
        assert_eq!(bytes.available_permits(), 4);
    });
    assert_eq!(
        capture.sum("lingshu_sdk_channel_reserved_queries", None, None),
        0.0
    );
    assert_eq!(
        capture.sum("lingshu_sdk_channel_reserved_bytes", None, None),
        0.0
    );
    assert_eq!(capture.count("lingshu_sdk_channel_queue_wait_seconds"), 1);
    assert_eq!(capture.count("lingshu_sdk_channel_reservation_seconds"), 1);
}

#[test]
fn siblings_and_worker_thread_keep_their_own_recorder_contribution() {
    let capture = Capture::default();
    let (first, second) = metrics::with_local_recorder(&capture, || {
        let (count, bytes) = budgets(1, 8);
        let first = QueryReservation::acquire(Plane::RoleBusiness, &count, &bytes, 8).unwrap();
        let (count, bytes) = budgets(1, 16);
        let second = QueryReservation::acquire(Plane::RoleBusiness, &count, &bytes, 16).unwrap();
        (first, second)
    });
    assert_eq!(
        capture.sum("lingshu_sdk_channel_reserved_queries", None, None),
        2.0
    );
    std::thread::spawn(move || drop(first)).join().unwrap();
    assert_eq!(
        capture.sum("lingshu_sdk_channel_reserved_queries", None, None),
        1.0
    );
    assert_eq!(
        capture.sum("lingshu_sdk_channel_reserved_bytes", None, None),
        16.0
    );
    drop(second);
    assert_eq!(
        capture.sum("lingshu_sdk_channel_reserved_queries", None, None),
        0.0
    );
    assert_eq!(
        capture.sum("lingshu_sdk_channel_reserved_bytes", None, None),
        0.0
    );
}

#[cfg(feature = "service-call-zenoh")]
#[test]
fn accepted_result_owner_records_once_and_cancellation_balances_original_recorder() {
    let capture = Capture::default();
    let result = metrics::with_local_recorder(&capture, || BusinessObservation::new(Plane::Call));
    let reporter = result.clone();
    drop(result);
    assert_eq!(
        capture.sum("lingshu_sdk_channel_business_inflight", Some("call"), None),
        1.0
    );
    std::thread::spawn(move || {
        reporter.finish(BusinessOutcome::Succeeded);
        reporter.finish(BusinessOutcome::Succeeded);
        reporter.finish(BusinessOutcome::Failed);
    })
    .join()
    .unwrap();
    assert_eq!(
        capture.sum(
            "lingshu_sdk_channel_business_results_total",
            Some("call"),
            Some("succeeded")
        ),
        1.0
    );
    assert_eq!(
        capture.sum(
            "lingshu_sdk_channel_business_results_total",
            Some("call"),
            Some("failed")
        ),
        0.0
    );
    let (pending, reply) = metrics::with_local_recorder(&capture, || {
        (
            BusinessObservation::new(Plane::Call),
            InboundExchangeObservation::new(Plane::Call),
        )
    });
    std::thread::spawn(move || drop((pending, reply)))
        .join()
        .unwrap();
    assert_eq!(
        capture.sum(
            "lingshu_sdk_channel_business_results_total",
            Some("call"),
            Some("unknown")
        ),
        1.0
    );
    assert_eq!(
        capture.sum(
            "lingshu_sdk_channel_inbound_exchanges_total",
            Some("call"),
            Some("unknown")
        ),
        1.0
    );
    capture.assert_balanced_resource_gauges();
    capture.assert_bounded_labels();
}
