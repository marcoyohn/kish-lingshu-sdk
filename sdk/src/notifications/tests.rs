use super::*;
use std::sync::atomic::AtomicUsize;

#[tokio::test]
async fn fans_out_concurrently_and_filters_kinds_without_retrying_failures() {
    let d = NotificationDispatcher::default();
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut guards = Vec::new();
    for fail in [false, true] {
        let barrier = barrier.clone();
        let calls = calls.clone();
        guards.push(
            d.register_fn("cache.changed", move |_| {
                let barrier = barrier.clone();
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    barrier.wait().await;
                    if fail {
                        Err(NotificationError::Handler)
                    } else {
                        Ok(())
                    }
                }
            })
            .unwrap(),
        );
    }
    let _other = d
        .register_fn("inbox.changed", |_| async { panic!("wrong kind") })
        .unwrap();
    assert_eq!(d.dispatch(Notification::hint("cache.changed")).unwrap(), 2);
    tokio::time::timeout(Duration::from_secs(1), barrier.wait())
        .await
        .unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(d.stats().failed, 1);
    assert_eq!(d.stats().invoked, 2);
}

#[tokio::test(start_paused = true)]
async fn bounds_mailboxes_and_isolates_panic_timeout_and_unregister() {
    let d = NotificationDispatcher::new(DispatcherOptions {
        mailbox_capacity: 1,
        ..Default::default()
    })
    .unwrap();
    let guard = d
        .register_fn("a", |_| async { std::future::pending().await })
        .unwrap();
    let _panic = d
        .register_fn("b", |_| async { panic!("handler panic") })
        .unwrap();
    d.dispatch(Notification::hint("a")).unwrap();
    d.dispatch(Notification::hint("b")).unwrap();
    tokio::task::yield_now().await;
    d.dispatch(Notification::hint("a")).unwrap();
    assert_eq!(d.dispatch(Notification::hint("a")).unwrap(), 0);
    assert_eq!(d.stats().dropped, 1);
    tokio::time::advance(Duration::from_secs(6)).await;
    tokio::task::yield_now().await;
    assert_eq!(d.stats().panicked, 1);
    assert_eq!(d.stats().timed_out, 1);
    guard.close().await;
    assert_eq!(d.dispatch(Notification::hint("a")).unwrap(), 0);
    assert_eq!(d.stats().invoked, 3); // queued new notification, never a retry
}

struct StaticHandlers(Arc<AtomicUsize>);
#[crate::lingshu_notifications]
impl StaticHandlers {
    #[notification_handler(kind = "changed")]
    async fn first(&self, _: NotificationContext) -> NotificationResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    #[notification_handler(kind = "changed")]
    async fn second(&self, _: NotificationContext) -> NotificationResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn annotated_methods_share_fanout_and_duplicate_binding_is_rejected() {
    let d = NotificationDispatcher::default();
    let handlers = Arc::new(StaticHandlers(Arc::new(AtomicUsize::new(0))));
    let registrations = handlers.bind_notifications(&d).unwrap();
    assert!(matches!(
        handlers.bind_notifications(&d),
        Err(NotificationError::Duplicate)
    ));
    d.dispatch(Notification::hint("changed")).unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(handlers.0.load(Ordering::SeqCst), 2);
    drop(registrations);
    assert_eq!(d.dispatch(Notification::hint("changed")).unwrap(), 0);
}
#[tokio::test]
async fn ready_is_separate_from_business_notifications() {
    let d = NotificationDispatcher::default();
    let mut ready = d.subscribe_ready();
    let _handler = d
        .register_fn("changed", |_| async {
            panic!("ready is not business data")
        })
        .unwrap();
    d.transport_ready();
    ready.changed().await.unwrap();
    assert_eq!(d.stats().invoked, 0);
}

struct BlockedTransport(Arc<AtomicUsize>);
#[async_trait]
impl NotificationTransport for BlockedTransport {
    async fn publish(&self, _: &Notification) -> NotificationResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
}
#[tokio::test(start_paused = true)]
async fn publication_times_out_once_without_local_dispatch_or_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let d = NotificationDispatcher::default();
    let bus = Notifications::new(d.clone(), Arc::new(BlockedTransport(calls.clone())));
    let _registration = bus
        .register_fn("a", |_| async { panic!("outbound is not loopback") })
        .unwrap();
    assert!(matches!(
        bus.notify("a").await,
        Err(NotificationError::Timeout)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(d.stats().invoked, 0);
}
#[tokio::test]
async fn registration_capacity_and_release_are_enforced() {
    let d = NotificationDispatcher::new(DispatcherOptions {
        max_handlers: 1,
        ..Default::default()
    })
    .unwrap();
    let first = d.register_fn("a", |_| async { Ok(()) }).unwrap();
    assert!(matches!(
        d.register_fn("a", |_| async { Ok(()) }),
        Err(NotificationError::Capacity)
    ));
    first.close().await;
    let _second = d.register_fn("a", |_| async { Ok(()) }).unwrap();
}
