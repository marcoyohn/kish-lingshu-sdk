//! Transport-neutral best-effort notifications. No persistence, ACK or retry.
//! Keep registration guards alive. Handlers should signal a business-owned
//! coordinator; authoritative reconciliation belongs to the application.
use async_trait::async_trait;
use futures::FutureExt;
use kish_lingshu_foundation_contract::notifications::valid_kind;
pub use kish_lingshu_foundation_contract::notifications::{
    InvalidNotification, Notification, MAX_NOTIFICATION_BYTES,
};
use std::{
    collections::BTreeMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch, Semaphore},
    task::JoinHandle,
};

pub type NotificationResult = Result<(), NotificationError>;
#[derive(Debug, thiserror::Error)]
pub enum NotificationError {
    #[error(transparent)]
    Invalid(#[from] InvalidNotification),
    #[error("notification capacity exceeded")]
    Capacity,
    #[error("duplicate static notification registration")]
    Duplicate,
    #[error("notification transport unavailable")]
    Unavailable,
    #[error("notification operation timed out")]
    Timeout,
    #[error("invalid notification configuration or missing Tokio runtime")]
    Configuration,
    #[error("notification handler failed")]
    Handler,
}

#[derive(Clone, Debug)]
pub struct NotificationContext {
    pub notification: Arc<Notification>,
}
impl NotificationContext {
    pub fn decode<T: serde::de::DeserializeOwned>(&self) -> Result<T, NotificationError> {
        serde_json::from_value(
            self.notification
                .data
                .clone()
                .unwrap_or(serde_json::Value::Null),
        )
        .map_err(|_| NotificationError::Invalid(InvalidNotification))
    }
}
#[async_trait]
pub trait NotificationHandler: Send + Sync + 'static {
    async fn handle(&self, context: NotificationContext) -> NotificationResult;
}
struct ClosureHandler<F>(F);
#[async_trait]
impl<F, Fut> NotificationHandler for ClosureHandler<F>
where
    F: Fn(NotificationContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = NotificationResult> + Send + 'static,
{
    async fn handle(&self, context: NotificationContext) -> NotificationResult {
        (self.0)(context).await
    }
}

/// Outbound operation only. Adapters feed validated inbound messages to the
/// shared dispatcher and announce reconnects through `transport_ready`.
#[async_trait]
pub trait NotificationTransport: Send + Sync + 'static {
    async fn publish(&self, notification: &Notification) -> NotificationResult;
}
#[derive(Clone)]
pub struct Notifications {
    dispatcher: NotificationDispatcher,
    transport: Arc<dyn NotificationTransport>,
    sending: Arc<Semaphore>,
}
impl Notifications {
    pub fn new(
        dispatcher: NotificationDispatcher,
        transport: Arc<dyn NotificationTransport>,
    ) -> Self {
        Self {
            dispatcher,
            transport,
            sending: Arc::new(Semaphore::new(32)),
        }
    }
    pub fn dispatcher(&self) -> &NotificationDispatcher {
        &self.dispatcher
    }
    pub async fn publish(&self, notification: Notification) -> NotificationResult {
        notification.encode()?;
        let _permit = self
            .sending
            .try_acquire()
            .map_err(|_| NotificationError::Capacity)?;
        tokio::time::timeout(
            Duration::from_secs(1),
            self.transport.publish(&notification),
        )
        .await
        .map_err(|_| NotificationError::Timeout)?
    }
    pub async fn notify(&self, kind: impl Into<String>) -> NotificationResult {
        self.publish(Notification::hint(kind)).await
    }
    pub fn register(
        &self,
        kind: &str,
        handler: Arc<dyn NotificationHandler>,
    ) -> Result<NotificationRegistration, NotificationError> {
        self.dispatcher.register(kind, handler)
    }
    pub fn register_fn<F, Fut>(
        &self,
        kind: &str,
        handler: F,
    ) -> Result<NotificationRegistration, NotificationError>
    where
        F: Fn(NotificationContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = NotificationResult> + Send + 'static,
    {
        self.dispatcher.register_fn(kind, handler)
    }
}
#[derive(Clone, Debug)]
pub struct DispatcherOptions {
    pub max_handlers: usize,
    pub mailbox_capacity: usize,
    pub max_concurrency: usize,
    pub handler_timeout: Duration,
}
impl Default for DispatcherOptions {
    fn default() -> Self {
        Self {
            max_handlers: 256,
            mailbox_capacity: 16,
            max_concurrency: 32,
            handler_timeout: Duration::from_secs(5),
        }
    }
}
#[derive(Default)]
struct Counters {
    invoked: AtomicU64,
    failed: AtomicU64,
    panicked: AtomicU64,
    timed_out: AtomicU64,
    dropped: AtomicU64,
}
#[derive(Debug, Default, PartialEq, Eq)]
pub struct NotificationStats {
    pub invoked: u64,
    pub failed: u64,
    pub panicked: u64,
    pub timed_out: u64,
    pub dropped: u64,
}
struct Entry {
    kind: String,
    static_key: Option<String>,
    sender: mpsc::Sender<NotificationContext>,
}
struct Inner {
    options: DispatcherOptions,
    handlers: Mutex<BTreeMap<u64, Entry>>,
    next_id: AtomicU64,
    permits: Arc<Semaphore>,
    counters: Arc<Counters>,
    ready: watch::Sender<u64>,
}
#[derive(Clone)]
pub struct NotificationDispatcher {
    inner: Arc<Inner>,
}
impl Default for NotificationDispatcher {
    fn default() -> Self {
        Self::new(DispatcherOptions::default()).expect("valid defaults")
    }
}
impl NotificationDispatcher {
    pub fn new(options: DispatcherOptions) -> Result<Self, NotificationError> {
        if options.max_handlers == 0
            || options.max_handlers > 65536
            || options.mailbox_capacity == 0
            || options.mailbox_capacity > 65536
            || options.max_concurrency == 0
            || options.max_concurrency > 65536
            || options.handler_timeout.is_zero()
        {
            return Err(NotificationError::Configuration);
        }
        let (ready, _) = watch::channel(0);
        Ok(Self {
            inner: Arc::new(Inner {
                permits: Arc::new(Semaphore::new(options.max_concurrency)),
                options,
                handlers: Mutex::new(BTreeMap::new()),
                next_id: AtomicU64::new(1),
                counters: Arc::new(Counters::default()),
                ready,
            }),
        })
    }
    pub fn register(
        &self,
        kind: &str,
        handler: Arc<dyn NotificationHandler>,
    ) -> Result<NotificationRegistration, NotificationError> {
        self.insert(kind, None, handler)
    }
    pub fn register_fn<F, Fut>(
        &self,
        kind: &str,
        handler: F,
    ) -> Result<NotificationRegistration, NotificationError>
    where
        F: Fn(NotificationContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = NotificationResult> + Send + 'static,
    {
        self.register(kind, Arc::new(ClosureHandler(handler)))
    }
    /// Used by method bindings; duplicate method bindings fail within this dispatcher.
    #[doc(hidden)]
    pub fn register_static_fn<F, Fut>(
        &self,
        kind: &str,
        key: &str,
        handler: F,
    ) -> Result<NotificationRegistration, NotificationError>
    where
        F: Fn(NotificationContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = NotificationResult> + Send + 'static,
    {
        self.insert(kind, Some(key.into()), Arc::new(ClosureHandler(handler)))
    }
    fn insert(
        &self,
        kind: &str,
        static_key: Option<String>,
        handler: Arc<dyn NotificationHandler>,
    ) -> Result<NotificationRegistration, NotificationError> {
        if !valid_kind(kind) {
            return Err(InvalidNotification.into());
        }
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| NotificationError::Configuration)?;
        let mut handlers = self
            .inner
            .handlers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if handlers.len() >= self.inner.options.max_handlers {
            return Err(NotificationError::Capacity);
        }
        if static_key.as_ref().is_some_and(|key| {
            handlers
                .values()
                .any(|e| e.static_key.as_ref() == Some(key))
        }) {
            return Err(NotificationError::Duplicate);
        }
        let (sender, mut receiver) =
            mpsc::channel::<NotificationContext>(self.inner.options.mailbox_capacity);
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let permits = self.inner.permits.clone();
        let counters = self.inner.counters.clone();
        let timeout = self.inner.options.handler_timeout;
        let worker = runtime.spawn(async move {
            while let Some(context) = receiver.recv().await {
                let Ok(_permit) = permits.acquire().await else {
                    break;
                };
                counters.invoked.fetch_add(1, Ordering::Relaxed);
                // Include future construction in the unwind boundary as well.
                let call = AssertUnwindSafe(async { handler.handle(context).await }).catch_unwind();
                match tokio::time::timeout(timeout, call).await {
                    Ok(Ok(Ok(()))) => {}
                    Ok(Ok(Err(_))) => {
                        counters.failed.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Err(_)) => {
                        counters.panicked.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        counters.timed_out.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
        handlers.insert(
            id,
            Entry {
                kind: kind.into(),
                static_key,
                sender,
            },
        );
        Ok(NotificationRegistration {
            owner: Arc::downgrade(&self.inner),
            id,
            worker: Some(worker),
        })
    }
    /// Nonblocking fanout. Returns the number enqueued, not the number executed.
    pub fn dispatch(&self, notification: Notification) -> Result<usize, NotificationError> {
        notification.encode()?;
        let context = NotificationContext {
            notification: Arc::new(notification),
        };
        let handlers = self
            .inner
            .handlers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut queued = 0;
        for entry in handlers
            .values()
            .filter(|e| e.kind == context.notification.kind)
        {
            if entry.sender.try_send(context.clone()).is_ok() {
                queued += 1;
            } else {
                self.inner.counters.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(queued)
    }
    pub fn receive(&self, bytes: &[u8]) -> Result<usize, NotificationError> {
        self.dispatch(Notification::decode(bytes)?)
    }
    /// A separate transport status signal; never synthesizes a business kind.
    pub fn transport_ready(&self) {
        self.inner.ready.send_modify(|n| *n = n.wrapping_add(1));
    }
    pub fn subscribe_ready(&self) -> watch::Receiver<u64> {
        self.inner.ready.subscribe()
    }
    pub fn stats(&self) -> NotificationStats {
        let c = &self.inner.counters;
        NotificationStats {
            invoked: c.invoked.load(Ordering::Relaxed),
            failed: c.failed.load(Ordering::Relaxed),
            panicked: c.panicked.load(Ordering::Relaxed),
            timed_out: c.timed_out.load(Ordering::Relaxed),
            dropped: c.dropped.load(Ordering::Relaxed),
        }
    }
}
/// Dropping unregisters and cancels queued/in-flight futures. Already performed
/// side effects are not rolled back. Use `close` to wait for cancellation.
#[must_use = "keep the registration alive to receive notifications"]
pub struct NotificationRegistration {
    owner: Weak<Inner>,
    id: u64,
    worker: Option<JoinHandle<()>>,
}
impl NotificationRegistration {
    fn unregister(&self) {
        if let Some(owner) = self.owner.upgrade() {
            owner
                .handlers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.id);
        }
        if let Some(worker) = &self.worker {
            worker.abort();
        }
    }
    pub async fn close(mut self) {
        self.unregister();
        if let Some(worker) = self.worker.take() {
            let _ = worker.await;
        }
    }
}
impl Drop for NotificationRegistration {
    fn drop(&mut self) {
        self.unregister();
    }
}

#[cfg(test)]
mod tests;
