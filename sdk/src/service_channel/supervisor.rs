//! Serial observation of existing transport authority. Business lease renewal,
//! certificate replacement and native reconnect remain separate responsibilities.
use super::{ChannelCloseReason, ChannelSessionError, ServiceChannelSessions};
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

const OBSERVATION_INTERVAL: Duration = Duration::from_secs(10);

/// Physical authorization state only; Active never implies business readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelSupervisorStatus {
    Active {
        authorization_deadline: Instant,
        observations: u64,
        last_error: Option<ChannelSessionError>,
    },
    /// New observations have stopped, but native cleanup has not completed yet.
    Stopping {
        reason: ChannelCloseReason,
    },
    Closed {
        reason: ChannelCloseReason,
    },
    CleanupFailed,
}

/// Owns one pool and one serial control observer. The first observation is due
/// after ten seconds; each completed query is followed by another ten-second
/// delay. Errors retain the existing deadline and do not trigger immediate
/// retries. Native Zenoh alone reconnects within that finite window.
/// Call `close` to join cleanup. Drop requests shutdown without blocking.
pub struct ManagedServiceChannel {
    stop: watch::Sender<bool>,
    status: watch::Receiver<ChannelSupervisorStatus>,
    task: Option<JoinHandle<Result<(), ChannelSessionError>>>,
    cleanup_failed: bool,
}
impl fmt::Debug for ManagedServiceChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ManagedServiceChannel")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}
impl ServiceChannelSessions {
    /// Transfer sole physical ownership into the observation supervisor. This
    /// performs no bootstrap, certificate rotation, role renewal or route bind.
    pub fn manage(self) -> Result<ManagedServiceChannel, ChannelSessionError> {
        if !tokio::runtime::Handle::try_current().is_ok_and(|handle| {
            handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
        }) {
            return Err(ChannelSessionError::UnsupportedRuntime);
        }
        Ok(ManagedServiceChannel::start(self))
    }
}
impl ManagedServiceChannel {
    pub(super) fn start(owner: impl AuthorizationOwner + Send + 'static) -> Self {
        let (stop, stopped) = watch::channel(false);
        let (sender, status) = watch::channel(ChannelSupervisorStatus::Active {
            authorization_deadline: owner.deadline(),
            observations: 0,
            last_error: None,
        });
        let task = tokio::spawn(observe(owner, stopped, sender));
        Self {
            stop,
            status,
            task: Some(task),
            cleanup_failed: false,
        }
    }
    pub fn status(&self) -> ChannelSupervisorStatus {
        let status = *self.status.borrow();
        if self.cleanup_failed
            || (self.task.as_ref().is_some_and(|task| task.is_finished())
                && !matches!(
                    status,
                    ChannelSupervisorStatus::Closed { .. } | ChannelSupervisorStatus::CleanupFailed
                ))
        {
            ChannelSupervisorStatus::CleanupFailed
        } else {
            status
        }
    }
    /// A lost sender without a terminal status also means supervisor failure.
    /// Notifications coalesce; observations is a cumulative successful ACK count.
    pub fn subscribe_status(&self) -> watch::Receiver<ChannelSupervisorStatus> {
        self.status.clone()
    }
    pub async fn close(&mut self) -> Result<(), ChannelSessionError> {
        self.stop.send_replace(true);
        if let Some(task) = self.task.as_mut() {
            // Preserve the handle if the caller cancels while native cleanup runs.
            let result = task.await;
            self.task.take();
            self.cleanup_failed = !matches!(result, Ok(Ok(())));
        }
        if self.cleanup_failed {
            Err(ChannelSessionError::CleanupFailed)
        } else {
            Ok(())
        }
    }
}
impl Drop for ManagedServiceChannel {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

// Keep policy testable without trusting a mock transport proof. The production
// implementation delegates proof and hard-deadline enforcement to the pool.
pub(super) trait AuthorizationOwner {
    fn deadline(&self) -> Instant;
    fn closed(&self) -> watch::Receiver<Option<ChannelCloseReason>>;
    /// Only the serial owner can mark its intentional predecessor shutdown.
    fn replacing_pool(&self) -> Option<Arc<AtomicBool>> {
        None
    }
    fn notification_deadline(&self) -> Option<Instant> {
        None
    }
    fn notify(&mut self) {}
    fn changed(&mut self) -> impl std::future::Future<Output = ()> + Send {
        std::future::pending::<()>()
    }
    fn process_change(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), ChannelSessionError>> + Send {
        async { Ok(()) }
    }
    fn refresh(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), ChannelSessionError>> + Send;
    fn close(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), ChannelSessionError>> + Send;
}
impl AuthorizationOwner for ServiceChannelSessions {
    fn deadline(&self) -> Instant {
        self.authorization_deadline()
    }
    fn closed(&self) -> watch::Receiver<Option<ChannelCloseReason>> {
        self.subscribe_closed()
    }
    async fn refresh(&mut self) -> Result<(), ChannelSessionError> {
        self.refresh_authorization().await.map(|_| ())
    }
    async fn close(&mut self) -> Result<(), ChannelSessionError> {
        self.close().await
    }
}
async fn stop_requested(stopped: &mut watch::Receiver<bool>) {
    loop {
        if *stopped.borrow_and_update() || stopped.changed().await.is_err() {
            return;
        }
    }
}
async fn pool_closed(
    closed: &mut watch::Receiver<Option<ChannelCloseReason>>,
) -> ChannelCloseReason {
    loop {
        if let Some(reason) = *closed.borrow_and_update() {
            return reason;
        }
        if closed.changed().await.is_err() {
            return ChannelCloseReason::CleanupFailed;
        }
    }
}
async fn observe(
    mut owner: impl AuthorizationOwner,
    mut stopped: watch::Receiver<bool>,
    status: watch::Sender<ChannelSupervisorStatus>,
) -> Result<(), ChannelSessionError> {
    let mut closed = owner.closed();
    let mut next_observation = Instant::now() + OBSERVATION_INTERVAL;
    let mut observations = 0u64;
    let reason = loop {
        let notification = owner.notification_deadline();
        // Expiry/close has priority over a timer or a late successful response.
        let deadline = owner.deadline();
        let mutation = tokio::select! {
            biased;
            _ = stop_requested(&mut stopped) => break ChannelCloseReason::Explicit,
            reason = pool_closed(&mut closed) => break reason,
            _ = tokio::time::sleep_until(deadline) => break ChannelCloseReason::AuthorityExpired,
            _ = async {
                match notification {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => { owner.notify(); continue; },
            _ = tokio::time::sleep_until(next_observation) => false,
            _ = owner.changed() => true,
        };
        let deadline = owner.deadline();
        let replacing = owner.replacing_pool();
        let result = tokio::select! {
            biased;
            _ = stop_requested(&mut stopped) => break ChannelCloseReason::Explicit,
            reason = pool_closed_during_refresh(&mut closed, replacing.as_deref()) => break reason,
            _ = tokio::time::sleep_until(deadline) => break ChannelCloseReason::AuthorityExpired,
            result = async {
                if mutation { owner.process_change().await } else { owner.refresh().await }
            } => result,
        };
        // A successful owner handoff may replace and stop its old physical pool.
        // Follow the new pool before polling closure again; the old cleanup is
        // retained and joined by that same owner, never a second supervisor.
        closed = owner.closed();
        if let Some(replacing) = replacing {
            replacing.store(false, Ordering::Release);
        }
        // The real pool refuses a late ACK, and this guard also ensures no new
        // observation starts when its existing owner has lost authority.
        if Instant::now() >= owner.deadline() {
            break ChannelCloseReason::AuthorityExpired;
        }
        match result {
            Err(ChannelSessionError::CleanupFailed) => break ChannelCloseReason::CleanupFailed,
            Err(ChannelSessionError::RotationFailed) => break ChannelCloseReason::RotationFailed,
            Err(ChannelSessionError::Closed) => break ChannelCloseReason::ConnectionClosed,
            Err(ChannelSessionError::AuthorityExpired) => {
                break ChannelCloseReason::AuthorityExpired
            }
            Ok(()) if !mutation => observations = observations.saturating_add(1),
            _ => {}
        }
        status.send_replace(ChannelSupervisorStatus::Active {
            authorization_deadline: owner.deadline(),
            observations,
            last_error: result.err(),
        });
        // Start a new delay after completion: even a query spanning scheduler
        // suspension cannot be followed by an immediate failure retry or burst.
        if !mutation {
            next_observation = Instant::now() + OBSERVATION_INTERVAL;
        }
    };
    status.send_replace(ChannelSupervisorStatus::Stopping { reason });
    if owner.close().await.is_err() || reason == ChannelCloseReason::CleanupFailed {
        status.send_replace(ChannelSupervisorStatus::CleanupFailed);
        Err(ChannelSessionError::CleanupFailed)
    } else {
        status.send_replace(ChannelSupervisorStatus::Closed { reason });
        Ok(())
    }
}

async fn pool_closed_during_refresh(
    closed: &mut watch::Receiver<Option<ChannelCloseReason>>,
    replacing: Option<&AtomicBool>,
) -> ChannelCloseReason {
    let reason = pool_closed(closed).await;
    if reason != ChannelCloseReason::Explicit
        || !replacing.is_some_and(|flag| flag.load(Ordering::Acquire))
    {
        return reason;
    }
    // The owner must join cleanup before opening another pool. Continue to
    // observe cleanup failure; stop and the original deadline still win above.
    loop {
        if closed.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
        if let Some(reason) = *closed.borrow_and_update() {
            if reason != ChannelCloseReason::Explicit {
                return reason;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{mpsc, oneshot};

    #[tokio::test]
    async fn intentional_predecessor_close_keeps_observing_cleanup_failure() {
        let (sender, mut closed) = watch::channel(Some(ChannelCloseReason::Explicit));
        let replacing = AtomicBool::new(true);
        let observe = pool_closed_during_refresh(&mut closed, Some(&replacing));
        tokio::pin!(observe);
        assert!(tokio::time::timeout(Duration::from_millis(1), &mut observe)
            .await
            .is_err());
        sender.send_replace(Some(ChannelCloseReason::CleanupFailed));
        assert_eq!(observe.await, ChannelCloseReason::CleanupFailed);
    }

    #[tokio::test]
    async fn replacement_never_masks_revocation_or_expiry() {
        for reason in [
            ChannelCloseReason::ConnectionClosed,
            ChannelCloseReason::AuthorityExpired,
        ] {
            let (_sender, mut closed) = watch::channel(Some(reason));
            assert_eq!(
                pool_closed_during_refresh(&mut closed, Some(&AtomicBool::new(true))).await,
                reason
            );
        }
        let (_sender, mut closed) = watch::channel(Some(ChannelCloseReason::Explicit));
        assert_eq!(
            pool_closed_during_refresh(&mut closed, None).await,
            ChannelCloseReason::Explicit
        );
    }

    struct ReplacingOwner {
        inner: FakeOwner,
        predecessor: watch::Sender<Option<ChannelCloseReason>>,
        candidate: watch::Receiver<Option<ChannelCloseReason>>,
        replacing: Arc<AtomicBool>,
    }
    impl AuthorizationOwner for ReplacingOwner {
        fn deadline(&self) -> Instant {
            self.inner.deadline()
        }
        fn closed(&self) -> watch::Receiver<Option<ChannelCloseReason>> {
            self.inner.closed()
        }
        fn replacing_pool(&self) -> Option<Arc<AtomicBool>> {
            Some(self.replacing.clone())
        }
        async fn refresh(&mut self) -> Result<(), ChannelSessionError> {
            self.replacing.store(true, Ordering::Release);
            self.predecessor
                .send_replace(Some(ChannelCloseReason::Explicit));
            tokio::task::yield_now().await;
            self.inner.refresh().await?;
            self.inner.closed = self.candidate.clone();
            Ok(())
        }
        async fn close(&mut self) -> Result<(), ChannelSessionError> {
            self.inner.close().await
        }
    }
    #[tokio::test(start_paused = true)]
    async fn intentional_close_follows_new_pool_but_cannot_postpone_original_expiry() {
        for expires in [false, true] {
            let (queries, mut requests) = mpsc::unbounded_channel();
            let (cleanup, mut cleans) = mpsc::unbounded_channel();
            let (predecessor, old) = watch::channel(None);
            let (candidate, new) = watch::channel(None);
            let replacing = Arc::new(AtomicBool::new(false));
            let mut channel = ManagedServiceChannel::start(ReplacingOwner {
                inner: FakeOwner {
                    deadline: Instant::now() + Duration::from_secs(15),
                    closed: old,
                    queries,
                    cleanup,
                },
                predecessor,
                candidate: new,
                replacing: replacing.clone(),
            });
            settle().await;
            advance(10).await;
            let ack = requests.try_recv().unwrap();
            assert!(cleans.try_recv().is_err());
            if expires {
                advance(5).await;
                assert!(ack
                    .send(Ok(Instant::now() + Duration::from_secs(100)))
                    .is_err());
            } else {
                ack.send(Ok(Instant::now() + Duration::from_secs(30)))
                    .unwrap();
                settle().await;
                assert!(!replacing.load(Ordering::Acquire));
                assert!(matches!(
                    channel.status(),
                    ChannelSupervisorStatus::Active {
                        observations: 1,
                        ..
                    }
                ));
                candidate.send_replace(Some(ChannelCloseReason::ConnectionClosed));
            }
            settle().await;
            cleans.try_recv().unwrap().send(Ok(())).unwrap();
            settle().await;
            assert_eq!(
                channel.status(),
                ChannelSupervisorStatus::Closed {
                    reason: if expires {
                        ChannelCloseReason::AuthorityExpired
                    } else {
                        ChannelCloseReason::ConnectionClosed
                    }
                }
            );
            channel.close().await.unwrap();
        }
    }

    struct FakeOwner {
        deadline: Instant,
        closed: watch::Receiver<Option<ChannelCloseReason>>,
        queries: mpsc::UnboundedSender<oneshot::Sender<Result<Instant, ChannelSessionError>>>,
        cleanup: mpsc::UnboundedSender<oneshot::Sender<Result<(), ChannelSessionError>>>,
    }
    impl AuthorizationOwner for FakeOwner {
        fn deadline(&self) -> Instant {
            self.deadline
        }
        fn closed(&self) -> watch::Receiver<Option<ChannelCloseReason>> {
            self.closed.clone()
        }
        async fn refresh(&mut self) -> Result<(), ChannelSessionError> {
            let (reply, ack) = oneshot::channel();
            self.queries.send(reply).unwrap();
            self.deadline = ack.await.unwrap()?;
            Ok(())
        }
        async fn close(&mut self) -> Result<(), ChannelSessionError> {
            let (reply, ack) = oneshot::channel();
            self.cleanup.send(reply).unwrap();
            ack.await.unwrap()
        }
    }
    struct Fixture {
        channel: ManagedServiceChannel,
        queries: mpsc::UnboundedReceiver<oneshot::Sender<Result<Instant, ChannelSessionError>>>,
        cleanup: mpsc::UnboundedReceiver<oneshot::Sender<Result<(), ChannelSessionError>>>,
        closed: watch::Sender<Option<ChannelCloseReason>>,
        initial: Instant,
    }
    fn fixture(seconds: u64) -> Fixture {
        let (queries, requests) = mpsc::unbounded_channel();
        let (cleanup, cleans) = mpsc::unbounded_channel();
        let (closed, receiver) = watch::channel(None);
        let initial = Instant::now() + Duration::from_secs(seconds);
        Fixture {
            channel: ManagedServiceChannel::start(FakeOwner {
                deadline: initial,
                closed: receiver,
                queries,
                cleanup,
            }),
            queries: requests,
            cleanup: cleans,
            closed,
            initial,
        }
    }
    async fn settle() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }
    async fn advance(seconds: u64) {
        tokio::time::advance(Duration::from_secs(seconds)).await;
        settle().await;
    }
    struct ChangingOwner {
        inner: FakeOwner,
        changes: mpsc::UnboundedReceiver<()>,
        processed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }
    impl AuthorizationOwner for ChangingOwner {
        fn deadline(&self) -> Instant {
            self.inner.deadline()
        }
        fn closed(&self) -> watch::Receiver<Option<ChannelCloseReason>> {
            self.inner.closed()
        }
        async fn changed(&mut self) {
            if self.changes.recv().await.is_none() {
                std::future::pending::<()>().await;
            }
        }
        async fn process_change(&mut self) -> Result<(), ChannelSessionError> {
            self.processed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        async fn refresh(&mut self) -> Result<(), ChannelSessionError> {
            self.inner.refresh().await
        }
        async fn close(&mut self) -> Result<(), ChannelSessionError> {
            self.inner.close().await
        }
    }
    #[tokio::test(start_paused = true)]
    async fn queued_mutations_cannot_postpone_renewal_or_run_after_shutdown() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        let (queries, mut requests) = mpsc::unbounded_channel();
        let (cleanup, mut cleans) = mpsc::unbounded_channel();
        let (_closed, receiver) = watch::channel(None);
        let (changes, mutations) = mpsc::unbounded_channel();
        let processed = Arc::new(AtomicUsize::new(0));
        let mut channel = ManagedServiceChannel::start(ChangingOwner {
            inner: FakeOwner {
                deadline: Instant::now() + Duration::from_secs(30),
                closed: receiver,
                queries,
                cleanup,
            },
            changes: mutations,
            processed: processed.clone(),
        });
        settle().await;
        changes.send(()).unwrap();
        settle().await;
        assert_eq!(processed.load(Ordering::SeqCst), 1);
        assert!(requests.try_recv().is_err());
        advance(9).await;
        changes.send(()).unwrap();
        settle().await;
        advance(1).await;
        let request = requests
            .try_recv()
            .expect("mutations must not reset the ten-second renewal delay");
        for _ in 0..16 {
            changes.send(()).unwrap();
        }
        request
            .send(Ok(Instant::now() + Duration::from_secs(30)))
            .unwrap();
        settle().await;
        assert!(matches!(
            channel.status(),
            ChannelSupervisorStatus::Active {
                observations: 1,
                ..
            }
        ));
        channel.stop.send_replace(true);
        changes.send(()).unwrap();
        let before = processed.load(Ordering::SeqCst);
        settle().await;
        assert_eq!(processed.load(Ordering::SeqCst), before);
        cleans.try_recv().unwrap().send(Ok(())).unwrap();
        channel.close().await.unwrap();
    }
    struct SwitchingOwner {
        inner: FakeOwner,
        old: watch::Sender<Option<ChannelCloseReason>>,
        next: Option<watch::Receiver<Option<ChannelCloseReason>>>,
    }
    impl AuthorizationOwner for SwitchingOwner {
        fn deadline(&self) -> Instant {
            self.inner.deadline()
        }
        fn closed(&self) -> watch::Receiver<Option<ChannelCloseReason>> {
            self.inner.closed()
        }
        async fn refresh(&mut self) -> Result<(), ChannelSessionError> {
            self.inner.refresh().await?;
            if let Some(next) = self.next.take() {
                self.inner.closed = next;
                self.old.send_replace(Some(ChannelCloseReason::Explicit));
            }
            Ok(())
        }
        async fn close(&mut self) -> Result<(), ChannelSessionError> {
            self.inner.close().await
        }
    }
    #[tokio::test(start_paused = true)]
    async fn handoff_follows_new_pool_closure_and_deadline_after_stopping_old_pool() {
        let (queries, mut requests) = mpsc::unbounded_channel();
        let (cleanup, mut cleans) = mpsc::unbounded_channel();
        let (old, receiver) = watch::channel(None);
        let (new, next) = watch::channel(None);
        let initial = Instant::now() + Duration::from_secs(30);
        let renewed = Instant::now() + Duration::from_secs(100);
        let mut channel = ManagedServiceChannel::start(SwitchingOwner {
            inner: FakeOwner {
                deadline: initial,
                closed: receiver,
                queries,
                cleanup,
            },
            old,
            next: Some(next),
        });
        settle().await;
        advance(10).await;
        requests.try_recv().unwrap().send(Ok(renewed)).unwrap();
        settle().await;
        assert!(matches!(
            channel.status(),
            ChannelSupervisorStatus::Active {
                observations: 1,
                ..
            }
        ));
        advance(10).await;
        requests.try_recv().unwrap().send(Ok(renewed)).unwrap();
        settle().await;
        advance(11).await;
        requests.try_recv().unwrap().send(Ok(renewed)).unwrap();
        settle().await;
        assert!(Instant::now() > initial);
        assert!(matches!(
            channel.status(),
            ChannelSupervisorStatus::Active {
                observations: 3,
                ..
            }
        ));
        new.send_replace(Some(ChannelCloseReason::ConnectionClosed));
        cleans.recv().await.unwrap().send(Ok(())).unwrap();
        channel.close().await.unwrap();
        assert_eq!(
            channel.status(),
            ChannelSupervisorStatus::Closed {
                reason: ChannelCloseReason::ConnectionClosed
            }
        );
    }
    #[tokio::test(start_paused = true)]
    async fn uncertain_rotation_failure_is_terminal_and_never_retried() {
        let mut f = fixture(100);
        settle().await;
        advance(10).await;
        f.queries
            .try_recv()
            .unwrap()
            .send(Err(ChannelSessionError::RotationFailed))
            .unwrap();
        f.cleanup.recv().await.unwrap().send(Ok(())).unwrap();
        f.channel.close().await.unwrap();
        advance(40).await;
        assert!(f.queries.try_recv().is_err());
        assert_eq!(
            f.channel.status(),
            ChannelSupervisorStatus::Closed {
                reason: ChannelCloseReason::RotationFailed
            }
        );
    }
    #[tokio::test(start_paused = true)]
    async fn observations_are_serial_and_missed_ticks_do_not_burst() {
        let mut f = fixture(200);
        settle().await;
        assert!(f.queries.try_recv().is_err());
        advance(10).await;
        let reply = f.queries.try_recv().unwrap();
        advance(25).await;
        assert!(
            f.queries.try_recv().is_err(),
            "a second query cannot overlap"
        );
        reply.send(Ok(f.initial)).unwrap();
        settle().await;
        assert!(f.queries.try_recv().is_err());
        advance(9).await;
        assert!(f.queries.try_recv().is_err());
        advance(1).await;
        let next = f.queries.try_recv().unwrap();
        next.send(Ok(f.initial)).unwrap();
        settle().await;
        assert!(f.queries.try_recv().is_err());
        assert!(matches!(
            f.channel.status(),
            ChannelSupervisorStatus::Active {
                observations: 2,
                last_error: None,
                ..
            }
        ));
        f.closed
            .send_replace(Some(ChannelCloseReason::ConnectionClosed));
        f.cleanup.recv().await.unwrap().send(Ok(())).unwrap();
        f.channel.close().await.unwrap();
        assert_eq!(
            f.channel.status(),
            ChannelSupervisorStatus::Closed {
                reason: ChannelCloseReason::ConnectionClosed
            }
        );
    }
    #[tokio::test(start_paused = true)]
    async fn errors_retain_deadline_and_expiry_cancels_pending_observation() {
        let mut f = fixture(25);
        settle().await;
        advance(10).await;
        f.queries
            .try_recv()
            .unwrap()
            .send(Err(ChannelSessionError::Transport))
            .unwrap();
        settle().await;
        assert_eq!(
            f.channel.status(),
            ChannelSupervisorStatus::Active {
                authorization_deadline: f.initial,
                observations: 0,
                last_error: Some(ChannelSessionError::Transport)
            }
        );
        advance(9).await;
        assert!(
            f.queries.try_recv().is_err(),
            "no immediate retry after failure"
        );
        advance(1).await;
        let reply = f.queries.try_recv().unwrap();
        advance(5).await;
        assert!(reply.is_closed(), "expiry cancels the pending query");
        assert_eq!(
            f.channel.status(),
            ChannelSupervisorStatus::Stopping {
                reason: ChannelCloseReason::AuthorityExpired
            }
        );
        f.cleanup.try_recv().unwrap().send(Ok(())).unwrap();
        f.channel.close().await.unwrap();
        assert_eq!(
            f.channel.status(),
            ChannelSupervisorStatus::Closed {
                reason: ChannelCloseReason::AuthorityExpired
            }
        );
    }
    #[tokio::test(start_paused = true)]
    async fn explicit_close_cancels_query_and_cancelled_wait_retains_cleanup_handle() {
        let mut f = fixture(30);
        settle().await;
        advance(10).await;
        let reply = f.queries.try_recv().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1), f.channel.close())
                .await
                .is_err()
        );
        assert!(reply.is_closed());
        assert!(f.channel.task.is_some());
        let cleanup = f.cleanup.try_recv().unwrap();
        assert_eq!(
            f.channel.status(),
            ChannelSupervisorStatus::Stopping {
                reason: ChannelCloseReason::Explicit
            }
        );
        cleanup.send(Ok(())).unwrap();
        f.channel.close().await.unwrap();
        f.channel.close().await.unwrap();
        assert_eq!(
            f.channel.status(),
            ChannelSupervisorStatus::Closed {
                reason: ChannelCloseReason::Explicit
            }
        );
    }
    #[tokio::test(start_paused = true)]
    async fn drop_stops_observation_and_waits_for_native_cleanup() {
        let mut f = fixture(30);
        let mut status = f.channel.subscribe_status();
        settle().await;
        advance(10).await;
        let reply = f.queries.try_recv().unwrap();
        drop(f.channel);
        let cleanup = f.cleanup.recv().await.unwrap();
        assert!(reply.is_closed());
        assert_eq!(
            *status.borrow_and_update(),
            ChannelSupervisorStatus::Stopping {
                reason: ChannelCloseReason::Explicit
            }
        );
        cleanup.send(Ok(())).unwrap();
        status.changed().await.unwrap();
        assert_eq!(
            *status.borrow(),
            ChannelSupervisorStatus::Closed {
                reason: ChannelCloseReason::Explicit
            }
        );
    }
    #[tokio::test(start_paused = true)]
    async fn failed_cleanup_and_aborted_supervisor_remain_terminal_failures() {
        let mut f = fixture(30);
        f.closed
            .send_replace(Some(ChannelCloseReason::AuthorityExpired));
        f.cleanup
            .recv()
            .await
            .unwrap()
            .send(Err(ChannelSessionError::CleanupFailed))
            .unwrap();
        assert_eq!(
            f.channel.close().await,
            Err(ChannelSessionError::CleanupFailed)
        );
        assert_eq!(
            f.channel.close().await,
            Err(ChannelSessionError::CleanupFailed)
        );
        assert_eq!(f.channel.status(), ChannelSupervisorStatus::CleanupFailed);
        let mut aborted = fixture(30);
        aborted.channel.task.as_ref().unwrap().abort();
        settle().await;
        assert_eq!(
            aborted.channel.status(),
            ChannelSupervisorStatus::CleanupFailed
        );
        assert_eq!(
            aborted.channel.close().await,
            Err(ChannelSessionError::CleanupFailed)
        );
        assert_eq!(
            aborted.channel.close().await,
            Err(ChannelSessionError::CleanupFailed)
        );
    }
    #[tokio::test(start_paused = true)]
    async fn successful_observation_can_extend_only_the_physical_window() {
        let mut f = fixture(30);
        settle().await;
        advance(10).await;
        let extended = Instant::now() + Duration::from_secs(30);
        f.queries.try_recv().unwrap().send(Ok(extended)).unwrap();
        settle().await;
        assert_eq!(
            f.channel.status(),
            ChannelSupervisorStatus::Active {
                authorization_deadline: extended,
                observations: 1,
                last_error: None
            }
        );
        f.closed
            .send_replace(Some(ChannelCloseReason::AuthorityExpired));
        f.cleanup.recv().await.unwrap().send(Ok(())).unwrap();
        f.channel.close().await.unwrap();
        assert_eq!(
            f.channel.status(),
            ChannelSupervisorStatus::Closed {
                reason: ChannelCloseReason::AuthorityExpired
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn already_expired_owner_and_lost_lifecycle_never_start_observations() {
        let mut expired = fixture(0);
        expired.cleanup.recv().await.unwrap().send(Ok(())).unwrap();
        expired.channel.close().await.unwrap();
        assert!(expired.queries.try_recv().is_err());
        assert_eq!(
            expired.channel.status(),
            ChannelSupervisorStatus::Closed {
                reason: ChannelCloseReason::AuthorityExpired
            }
        );
        let lost = fixture(30);
        let Fixture {
            mut channel,
            mut queries,
            mut cleanup,
            closed,
            ..
        } = lost;
        drop(closed);
        cleanup.recv().await.unwrap().send(Ok(())).unwrap();
        assert_eq!(
            channel.close().await,
            Err(ChannelSessionError::CleanupFailed)
        );
        assert!(queries.try_recv().is_err());
        assert_eq!(channel.status(), ChannelSupervisorStatus::CleanupFailed);
    }

    #[tokio::test(start_paused = true)]
    async fn role_expiry_notifications_do_not_start_early_observations_or_delay_stop() {
        struct NotifyingOwner {
            owner: FakeOwner,
            notification: Option<Instant>,
            notifications: mpsc::UnboundedSender<()>,
        }
        impl AuthorizationOwner for NotifyingOwner {
            fn deadline(&self) -> Instant {
                self.owner.deadline()
            }
            fn closed(&self) -> watch::Receiver<Option<ChannelCloseReason>> {
                self.owner.closed()
            }
            fn notification_deadline(&self) -> Option<Instant> {
                self.notification
            }
            fn notify(&mut self) {
                self.notification = None;
                self.notifications.send(()).unwrap();
            }
            async fn refresh(&mut self) -> Result<(), ChannelSessionError> {
                self.owner.refresh().await
            }
            async fn close(&mut self) -> Result<(), ChannelSessionError> {
                self.owner.close().await
            }
        }
        let (queries, mut requests) = mpsc::unbounded_channel();
        let (cleanup, mut cleans) = mpsc::unbounded_channel();
        let (notifications, mut notices) = mpsc::unbounded_channel();
        let (_closed, receiver) = watch::channel(None);
        let mut channel = ManagedServiceChannel::start(NotifyingOwner {
            owner: FakeOwner {
                deadline: Instant::now() + Duration::from_secs(30),
                closed: receiver,
                queries,
                cleanup,
            },
            notification: Some(Instant::now() + Duration::from_secs(3)),
            notifications,
        });
        settle().await;
        advance(3).await;
        notices.try_recv().unwrap();
        assert!(requests.try_recv().is_err());
        advance(7).await;
        let query = requests.try_recv().unwrap();
        assert!(notices.try_recv().is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(1), channel.close())
                .await
                .is_err()
        );
        assert!(query.is_closed());
        cleans.try_recv().unwrap().send(Ok(())).unwrap();
        channel.close().await.unwrap();
        assert_eq!(
            channel.status(),
            ChannelSupervisorStatus::Closed {
                reason: ChannelCloseReason::Explicit
            }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires isolated native host and real leases; run zenss_channel_bootstrap_acceptance.py --sdk-test"]
    async fn native_managed_observation_does_not_renew_common_authority() {
        use kish_lingshu_foundation_contract::ServiceInstanceRegistration;
        let connection = crate::ServiceConnection::connect(
            &std::env::var("LINGSHU_CHANNEL_TEST_URL").unwrap(),
            crate::ServiceCredential::new(
                std::env::var("LINGSHU_CHANNEL_TEST_APP").unwrap(),
                std::env::var("LINGSHU_CHANNEL_TEST_KEY").unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let registration = ServiceInstanceRegistration {
            instance_id: "managed-native".into(),
            incarnation_id: "managed-native-boot".into(),
            generation: None,
        };
        let identity = connection
            .bootstrap_test_channel(registration.clone(), None)
            .await
            .unwrap();
        let common_lease_expired = Instant::now() + Duration::from_millis(30_250);
        let initial = identity.authorization_deadline;
        let base = identity.bootstrap_response().instance.generation.clone();
        let pool = identity
            .open_sessions(super::super::ChannelSessionConfig::new(2).unwrap())
            .await
            .unwrap();
        assert_eq!(pool.connected_lanes().await, 2);
        let mut managed = pool.manage().unwrap();
        let mut status = managed.subscribe_status();
        tokio::time::timeout(Duration::from_secs(25), async {
            loop {
                if matches!(*status.borrow_and_update(), ChannelSupervisorStatus::Active { observations: 2.., authorization_deadline, last_error: None } if authorization_deadline > initial) { break; }
                status.changed().await.unwrap();
            }
        }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                if matches!(
                    *status.borrow_and_update(),
                    ChannelSupervisorStatus::Closed {
                        reason: ChannelCloseReason::AuthorityExpired
                    }
                ) {
                    break;
                }
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        managed.close().await.unwrap();
        managed.close().await.unwrap();
        connection.ensure_open().unwrap();
        assert_eq!(connection.channel_session_budget().available_permits(), 4);
        tokio::time::sleep_until(common_lease_expired).await;
        assert!(matches!(
            connection
                .bootstrap_test_channel(
                    ServiceInstanceRegistration {
                        generation: Some(base),
                        ..registration
                    },
                    None
                )
                .await,
            Err(crate::ServiceAuthError::Http(409))
        ));
        connection.ensure_open().unwrap();
        connection.shutdown().await;
    }
}
