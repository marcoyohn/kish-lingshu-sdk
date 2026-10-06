//! Test-only faults owned by one execution binding. Production builds contain
//! neither these controls nor their hooks; there is no process-global override.
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FaultMode {
    LoseAccepted,
    CompleteBeforeAccepted,
    LoseCompletionAck,
    CapacityRejected,
    ObserveCheckpointFailure,
}
impl FaultMode {
    pub(super) fn label(self) -> &'static str {
        match self {
            Self::LoseAccepted => "lost-accepted",
            Self::CompleteBeforeAccepted => "early-completion",
            Self::LoseCompletionAck => "lost-completion-ack",
            Self::CapacityRejected => "capacity-rejected",
            Self::ObserveCheckpointFailure => "checkpoint-failure",
        }
    }
}
pub(super) struct Faults {
    pub(super) mode: FaultMode,
    pub(super) invocations: AtomicUsize,
    pub(super) accepted: AtomicUsize,
    pub(super) accepted_released: AtomicUsize,
    pub(super) completions: AtomicUsize,
    pub(super) recorded: AtomicUsize,
    pub(super) duplicates: AtomicUsize,
    pub(super) unavailable: AtomicUsize,
    pub(super) rejected: watch::Sender<usize>,
    pub(super) requests: Mutex<Vec<ServiceInvocation>>,
    immutable: Mutex<Option<ServiceCompletion>>,
    checkpointed: watch::Sender<bool>,
    accepted_finished: watch::Sender<bool>,
    settled: watch::Sender<bool>,
}
impl Faults {
    pub(super) fn new(mode: FaultMode) -> Arc<Self> {
        Arc::new(Self {
            mode,
            invocations: AtomicUsize::new(0),
            accepted: AtomicUsize::new(0),
            accepted_released: AtomicUsize::new(0),
            completions: AtomicUsize::new(0),
            recorded: AtomicUsize::new(0),
            duplicates: AtomicUsize::new(0),
            unavailable: AtomicUsize::new(0),
            rejected: watch::channel(0).0,
            requests: Mutex::new(Vec::new()),
            immutable: Mutex::new(None),
            checkpointed: watch::channel(false).0,
            accepted_finished: watch::channel(false).0,
            settled: watch::channel(false).0,
        })
    }
    pub(super) async fn before_accepted(&self) -> Result<(), ChannelSessionError> {
        self.accepted.fetch_add(1, Ordering::SeqCst);
        let result = match self.mode {
            // Suppress the actual Invoke reply after the core accepted work.
            FaultMode::LoseAccepted => Err(ChannelSessionError::InvalidResponse),
            FaultMode::CompleteBeforeAccepted => {
                let mut ack = self.checkpointed.subscribe();
                let checkpoint =
                    tokio::time::timeout(Duration::from_secs(2), ack.wait_for(|v| *v)).await;
                match checkpoint {
                    Ok(Ok(_)) => Ok(()),
                    _ => Err(ChannelSessionError::InvalidResponse),
                }
            }
            FaultMode::LoseCompletionAck
            | FaultMode::CapacityRejected
            | FaultMode::ObserveCheckpointFailure => Ok(()),
        };
        if result.is_ok() {
            self.accepted_released.fetch_add(1, Ordering::SeqCst);
        }
        self.accepted_finished.send_replace(true);
        result
    }

    pub(super) fn wrap(self: &Arc<Self>, inner: Arc<dyn CallReporter>) -> Arc<dyn CallReporter> {
        Arc::new(Reporter {
            inner,
            faults: self.clone(),
        })
    }
    pub(super) async fn wait_settled(&self) {
        let mut settled = self.settled.subscribe();
        let mut accepted = self.accepted_finished.subscribe();
        tokio::time::timeout(Duration::from_secs(5), async {
            settled.wait_for(|v| *v).await.unwrap();
            accepted.wait_for(|v| *v).await.unwrap();
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
            "native {:?} did not settle: accepted={}, recorded={}, submissions={}, duplicates={}",
            self.mode,
            self.accepted.load(Ordering::SeqCst),
            self.recorded.load(Ordering::SeqCst),
            self.completions.load(Ordering::SeqCst),
            self.duplicates.load(Ordering::SeqCst),
        )
        });
    }
}
struct Reporter {
    inner: Arc<dyn CallReporter>,
    faults: Arc<Faults>,
}
#[async_trait::async_trait]
impl CallReporter for Reporter {
    async fn authority_lost(&self) {
        self.inner.authority_lost().await;
    }
    async fn progress(
        &self,
        target: &CompletionTarget,
        progress: &ServiceProgress,
    ) -> Result<ProgressDisposition, CallReportError> {
        self.inner.progress(target, progress).await
    }
    async fn heartbeat(
        &self,
        target: &CompletionTarget,
        heartbeat: &ServiceHeartbeat,
    ) -> Result<HeartbeatDisposition, CallReportError> {
        self.inner.heartbeat(target, heartbeat).await
    }
    async fn complete(
        &self,
        target: &CompletionTarget,
        result: &ServiceCompletion,
        timeout: Option<Duration>,
    ) -> Result<CompletionDisposition, CallReportError> {
        {
            let mut original = self.faults.immutable.lock().unwrap();
            match original.as_ref() {
                Some(original) => assert_eq!(original, result, "retry must keep the same result"),
                None => *original = Some(result.clone()),
            }
        }
        self.faults.completions.fetch_add(1, Ordering::SeqCst);
        let response = self.inner.complete(target, result, timeout).await;
        if self.faults.mode == FaultMode::ObserveCheckpointFailure
            && matches!(response, Err(CallReportError::Unavailable))
            && self.faults.unavailable.fetch_add(1, Ordering::SeqCst) == 0
        {
            // Observe a real failed report; the harness restores the database
            // before the unchanged execution core performs its bounded retry.
            let markers = std::path::PathBuf::from(
                std::env::var("LINGSHU_REMOTE_WRITER_MARKER_DIR").unwrap(),
            );
            assert_eq!(self.faults.recorded.load(Ordering::SeqCst), 0);
            assert_eq!(self.faults.duplicates.load(Ordering::SeqCst), 0);
            std::fs::write(markers.join("completion-unavailable"), "no durable ACK").unwrap();
            tokio::time::timeout(Duration::from_secs(6), async {
                while !markers.join("resume-report").exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("checkpoint fault must be removed within the original report budget");
        }
        let disposition = response?;
        match disposition {
            CompletionDisposition::Recorded => {
                let previous = self.faults.recorded.fetch_add(1, Ordering::SeqCst);
                self.faults.checkpointed.send_replace(true);
                if self.faults.mode == FaultMode::LoseCompletionAck && previous == 0 {
                    // Discard one fully verified native checkpoint ACK at the
                    // client report boundary; do not fabricate platform success.
                    return Err(CallReportError::Unavailable);
                }
            }
            CompletionDisposition::Duplicate => {
                self.faults.duplicates.fetch_add(1, Ordering::SeqCst);
            }
            CompletionDisposition::Invalidated => panic!("original finite report was invalidated"),
        }
        self.faults.settled.send_replace(true);
        Ok(disposition)
    }
}
