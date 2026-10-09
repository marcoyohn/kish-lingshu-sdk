//! A passive stream from a previous process must not hide a durable result.
use super::*;
use kish_lingshu_runtime_contract::{test_support::ContractProductRuntimeFixture, *};
use std::sync::atomic::{AtomicUsize, Ordering};

struct SnapshotRuntime {
    inner: Arc<dyn WorkflowRuntime>,
    mode: u8,
    eof: bool,
    mutations: AtomicUsize,
    reads: AtomicUsize,
}
#[async_trait::async_trait]
impl WorkflowRuntime for SnapshotRuntime {
    async fn start(&self, request: StartWorkflowRequest) -> RuntimeResult<WorkflowRunHandle> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.start(request).await
    }
    async fn signal(&self, request: SignalWorkflowRequest) -> RuntimeResult<CommandAck> {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        self.inner.signal(request).await
    }
    async fn terminate(&self, _: TerminateWorkflowRequest) -> RuntimeResult<CommandAck> {
        panic!("observation cannot terminate work")
    }
    async fn snapshot(&self, request: WorkflowSnapshotRequest) -> RuntimeResult<WorkflowSnapshot> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.mode == 4 {
            return Err(RuntimeError::new(
                RuntimeErrorCode::Connectivity,
                "snapshot unavailable",
            ));
        }
        if self.mode == 5 {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        let mut snapshot = self.inner.snapshot(request).await?;
        match self.mode {
            1 => snapshot.output = None,
            2 => snapshot.workflow_instance_id = WorkflowInstanceId(1),
            3 => snapshot.state = WorkflowState::Running,
            _ => {}
        }
        Ok(snapshot)
    }
    async fn subscribe(
        &self,
        _: SubscribeWorkflowRequest,
    ) -> RuntimeResult<kish_lingshu_runtime_contract::WorkflowEventStream> {
        if self.eof {
            Ok(Box::pin(futures::stream::empty()))
        } else {
            Ok(Box::pin(futures::stream::pending()))
        }
    }
}
async fn run(mode: u8, eof: bool, output: Value) -> (WorkflowRun, Arc<SnapshotRuntime>) {
    let fixture = ContractProductRuntimeFixture::default();
    let runtime = Arc::new(SnapshotRuntime {
        inner: fixture.facade.workflow().clone(),
        mode,
        eof,
        mutations: AtomicUsize::new(0),
        reads: AtomicUsize::new(0),
    });
    let facade = ProductRuntimeFacade::new(fixture.facade.conversation().clone(), runtime.clone());
    let client = crate::ClientBuilder::new(crate::ClientConfig::in_process())
        .service_credential(
            crate::ServiceCredential::new("contract-app", "fixture-secret").unwrap(),
        )
        .bind_runtime(
            facade,
            TrustedContextFactory::new(
                "contract-service",
                PrincipalKind::Service,
                Some("contract-app".into()),
                InvocationSource::EmbeddedSdk,
            )
            .unwrap(),
        )
        .unwrap();
    let run = client
        .workflows()
        .select(WorkflowId(42))
        .unwrap()
        .start(
            WorkflowStart::structured(Value::Null),
            MutationOptions::new("snapshot/start").unwrap(),
        )
        .await
        .unwrap();
    run.signal(
        super::WorkflowSignal::business("contract_complete", output),
        MutationOptions::new("snapshot/complete").unwrap(),
    )
    .await
    .unwrap();
    (run, runtime)
}
#[tokio::test]
async fn idle_or_ended_old_stream_uses_one_durable_snapshot_without_mutations_or_cursor_changes() {
    for eof in [false, true] {
        for output in [Value::Null, serde_json::json!({"answer":42})] {
            let (run, runtime) = run(0, eof, output.clone()).await;
            let cursor = run.cursor();
            assert_eq!(
                run.wait_with_options(
                    WorkflowWaitOptions {
                        timeout_ms: 50,
                        ..Default::default()
                    },
                    RequestOptions::new()
                )
                .await
                .unwrap(),
                WorkflowRunResult::Completed {
                    run: run.handle().clone(),
                    output
                }
            );
            assert_eq!(runtime.mutations.load(Ordering::SeqCst), 2);
            assert_eq!(runtime.reads.load(Ordering::SeqCst), 1);
            assert_eq!(run.cursor(), cursor);
        }
    }
}
#[tokio::test]
async fn missing_foreign_running_unreadable_or_late_snapshot_cannot_mint_completion() {
    for mode in 1..=5 {
        let (run, runtime) = run(mode, true, Value::Null).await;
        let before = tokio::time::Instant::now();
        assert!(matches!(
            run.wait_with_options(
                WorkflowWaitOptions {
                    timeout_ms: 20,
                    ..Default::default()
                },
                RequestOptions::new()
            )
            .await,
            Err(Error::Transport(failure)) if failure.kind == TransportKind::Stream
        ));
        assert!(before.elapsed() < std::time::Duration::from_millis(150));
        assert_eq!(runtime.mutations.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.reads.load(Ordering::SeqCst), 1);
    }
}
