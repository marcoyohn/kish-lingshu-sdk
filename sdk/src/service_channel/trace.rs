//! Opt-in trace correlation. Install this layer on the application's subscriber;
//! the SDK never installs a subscriber/exporter, changes sampling or authorizes work.
use kish_lingshu_foundation_contract::trace::TraceParent;
use tracing::{field::Visit, span::Attributes, Id, Subscriber};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

pub struct TraceContextLayer;
#[derive(Default)]
struct ParentVisitor(Option<TraceParent>, Option<TraceParent>);
impl Visit for ParentVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "trace_context" {
            self.1 = TraceParent::parse(value);
        } else if field.name() == "trace_parent" {
            self.0 = TraceParent::parse(value);
        }
    }
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}
impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for TraceContextLayer {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("new span");
        let mut visitor = ParentVisitor::default();
        attrs.record(&mut visitor);
        let parent = visitor.0.or_else(|| {
            span.parent()
                .and_then(|p| p.extensions().get::<TraceParent>().cloned())
        });
        let span_id = uuid::Uuid::new_v4().simple().to_string()[..16].to_owned();
        let value = visitor.1.unwrap_or_else(|| {
            parent.and_then(|p| p.child(&span_id)).unwrap_or_else(|| {
                TraceParent::root(&uuid::Uuid::new_v4().simple().to_string(), &span_id).unwrap()
            })
        });
        span.extensions_mut().insert(value);
    }
}
/// Return the finite operation context, or the optional application layer context.
pub fn current_trace_parent() -> Option<String> {
    if let Ok(value) = WIRE_CONTEXT.try_with(ToString::to_string) {
        return Some(value);
    }
    tracing::Span::current()
        .with_subscriber(|(id, dispatch)| {
            let registry = dispatch.downcast_ref::<tracing_subscriber::Registry>()?;
            let span = registry.span(id)?;
            let extensions = span.extensions();
            extensions.get::<TraceParent>().map(ToString::to_string)
        })
        .flatten()
}
/// Invoke after existing authentication/validation. Never log a raw remote value.
#[cfg(test)]
fn receive_span(parent: Option<&str>) -> tracing::Span {
    let parent = parent
        .and_then(TraceParent::parse)
        .map(|p| p.to_string())
        .unwrap_or_default();
    tracing::info_span!(parent: None, "lingshu.sdk.native.receive", trace_parent=parent.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::{subscriber::with_default, Instrument};
    use tracing_subscriber::prelude::*;
    const PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
    #[test]
    fn remote_parent_normalizes_sampling_and_local_children_have_independent_ids() {
        let subscriber = tracing_subscriber::registry().with(TraceContextLayer);
        with_default(subscriber, || {
            let remote = receive_span(Some(PARENT));
            let _entered = remote.enter();
            let one = TraceParent::parse(&current_trace_parent().unwrap()).unwrap();
            assert_eq!(
                one.trace_id(),
                TraceParent::parse(PARENT).unwrap().trace_id()
            );
            assert_ne!(one.span_id(), TraceParent::parse(PARENT).unwrap().span_id());
            assert_eq!(one.flags(), "00");
            {
                let child = tracing::info_span!("child");
                let _entered = child.enter();
                let two = TraceParent::parse(&current_trace_parent().unwrap()).unwrap();
                assert_eq!(two.trace_id(), one.trace_id());
                assert_ne!(two.span_id(), one.span_id());
                assert_eq!(two.flags(), "00");
            }
            assert_eq!(
                TraceParent::parse(&current_trace_parent().unwrap()).unwrap(),
                one
            );
        });
    }
    #[tokio::test]
    async fn simultaneous_futures_and_accepted_tasks_keep_their_original_parent_without_installing_globals(
    ) {
        let dispatch =
            tracing::Dispatch::new(tracing_subscriber::registry().with(TraceContextLayer));
        use tracing::instrument::WithSubscriber;
        async {
            let make = |id: &'static str| async move {
                let parent = format!("00-{id}-00f067aa0ba902b7-01");
                async move {
                    let original = current_trace_parent().unwrap();
                    tokio::task::yield_now().await;
                    assert_eq!(current_trace_parent().as_deref(), Some(original.as_str()));
                    let task = tokio::spawn(
                        async {
                            tokio::task::yield_now().await;
                            current_trace_parent().unwrap()
                        }
                        .instrument(tracing::Span::current())
                        .with_subscriber(tracing::dispatcher::get_default(Clone::clone)),
                    );
                    assert_eq!(task.await.unwrap(), original);
                    TraceParent::parse(&original).unwrap().trace_id().to_owned()
                }
                .instrument(receive_span(Some(&parent)))
                .await
            };
            let (a, b) = tokio::join!(
                make("4bf92f3577b34da6a3ce929d0e0e4736"),
                make("0123456789abcdef0123456789abcdef")
            );
            assert_ne!(a, b);
            assert!(current_trace_parent().is_none());
        }
        .with_subscriber(dispatch)
        .await;
    }
    #[test]
    fn malformed_trace_is_not_recorded_or_used_as_authority() {
        let subscriber = tracing_subscriber::registry().with(TraceContextLayer);
        with_default(subscriber, || {
            let span = receive_span(Some("secret-token\r\nforged=payload"));
            let _entered = span.enter();
            let wire = current_trace_parent().unwrap();
            assert!(TraceParent::parse(&wire).is_some());
            assert!(!wire.contains("secret"));
        });
    }
}

// The host/application subscriber is optional. Task-local state is restored on
// every poll and never crosses an unrelated task, cancellation or timeout.
tokio::task_local! { static WIRE_CONTEXT: TraceParent; }

/// Establish one bounded causal operation without installing a subscriber.
/// A remote parent is observational only and must be passed after authentication.
pub async fn scope<F: std::future::Future>(parent: Option<&str>, future: F) -> F::Output {
    use tracing::Instrument;
    let (value, span) = new_operation(parent);
    WIRE_CONTEXT
        .scope(
            value,
            async {
                tracing::debug!("native trace scope entered");
                future.await
            }
            .instrument(span),
        )
        .await
}

/// Synchronous reply construction after the existing signature/binding checks.
pub fn scope_sync<R>(parent: Option<&str>, operation: impl FnOnce() -> R) -> R {
    let (value, span) = new_operation(parent);
    WIRE_CONTEXT.sync_scope(value, || {
        span.in_scope(|| {
            tracing::debug!("native trace scope entered");
            operation()
        })
    })
}

fn new_operation(parent: Option<&str>) -> (TraceParent, tracing::Span) {
    let parent = parent.and_then(TraceParent::parse).or_else(|| {
        current_trace_parent()
            .as_deref()
            .and_then(TraceParent::parse)
    });
    let span_id = uuid::Uuid::new_v4().simple().to_string()[..16].to_owned();
    let value = parent
        .as_ref()
        .and_then(|p| p.child(&span_id))
        .unwrap_or_else(|| {
            TraceParent::root(&uuid::Uuid::new_v4().simple().to_string(), &span_id).unwrap()
        });
    let wire = value.to_string();
    let parent_id = parent.as_ref().map(TraceParent::span_id).unwrap_or("");
    // The canonical context is understood by the optional layers so logs and
    // outgoing envelopes use the same span ID, rather than making another child.
    let span = tracing::info_span!(parent: None, "lingshu.native.operation",
        trace_context=wire.as_str(), trace_id=value.trace_id(),
        span_id=value.span_id(), p_span_id=parent_id, trace_flags=value.flags());
    (value, span)
}

/// Explicitly carry the original operation into an accepted execution task.
/// Tokio task-local values are deliberately not inherited by spawn.
pub(crate) fn carry<F: std::future::Future>(
    future: F,
) -> impl std::future::Future<Output = F::Output> {
    use tracing::Instrument;
    let context = current_trace_parent()
        .as_deref()
        .and_then(TraceParent::parse);
    let span = tracing::Span::current();
    async move {
        match context {
            Some(value) => WIRE_CONTEXT.scope(value, future.instrument(span)).await,
            None => future.instrument(span).await,
        }
    }
}

#[cfg(test)]
mod scope_tests {
    use super::*;
    const PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
    #[tokio::test]
    async fn no_layer_concurrent_scopes_are_isolated_and_cancelled_state_is_restored() {
        let a = scope(Some(PARENT), async {
            let original = current_trace_parent().unwrap();
            tokio::task::yield_now().await;
            assert_eq!(current_trace_parent().as_deref(), Some(original.as_str()));
            let parent = TraceParent::parse(&original).unwrap();
            assert_eq!(
                parent.trace_id(),
                TraceParent::parse(PARENT).unwrap().trace_id()
            );
            assert_eq!(parent.flags(), "00");
            assert_ne!(
                parent.span_id(),
                TraceParent::parse(PARENT).unwrap().span_id()
            );
            assert!(tokio::spawn(async { current_trace_parent() })
                .await
                .unwrap()
                .is_none());
            scope(None, async {
                let child = TraceParent::parse(&current_trace_parent().unwrap()).unwrap();
                assert_eq!(child.trace_id(), parent.trace_id());
                assert_ne!(child.span_id(), parent.span_id());
            })
            .await;
            assert_eq!(current_trace_parent().as_deref(), Some(original.as_str()));
            original
        });
        let b = scope(Some("bad\r\ncredential"), async {
            tokio::task::yield_now().await;
            let value = current_trace_parent().unwrap();
            assert!(!value.contains("credential"));
            value
        });
        let (a, b) = tokio::join!(a, b);
        assert_ne!(
            TraceParent::parse(&a).unwrap().trace_id(),
            TraceParent::parse(&b).unwrap().trace_id()
        );
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(1),
            scope(Some(PARENT), std::future::pending::<()>()),
        )
        .await;
        assert!(result.is_err());
        assert!(current_trace_parent().is_none());
    }
    #[tokio::test]
    async fn synchronous_reply_restores_async_context_even_after_unwind() {
        assert!(current_trace_parent().is_none());
        scope(Some(PARENT), async {
            let outer = current_trace_parent().unwrap();
            let parent = TraceParent::parse(&outer).unwrap();
            let first = scope_sync(None, || current_trace_parent().unwrap());
            let second = scope_sync(None, || current_trace_parent().unwrap());
            for child in [&first, &second] {
                let child = TraceParent::parse(child).unwrap();
                assert_eq!(child.trace_id(), parent.trace_id());
                assert_eq!(child.flags(), parent.flags());
                assert_ne!(child.span_id(), parent.span_id());
            }
            assert_ne!(first, second);
            let result = std::panic::catch_unwind(|| scope_sync(None, || panic!("fixture")));
            assert!(result.is_err());
            tokio::task::yield_now().await;
            assert_eq!(current_trace_parent().as_deref(), Some(outer.as_str()));
        })
        .await;
        assert!(current_trace_parent().is_none());
    }
    #[tokio::test]
    async fn optional_layer_and_wire_use_exactly_the_same_span() {
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::prelude::*;

        let dispatch =
            tracing::Dispatch::new(tracing_subscriber::registry().with(TraceContextLayer));
        scope(Some(PARENT), async {
            let wire = current_trace_parent().unwrap();
            let extension =
                tracing::Span::with_subscriber(&tracing::Span::current(), |(id, dispatch)| {
                    let registry = dispatch
                        .downcast_ref::<tracing_subscriber::Registry>()
                        .unwrap();
                    registry
                        .span(id)
                        .unwrap()
                        .extensions()
                        .get::<TraceParent>()
                        .unwrap()
                        .to_string()
                })
                .unwrap();
            assert_eq!(wire, extension);
        })
        .with_subscriber(dispatch)
        .await;
    }
}

#[cfg(test)]
#[tokio::test]
async fn accepted_task_explicit_carrier_preserves_wire_without_a_subscriber() {
    scope(
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"),
        async {
            let wire = current_trace_parent().unwrap();
            let accepted = tokio::spawn(carry(async {
                tokio::task::yield_now().await;
                current_trace_parent().unwrap()
            }));
            assert_eq!(accepted.await.unwrap(), wire);
        },
    )
    .await;
    assert!(current_trace_parent().is_none());
}

// Fixture-only: verified wire replies are correlation observations, never a
// production acceptance condition. Legacy peers may omit trace metadata.
#[cfg(test)]
pub(super) fn assert_verified_reply_trace(
    response: &kish_lingshu_foundation_contract::service_transport::TransportEnvelope,
) {
    if std::env::var_os("LINGSHU_VERIFY_NATIVE_TRACE").is_none() {
        return;
    }
    let parent = TraceParent::parse(&current_trace_parent().unwrap()).unwrap();
    let child = TraceParent::parse(response.trace_parent.as_deref().unwrap()).unwrap();
    assert_eq!(parent.trace_id(), child.trace_id());
    assert_eq!(parent.flags(), child.flags());
    assert_ne!(parent.span_id(), child.span_id());
    VERIFIED_CONTROL_TRACES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
}
#[cfg(test)]
pub(super) static VERIFIED_CONTROL_TRACES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
