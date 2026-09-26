//! Transport-neutral failure facts. Runtime adapters decide whether replay is safe.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureOutcome {
    /// No request was sent (for example, connection establishment failed).
    NotDispatched,
    /// A response completed or rejected the request.
    Responded,
    /// The peer may still be processing the request.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("remote operation failed (status={status:?}, outcome={outcome:?})")]
pub struct RemoteFailure {
    pub status: Option<u16>,
    pub outcome: FailureOutcome,
    /// Absolute UTC milliseconds, already interpreted by the transport.
    pub retry_after_ms: Option<i64>,
}

impl RemoteFailure {
    pub fn find<'a>(error: &'a (dyn std::error::Error + 'static)) -> Option<&'a Self> {
        if let Some(failure) = error.downcast_ref::<Self>() {
            Some(failure)
        } else {
            error.source().and_then(Self::find)
        }
    }
    pub fn request_finished(&self) -> bool {
        self.outcome != FailureOutcome::Unknown
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ReplaySafety {
    /// Replay only requests known not to have run, including explicit throttling.
    RejectedOnly,
    /// The adapter asserts replay of a completed server error has no business effects.
    NoSideEffects,
    /// The remote operation has a stable, enforced idempotency key.
    Idempotent,
}

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub maximum_attempts: u32,
    pub timeout_ms: u64,
    pub replay: ReplaySafety,
}
impl RetryPolicy {
    pub const MODEL: Self = Self {
        maximum_attempts: 3,
        timeout_ms: 60_000,
        replay: ReplaySafety::NoSideEffects,
    };
    pub fn allows(&self, failure: &RemoteFailure, output_started: bool) -> bool {
        if output_started {
            return false;
        }
        match failure.outcome {
            FailureOutcome::NotDispatched => true,
            FailureOutcome::Unknown => matches!(self.replay, ReplaySafety::Idempotent),
            FailureOutcome::Responded => {
                failure.status == Some(429)
                    || (matches!(
                        self.replay,
                        ReplaySafety::NoSideEffects | ReplaySafety::Idempotent
                    ) && matches!(failure.status, Some(500 | 502 | 503 | 504)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replay_requires_both_failure_and_operation_safety() {
        let mut failure = RemoteFailure {
            status: Some(503),
            outcome: FailureOutcome::Responded,
            retry_after_ms: None,
        };
        let policy = RetryPolicy::MODEL;
        assert!(policy.allows(&failure, false));
        assert!(!policy.allows(&failure, true));
        assert!(!RetryPolicy {
            replay: ReplaySafety::RejectedOnly,
            ..policy
        }
        .allows(&failure, false));
        failure.outcome = FailureOutcome::Unknown;
        assert!(!policy.allows(&failure, false));
        assert!(RetryPolicy {
            replay: ReplaySafety::Idempotent,
            ..policy
        }
        .allows(&failure, false));
        failure.outcome = FailureOutcome::Responded;
        failure.status = Some(401);
        assert!(!policy.allows(&failure, false));
    }
}
