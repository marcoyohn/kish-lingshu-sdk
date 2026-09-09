use std::{future::Future, sync::Arc, time::Duration};

use chrono::{DateTime, TimeDelta, Utc};
use rand::Rng;

use crate::{request::TransportAttempt, Error, RequestOptions};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplaySafety {
    Read,
    IdempotentMutation,
    NonIdempotentMutation,
    StreamReattachment,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryReason {
    Transport,
    HttpStatus {
        status: u16,
        retry_after: Option<Duration>,
    },
    StreamDisconnected,
}

pub(crate) struct RetryFailure {
    pub error: Error,
    pub reason: RetryReason,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryPolicy {
    retry_limit: u32,
    base_delay: Duration,
    max_delay: Duration,
}

impl RetryPolicy {
    pub(crate) fn new(retry_limit: u32) -> Self {
        Self {
            retry_limit,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
        }
    }

    #[cfg(test)]
    fn with_delays(mut self, base_delay: Duration, max_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self.max_delay = max_delay;
        self
    }

    pub(crate) fn next_delay(
        &self,
        safety: ReplaySafety,
        failed_attempt: u32,
        reason: RetryReason,
        now: DateTime<Utc>,
        deadline: Option<DateTime<Utc>>,
        jitter: f64,
    ) -> Option<Duration> {
        if safety == ReplaySafety::NonIdempotentMutation
            || failed_attempt == 0
            || failed_attempt > self.retry_limit
            || !reason.is_retryable()
        {
            return None;
        }
        let delay = match reason {
            RetryReason::HttpStatus {
                retry_after: Some(delay),
                ..
            } => delay.min(self.max_delay),
            _ => {
                let exponent = failed_attempt.saturating_sub(1).min(31);
                let multiplier = 1_u32 << exponent;
                let backoff = self.base_delay.saturating_mul(multiplier);
                let jitter = jitter.clamp(0.5, 1.5);
                backoff.mul_f64(jitter).min(self.max_delay)
            }
        };
        if let Some(deadline) = deadline {
            if deadline <= now {
                return None;
            }
            let chrono_delay = TimeDelta::from_std(delay).ok()?;
            if now + chrono_delay >= deadline {
                return None;
            }
        }
        Some(delay)
    }
}

impl RetryReason {
    fn is_retryable(self) -> bool {
        match self {
            Self::Transport | Self::StreamDisconnected => true,
            Self::HttpStatus { status, .. } => matches!(status, 429 | 502 | 503),
        }
    }
}

/// Executes one logical request while preserving its serialized body and IDs.
pub(crate) async fn execute_with_retry<T, F, Fut>(
    policy: RetryPolicy,
    safety: ReplaySafety,
    options: &RequestOptions,
    body: Arc<[u8]>,
    mut send: F,
) -> Result<T, Error>
where
    F: FnMut(TransportAttempt, Arc<[u8]>) -> Fut,
    Fut: Future<Output = Result<T, RetryFailure>>,
{
    let retry_limit = options.retry_limit().unwrap_or(policy.retry_limit);
    let policy = RetryPolicy {
        retry_limit,
        ..policy
    };
    let mut attempt_number = 1;
    loop {
        let attempt = options.attempt(attempt_number);
        match send(attempt, body.clone()).await {
            Ok(output) => return Ok(output),
            Err(failure) => {
                let now = Utc::now();
                let jitter = rand::thread_rng().gen_range(0.5..=1.5);
                let delay = policy.next_delay(
                    safety,
                    attempt_number,
                    failure.reason,
                    now,
                    options.deadline().map(|value| *value.as_datetime()),
                    jitter,
                );
                let Some(delay) = delay else {
                    return Err(failure.error);
                };
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                attempt_number += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use kish_lingshu_runtime_contract::RequestId;

    use super::*;
    use crate::{TransportFailure, TransportKind};

    fn failure(options: &RequestOptions, reason: RetryReason) -> RetryFailure {
        RetryFailure {
            error: Error::Transport(TransportFailure {
                kind: TransportKind::Request,
                message: "transient".to_string(),
                request_id: Some(options.request_id().clone()),
                retryable: true,
            }),
            reason,
        }
    }

    #[test]
    fn policy_retries_only_replay_safe_transient_failures_before_deadline() {
        let now = "2026-09-06T08:00:00Z".parse().unwrap();
        let policy =
            RetryPolicy::new(2).with_delays(Duration::from_secs(1), Duration::from_secs(10));
        assert_eq!(
            policy.next_delay(
                ReplaySafety::Read,
                1,
                RetryReason::HttpStatus {
                    status: 503,
                    retry_after: None,
                },
                now,
                None,
                1.0,
            ),
            Some(Duration::from_secs(1))
        );
        assert!(policy
            .next_delay(
                ReplaySafety::NonIdempotentMutation,
                1,
                RetryReason::Transport,
                now,
                None,
                1.0,
            )
            .is_none());
        assert!(policy
            .next_delay(
                ReplaySafety::Read,
                1,
                RetryReason::HttpStatus {
                    status: 400,
                    retry_after: None,
                },
                now,
                None,
                1.0,
            )
            .is_none());
        assert!(policy
            .next_delay(
                ReplaySafety::IdempotentMutation,
                1,
                RetryReason::Transport,
                now,
                Some(now + TimeDelta::milliseconds(500)),
                1.0,
            )
            .is_none());
        assert_eq!(
            policy.next_delay(
                ReplaySafety::Read,
                1,
                RetryReason::HttpStatus {
                    status: 429,
                    retry_after: Some(Duration::from_secs(30)),
                },
                now,
                None,
                1.0,
            ),
            Some(Duration::from_secs(10))
        );
    }

    #[tokio::test]
    async fn executor_reuses_body_and_logical_ids_but_increments_attempt_number() {
        let options = RequestOptions::new().with_retry_limit(2).unwrap();
        let expected_request = options.request_id().clone();
        let expected_correlation = options.correlation_id().clone();
        let body: Arc<[u8]> = Arc::from(br#"{"order_id":42}"#.as_slice());
        let body_ptr = body.as_ptr() as usize;
        let attempts = Mutex::new(Vec::new());
        let output = execute_with_retry(
            RetryPolicy::new(2).with_delays(Duration::ZERO, Duration::ZERO),
            ReplaySafety::IdempotentMutation,
            &options,
            body,
            |attempt, body| {
                attempts.lock().unwrap().push((
                    attempt.clone(),
                    body.as_ptr() as usize,
                    body.to_vec(),
                ));
                let result = if attempt.number < 3 {
                    Err(failure(&options, RetryReason::Transport))
                } else {
                    Ok("accepted")
                };
                std::future::ready(result)
            },
        )
        .await
        .unwrap();
        assert_eq!(output, "accepted");
        let attempts = attempts.into_inner().unwrap();
        assert_eq!(attempts.len(), 3);
        for (index, (attempt, ptr, bytes)) in attempts.into_iter().enumerate() {
            assert_eq!(attempt.request_id, expected_request);
            assert_eq!(attempt.correlation_id, expected_correlation);
            assert_eq!(attempt.number, u32::try_from(index).unwrap() + 1);
            assert_eq!(ptr, body_ptr);
            assert_eq!(bytes, br#"{"order_id":42}"#);
        }
    }

    #[tokio::test]
    async fn unsafe_mutation_returns_first_ambiguous_failure_without_replay() {
        let options = RequestOptions::new();
        let sends = Mutex::new(0_u32);
        let result = execute_with_retry::<(), _, _>(
            RetryPolicy::new(3).with_delays(Duration::ZERO, Duration::ZERO),
            ReplaySafety::NonIdempotentMutation,
            &options,
            Arc::from([]),
            |_, _| {
                *sends.lock().unwrap() += 1;
                std::future::ready(Err(failure(&options, RetryReason::Transport)))
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(*sends.lock().unwrap(), 1);
    }

    #[test]
    fn request_identity_type_remains_transport_neutral() {
        let _: RequestId = RequestOptions::new().request_id().clone();
    }
}
