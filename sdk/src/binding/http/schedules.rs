use super::*;
use kish_lingshu_event_dispatch_contract::{ScheduleDefinition, ScheduleReceipt};

const MAX_RESPONSE_BYTES: usize = 64 * 1024;

impl HttpBinding {
    pub(super) async fn ensure_schedule_http(
        &self,
        definition: ScheduleDefinition,
        options: MutationOptions,
    ) -> Result<ScheduleReceipt, Error> {
        let app_id = self.state.application_scope.as_deref().ok_or_else(|| {
            Error::configuration(
                "application_id",
                "Schedule creation requires an application scope",
            )
        })?;
        let url = self.endpoint("api/user/event-dispatch/v1/schedules")?;
        let body = self.encode_body(&definition, options.request().request_id())?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(self.state.timeout)
            .build()
            .map_err(|_| {
                Error::configuration("schedule.transport", "could not construct HTTP client")
            })?;
        let receipt: ScheduleReceipt = execute_with_retry(
            self.state.retry,
            ReplaySafety::IdempotentMutation,
            options.request(),
            body,
            |attempt, body| {
                let client = client.clone();
                let url = url.clone();
                let options = options.clone();
                async move {
                    let protocol = |message: &'static str| RetryFailure {
                        error: Error::Protocol(ProtocolError {
                            direction: ProtocolDirection::DecodeResponse,
                            message: message.into(),
                            request_id: Some(attempt.request_id.clone()),
                        }),
                        reason: RetryReason::HttpStatus {
                            status: 400,
                            retry_after: None,
                        },
                    };
                    let request = self
                        .request(
                            Method::POST,
                            url,
                            options.request(),
                            &attempt,
                            &body,
                            Some(options.idempotency_key().as_str()),
                            "application/json",
                        )
                        .map_err(|error| RetryFailure {
                            error,
                            reason: RetryReason::Transport,
                        })?
                        .build()
                        .map_err(|_| protocol("invalid Schedule HTTP request"))?;
                    let mut response =
                        client
                            .execute(request)
                            .await
                            .map_err(|error| RetryFailure {
                                error: request_transport_error(
                                    error.without_url().to_string(),
                                    attempt.request_id.clone(),
                                    true,
                                ),
                                reason: RetryReason::Transport,
                            })?;
                    let status = response.status();
                    let retry_after = response
                        .headers()
                        .get(header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned);
                    if response
                        .content_length()
                        .is_some_and(|n| n > MAX_RESPONSE_BYTES as u64)
                    {
                        return Err(protocol("Schedule response exceeds size limit"));
                    }
                    let mut bytes = Vec::new();
                    while let Some(chunk) =
                        response.chunk().await.map_err(|error| RetryFailure {
                            error: request_transport_error(
                                error.without_url().to_string(),
                                attempt.request_id.clone(),
                                true,
                            ),
                            reason: RetryReason::Transport,
                        })?
                    {
                        if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
                            return Err(protocol("Schedule response exceeds size limit"));
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                    if status.is_success() {
                        serde_json::from_slice(&bytes)
                            .map_err(|_| protocol("invalid Schedule receipt"))
                    } else {
                        Err(RetryFailure {
                            error: decode_application_error(
                                status.as_u16(),
                                &bytes,
                                retry_after.as_deref(),
                                attempt.request_id.clone(),
                                Utc::now(),
                            ),
                            reason: RetryReason::HttpStatus {
                                status: status.as_u16(),
                                retry_after: retry_after
                                    .as_deref()
                                    .and_then(|v| parse_retry_after(v, Utc::now())),
                            },
                        })
                    }
                }
            },
        )
        .await?;
        if !receipt.matches_definition(app_id, &definition) {
            return Err(Error::Protocol(ProtocolError {
                direction: ProtocolDirection::DecodeResponse,
                message:
                    "Schedule receipt does not match application, definition or lifecycle state"
                        .into(),
                request_id: Some(options.request().request_id().clone()),
            }));
        }
        Ok(receipt)
    }
}
