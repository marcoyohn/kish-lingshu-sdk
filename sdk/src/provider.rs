//! Authenticated source discovery. Registration grants no execution permission.
use crate::{ServiceAuthError, ServiceConnection};
use axum::{response::IntoResponse, routing::get, Router};
pub use kish_lingshu_runtime_contract::provider::*;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};

pub struct ProviderHttpAdapter {
    connection: ServiceConnection,
    catalog: Arc<ProviderCatalog>,
}
impl ProviderHttpAdapter {
    pub fn new(
        connection: ServiceConnection,
        catalog: ProviderCatalog,
    ) -> Result<Self, ServiceAuthError> {
        catalog
            .validate()
            .map_err(|_| ServiceAuthError::InvalidNodeConfig)?;
        if catalog.application_id != connection.application_id() {
            return Err(ServiceAuthError::InvalidNodeConfig);
        }
        Ok(Self {
            connection,
            catalog: Arc::new(catalog),
        })
    }
    /// Nest this router at the exact externally reachable catalog URL path.
    pub fn router(&self, catalog_url: &str) -> Result<Router, ServiceAuthError> {
        let body = serde_json::to_vec(self.catalog.as_ref())
            .map_err(|_| ServiceAuthError::InvalidNodeConfig)?;
        let etag = format!(
            "\"{}\"",
            self.catalog
                .digest()
                .map_err(|_| ServiceAuthError::InvalidNodeConfig)?
        );
        self.connection.protect(
            Router::new().route(
                "/",
                get(move || {
                    let body = body.clone();
                    let etag = etag.clone();
                    async move {
                        (
                            [
                                ("content-type", "application/json".to_string()),
                                ("etag", etag),
                                ("cache-control", "private, no-cache".to_string()),
                            ],
                            body,
                        )
                            .into_response()
                    }
                }),
            ),
            catalog_url,
        )
    }
    pub async fn enroll(
        &self,
        instance_id: &str,
        catalog_url: &str,
    ) -> Result<EnrolledProvider, ServiceAuthError> {
        crate::service_auth::callback_url(catalog_url)?;
        let mut registration = self.connection.registration().await;
        let request = ProviderEnrollment {
            instance: registration.request(instance_id),
            provider_key: self.catalog.provider_key.clone(),
            release: self.catalog.release.clone(),
            catalog_url: catalog_url.into(),
            catalog_digest: self
                .catalog
                .digest()
                .map_err(|_| ServiceAuthError::InvalidNodeConfig)?,
        };
        let started = tokio::time::Instant::now();
        let session: ProviderSession = self
            .connection
            .root_response_json(
                self.connection
                    .service_request(reqwest::Method::POST, "provider-enrollments", None)?
                    .json(&request),
            )
            .await?;
        validate_session(&session, None)?;
        registration.accept(Some(&session.instance))?;
        let (cancel, mut stopped) = watch::channel(false);
        let (status, alive) = watch::channel(true);
        let mut heartbeat = self.connection.track_heartbeat(
            kish_lingshu_foundation_contract::instance_heartbeat::InstanceRoleKind::Provider,
            vec![self.catalog.provider_key.clone()],
            session.instance.clone(),
            &session.credential,
            Duration::from_millis(session.heartbeat_interval_ms),
        )?;
        drop(registration);
        let connection = self.connection.clone();
        let task = tokio::spawn(async move {
            let mut session = session;
            let mut deadline = started + Duration::from_secs(30);
            let mut closed = connection.subscribe_closed();
            loop {
                let (started, result) = tokio::select! {
                    _=stopped.changed()=>break,
                    _=closed.changed()=>break,
                    _=tokio::time::sleep_until(deadline)=>break,
                    result=heartbeat.next::<ProviderSession>()=>result,
                };
                match result {
                    Ok(next) if validate_session(&next, Some(&session)).is_ok() => {
                        session = next;
                        deadline = started + Duration::from_secs(30);
                        heartbeat.update(&session.credential);
                    }
                    Err(error @ ServiceAuthError::Http(401)) => {
                        connection.reject_authentication(error);
                        break;
                    }
                    Ok(_) | Err(ServiceAuthError::Http(403 | 404 | 409 | 422)) => break,
                    _ => {}
                }
            }
            drop(heartbeat);
            status.send_replace(false);
            if let Ok(request) = connection.service_request(
                reqwest::Method::POST,
                "provider-sessions/deregister",
                Some(&session.credential),
            ) {
                let _ =
                    crate::service_auth::response_bytes(request.timeout(Duration::from_secs(3)))
                        .await;
            }
        });
        Ok(EnrolledProvider {
            cancel,
            alive,
            task: Some(task),
        })
    }
}
fn validate_session(
    session: &ProviderSession,
    previous: Option<&ProviderSession>,
) -> Result<(), ServiceAuthError> {
    let now = chrono::Utc::now().timestamp_millis();
    if session.generation.is_empty()
        || session.credential.is_empty()
        || session.heartbeat_interval_ms != 10_000
        || session.lease_expires_at_ms <= now
        || session.lease_expires_at_ms > now + 31_000
        || previous
            .is_some_and(|p| p.instance != session.instance || p.generation != session.generation)
    {
        return Err(ServiceAuthError::InvalidResponse);
    }
    Ok(())
}
pub struct EnrolledProvider {
    cancel: watch::Sender<bool>,
    alive: watch::Receiver<bool>,
    task: Option<JoinHandle<()>>,
}
impl EnrolledProvider {
    pub fn is_live(&self) -> bool {
        *self.alive.borrow()
    }
    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.alive.clone()
    }
    pub async fn shutdown(mut self) {
        self.cancel.send_replace(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for EnrolledProvider {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
    }
}
