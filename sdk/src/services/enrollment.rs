use super::*;
use crate::{ServiceAuthError, ServiceConnection};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

#[derive(Debug, Clone)]
pub enum ServiceEnrollmentStatus {
    Ready {
        node_id: String,
        generation: String,
        lease_expires_at_ms: i64,
    },
    /// Discovery is unavailable; accepted calls retain their own finite authority.
    Unavailable,
    Stopped,
}

pub struct EnrolledService {
    cancel: watch::Sender<bool>,
    status: watch::Receiver<ServiceEnrollmentStatus>,
    task: Option<JoinHandle<()>>,
}
impl EnrolledService {
    pub fn status(&self) -> ServiceEnrollmentStatus {
        self.status.borrow().clone()
    }
    pub fn subscribe(&self) -> watch::Receiver<ServiceEnrollmentStatus> {
        self.status.clone()
    }
    pub async fn shutdown(mut self) {
        self.cancel.send_replace(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for EnrolledService {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
    }
}

impl ServiceConnection {
    pub async fn enroll_service(
        &self,
        node_id: impl Into<String>,
        invocation_url: impl Into<String>,
        maximum_in_flight: u32,
        registry: Arc<ServiceRegistry>,
    ) -> Result<EnrolledService, ServiceAuthError> {
        let mut request = ServiceEnrollment {
            instance: None,
            node_id: node_id.into(),
            invocation_url: invocation_url.into(),
            maximum_in_flight,
            capabilities: registry.capabilities(),
        };
        if !valid_key(&request.node_id)
            || maximum_in_flight == 0
            || self.application_id() != registry.manifest().application_id
        {
            return Err(ServiceAuthError::InvalidNodeConfig);
        }
        crate::service_auth::callback_url(&request.invocation_url)?;
        let mut registration = self.registration().await;
        request.instance = Some(registration.request(&request.node_id));
        let started = Instant::now();
        let session: ServiceSession = self
            .root_response_json(
                self.service_request(reqwest::Method::POST, "enrollments", None)?
                    .json(&request),
            )
            .await?;
        validate_session(&session, &request.node_id, None)?;
        registration.accept(session.instance.as_ref())?;
        drop(registration);
        let deadline = started + Duration::from_secs(30);
        let (status_tx, status) = watch::channel(ServiceEnrollmentStatus::Ready {
            node_id: session.node_id.clone(),
            generation: session.generation.clone(),
            lease_expires_at_ms: session.lease_expires_at_ms,
        });
        let (cancel, cancel_rx) = watch::channel(false);
        let connection = self.clone();
        let task = tokio::spawn(renew(connection, session, deadline, status_tx, cancel_rx));
        Ok(EnrolledService {
            cancel,
            status,
            task: Some(task),
        })
    }
}
fn validate_session(
    session: &ServiceSession,
    node: &str,
    previous: Option<&ServiceSession>,
) -> Result<(), ServiceAuthError> {
    if session.node_id != node
        || session.generation.is_empty()
        || session.credential.is_empty()
        || session.heartbeat_interval_ms != 10_000
        || session.lease_expires_at_ms <= chrono::Utc::now().timestamp_millis()
        || session.lease_expires_at_ms > chrono::Utc::now().timestamp_millis() + 31_000
        || previous
            .is_some_and(|p| p.generation != session.generation || p.instance != session.instance)
    {
        return Err(ServiceAuthError::InvalidResponse);
    }
    Ok(())
}
async fn renew(
    connection: ServiceConnection,
    mut session: ServiceSession,
    mut deadline: Instant,
    status: watch::Sender<ServiceEnrollmentStatus>,
    mut cancel: watch::Receiver<bool>,
) {
    let mut closed = connection.subscribe_closed();
    let mut unavailable = false;
    loop {
        tokio::select! {
            _=cancel.changed()=>break,
            _=closed.changed()=>break,
            _=tokio::time::sleep_until(deadline)=>{unavailable = true; break;},
            _=tokio::time::sleep(Duration::from_secs(10))=>{}
        }
        let started = Instant::now();
        let request = match connection.service_request(
            reqwest::Method::POST,
            "sessions/heartbeat",
            Some(&session.credential),
        ) {
            Ok(r) => r,
            Err(_) => break,
        };
        let result: Result<ServiceSession, ServiceAuthError> = tokio::select! {
            _=cancel.changed()=>break,
            _=closed.changed()=>break,
            _=tokio::time::sleep_until(deadline)=>{unavailable = true; break;},
            r=crate::service_auth::response_json(request)=>r
        };
        match result {
            Ok(next) => {
                if validate_session(&next, &session.node_id, Some(&session)).is_err() {
                    break;
                }
                session = next;
                deadline = started + Duration::from_secs(30);
                status.send_replace(ServiceEnrollmentStatus::Ready {
                    node_id: session.node_id.clone(),
                    generation: session.generation.clone(),
                    lease_expires_at_ms: session.lease_expires_at_ms,
                });
            }
            Err(e @ ServiceAuthError::Http(401)) => {
                connection.reject_authentication(e);
                break;
            }
            Err(ServiceAuthError::Http(409)) => {
                unavailable = true;
                break;
            }
            Err(ServiceAuthError::Http(403 | 404)) => break,
            Err(_) => {}
        }
    }
    status.send_replace(if unavailable {
        ServiceEnrollmentStatus::Unavailable
    } else {
        ServiceEnrollmentStatus::Stopped
    });
    if unavailable {
        tokio::select! { _=cancel.changed()=>{}, _=closed.changed()=>{} }
        status.send_replace(ServiceEnrollmentStatus::Stopped);
    }
    if let Ok(request) = connection.service_request(
        reqwest::Method::POST,
        "sessions/deregister",
        Some(&session.credential),
    ) {
        let _ = crate::service_auth::response_bytes(request).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renewal_rotates_credentials_without_changing_base_or_role_identity() {
        let previous = ServiceSession {
            instance: Some(ServiceInstanceIdentity {
                instance_id: "host".into(),
                generation: "base-one".into(),
            }),
            node_id: "call".into(),
            generation: "role-one".into(),
            credential: "old".into(),
            lease_expires_at_ms: chrono::Utc::now().timestamp_millis() + 30_000,
            heartbeat_interval_ms: 10_000,
        };
        let mut next = previous.clone();
        next.credential = "rotated".into();
        assert!(validate_session(&next, "call", Some(&previous)).is_ok());
        next.instance.as_mut().unwrap().generation = "base-two".into();
        assert!(validate_session(&next, "call", Some(&previous)).is_err());
        next.instance = None;
        assert!(validate_session(&next, "call", Some(&previous)).is_err());
        next = previous.clone();
        next.generation = "role-two".into();
        assert!(validate_session(&next, "call", Some(&previous)).is_err());
    }
}
