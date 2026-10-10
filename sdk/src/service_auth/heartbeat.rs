//! One renewal transport task per connection, with independent role lifetimes.
use super::*;
use kish_lingshu_foundation_contract::{instance_heartbeat::*, ServiceInstanceIdentity};
use std::collections::BTreeMap;
use tokio::{sync::Notify, time::Instant};

type Renewal = (Instant, Result<serde_json::Value, ServiceAuthError>);
struct Entry {
    request: InstanceHeartbeatRole,
    // Logical role identity, separate from its replaceable session generation.
    key: Vec<String>,
    instance: ServiceInstanceIdentity,
    interval: Duration,
    result: watch::Sender<Option<Renewal>>,
}
#[derive(Default)]
pub(super) struct Heartbeats {
    entries: Mutex<BTreeMap<u64, Entry>>,
    next_id: std::sync::atomic::AtomicU64,
    changed: Notify,
}

impl Heartbeats {
    #[cfg(feature = "service-zenoh")]
    pub(super) fn is_empty(&self) -> bool {
        self.entries.lock().unwrap_or_else(|error| error.into_inner()).is_empty()
    }
}

pub(crate) struct RoleHeartbeat {
    id: u64,
    registry: Arc<Heartbeats>,
    result: watch::Receiver<Option<Renewal>>,
}
impl Drop for RoleHeartbeat {
    fn drop(&mut self) {
        self.registry
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
        self.registry.changed.notify_one();
    }
}
impl RoleHeartbeat {
    pub(crate) async fn next<T: DeserializeOwned>(
        &mut self,
    ) -> (Instant, Result<T, ServiceAuthError>) {
        if self.result.changed().await.is_err() {
            return (Instant::now(), Err(ServiceAuthError::Closed));
        }
        let (started, result) = self
            .result
            .borrow_and_update()
            .clone()
            .expect("renewal notification");
        (
            started,
            result.and_then(|v| {
                serde_json::from_value(v).map_err(|_| ServiceAuthError::InvalidResponse)
            }),
        )
    }
    pub(crate) fn update(&self, credential: &str) {
        if let Some(entry) = self
            .registry
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&self.id)
        {
            entry.request.credential = credential.into();
        }
    }
}
impl ServiceConnection {
    pub(crate) fn track_heartbeat(
        &self,
        kind: InstanceRoleKind,
        key: Vec<String>,
        instance: ServiceInstanceIdentity,
        credential: &str,
        interval: Duration,
    ) -> Result<RoleHeartbeat, ServiceAuthError> {
        self.ensure_open()?;
        let registry = self.0.shared.heartbeats.clone();
        let id = registry
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (result, receiver) = watch::channel(None);
        {
            let mut entries = registry.entries.lock().unwrap_or_else(|e| e.into_inner());
            let replaced = entries
                .iter()
                .find(|(_, e)| e.request.kind == kind && e.key == key)
                .map(|(id, _)| *id);
            if (replaced.is_none() && entries.len() >= MAX_INSTANCE_HEARTBEAT_ROLES)
                || entries.values().any(|e| e.instance != instance)
            {
                return Err(ServiceAuthError::InvalidNodeConfig);
            }
            if let Some(previous) = replaced.and_then(|id| entries.remove(&id)) {
                previous
                    .result
                    .send_replace(Some((Instant::now(), Err(ServiceAuthError::Http(409)))));
            }
            entries.insert(
                id,
                Entry {
                    request: InstanceHeartbeatRole {
                        id,
                        kind,
                        credential: credential.into(),
                    },
                    key,
                    instance,
                    interval,
                    result,
                },
            );
        }
        registry.changed.notify_one();
        Ok(RoleHeartbeat {
            id,
            registry,
            result: receiver,
        })
    }
}

pub(super) async fn run(shared: Arc<Shared>) {
    let registry = &shared.heartbeats;
    let mut closed = shared.closed.subscribe();
    let mut next_tick: Option<Instant> = None;
    loop {
        if closed.borrow().is_some() {
            break;
        }
        let interval = registry
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|e| e.interval)
            .min();
        next_tick = interval.map(|interval| {
            next_tick
                .unwrap_or_else(|| Instant::now() + interval)
                .min(Instant::now() + interval)
        });
        tokio::select! {
            _ = closed.changed() => break,
            _ = registry.changed.notified() => continue,
            _ = async { match next_tick { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {}
        }
        let snapshot = {
            let entries = registry.entries.lock().unwrap_or_else(|e| e.into_inner());
            entries
                .values()
                .next()
                .map(|first| InstanceHeartbeatRequest {
                    instance: first.instance.clone(),
                    roles: entries.values().map(|e| e.request.clone()).collect(),
                })
        };
        let Some(snapshot) = snapshot else {
            next_tick = None;
            continue;
        };
        let started = Instant::now();
        let result = tokio::select! {
            _ = closed.changed() => break,
            result = send(&shared, &snapshot) => result,
        };
        if let Err(error @ ServiceAuthError::Http(401 | 403)) = &result {
            shared.close(error.clone());
        }
        let mut entries = registry.entries.lock().unwrap_or_else(|e| e.into_inner());
        for requested in &snapshot.roles {
            if let Some(entry) = entries.get_mut(&requested.id) {
                let result = match &result {
                    Ok(response) => {
                        let item = response
                            .roles
                            .iter()
                            .find(|r| r.id == requested.id)
                            .expect("validated result IDs");
                        if item.status == 200 {
                            Ok(item.session.clone().expect("validated session"))
                        } else {
                            Err(ServiceAuthError::Http(item.status))
                        }
                    }
                    Err(error) => Err(error.clone()),
                };
                entry.result.send_replace(Some((started, result)));
            }
        }
        drop(entries);
        // Transport failures retry on this same clock; never fan out per role.
        next_tick = interval.map(|interval| started + interval);
    }
}

async fn send(
    shared: &Shared,
    request: &InstanceHeartbeatRequest,
) -> Result<InstanceHeartbeatResponse, ServiceAuthError> {
    let body = serde_json::to_vec(request).map_err(|_| ServiceAuthError::InvalidResponse)?;
    if body.len() > MAX_INSTANCE_HEARTBEAT_BYTES {
        return Err(ServiceAuthError::InvalidResponse);
    }
    let url = shared
        .base
        .join(INSTANCE_HEARTBEAT_PATH)
        .map_err(|_| ServiceAuthError::InvalidUrl)?;
    let response = shared
        .client
        .post(url)
        .bearer_auth(shared.credential.expose())
        .header(
            "x-kish-app-id",
            utf8_percent_encode(shared.credential.application_id(), NON_ALPHANUMERIC).to_string(),
        )
        .header("content-type", "application/json")
        .body(body);
    let response: InstanceHeartbeatResponse = serde_json::from_slice(
        &response_bytes_limited(response, MAX_INSTANCE_HEARTBEAT_BYTES).await?,
    )
    .map_err(|_| ServiceAuthError::InvalidResponse)?;
    let expected = request
        .roles
        .iter()
        .map(|r| r.id)
        .collect::<std::collections::BTreeSet<_>>();
    let received = response
        .roles
        .iter()
        .map(|r| r.id)
        .collect::<std::collections::BTreeSet<_>>();
    if response.instance != request.instance
        || response.roles.len() != expected.len()
        || received != expected
        || response.roles.iter().any(|r| match r.status {
            200 => r.session.is_none(),
            403 | 404 | 409 | 422 | 429 | 503 => r.session.is_some(),
            _ => true,
        })
    {
        return Err(ServiceAuthError::InvalidResponse);
    }
    Ok(response)
}
