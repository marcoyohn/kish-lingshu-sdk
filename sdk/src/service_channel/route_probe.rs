//! Reply core for an independently authorized role's exact route. No declaration,
//! readiness, role renewal or business execution is performed by this module.
use super::{ChannelSessionError, ServiceChannelSessions};
use kish_lingshu_foundation_contract::{
    service_auth::{ChannelMessageSigner, ClientChannelIdentity},
    service_transport::{
        enrollment::validate_route_binding,
        probe::{RouteProbeChallenge, MAX_ROUTE_PROBE_BYTES, ROUTE_PROBE_TIMEOUT_MS},
        ExactRouteKey, MessageKind, ProtocolVersion, ServiceEndpoint, TransportEnvelope,
    },
};
use tokio::time::Instant;

pub(super) fn validate_challenge(
    endpoint: &ServiceEndpoint,
    target: &ExactRouteKey,
    bytes: &[u8],
    now: i64,
) -> Result<TransportEnvelope, ChannelSessionError> {
    if bytes.len() > MAX_ROUTE_PROBE_BYTES {
        return Err(ChannelSessionError::InvalidResponse);
    }
    endpoint
        .validate()
        .map_err(|_| ChannelSessionError::InvalidConfig)?;
    let ServiceEndpoint::Zenoh {
        route,
        route_revision,
        lanes,
        ..
    } = endpoint
    else {
        return Err(ChannelSessionError::InvalidConfig);
    };
    let request = TransportEnvelope::decode(bytes, target, &route.application_id, now)
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
    if request.kind != MessageKind::BindLane
        || request.deadline_unix_ms > now.saturating_add(ROUTE_PROBE_TIMEOUT_MS)
    {
        return Err(ChannelSessionError::InvalidResponse);
    }
    let challenge: RouteProbeChallenge = serde_json::from_str(request.payload.get())
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
    if challenge.route_revision != *route_revision
        || !lanes.contains(&challenge.lane)
        || route
            .invoke_key(&challenge.lane)
            .map_err(|_| ChannelSessionError::InvalidResponse)?
            != *target
    {
        return Err(ChannelSessionError::InvalidResponse);
    }
    Ok(request)
}

pub(super) fn sign_reply(
    subject: &ClientChannelIdentity,
    signer: &ChannelMessageSigner,
    request: TransportEnvelope,
    now: i64,
) -> Result<Vec<u8>, ChannelSessionError> {
    let parent = request.trace_parent.clone();
    super::trace::scope_sync(parent.as_deref(), || {
        let proof = signer
            .sign_message(
                subject,
                MessageKind::BindLane,
                &request.target,
                &request.request_id,
                request.payload.get().as_bytes(),
                now,
                request.deadline_unix_ms,
            )
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let response = TransportEnvelope {
            protocol_version: ProtocolVersion::V1,
            kind: MessageKind::BindLane,
            request_id: request.request_id,
            application_id: subject.application_id.clone(),
            target: request.target,
            deadline_unix_ms: request.deadline_unix_ms,
            proof,
            trace_parent: super::trace::current_trace_parent(),
            payload: request.payload,
        }
        .encode(now)
        .map_err(|_| ChannelSessionError::InvalidResponse)?;
        if response.len() > MAX_ROUTE_PROBE_BYTES {
            return Err(ChannelSessionError::InvalidResponse);
        }
        Ok(response)
    })
}

impl ServiceChannelSessions {
    /// Produce a CSR-signed reply for a route issued to this pool's instance.
    /// The role adapter must supply its installed endpoint and check its own
    /// live role before calling this core. This does not install a Queryable,
    /// grant execution permission, or report the route as ready locally.
    pub fn route_probe_reply(
        &self,
        endpoint: &ServiceEndpoint,
        role_generation: &str,
        role_expires_at_ms: i64,
        target: &ExactRouteKey,
        bytes: &[u8],
    ) -> Result<Vec<u8>, ChannelSessionError> {
        self.identity
            .connection
            .ensure_open()
            .map_err(|_| ChannelSessionError::Closed)?;
        if self.closed.borrow().is_some() || Instant::now() >= *self.authority.borrow() {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        if self.task.as_ref().is_none_or(|task| task.is_finished()) {
            return Err(ChannelSessionError::CleanupFailed);
        }
        let initial = &self.identity.credential.response;
        let ServiceEndpoint::Zenoh { route, lanes, .. } = endpoint else {
            return Err(ChannelSessionError::InvalidConfig);
        };
        if lanes
            .iter()
            .any(|lane| usize::from(lane.lane.index()) >= self.lane_count())
        {
            return Err(ChannelSessionError::InvalidConfig);
        }
        validate_route_binding(
            route,
            initial.deployment.as_str(),
            initial.application_id.as_str(),
            &initial.instance,
            role_generation,
        )
        .map_err(|_| ChannelSessionError::InvalidConfig)?;
        let now = chrono::Utc::now().timestamp_millis();
        if role_expires_at_ms <= now {
            return Err(ChannelSessionError::AuthorityExpired);
        }
        let request = validate_challenge(endpoint, target, bytes, now)?;
        if request.deadline_unix_ms > role_expires_at_ms {
            return Err(ChannelSessionError::InvalidResponse);
        }
        self.identity
            .connection
            .verify_channel_message(&initial.transport_trust, &request)
            .map_err(|_| ChannelSessionError::InvalidResponse)?;
        let subject = ClientChannelIdentity {
            application_id: initial.application_id.clone(),
            instance_id: route.instance_id.clone(),
            base_generation: route.base_generation.clone(),
            certificate_identity: initial.certificate.certificate_identity.clone(),
        };
        sign_reply(
            &subject,
            &self.identity.credential.signer,
            request,
            chrono::Utc::now().timestamp_millis(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kish_lingshu_foundation_contract::{
        service_auth::{verify_client_transport_message, verify_transport_message, ServiceSigner},
        service_transport::{DataLaneId, InstanceRoute, LaneIdentity, RouteIdentity},
    };
    use rcgen::{KeyPair, PKCS_ED25519};
    fn id(value: &str) -> RouteIdentity {
        RouteIdentity::new(value).unwrap()
    }
    fn endpoint() -> ServiceEndpoint {
        ServiceEndpoint::Zenoh {
            protocol_version: ProtocolVersion::V1,
            route: InstanceRoute {
                deployment: id("dev"),
                application_id: id("app"),
                instance_id: id("sdk"),
                base_generation: id("base"),
                role_generation: id("role"),
            },
            route_revision: 1,
            lanes: vec![LaneIdentity {
                lane: DataLaneId::new(0).unwrap(),
                epoch: id("epoch"),
            }],
        }
    }
    fn request(
        platform: &ServiceSigner,
        endpoint: &ServiceEndpoint,
        now: i64,
    ) -> TransportEnvelope {
        let ServiceEndpoint::Zenoh {
            route,
            route_revision,
            lanes,
            ..
        } = endpoint
        else {
            unreachable!()
        };
        let payload = serde_json::value::to_raw_value(&RouteProbeChallenge {
            challenge: id("challenge"),
            lane: lanes[0].clone(),
            route_revision: *route_revision,
        })
        .unwrap();
        let target = route.invoke_key(&lanes[0]).unwrap();
        let request_id = id("request");
        let proof = platform
            .sign_transport_message(
                "app",
                MessageKind::BindLane,
                &target,
                &request_id,
                payload.get().as_bytes(),
                now,
                now + 5_000,
            )
            .unwrap();
        TransportEnvelope {
            protocol_version: ProtocolVersion::V1,
            kind: MessageKind::BindLane,
            request_id,
            application_id: id("app"),
            target,
            deadline_unix_ms: now + 5_000,
            proof,
            trace_parent: None,
            payload,
        }
    }

    #[test]
    fn reply_binds_both_signing_directions_and_never_executes() {
        let now = chrono::Utc::now().timestamp_millis();
        let platform = ServiceSigner::new(&[42; 32]).unwrap();
        let endpoint = endpoint();
        let mut req = request(&platform, &endpoint, now);
        req.trace_parent = Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".into());
        let trust = platform.transport_trust("app", now / 1000).unwrap();
        let decoded =
            validate_challenge(&endpoint, &req.target, &req.encode(now).unwrap(), now).unwrap();
        verify_transport_message(
            &trust,
            &decoded.proof,
            "app",
            decoded.kind,
            &decoded.target,
            &decoded.request_id,
            decoded.payload.get().as_bytes(),
            now,
        )
        .unwrap();
        let key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
        let signer = ChannelMessageSigner::from_pkcs8(&key.serialize_der()).unwrap();
        let subject = ClientChannelIdentity {
            application_id: id("app"),
            instance_id: id("sdk"),
            base_generation: id("base"),
            certificate_identity: id("certificate"),
        };
        let bytes = sign_reply(&subject, &signer, decoded, now).unwrap();
        let response =
            TransportEnvelope::decode(&bytes, &req.target, &req.application_id, now).unwrap();
        assert_eq!(response.payload.get(), req.payload.get());
        use kish_lingshu_foundation_contract::trace::TraceParent;
        let parent = TraceParent::parse(req.trace_parent.as_deref().unwrap()).unwrap();
        let child = TraceParent::parse(response.trace_parent.as_deref().unwrap()).unwrap();
        assert_eq!(child.trace_id(), parent.trace_id());
        assert_eq!(child.flags(), parent.flags());
        assert_ne!(child.span_id(), parent.span_id());
        assert!(super::super::trace::current_trace_parent().is_none());
        let claims = verify_client_transport_message(
            signer.public_key(),
            &subject,
            &response.proof,
            response.kind,
            &response.target,
            &response.request_id,
            response.payload.get().as_bytes(),
            now,
        )
        .unwrap();
        assert_eq!(claims.deadline_unix_ms, req.deadline_unix_ms);
        assert!(verify_transport_message(
            &trust,
            &response.proof,
            "app",
            response.kind,
            &response.target,
            &response.request_id,
            response.payload.get().as_bytes(),
            now
        )
        .is_err());
    }

    #[test]
    fn foreign_epochs_revisions_families_deadlines_and_oversize_are_rejected() {
        let now = chrono::Utc::now().timestamp_millis();
        let platform = ServiceSigner::new(&[42; 32]).unwrap();
        let endpoint = endpoint();
        let req = request(&platform, &endpoint, now);
        for index in 0..6 {
            let mut changed = req.clone();
            match index {
                0 => changed.kind = MessageKind::InvokeCall,
                1 => changed.deadline_unix_ms = now + 5_001,
                2 => {
                    changed.payload = serde_json::value::to_raw_value(&RouteProbeChallenge {
                        challenge: id("c"),
                        lane: LaneIdentity {
                            lane: DataLaneId::new(0).unwrap(),
                            epoch: id("other"),
                        },
                        route_revision: 1,
                    })
                    .unwrap()
                }
                3 => {
                    changed.payload = serde_json::value::to_raw_value(&RouteProbeChallenge {
                        challenge: id("c"),
                        lane: LaneIdentity {
                            lane: DataLaneId::new(0).unwrap(),
                            epoch: id("epoch"),
                        },
                        route_revision: 2,
                    })
                    .unwrap()
                }
                4 => {
                    changed.payload = serde_json::value::RawValue::from_string(
                        "{\"challenge\":\"c\",\"lane\":{},\"route_revision\":1,\"execute\":true}"
                            .into(),
                    )
                    .unwrap()
                }
                _ => changed.application_id = id("foreign"),
            }
            assert!(
                validate_challenge(&endpoint, &req.target, &changed.encode(now).unwrap(), now)
                    .is_err(),
                "case {index}"
            );
        }
        assert!(validate_challenge(
            &endpoint,
            &req.target,
            &req.encode(now).unwrap(),
            now + 5_000
        )
        .is_err());
        assert!(validate_challenge(
            &endpoint,
            &req.target,
            &vec![0; MAX_ROUTE_PROBE_BYTES + 1],
            now
        )
        .is_err());
    }
}
