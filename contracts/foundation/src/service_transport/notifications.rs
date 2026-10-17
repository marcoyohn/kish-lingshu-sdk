//! Ephemeral application wakeups. Never a durable Event or acknowledgement.
use super::{ExactRouteKey, RouteIdentity, TransportContractError};

pub const BRIDGE_TOPIC: &str = "application-notification.v2";

pub fn application_notification_key(
    deployment: &RouteIdentity,
    application: &RouteIdentity,
) -> Result<ExactRouteKey, TransportContractError> {
    ExactRouteKey::new(format!(
        "ls/v1/{}/apps/{}/notifications",
        deployment.key_segment(),
        application.key_segment()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn topic_is_exact_and_isolated_by_deployment_and_application() {
        let key = |d: &str, a: &str| {
            application_notification_key(
                &RouteIdentity::new(d).unwrap(),
                &RouteIdentity::new(a).unwrap(),
            )
            .unwrap()
        };
        assert_eq!(
            key("dev", "ykm").as_str(),
            "ls/v1/646576/apps/796b6d/notifications"
        );
        assert_ne!(key("dev", "ykm"), key("prod", "ykm"));
        assert_ne!(key("dev", "ykm"), key("dev", "other"));
        assert!(!key("dev", "ykm").as_str().contains('*'));
    }
}
