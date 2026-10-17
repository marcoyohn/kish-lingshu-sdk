//! Bounded, advisory application notifications. No delivery or replay guarantee.
use serde::{Deserialize, Serialize};

pub const MAX_NOTIFICATION_BYTES: usize = 4096;
pub const MAX_KIND_BYTES: usize = 128;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Notification {
    pub version: u8,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid or oversized application notification")]
pub struct InvalidNotification;

impl Notification {
    pub fn hint(kind: impl Into<String>) -> Self {
        Self {
            version: 1,
            kind: kind.into(),
            data: None,
        }
    }
    pub fn encode(&self) -> Result<Vec<u8>, InvalidNotification> {
        if self.version != 1 || !valid_kind(&self.kind) {
            return Err(InvalidNotification);
        }
        let encoded = serde_json::to_vec(self).map_err(|_| InvalidNotification)?;
        if encoded.len() > MAX_NOTIFICATION_BYTES {
            return Err(InvalidNotification);
        }
        Ok(encoded)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, InvalidNotification> {
        if bytes.len() > MAX_NOTIFICATION_BYTES {
            return Err(InvalidNotification);
        }
        let value: Self = serde_json::from_slice(bytes).map_err(|_| InvalidNotification)?;
        if value.version != 1 || !valid_kind(&value.kind) {
            return Err(InvalidNotification);
        }
        Ok(value)
    }
}

pub fn valid_kind(kind: &str) -> bool {
    !kind.is_empty()
        && kind.len() <= MAX_KIND_BYTES
        && kind
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn envelope_is_bounded_and_preserves_business_metadata() {
        let n = Notification {
            data: Some(serde_json::json!({"category":"auth","revision":7})),
            ..Notification::hint("cache.category.changed")
        };
        assert_eq!(Notification::decode(&n.encode().unwrap()).unwrap(), n);
        for raw in [
            br#"{"version":2,"kind":"a"}"#.as_slice(),
            br#"{"version":1,"kind":"*"}"#,
            br#"{"version":1,"kind":"a","extra":true}"#,
        ] {
            assert!(Notification::decode(raw).is_err());
        }
        assert!(Notification {
            data: Some("x".repeat(MAX_NOTIFICATION_BYTES).into()),
            ..n
        }
        .encode()
        .is_err());
    }
}
