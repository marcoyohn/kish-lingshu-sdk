use std::{collections::BTreeMap, fmt, str::FromStr, time::Duration};

use chrono::{DateTime, Utc};
use kish_lingshu_foundation_contract::{CorrelationId, MutationReceipt};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use thiserror::Error;

/// Platform-assigned identity of a durably accepted Event.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventId(u64);

impl EventId {
    pub fn new(value: u64) -> Result<Self, InvalidEventId> {
        if value == 0 {
            return Err(InvalidEventId);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for EventId {
    type Err = InvalidEventId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<u64>()
            .ok()
            .and_then(|value| Self::new(value).ok())
            .ok_or(InvalidEventId)
    }
}

impl TryFrom<u64> for EventId {
    type Error = InvalidEventId;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<EventId> for u64 {
    fn from(value: EventId) -> Self {
        value.get()
    }
}

impl Serialize for EventId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.get())
    }
}

impl<'de> Deserialize<'de> for EventId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(u64::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("Event identity must be a positive integer")]
pub struct InvalidEventId;

/// Stable routing metadata declared by an Event payload type.
pub trait EventPayload: Serialize {
    /// Stable source declaration key used by Consumer and Job references.
    const DEFINITION_KEY: &'static str = Self::EVENT_TYPE;
    const TOPIC: &'static str;
    const EVENT_TYPE: &'static str;
    const SCHEMA_VERSION: &'static str;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventRoute {
    pub topic: String,
    pub event_type: String,
    pub schema_version: String,
}

impl EventRoute {
    pub fn new(
        topic: impl Into<String>,
        event_type: impl Into<String>,
        schema_version: impl Into<String>,
    ) -> Result<Self, InvalidEventRoute> {
        let route = Self {
            topic: topic.into(),
            event_type: event_type.into(),
            schema_version: schema_version.into(),
        };
        route.validate()?;
        Ok(route)
    }

    pub fn for_payload<T: EventPayload>() -> Result<Self, InvalidEventRoute> {
        Self::new(T::TOPIC, T::EVENT_TYPE, T::SCHEMA_VERSION)
    }

    pub fn validate(&self) -> Result<(), InvalidEventRoute> {
        validate_route_component("topic", &self.topic)?;
        validate_route_component("event_type", &self.event_type)?;
        validate_route_component("schema_version", &self.schema_version)
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid Event route {field}: {reason}")]
pub struct InvalidEventRoute {
    field: &'static str,
    reason: &'static str,
}

fn validate_route_component(field: &'static str, value: &str) -> Result<(), InvalidEventRoute> {
    if value.is_empty() {
        return Err(InvalidEventRoute {
            field,
            reason: "value must not be empty",
        });
    }
    if value.len() > 255 {
        return Err(InvalidEventRoute {
            field,
            reason: "value is longer than 255 bytes",
        });
    }
    if !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
        return Err(InvalidEventRoute {
            field,
            reason: "value must contain visible ASCII characters only",
        });
    }
    Ok(())
}

/// Explicit JSON Event used when the payload type cannot implement [`EventPayload`].
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DynamicEvent {
    pub topic: String,
    pub event_type: String,
    pub schema_version: String,
    pub payload: Value,
}

impl DynamicEvent {
    pub fn new(route: EventRoute, payload: Value) -> Result<Self, InvalidEventRoute> {
        route.validate()?;
        Ok(Self {
            topic: route.topic,
            event_type: route.event_type,
            schema_version: route.schema_version,
            payload,
        })
    }

    pub fn route(&self) -> Result<EventRoute, InvalidEventRoute> {
        EventRoute::new(&self.topic, &self.event_type, &self.schema_version)
    }
}

/// Exactly one initial-delivery timing choice.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeliveryTime {
    #[default]
    Immediate,
    At {
        timestamp: DateTime<Utc>,
    },
    After {
        #[serde(with = "duration_milliseconds")]
        delay: Duration,
    },
}

impl DeliveryTime {
    pub const fn at(timestamp: DateTime<Utc>) -> Self {
        Self::At { timestamp }
    }

    pub const fn after(delay: Duration) -> Self {
        Self::After { delay }
    }
}

/// Transport-neutral Event publication command.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PublishEvent {
    pub topic: String,
    pub event_type: String,
    pub schema_version: String,
    pub payload: Value,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occurred_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<CorrelationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, Value>,
    #[serde(default)]
    pub delivery: DeliveryTime,
}

impl PublishEvent {
    pub fn typed<T: EventPayload>(
        source: impl Into<String>,
        payload: &T,
    ) -> Result<Self, PublishEventBuildError> {
        let route = EventRoute::for_payload::<T>()?;
        let payload = serde_json::to_value(payload)?;
        Ok(Self::dynamic(source, DynamicEvent::new(route, payload)?))
    }

    pub fn dynamic(source: impl Into<String>, event: DynamicEvent) -> Self {
        let DynamicEvent {
            topic,
            event_type,
            schema_version,
            payload,
        } = event;
        Self {
            topic,
            event_type,
            schema_version,
            payload,
            source: source.into(),
            occurred_at: None,
            subject: None,
            partition_key: None,
            correlation_id: None,
            causation_id: None,
            headers: BTreeMap::new(),
            delivery: DeliveryTime::Immediate,
        }
    }

    pub fn validate(&self) -> Result<(), PublishEventBuildError> {
        EventRoute::new(&self.topic, &self.event_type, &self.schema_version)?;
        if self.source.is_empty() || self.source.len() > 255 {
            return Err(PublishEventBuildError::InvalidSource);
        }
        if self.source.chars().any(char::is_control) {
            return Err(PublishEventBuildError::InvalidSource);
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum PublishEventBuildError {
    #[error(transparent)]
    InvalidRoute(#[from] InvalidEventRoute),
    #[error("Event source must be 1..=255 non-control characters")]
    InvalidSource,
    #[error("Event payload could not be encoded as JSON: {0}")]
    Payload(#[from] serde_json::Error),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventVisibility {
    Ready,
    Delayed,
}

/// Application-scoped Event view returned after publication or lookup.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EventRecord {
    pub event_id: EventId,
    pub topic: String,
    pub event_type: String,
    pub schema_version: String,
    pub payload: Value,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub published_at: DateTime<Utc>,
    pub visible_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<CorrelationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, Value>,
    pub visibility: EventVisibility,
}

/// Receipt for one durably accepted Event publication.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PublishReceipt {
    #[serde(flatten)]
    pub mutation: MutationReceipt,
    pub event_id: EventId,
    pub published_at: DateTime<Utc>,
    pub visible_at: DateTime<Utc>,
    pub visibility: EventVisibility,
}

mod duration_milliseconds {
    use std::time::Duration;

    use serde::{de, Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let milliseconds = u64::try_from(duration.as_millis())
            .map_err(|_| serde::ser::Error::custom("duration exceeds u64 milliseconds"))?;
        serializer.serialize_u64(milliseconds)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let milliseconds = u64::deserialize(deserializer)?;
        if milliseconds == 0 {
            return Err(de::Error::custom("duration must be positive"));
        }
        Ok(Duration::from_millis(milliseconds))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct OrderCreated {
        order_id: u64,
    }

    impl EventPayload for OrderCreated {
        const TOPIC: &'static str = "orders";
        const EVENT_TYPE: &'static str = "order.created";
        const SCHEMA_VERSION: &'static str = "1";
    }

    #[test]
    fn typed_and_dynamic_publication_share_one_contract() {
        let typed = PublishEvent::typed("checkout", &OrderCreated { order_id: 42 }).unwrap();
        let dynamic = PublishEvent::dynamic(
            "checkout",
            DynamicEvent::new(
                EventRoute::new("orders", "order.created", "1").unwrap(),
                serde_json::json!({"order_id": 42}),
            )
            .unwrap(),
        );
        assert_eq!(typed, dynamic);
    }

    #[test]
    fn delivery_time_has_one_tagged_wire_choice() {
        let encoded = serde_json::to_value(DeliveryTime::after(Duration::from_secs(5))).unwrap();
        assert_eq!(encoded, serde_json::json!({"type": "after", "delay": 5000}));
        assert!(serde_json::from_value::<DeliveryTime>(serde_json::json!({
            "type": "after",
            "delay": 0
        }))
        .is_err());
    }

    #[test]
    fn event_identity_rejects_zero() {
        assert!(EventId::new(0).is_err());
        assert!(serde_json::from_str::<EventId>("0").is_err());
    }
}
