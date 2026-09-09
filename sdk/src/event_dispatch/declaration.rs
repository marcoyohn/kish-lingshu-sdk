use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use kish_lingshu_event_dispatch_contract::{
    ConsumerDefinition, DeliveryMode, EventDefinition, EventDispatchManifestV1, JobDefinition,
    JobTrigger, ProducerMetadata,
};
use thiserror::Error;

#[doc(hidden)]
pub struct EventDefinitionDescriptor {
    pub diagnostic_name: &'static str,
    pub build: fn() -> Result<EventDefinition, String>,
}

inventory::collect!(EventDefinitionDescriptor);

#[doc(hidden)]
pub struct JobDefinitionDescriptor {
    pub diagnostic_name: &'static str,
    pub build: fn() -> Result<JobDefinition, String>,
}

inventory::collect!(JobDefinitionDescriptor);

/// Validated Event, Consumer, and Job declarations linked into an application.
#[derive(Clone, Debug, PartialEq)]
pub struct SourceCatalog {
    events: Vec<EventDefinition>,
    consumers: Vec<ConsumerDefinition>,
    jobs: Vec<JobDefinition>,
}

impl SourceCatalog {
    /// Collects linked source declarations without constructing a Client or Handler.
    pub fn collect() -> Result<Self, SourceCatalogError> {
        let events = inventory::iter::<EventDefinitionDescriptor>
            .into_iter()
            .map(|descriptor| {
                (descriptor.build)().map_err(|message| SourceCatalogError::Descriptor {
                    declaration: descriptor.diagnostic_name.to_string(),
                    message,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let jobs = inventory::iter::<JobDefinitionDescriptor>
            .into_iter()
            .map(|descriptor| {
                (descriptor.build)().map_err(|message| SourceCatalogError::Descriptor {
                    declaration: descriptor.diagnostic_name.to_string(),
                    message,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        #[cfg(feature = "event-consumer")]
        let consumers = super::consumer::linked_consumer_definitions()?;
        #[cfg(not(feature = "event-consumer"))]
        let consumers = Vec::new();

        Self::from_parts(events, consumers, jobs)
    }

    pub fn events(&self) -> &[EventDefinition] {
        &self.events
    }

    pub fn consumers(&self) -> &[ConsumerDefinition] {
        &self.consumers
    }

    pub fn jobs(&self) -> &[JobDefinition] {
        &self.jobs
    }

    /// Produces the portable canonical manifest for one application package.
    pub fn manifest(
        &self,
        producer: ProducerMetadata,
    ) -> Result<EventDispatchManifestV1, SourceCatalogError> {
        EventDispatchManifestV1::new(
            producer,
            Vec::new(),
            self.events.clone(),
            self.consumers.clone(),
            self.jobs.clone(),
        )
        .map_err(SourceCatalogError::Manifest)
    }

    /// Exports byte-stable canonical JSON with no environment identity or secret.
    pub fn export_json(&self, producer: ProducerMetadata) -> Result<Vec<u8>, SourceCatalogError> {
        self.manifest(producer)?
            .canonical_json()
            .map_err(SourceCatalogError::Manifest)
    }

    fn from_parts(
        mut events: Vec<EventDefinition>,
        mut consumers: Vec<ConsumerDefinition>,
        mut jobs: Vec<JobDefinition>,
    ) -> Result<Self, SourceCatalogError> {
        events.sort_by(|left, right| left.key.cmp(&right.key));
        consumers.sort_by(|left, right| left.key.cmp(&right.key));
        jobs.sort_by(|left, right| left.key.cmp(&right.key));
        validate_events(&events)?;
        validate_consumers(&events, &consumers)?;
        validate_jobs(&events, &jobs)?;
        Ok(Self {
            events,
            consumers,
            jobs,
        })
    }
}

#[derive(Debug, Error)]
pub enum SourceCatalogError {
    #[error("source declaration {declaration} could not be built: {message}")]
    Descriptor {
        declaration: String,
        message: String,
    },
    #[error("duplicate {kind} declaration key {key}")]
    DuplicateKey { kind: &'static str, key: String },
    #[error("invalid {kind} declaration {key}: {message}")]
    Invalid {
        kind: &'static str,
        key: String,
        message: String,
    },
    #[error("Topic {topic} has conflicting defaults in Event declarations")]
    ConflictingTopic { topic: String },
    #[error("{kind} declaration {key} references undeclared Event {event_key}")]
    UndeclaredEvent {
        kind: &'static str,
        key: String,
        event_key: String,
    },
    #[error(transparent)]
    Manifest(#[from] kish_lingshu_event_dispatch_contract::ManifestError),
}

fn validate_events(events: &[EventDefinition]) -> Result<(), SourceCatalogError> {
    let mut keys = BTreeSet::new();
    let mut routes = BTreeSet::new();
    let mut topics = BTreeMap::new();
    for event in events {
        validate_key("Event", &event.key)?;
        if !keys.insert(event.key.clone()) {
            return Err(SourceCatalogError::DuplicateKey {
                kind: "Event",
                key: event.key.clone(),
            });
        }
        kish_lingshu_event_dispatch_contract::EventRoute::new(
            &event.topic,
            &event.event_type,
            &event.schema_version,
        )
        .map_err(|error| invalid("Event", &event.key, error))?;
        if !routes.insert((event.topic.clone(), event.event_type.clone())) {
            return Err(invalid(
                "Event",
                &event.key,
                "Topic and Event type route is declared more than once",
            ));
        }
        if event.topic_defaults.name.trim().is_empty()
            || event.topic_defaults.partition_count == 0
            || event.topic_defaults.name.chars().any(char::is_control)
        {
            return Err(invalid(
                "Event",
                &event.key,
                "Topic defaults require a name and positive partition count",
            ));
        }
        if let Some(existing) = topics.insert(event.topic.clone(), &event.topic_defaults) {
            if existing != &event.topic_defaults {
                return Err(SourceCatalogError::ConflictingTopic {
                    topic: event.topic.clone(),
                });
            }
        }
        jsonschema::meta::validate(&event.payload_schema)
            .map_err(|error| invalid("Event", &event.key, error))?;
    }
    Ok(())
}

fn validate_consumers(
    events: &[EventDefinition],
    consumers: &[ConsumerDefinition],
) -> Result<(), SourceCatalogError> {
    let events = events
        .iter()
        .map(|event| (event.key.as_str(), event))
        .collect::<BTreeMap<_, _>>();
    let mut keys = BTreeSet::new();
    for consumer in consumers {
        validate_key("Consumer", &consumer.key)?;
        validate_key("Consumer Group", &consumer.group_key)?;
        if !keys.insert(consumer.key.clone()) {
            return Err(SourceCatalogError::DuplicateKey {
                kind: "Consumer",
                key: consumer.key.clone(),
            });
        }
        if consumer.delivery_mode != DeliveryMode::Sync {
            return Err(invalid(
                "Consumer",
                &consumer.key,
                "only synchronous delivery is supported",
            ));
        }
        if consumer.maximum_concurrency == 0 || consumer.selectors.is_empty() {
            return Err(invalid(
                "Consumer",
                &consumer.key,
                "maximum concurrency and selectors must be non-zero",
            ));
        }
        validate_policy(consumer)?;
        let mut selector_keys = BTreeSet::new();
        for selector in &consumer.selectors {
            if !selector_keys.insert(&selector.event_key) {
                return Err(invalid(
                    "Consumer",
                    &consumer.key,
                    "an Event selector is declared more than once",
                ));
            }
            let event = events.get(selector.event_key.as_str()).ok_or_else(|| {
                SourceCatalogError::UndeclaredEvent {
                    kind: "Consumer",
                    key: consumer.key.clone(),
                    event_key: selector.event_key.clone(),
                }
            })?;
            if event.topic != selector.topic || event.event_type != selector.event_type {
                return Err(invalid(
                    "Consumer",
                    &consumer.key,
                    "selector route disagrees with its Event declaration",
                ));
            }
        }
    }
    Ok(())
}

fn validate_policy(consumer: &ConsumerDefinition) -> Result<(), SourceCatalogError> {
    let retry = consumer.policy.retry;
    let throttle = consumer.policy.throttle;
    let timeout = consumer.policy.timeout;
    if retry.maximum_failure_attempts == 0
        || retry.initial_delay_milliseconds == 0
        || retry.maximum_delay_milliseconds < retry.initial_delay_milliseconds
        || !retry.multiplier.is_finite()
        || retry.multiplier < 1.0
        || !retry.jitter_ratio.is_finite()
        || !(0.0..=1.0).contains(&retry.jitter_ratio)
        || throttle.minimum_cooldown_milliseconds == 0
        || throttle.maximum_cooldown_milliseconds < throttle.minimum_cooldown_milliseconds
        || throttle.maximum_throttle_duration_milliseconds < throttle.maximum_cooldown_milliseconds
        || throttle.half_open_probe_limit == 0
        || timeout.invocation_milliseconds == 0
        || timeout.completion_milliseconds < timeout.invocation_milliseconds
        || timeout.maximum_completion_milliseconds < timeout.completion_milliseconds
    {
        return Err(invalid(
            "Consumer",
            &consumer.key,
            "policy defaults contain invalid limits",
        ));
    }
    if let Some(rate_limit) = consumer.policy.rate_limit {
        if rate_limit.requests == 0 || rate_limit.interval_milliseconds == 0 {
            return Err(invalid(
                "Consumer",
                &consumer.key,
                "rate limit values must be positive",
            ));
        }
    }
    Ok(())
}

fn validate_jobs(
    events: &[EventDefinition],
    jobs: &[JobDefinition],
) -> Result<(), SourceCatalogError> {
    let events = events
        .iter()
        .map(|event| (event.key.as_str(), event))
        .collect::<BTreeMap<_, _>>();
    let mut keys = BTreeSet::new();
    for job in jobs {
        validate_key("Job", &job.key)?;
        if !keys.insert(job.key.clone()) {
            return Err(SourceCatalogError::DuplicateKey {
                kind: "Job",
                key: job.key.clone(),
            });
        }
        if job.misfire_batch_cap == 0 {
            return Err(invalid(
                "Job",
                &job.key,
                "misfire batch cap must be positive",
            ));
        }
        match &job.trigger {
            JobTrigger::Cron {
                expression,
                timezone,
            } => {
                cron::Schedule::from_str(expression)
                    .map_err(|error| invalid("Job", &job.key, error))?;
                chrono_tz::Tz::from_str(timezone)
                    .map_err(|error| invalid("Job", &job.key, error))?;
            }
            JobTrigger::Interval {
                every_milliseconds, ..
            } if *every_milliseconds == 0 => {
                return Err(invalid("Job", &job.key, "interval must be positive"));
            }
            JobTrigger::Interval { .. } | JobTrigger::Once { .. } => {}
        }
        let event = events.get(job.event.event_key.as_str()).ok_or_else(|| {
            SourceCatalogError::UndeclaredEvent {
                kind: "Job",
                key: job.key.clone(),
                event_key: job.event.event_key.clone(),
            }
        })?;
        let validator = jsonschema::validator_for(&event.payload_schema)
            .map_err(|error| invalid("Event", &event.key, error))?;
        validator
            .validate(&job.event.payload)
            .map_err(|error| invalid("Job", &job.key, error))?;
        if job.event.source.trim().is_empty() || job.event.source.chars().any(char::is_control) {
            return Err(invalid(
                "Job",
                &job.key,
                "Event source must be non-empty and contain no control characters",
            ));
        }
    }
    Ok(())
}

fn validate_key(kind: &'static str, key: &str) -> Result<(), SourceCatalogError> {
    if key.is_empty()
        || key.len() > 255
        || !key
            .bytes()
            .all(|byte| (0x21..=0x7e).contains(&byte) && !byte.is_ascii_whitespace())
    {
        return Err(invalid(
            kind,
            key,
            "key must be 1..=255 visible ASCII bytes without whitespace",
        ));
    }
    Ok(())
}

fn invalid(kind: &'static str, key: &str, error: impl std::fmt::Display) -> SourceCatalogError {
    SourceCatalogError::Invalid {
        kind,
        key: key.to_string(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "event-consumer")]
    use kish_lingshu_event_dispatch_contract::EventSelector;
    use kish_lingshu_event_dispatch_contract::{
        ConsumptionOrder, IntervalBasis, JobEventTemplate, MisfirePolicy, OverlapPolicy,
        TopicDefaults,
    };
    use serde_json::json;

    use super::*;

    fn event(key: &str, topic: &str) -> EventDefinition {
        EventDefinition {
            key: key.to_string(),
            topic: topic.to_string(),
            event_type: key.to_string(),
            schema_version: "1".to_string(),
            description: None,
            payload_schema: json!({
                "type": "object",
                "properties": {"order_id": {"type": "integer"}},
                "required": ["order_id"]
            }),
            topic_defaults: TopicDefaults {
                name: "Orders".to_string(),
                description: None,
                partition_count: 8,
                consumption_order: ConsumptionOrder::PartitionOrdered,
            },
        }
    }

    fn job(event_key: &str) -> JobDefinition {
        JobDefinition {
            key: "orders.expire".to_string(),
            trigger: JobTrigger::Interval {
                every_milliseconds: 60_000,
                anchor_at: "2026-09-06T00:00:00Z".parse().unwrap(),
                basis: IntervalBasis::TriggeredAt,
            },
            event: JobEventTemplate {
                event_key: event_key.to_string(),
                source: "orders.expire".to_string(),
                subject: None,
                partition_key: None,
                headers: Default::default(),
                payload: json!({"order_id": 42}),
            },
            misfire_policy: MisfirePolicy::Skip,
            misfire_batch_cap: 1,
            overlap_policy: OverlapPolicy::Serialize,
        }
    }

    #[test]
    fn catalog_rejects_duplicate_keys_conflicting_topics_and_missing_job_events() {
        assert!(matches!(
            SourceCatalog::from_parts(
                vec![
                    event("order.created", "orders"),
                    event("order.created", "orders")
                ],
                vec![],
                vec![]
            ),
            Err(SourceCatalogError::DuplicateKey { kind: "Event", .. })
        ));

        let mut conflicting = event("order.updated", "orders");
        conflicting.topic_defaults.partition_count = 2;
        assert!(matches!(
            SourceCatalog::from_parts(
                vec![event("order.created", "orders"), conflicting],
                vec![],
                vec![]
            ),
            Err(SourceCatalogError::ConflictingTopic { .. })
        ));

        assert!(matches!(
            SourceCatalog::from_parts(
                vec![event("order.created", "orders")],
                vec![],
                vec![job("order.expired")]
            ),
            Err(SourceCatalogError::UndeclaredEvent { kind: "Job", .. })
        ));
    }

    #[test]
    fn catalog_rejects_invalid_schema_and_job_template() {
        let mut invalid_schema = event("order.created", "orders");
        invalid_schema.payload_schema = json!({"type": 7});
        assert!(matches!(
            SourceCatalog::from_parts(vec![invalid_schema], vec![], vec![]),
            Err(SourceCatalogError::Invalid { kind: "Event", .. })
        ));

        let mut invalid_template = job("order.created");
        invalid_template.event.payload = json!({"order_id": "wrong"});
        assert!(matches!(
            SourceCatalog::from_parts(
                vec![event("order.created", "orders")],
                vec![],
                vec![invalid_template]
            ),
            Err(SourceCatalogError::Invalid { kind: "Job", .. })
        ));
    }

    #[cfg(feature = "event-consumer")]
    #[test]
    fn catalog_rejects_duplicate_missing_and_disagreeing_consumer_references() {
        let consumer = super::super::consumer::default_consumer_definition(
            "orders.on-created",
            "order-workers",
            4,
            EventSelector {
                event_key: "order.created".to_string(),
                topic: "orders".to_string(),
                event_type: "order.created".to_string(),
            },
        );
        assert!(matches!(
            SourceCatalog::from_parts(
                vec![event("order.created", "orders")],
                vec![consumer.clone(), consumer.clone()],
                vec![]
            ),
            Err(SourceCatalogError::DuplicateKey {
                kind: "Consumer",
                ..
            })
        ));

        let mut missing = consumer.clone();
        missing.selectors[0].event_key = "order.missing".to_string();
        assert!(matches!(
            SourceCatalog::from_parts(
                vec![event("order.created", "orders")],
                vec![missing],
                vec![]
            ),
            Err(SourceCatalogError::UndeclaredEvent {
                kind: "Consumer",
                ..
            })
        ));

        let mut disagreeing = consumer;
        disagreeing.selectors[0].topic = "other".to_string();
        assert!(matches!(
            SourceCatalog::from_parts(
                vec![event("order.created", "orders")],
                vec![disagreeing],
                vec![]
            ),
            Err(SourceCatalogError::Invalid {
                kind: "Consumer",
                ..
            })
        ));
    }
}
