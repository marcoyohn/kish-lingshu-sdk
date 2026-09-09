use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{ConsumerDefinition, EnvironmentBindingRequirement, EventDefinition, JobDefinition};

pub const EVENT_DISPATCH_MANIFEST_FORMAT: &str = "kish-lingshu.event-dispatch";
pub const EVENT_DISPATCH_MANIFEST_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerMetadata {
    pub package_name: String,
    pub package_version: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DigestAlgorithm {
    Sha256,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestDigest {
    pub algorithm: DigestAlgorithm,
    pub value: String,
}

impl ManifestDigest {
    pub fn sha256(bytes: &[u8]) -> Self {
        Self {
            algorithm: DigestAlgorithm::Sha256,
            value: format!("{:x}", Sha256::digest(bytes)),
        }
    }

    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.value.len() != 64
            || !self
                .value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ManifestError::InvalidDigest);
        }
        Ok(())
    }
}

/// Portable, source-owned Event Dispatch configuration artifact.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventDispatchManifestV1 {
    pub format: String,
    pub version: u32,
    pub producer: ProducerMetadata,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub environment_bindings: Vec<EnvironmentBindingRequirement>,
    #[serde(default)]
    pub events: Vec<EventDefinition>,
    #[serde(default)]
    pub consumers: Vec<ConsumerDefinition>,
    #[serde(default)]
    pub jobs: Vec<JobDefinition>,
    pub digest: ManifestDigest,
}

impl EventDispatchManifestV1 {
    pub fn new(
        producer: ProducerMetadata,
        environment_bindings: Vec<EnvironmentBindingRequirement>,
        events: Vec<EventDefinition>,
        consumers: Vec<ConsumerDefinition>,
        jobs: Vec<JobDefinition>,
    ) -> Result<Self, ManifestError> {
        let mut manifest = Self {
            format: EVENT_DISPATCH_MANIFEST_FORMAT.to_owned(),
            version: EVENT_DISPATCH_MANIFEST_VERSION,
            producer,
            environment_bindings,
            events,
            consumers,
            jobs,
            digest: ManifestDigest::sha256(&[]),
        };
        manifest.normalize();
        manifest.digest = manifest.compute_digest()?;
        Ok(manifest)
    }

    pub fn compute_digest(&self) -> Result<ManifestDigest, ManifestError> {
        let mut manifest = self.clone();
        manifest.normalize();
        let body = ManifestBody {
            format: &manifest.format,
            version: manifest.version,
            producer: &manifest.producer,
            environment_bindings: &manifest.environment_bindings,
            events: &manifest.events,
            consumers: &manifest.consumers,
            jobs: &manifest.jobs,
        };
        Ok(ManifestDigest::sha256(&serde_json::to_vec(&body)?))
    }

    pub fn verify_digest(&self) -> Result<(), ManifestError> {
        if self.format != EVENT_DISPATCH_MANIFEST_FORMAT
            || self.version != EVENT_DISPATCH_MANIFEST_VERSION
        {
            return Err(ManifestError::UnsupportedVersion {
                format: self.format.clone(),
                version: self.version,
            });
        }
        self.digest.validate()?;
        let actual = self.compute_digest()?;
        if actual != self.digest {
            return Err(ManifestError::DigestMismatch {
                expected: self.digest.clone(),
                actual,
            });
        }
        Ok(())
    }

    pub fn canonical_json(&self) -> Result<Vec<u8>, ManifestError> {
        self.verify_digest()?;
        let mut manifest = self.clone();
        manifest.normalize();
        Ok(serde_json::to_vec(&manifest)?)
    }

    fn normalize(&mut self) {
        self.environment_bindings
            .sort_by(|left, right| left.key.cmp(&right.key));
        self.events.sort_by(|left, right| left.key.cmp(&right.key));
        for event in &mut self.events {
            normalize_json(&mut event.payload_schema);
        }
        self.consumers
            .sort_by(|left, right| left.key.cmp(&right.key));
        for consumer in &mut self.consumers {
            consumer
                .selectors
                .sort_by(|left, right| left.event_key.cmp(&right.event_key));
        }
        self.jobs.sort_by(|left, right| left.key.cmp(&right.key));
        for job in &mut self.jobs {
            for value in job.event.headers.values_mut() {
                normalize_json(value);
            }
            normalize_json(&mut job.event.payload);
        }
    }
}

#[derive(Serialize)]
struct ManifestBody<'a> {
    format: &'a str,
    version: u32,
    producer: &'a ProducerMetadata,
    environment_bindings: &'a [EnvironmentBindingRequirement],
    events: &'a [EventDefinition],
    consumers: &'a [ConsumerDefinition],
    jobs: &'a [JobDefinition],
}

fn normalize_json(value: &mut Value) {
    match value {
        Value::Object(object) => {
            let mut entries = std::mem::take(object).into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            for (_, value) in &mut entries {
                normalize_json(value);
            }
            object.extend(entries);
        }
        Value::Array(values) => {
            for value in values {
                normalize_json(value);
            }
        }
        _ => {}
    }
}

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("unsupported Event Dispatch manifest {format} version {version}")]
    UnsupportedVersion { format: String, version: u32 },
    #[error("manifest digest must be 64 lower-case hexadecimal characters")]
    InvalidDigest,
    #[error("manifest digest mismatch")]
    DigestMismatch {
        expected: ManifestDigest,
        actual: ManifestDigest,
    },
    #[error("manifest could not be encoded: {0}")]
    Encode(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConsumptionOrder, TopicDefaults};

    fn event(key: &str, schema: Value) -> EventDefinition {
        EventDefinition {
            key: key.into(),
            topic: "orders".into(),
            event_type: key.into(),
            schema_version: "1".into(),
            description: None,
            payload_schema: schema,
            topic_defaults: TopicDefaults {
                name: "Orders".into(),
                description: None,
                partition_count: 8,
                consumption_order: ConsumptionOrder::PartitionOrdered,
            },
        }
    }

    #[test]
    fn equivalent_declaration_order_and_json_keys_have_same_bytes_and_digest() {
        let producer = ProducerMetadata {
            package_name: "orders-app".into(),
            package_version: "1.2.3".into(),
        };
        let first = EventDispatchManifestV1::new(
            producer.clone(),
            vec![],
            vec![
                event("z", serde_json::json!({"type": "object", "title": "Z"})),
                event("a", serde_json::json!({"title": "A", "type": "object"})),
            ],
            vec![],
            vec![],
        )
        .unwrap();
        let second = EventDispatchManifestV1::new(
            producer,
            vec![],
            vec![
                event("a", serde_json::json!({"type": "object", "title": "A"})),
                event("z", serde_json::json!({"title": "Z", "type": "object"})),
            ],
            vec![],
            vec![],
        )
        .unwrap();

        assert_eq!(first.digest, second.digest);
        assert_eq!(
            first.canonical_json().unwrap(),
            second.canonical_json().unwrap()
        );
    }

    #[test]
    fn digest_detects_manifest_tampering() {
        let mut manifest = EventDispatchManifestV1::new(
            ProducerMetadata {
                package_name: "orders-app".into(),
                package_version: "1.0.0".into(),
            },
            vec![],
            vec![event(
                "order.created",
                serde_json::json!({"type": "object"}),
            )],
            vec![],
            vec![],
        )
        .unwrap();
        manifest.events[0].schema_version = "2".into();
        assert!(matches!(
            manifest.verify_digest(),
            Err(ManifestError::DigestMismatch { .. })
        ));
    }
}
