use sha2::{Digest, Sha256};

use super::EventPublicationJournalError;

/// Stable producer identity. Replicas sharing one journal use the same scope.
/// This is an application/service identity, never a credential or process ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventPublicationScope {
    application_id: String,
    publisher_id: String,
}

impl EventPublicationScope {
    pub fn new(
        application_id: impl Into<String>,
        publisher_id: impl Into<String>,
    ) -> Result<Self, EventPublicationJournalError> {
        let application_id = application_id.into();
        let publisher_id = publisher_id.into();
        for (field, value) in [
            ("application_id", &application_id),
            ("publisher_id", &publisher_id),
        ] {
            if value.is_empty()
                || value.len() > 255
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b':'))
            {
                return Err(EventPublicationJournalError::new(
                    "invalid_publication_scope",
                    format!("{field} must be 1..255 ASCII machine characters"),
                ));
            }
        }
        Ok(Self {
            application_id,
            publisher_id,
        })
    }

    pub fn application_id(&self) -> &str {
        &self.application_id
    }
    pub fn publisher_id(&self) -> &str {
        &self.publisher_id
    }

    pub fn ensure_matches(&self, other: &Self) -> Result<(), EventPublicationJournalError> {
        if self != other {
            return Err(EventPublicationJournalError::new(
                "publication_scope_mismatch",
                "publication owner does not match the bound journal",
            ));
        }
        Ok(())
    }

    /// Collision-safe wire identity, stable across replicas and restarts.
    /// Raw EventDispatch::publish does not apply this namespace.
    pub fn publication_key(&self, caller_key: &str) -> String {
        let mut hash = Sha256::new();
        for part in [self.application_id(), self.publisher_id(), caller_key] {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
        format!("sdk-publication:v1:{:x}", hash.finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_rejects_reserved_legacy_owner_and_unbounded_machine_ids() {
        for value in ["", " app", "app ", "app/other", "应用"] {
            assert!(EventPublicationScope::new(value, "publisher").is_err());
            assert!(EventPublicationScope::new("app", value).is_err());
        }
        assert!(EventPublicationScope::new("x".repeat(256), "publisher").is_err());
    }

    #[test]
    fn wire_key_is_stable_and_separates_all_identity_components() {
        let scope = EventPublicationScope::new("app", "publisher").unwrap();
        let key = scope.publication_key("key");
        assert_eq!(key, scope.clone().publication_key("key"));
        assert_ne!(key, scope.publication_key("other-key"));
        assert_ne!(
            key,
            EventPublicationScope::new("app", "other-publisher")
                .unwrap()
                .publication_key("key")
        );
        assert_ne!(
            key,
            EventPublicationScope::new("other-app", "publisher")
                .unwrap()
                .publication_key("key")
        );
        assert_ne!(
            EventPublicationScope::new("a:b", "c")
                .unwrap()
                .publication_key("d"),
            EventPublicationScope::new("a", "b:c")
                .unwrap()
                .publication_key("d")
        );
    }
}
