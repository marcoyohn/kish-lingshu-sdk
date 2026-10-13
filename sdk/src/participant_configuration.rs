//! Bounded read-only configuration providers. Authorization precedes every query.
pub use kish_lingshu_runtime_contract::participant_configuration::*;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

#[async_trait::async_trait]
pub trait ParticipantConfigurationProvider: Send + Sync {
    fn capability(&self) -> ParticipantConfigurationCapability;
    /// Validate provider-issued context ownership, application/editor binding,
    /// expiry/revocation, scope and operation permissions, including preview.
    /// An authenticated service channel alone MUST NOT satisfy this check.
    async fn authorize(
        &self,
        context: &ConfigurationQueryContext,
        query: &ConfigurationQuery,
    ) -> Result<(), ConfigurationError>;
    async fn query(
        &self,
        context: &ConfigurationQueryContext,
        query: &ConfigurationQuery,
    ) -> Result<ConfigurationResponse, ConfigurationError>;
}

#[derive(Clone, Default)]
pub struct ParticipantConfigurationRegistry {
    providers: BTreeMap<String, RegisteredProvider>,
}
#[derive(Clone)]
struct RegisteredProvider {
    capability: ParticipantConfigurationCapability,
    provider: Arc<dyn ParticipantConfigurationProvider>,
}
impl ParticipantConfigurationRegistry {
    pub fn register(
        &mut self,
        provider: Arc<dyn ParticipantConfigurationProvider>,
    ) -> Result<(), ConfigurationError> {
        let capability = provider.capability();
        capability.validate()?;
        if self.providers.len() >= 64 || self.providers.contains_key(&capability.key) {
            return Err(ConfigurationError::InvalidRequest);
        }
        self.providers.insert(
            capability.key.clone(),
            RegisteredProvider {
                capability,
                provider,
            },
        );
        Ok(())
    }
    pub fn capabilities(&self) -> Vec<ParticipantConfigurationCapability> {
        self.providers
            .values()
            .map(|p| p.capability.clone())
            .collect()
    }
    pub async fn query(
        &self,
        context: &ConfigurationQueryContext,
        query: &ConfigurationQuery,
    ) -> ConfigurationResponse {
        match tokio::time::timeout(Duration::from_secs(5), self.query_inner(context, query)).await {
            Ok(Ok(response)) => response,
            Ok(Err(code)) => ConfigurationResponse::Error { code },
            Err(_) => ConfigurationResponse::Error {
                code: ConfigurationError::Unavailable,
            },
        }
    }
    async fn query_inner(
        &self,
        context: &ConfigurationQueryContext,
        query: &ConfigurationQuery,
    ) -> Result<ConfigurationResponse, ConfigurationError> {
        context.validate()?;
        query.validate()?;
        let provider = self
            .providers
            .get(&query.capability_key)
            .ok_or(ConfigurationError::NotFound)?;
        let capability = &provider.capability;
        if capability.key != query.capability_key
            || !capability.compatibility.contains(&query.compatibility)
            || (matches!(query.operation, ConfigurationOperation::Preview { .. })
                && !capability.supports_preview)
        {
            return Err(ConfigurationError::Incompatible);
        }
        provider.provider.authorize(context, query).await?;
        let response = provider.provider.query(context, query).await?;
        response.validate_for(query, capability)?;
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kish_lingshu_runtime_contract::human_task::{HumanTaskClaimPolicy, HumanTaskKind};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Provider {
        calls: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl ParticipantConfigurationProvider for Provider {
        fn capability(&self) -> ParticipantConfigurationCapability {
            ParticipantConfigurationCapability {
                key: "rules".into(),
                label: "Rules".into(),
                compatibility: vec![mode()],
                supports_preview: false,
                management_site_keys: vec![],
            }
        }
        async fn authorize(
            &self,
            ctx: &ConfigurationQueryContext,
            query: &ConfigurationQuery,
        ) -> Result<(), ConfigurationError> {
            if ctx.application_id != "app"
                || ctx.editor_subject != "editor"
                || query.context_ref != "valid-grant"
            {
                return Err(ConfigurationError::Forbidden);
            }
            Ok(())
        }
        async fn query(
            &self,
            _: &ConfigurationQueryContext,
            _: &ConfigurationQuery,
        ) -> Result<ConfigurationResponse, ConfigurationError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ConfigurationResponse::Options {
                items: vec![],
                next_cursor: None,
            })
        }
    }
    fn mode() -> ParticipantCompatibility {
        ParticipantCompatibility {
            task_type: "review".into(),
            node_kind: HumanTaskKind::Todo,
            claim_policy: HumanTaskClaimPolicy::Required,
        }
    }
    #[tokio::test]
    async fn rejected_context_never_invokes_query_and_registration_is_unique() {
        let p = Arc::new(Provider {
            calls: AtomicUsize::new(0),
        });
        let mut registry = ParticipantConfigurationRegistry::default();
        registry.register(p.clone()).unwrap();
        assert!(registry.register(p.clone()).is_err());
        let context = ConfigurationQueryContext {
            application_id: "app".into(),
            provider_key: "business".into(),
            editor_subject: "editor".into(),
        };
        let mut q = ConfigurationQuery {
            protocol: PARTICIPANT_CONFIGURATION_PROTOCOL.into(),
            capability_key: "rules".into(),
            compatibility: mode(),
            context_ref: "wrong".into(),
            operation: ConfigurationOperation::List {
                search: "".into(),
                cursor: None,
                limit: 10,
            },
        };
        assert_eq!(
            registry.query(&context, &q).await,
            ConfigurationResponse::Error {
                code: ConfigurationError::Forbidden
            }
        );
        assert_eq!(p.calls.load(Ordering::SeqCst), 0);
        q.context_ref = "valid-grant".into();
        assert!(matches!(
            registry.query(&context, &q).await,
            ConfigurationResponse::Options { .. }
        ));
        assert_eq!(p.calls.load(Ordering::SeqCst), 1);
        let other = ConfigurationQueryContext {
            application_id: "other".into(),
            ..context
        };
        assert_eq!(
            registry.query(&other, &q).await,
            ConfigurationResponse::Error {
                code: ConfigurationError::Forbidden
            }
        );
        assert_eq!(p.calls.load(Ordering::SeqCst), 1);
    }
}
