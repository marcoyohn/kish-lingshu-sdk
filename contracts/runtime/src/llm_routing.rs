//! Portable model-routing configuration. Credentials belong to platform records.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

pub const ROUTING_KEY: &str = "__lingshu_routing";
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    #[default]
    Priority,
    Weighted,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfig {
    pub version: u8,
    #[serde(default)]
    pub strategy: Strategy,
    #[serde(default = "attempts")]
    pub maximum_attempts: u32,
    #[serde(default = "timeout")]
    pub timeout_ms: u64,
    pub targets: Vec<Target>,
}
fn attempts() -> u32 {
    3
}
fn timeout() -> u64 {
    60_000
}
fn weight() -> u32 {
    1
}
fn cooldown() -> u64 {
    60_000
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub id: String,
    pub provider: String,
    pub protocol: String,
    pub model: String,
    #[serde(default)]
    pub priority: u32,
    #[serde(default = "weight")]
    pub weight: u32,
    pub context_window_tokens: u64,
    pub max_output_tokens: u64,
    #[serde(default)]
    pub capabilities: Capabilities,
    pub accounts: Vec<Account>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Capabilities {
    pub tools: bool,
    pub structured_output: bool,
    pub multimodal: bool,
    pub reasoning: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Account {
    pub platform_name: String,
    /// Accounts sharing an organization/project quota must declare the same group and limits.
    pub quota_group: Option<String>,
    #[serde(default = "weight")]
    pub weight: u32,
    #[serde(default = "cooldown")]
    pub quota_cooldown_ms: u64,
    pub limits: Limits,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub max_concurrency: u32,
    /// Zero means no configured local RPM/TPM cap; upstream restrictions still apply.
    #[serde(default)]
    pub rpm: u64,
    #[serde(default)]
    pub tpm: u64,
}
impl RoutingConfig {
    pub fn from_params(params: Option<&Value>) -> Result<Option<Self>, String> {
        params
            .and_then(|p| p.get(ROUTING_KEY))
            .map(|value| {
                if value.get("backend").is_some() {
                    return Err("Model routing backend is deployment-wide; remove backend from __lingshu_routing and use global llm_routing.backend".into());
                }
                let config: Self = serde_json::from_value(value.clone())
                    .map_err(|e| format!("Invalid model routing: {e}"))?;
                config.validate()?;
                Ok(config)
            })
            .transpose()
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1
            || !(1..=32).contains(&self.maximum_attempts)
            || !(100..=600_000).contains(&self.timeout_ms)
            || self.targets.is_empty()
            || self.targets.len() > 32
        {
            return Err(
                "Routing requires version=1, 1..32 attempts/targets and timeout_ms=100..600000"
                    .into(),
            );
        }
        let valid =
            |s: &str| !s.trim().is_empty() && s.len() <= 128 && !s.chars().any(char::is_control);
        let mut ids = HashSet::new();
        let mut groups = std::collections::HashMap::new();
        for t in &self.targets {
            if !valid(&t.id)
                || !ids.insert(&t.id)
                || !valid(&t.provider)
                || !valid(&t.model)
                || !["openai", "anthropic", "gemini", "cohere", "azure"]
                    .contains(&t.protocol.as_str())
                || !(1..=10000).contains(&t.weight)
                || t.context_window_tokens == 0
                || t.context_window_tokens > 100_000_000
                || t.max_output_tokens == 0
                || t.max_output_tokens > t.context_window_tokens
                || t.accounts.is_empty()
                || t.accounts.len() > 32
            {
                return Err(format!("Invalid routing target {}", t.id));
            }
            let mut accounts = HashSet::new();
            for a in &t.accounts {
                if !valid(&a.platform_name)
                    || !accounts.insert(&a.platform_name)
                    || !(1..=10000).contains(&a.weight)
                    || !(1..=100_000).contains(&a.limits.max_concurrency)
                    || a.limits.rpm > 1_000_000_000
                    || a.limits.tpm > 1_000_000_000_000
                    || !(1000..=86_400_000).contains(&a.quota_cooldown_ms)
                    || a.quota_group.as_ref().is_some_and(|g| !valid(g))
                {
                    return Err(format!("Invalid routing account {}", a.platform_name));
                }
                let key = (
                    &t.provider,
                    a.quota_group.as_ref().unwrap_or(&a.platform_name),
                );
                let limits = (a.limits.max_concurrency, a.limits.rpm, a.limits.tpm);
                if groups.insert(key, limits).is_some_and(|old| old != limits) {
                    return Err("Shared quota groups must use identical limits".into());
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingTrace {
    pub logical_call_id: String,
    pub alias: String,
    pub attempts: Vec<RouteAttempt>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteAttempt {
    pub attempt: u32,
    pub target: String,
    pub provider: String,
    pub platform_name: String,
    pub protocol: String,
    pub model: String,
    pub started_at_ms: i64,
    pub finished_at_ms: Option<i64>,
    pub outcome: String,
    pub retry_at_ms: Option<i64>,
    /// Missing means unknown, including interrupted/failed requests.
    pub usage: Option<Value>,
}
/// Terminal routing errors intentionally do not expose a retryable HTTP source.
#[derive(Debug, thiserror::Error)]
#[error("Model routing failed: {reason} (retry_at_ms={retry_at_ms:?})")]
pub struct RoutingFailure {
    pub reason: String,
    pub retry_at_ms: Option<i64>,
}
impl RoutingFailure {
    pub fn find<'a>(mut error: &'a (dyn std::error::Error + 'static)) -> Option<&'a Self> {
        loop {
            if let Some(v) = error.downcast_ref::<Self>() {
                return Some(v);
            }
            error = error.source()?;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_limits_and_rejects_credentials() {
        let mut v = serde_json::json!({"version":1,"targets":[{"id":"a","provider":"p","protocol":"openai","model":"m","context_window_tokens":8192,"max_output_tokens":1024,"accounts":[{"platform_name":"account-a","limits":{"max_concurrency":2}}]}]});
        let parse =
            |v: Value| RoutingConfig::from_params(Some(&serde_json::json!({ROUTING_KEY:v})));
        assert!(parse(v.clone()).unwrap().is_some());
        v["backend"] = "memory".into();
        assert!(parse(v.clone())
            .unwrap_err()
            .contains("global llm_routing.backend"));
        v.as_object_mut().unwrap().remove("backend");
        v["targets"][0]["accounts"][0]["api_key"] = "secret".into();
        assert!(parse(v.clone()).is_err());
        v["targets"][0]["accounts"][0]
            .as_object_mut()
            .unwrap()
            .remove("api_key");
        v["maximum_attempts"] = 0.into();
        assert!(parse(v).is_err());
    }
}
