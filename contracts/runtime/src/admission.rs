//! Host-load admission configuration and continuation probe contracts.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionAdmissionConfig {
    pub enabled: bool,
    pub cpu_high_percent: u8,
    pub cpu_high_duration_secs: u64,
    pub cpu_recover_percent: u8,
    pub memory_high_percent: u8,
    pub memory_recover_percent: u8,
    pub sample_interval_ms: u64,
    pub stale_after_ms: u64,
    pub maximum_wait_ms: u64,
    pub prefer_cgroup_cpu: bool,
    pub psi: LoadPsiConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoadPsiConfig {
    pub enabled: bool,
    pub cpu_high_percent: u8,
    pub cpu_recover_percent: u8,
    pub memory_high_percent: u8,
    pub memory_recover_percent: u8,
}
impl Default for LoadPsiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cpu_high_percent: 20,
            cpu_recover_percent: 10,
            memory_high_percent: 10,
            memory_recover_percent: 5,
        }
    }
}
impl Default for ExecutionAdmissionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cpu_high_percent: 90,
            cpu_high_duration_secs: 30,
            cpu_recover_percent: 75,
            memory_high_percent: 95,
            memory_recover_percent: 80,
            sample_interval_ms: 1000,
            stale_after_ms: 5000,
            maximum_wait_ms: 60000,
            prefer_cgroup_cpu: true,
            psi: LoadPsiConfig::default(),
        }
    }
}
impl ExecutionAdmissionConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.cpu_high_percent > 100
            || self.cpu_high_duration_secs > 86400
            || self.memory_high_percent > 100
            || self.cpu_recover_percent >= self.cpu_high_percent
            || self.memory_recover_percent >= self.memory_high_percent
            || !(250..=60000).contains(&self.sample_interval_ms)
            || self.stale_after_ms < self.sample_interval_ms.saturating_mul(2)
            || self.stale_after_ms > 300000
            || self.maximum_wait_ms == 0
            || self.maximum_wait_ms > 86400000
            || self.psi.cpu_high_percent > 100
            || self.psi.memory_high_percent > 100
            || self.psi.cpu_recover_percent >= self.psi.cpu_high_percent
            || self.psi.memory_recover_percent >= self.psi.memory_high_percent
        {
            return Err("Invalid workflow_load thresholds, sampling interval or deadline".into());
        }
        Ok(())
    }
    pub fn enabled(&self) -> bool {
        self.enabled
    }
}
pub fn default_maximum_receipts() -> u32 {
    1_048_576
}

pub enum LoadDecision {
    Ready,
    Waiting {
        next_check_at_ms: i64,
        reason: String,
    },
}
/// A cached local pressure check, independent of workflow identity or distributed quotas.
pub trait LoadAdmission: Send + Sync {
    fn check(&self) -> LoadDecision;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn load_defaults_and_validation_reject_removed_quota_settings() {
        let config: ExecutionAdmissionConfig = serde_json::from_str("{}").unwrap();
        config.validate().unwrap();
        assert!(config.enabled);
        assert_eq!(config.sample_interval_ms, 1000);
        assert_eq!(config.cpu_high_duration_secs, 30);
        assert_eq!(config.memory_high_percent, 95);
        assert!(config.prefer_cgroup_cpu && config.psi.enabled);
        let disabled: ExecutionAdmissionConfig =
            serde_json::from_str(r#"{"psi":{"enabled":false}}"#).unwrap();
        assert!(!disabled.psi.enabled);
        disabled.validate().unwrap();
        assert!(
            serde_json::from_str::<ExecutionAdmissionConfig>(r#"{"psi":{"enabld":false}}"#)
                .is_err()
        );
        for value in [
            serde_json::json!({"active_roots":8}),
            serde_json::json!({"models":{"*":8}}),
            serde_json::json!({"resources":{"x":2}}),
        ] {
            assert!(serde_json::from_value::<ExecutionAdmissionConfig>(value).is_err());
        }
        for value in [
            serde_json::json!({"cpu_high_percent":75,"cpu_recover_percent":75}),
            serde_json::json!({"sample_interval_ms":100}),
            serde_json::json!({"stale_after_ms":1000}),
            serde_json::json!({"memory_high_percent":101}),
            serde_json::json!({"cpu_high_duration_secs":86401}),
            serde_json::json!({"psi":{"cpu_high_percent":10,"cpu_recover_percent":10}}),
            serde_json::json!({"psi":{"memory_high_percent":101}}),
        ] {
            assert!(serde_json::from_value::<ExecutionAdmissionConfig>(value)
                .unwrap()
                .validate()
                .is_err());
        }
    }

    #[test]
    fn load_duration_and_memory_overrides_preserve_partial_configurations() {
        for seconds in [0, 12, 86400] {
            let config: ExecutionAdmissionConfig = serde_json::from_value(serde_json::json!({
                "cpu_high_duration_secs": seconds,
                "memory_high_percent": 98
            }))
            .unwrap();
            config.validate().unwrap();
            assert_eq!(config.cpu_high_duration_secs, seconds);
            assert_eq!(config.memory_high_percent, 98);
            assert_eq!(config.cpu_recover_percent, 75);
            assert_eq!(config.memory_recover_percent, 80);
            assert_eq!(
                serde_json::to_value(&config).unwrap()["cpu_high_duration_secs"],
                seconds
            );
        }
        assert!(
            serde_json::from_value::<ExecutionAdmissionConfig>(serde_json::json!({
                "cpu_high_duration_secs": -1
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ExecutionAdmissionConfig>(serde_json::json!({
                "cpu_high_duration_sec": 30
            }))
            .is_err()
        );
    }
}
