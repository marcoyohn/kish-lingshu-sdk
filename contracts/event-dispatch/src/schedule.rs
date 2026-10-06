use crate::{IntervalBasis, MisfirePolicy, OverlapPolicy};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Deployed Schedule trigger. Unlike source Job declarations, intervals use seconds.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScheduleTrigger {
    Cron {
        expression: String,
        timezone: String,
    },
    Interval {
        every_seconds: u64,
        anchor_at: DateTime<Utc>,
        #[serde(default)]
        basis: IntervalBasis,
    },
    Once {
        execute_at: DateTime<Utc>,
    },
}

/// Existing Schedule creation contract; name is unique within the authenticated application.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleDefinition {
    pub name: String,
    pub trigger: ScheduleTrigger,
    pub event: Value,
    pub misfire_policy: MisfirePolicy,
    pub misfire_batch_cap: u32,
    pub overlap_policy: OverlapPolicy,
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ScheduleReceipt {
    pub schedule_id: u64,
    pub app_id: String,
    pub name: String,
    pub trigger: ScheduleTrigger,
    pub event_template: Value,
    pub misfire_policy: MisfirePolicy,
    pub misfire_batch_cap: u32,
    pub overlap_policy: OverlapPolicy,
    pub state: String,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub revision: u64,
}

impl ScheduleReceipt {
    pub fn matches_definition(&self, app_id: &str, definition: &ScheduleDefinition) -> bool {
        self.schedule_id > 0
            && self.revision > 0
            && self.app_id == app_id
            && self.name == definition.name
            && self.trigger == definition.trigger
            && self.event_template == definition.event
            && self.misfire_policy == definition.misfire_policy
            && self.misfire_batch_cap == definition.misfire_batch_cap
            && self.overlap_policy == definition.overlap_policy
            && self.state
                == if definition.enabled {
                    "active"
                } else {
                    "paused"
                }
    }
}
