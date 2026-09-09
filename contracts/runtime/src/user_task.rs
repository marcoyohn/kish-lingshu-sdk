use std::{fmt, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use thiserror::Error;

use crate::{
    EventCursor, IdempotencyKey, MutationReceipt, RootWorkflowInstanceId, WorkflowId,
    WorkflowInstanceId,
};

macro_rules! positive_identity {
    ($name:ident, $label:literal) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u64);

        impl $name {
            pub fn new(value: u64) -> Result<Self, InvalidUserTaskIdentity> {
                if value == 0 {
                    return Err(InvalidUserTaskIdentity { kind: $label });
                }
                Ok(Self(value))
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl TryFrom<u64> for $name {
            type Error = InvalidUserTaskIdentity;

            fn try_from(value: u64) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for u64 {
            fn from(value: $name) -> Self {
                value.get()
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_u64(self.get())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(u64::deserialize(deserializer)?).map_err(de::Error::custom)
            }
        }
    };
}

macro_rules! extensible_string_enum {
    (
        pub enum $name:ident {
            $($variant:ident => $value:literal),+ $(,)?
        }
    ) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub enum $name {
            $($variant,)+
            Unknown(String),
        }

        impl $name {
            pub fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $value,)+
                    Self::Unknown(value) => value,
                }
            }

            pub fn parse(value: impl Into<String>) -> Result<Self, InvalidUserTaskValue> {
                let value = value.into();
                validate_extensible_value(stringify!($name), &value)?;
                Ok(match value.as_str() {
                    $($value => Self::$variant,)+
                    _ => Self::Unknown(value),
                })
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = InvalidUserTaskValue;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::parse(String::deserialize(deserializer)?).map_err(de::Error::custom)
            }
        }
    };
}

positive_identity!(UserTaskId, "User Task identity");
positive_identity!(UserTaskRevision, "User Task revision");

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("{kind} must be a positive integer")]
pub struct InvalidUserTaskIdentity {
    kind: &'static str,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid {kind}: value must be 1..=128 non-control characters")]
pub struct InvalidUserTaskValue {
    kind: &'static str,
}

fn validate_extensible_value(kind: &'static str, value: &str) -> Result<(), InvalidUserTaskValue> {
    if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(InvalidUserTaskValue { kind });
    }
    Ok(())
}

extensible_string_enum! {
    pub enum UserTaskMode {
        Todo => "todo",
        Read => "read",
    }
}

extensible_string_enum! {
    pub enum UserTaskState {
        Pending => "pending",
        PendingUnclaimed => "pending_unclaimed",
        PendingClaimed => "pending_claimed",
        PendingRead => "pending_read",
        Completing => "completing",
        CompletionFailed => "completion_failed",
        Completed => "completed",
    }
}

extensible_string_enum! {
    pub enum UserTaskParticipantRole {
        TodoCandidate => "todo_candidate",
        Reader => "reader",
    }
}

extensible_string_enum! {
    pub enum UserTaskParticipantState {
        PendingUnclaimed => "pending_unclaimed",
        PendingClaimed => "pending_claimed",
        Completed => "completed",
        Missed => "missed",
        Unread => "unread",
        Read => "read",
    }
}

extensible_string_enum! {
    pub enum UserTaskPageSource {
        InlineHtml => "inline_html",
        TemplateReference => "template_reference",
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskParticipant {
    pub user_id: String,
    pub role: UserTaskParticipantRole,
    pub state: UserTaskParticipantState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acted_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskInitiator {
    pub user_id: String,
    pub display_name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskPage {
    pub source: UserTaskPageSource,
    pub value: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskPayloads {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub draft: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_schema: Option<Value>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskPermissions {
    #[serde(default)]
    pub can_claim: bool,
    #[serde(default)]
    pub can_mark_read: bool,
    #[serde(default)]
    pub can_save_draft: bool,
    #[serde(default)]
    pub can_submit: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskWorkflowReference {
    pub workflow_id: WorkflowId,
    pub workflow_instance_id: WorkflowInstanceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_workflow_instance_id: Option<RootWorkflowInstanceId>,
    pub workflow_name: String,
    pub flow_node_id: String,
    pub flow_node_name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskTimestamps {
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submitted_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskSummary {
    pub id: UserTaskId,
    pub revision: UserTaskRevision,
    pub application_id: String,
    pub title: String,
    pub state: UserTaskState,
    pub mode: UserTaskMode,
    pub task_type: String,
    pub workflow: UserTaskWorkflowReference,
    pub initiator: UserTaskInitiator,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_user_participant_state: Option<UserTaskParticipantState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_by: Option<String>,
    pub timestamps: UserTaskTimestamps,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTask {
    #[serde(flatten)]
    pub summary: UserTaskSummary,
    pub page: UserTaskPage,
    #[serde(default)]
    pub payloads: UserTaskPayloads,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub participants: Vec<UserTaskParticipant>,
    #[serde(default)]
    pub permissions: UserTaskPermissions,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion: Option<crate::UserTaskCompletionDetail>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PageRequest {
    pub page: u64,
    pub page_size: u64,
}

impl PageRequest {
    pub fn new(page: u64, page_size: u64) -> Result<Self, InvalidPageRequest> {
        if page == 0 {
            return Err(InvalidPageRequest::Page);
        }
        if page_size == 0 || page_size > 200 {
            return Err(InvalidPageRequest::PageSize);
        }
        Ok(Self { page, page_size })
    }
}

impl Default for PageRequest {
    fn default() -> Self {
        Self {
            page: 1,
            page_size: 20,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum InvalidPageRequest {
    #[error("page must be positive")]
    Page,
    #[error("page_size must be between 1 and 200")]
    PageSize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub total: u64,
    pub page: u64,
    pub page_size: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserTaskTimeRange {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_from: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_before: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationTaskQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<UserTaskState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<UserTaskMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participant_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participant_state: Option<UserTaskParticipantState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_by: Option<String>,
    #[serde(default)]
    pub created: UserTaskTimeRange,
    #[serde(default)]
    pub pagination: PageRequest,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CurrentUserTaskQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participant_state: Option<UserTaskParticipantState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<UserTaskMode>,
    #[serde(default)]
    pub created: UserTaskTimeRange,
    #[serde(default)]
    pub pagination: PageRequest,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationTaskSummary {
    pub pending_todo: u64,
    pub pending_read: u64,
    pub completed: u64,
    pub total: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TaskActionPrecondition {
    pub task_id: UserTaskId,
    pub expected_revision: UserTaskRevision,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimTask {
    #[serde(flatten)]
    pub precondition: TaskActionPrecondition,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MarkTaskRead {
    #[serde(flatten)]
    pub precondition: TaskActionPrecondition,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SaveTaskDraft {
    #[serde(flatten)]
    pub precondition: TaskActionPrecondition,
    pub draft: Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitTask {
    #[serde(flatten)]
    pub precondition: TaskActionPrecondition,
    pub submission: Value,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskAction {
    Claim,
    MarkRead,
    SaveDraft,
    Submit,
}

/// One current-participant action accepted by the User Task runtime port.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "action", content = "request", rename_all = "snake_case")]
pub enum UserTaskAction {
    Claim(ClaimTask),
    MarkRead(MarkTaskRead),
    SaveDraft(SaveTaskDraft),
    Submit(SubmitTask),
}

impl UserTaskAction {
    pub fn kind(&self) -> TaskAction {
        match self {
            Self::Claim(_) => TaskAction::Claim,
            Self::MarkRead(_) => TaskAction::MarkRead,
            Self::SaveDraft(_) => TaskAction::SaveDraft,
            Self::Submit(_) => TaskAction::Submit,
        }
    }

    pub fn precondition(&self) -> &TaskActionPrecondition {
        match self {
            Self::Claim(request) => &request.precondition,
            Self::MarkRead(request) => &request.precondition,
            Self::SaveDraft(request) => &request.precondition,
            Self::Submit(request) => &request.precondition,
        }
    }
}

/// Durable acceptance receipt for one participant action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TaskActionReceipt {
    #[serde(flatten)]
    pub mutation: MutationReceipt,
    pub task_id: UserTaskId,
    pub action: TaskAction,
    pub revision: UserTaskRevision,
    pub state: UserTaskState,
    pub workflow_id: WorkflowId,
    pub workflow_instance_id: WorkflowInstanceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation_cursor: Option<EventCursor>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_state_values_round_trip_without_loss() {
        let state: UserTaskState = serde_json::from_str("\"awaiting_delegate\"").unwrap();
        assert_eq!(
            state,
            UserTaskState::Unknown("awaiting_delegate".to_owned())
        );
        assert_eq!(
            serde_json::to_string(&state).unwrap(),
            "\"awaiting_delegate\""
        );
    }

    #[test]
    fn completion_states_are_known_and_stable() {
        for (wire, expected) in [
            ("completing", UserTaskState::Completing),
            ("completion_failed", UserTaskState::CompletionFailed),
        ] {
            let state: UserTaskState =
                serde_json::from_value(Value::String(wire.to_owned())).unwrap();
            assert_eq!(state, expected);
            assert_eq!(
                serde_json::to_value(&state).unwrap(),
                Value::String(wire.to_owned())
            );
        }
    }

    #[test]
    fn action_contract_has_no_execution_or_suspension_coordinates() {
        let action = SubmitTask {
            precondition: TaskActionPrecondition {
                task_id: UserTaskId::new(42).unwrap(),
                expected_revision: UserTaskRevision::new(7).unwrap(),
                idempotency_key: IdempotencyKey::new("task/42/submit").unwrap(),
            },
            submission: serde_json::json!({"approved": true}),
        };
        let encoded = serde_json::to_string(&action).unwrap();
        assert!(!encoded.contains("execution"));
        assert!(!encoded.contains("suspension"));
        assert!(encoded.contains("expected_revision"));
    }

    #[test]
    fn identities_and_pagination_reject_invalid_bounds() {
        assert!(UserTaskId::new(0).is_err());
        assert!(UserTaskRevision::new(0).is_err());
        assert!(PageRequest::new(0, 20).is_err());
        assert!(PageRequest::new(1, 201).is_err());
    }
}
