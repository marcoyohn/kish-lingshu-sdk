use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const TOOL_PERMISSION_MATCHER_VERSION: u32 = 2;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecutionSide {
    #[default]
    Backend,
    Client,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolConfirmationMode {
    #[default]
    None,
    Required,
    Policy,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceApprovalMode {
    #[default]
    FollowAgent,
    ReviewChanges,
    AutoApprove,
}

impl WorkspaceApprovalMode {
    pub const ALL: [Self; 3] = [Self::FollowAgent, Self::ReviewChanges, Self::AutoApprove];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FollowAgent => "follow_agent",
            Self::ReviewChanges => "review_changes",
            Self::AutoApprove => "auto_approve",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::FollowAgent => "Follow Agent",
            Self::ReviewChanges => "Review changes",
            Self::AutoApprove => "Auto approve",
        }
    }

    pub const fn description(self) -> &'static str {
        match self {
            Self::FollowAgent => "Use each tool's Agent-configured approval default",
            Self::ReviewChanges => "Review mutations and external side effects",
            Self::AutoApprove => "Auto-approve policy calls inside the existing Sandbox",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error(
    "unknown Workspace approval mode '{value}'; expected follow_agent, review_changes, or auto_approve"
)]
pub struct ParseWorkspaceApprovalModeError {
    value: String,
}

impl std::str::FromStr for WorkspaceApprovalMode {
    type Err = ParseWorkspaceApprovalModeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "follow_agent" => Ok(Self::FollowAgent),
            "review_changes" => Ok(Self::ReviewChanges),
            "auto_approve" => Ok(Self::AutoApprove),
            _ => Err(ParseWorkspaceApprovalModeError {
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolOperationEffect {
    ReadOnly,
    Mutation,
    ExternalSideEffect,
    #[default]
    Unknown,
}

impl ToolOperationEffect {
    pub const fn requires_review(self) -> bool {
        !matches!(self, Self::ReadOnly)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRuleEffect {
    Allow,
    Ask,
    Deny,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolAuthorizationDisposition {
    Allow,
    RequireApproval,
    Deny,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolRuleScope {
    Global,
    App { app_id: String },
    Agent { app_id: String, agent_id: String },
    Workflow { workflow_instance_id: u64 },
    Workspace { workspace_id: u64 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolRuleMatcher {
    BackendTool {
        tool_name: String,
    },
    Workspace {
        tool_name: String,
        mount: String,
        path_pattern: String,
    },
    ShellArgvPrefix {
        argv_prefix: Vec<String>,
        cwd_mount: Option<String>,
        sandbox_profile: String,
        network: String,
        #[serde(default)]
        mount_modes: Vec<String>,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct ToolAuthorizationRequestRef<'a> {
    pub execution_side: ToolExecutionSide,
    pub app_id: &'a str,
    pub agent_id: &'a str,
    pub workflow_instance_id: u64,
    pub workspace_id: Option<u64>,
    pub matcher: &'a ToolRuleMatcher,
    pub operation_effect: ToolOperationEffect,
}

#[derive(Clone, Copy, Debug)]
pub struct ToolAuthorizationRuleRef<'a, Id> {
    pub id: &'a Id,
    pub effect: ToolRuleEffect,
    pub execution_side: ToolExecutionSide,
    /// `None` means the enclosing permission document already supplies scope.
    pub scope: Option<&'a ToolRuleScope>,
    pub matcher_version: u32,
    pub matcher: &'a ToolRuleMatcher,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolAuthorizationEvaluation<Id> {
    pub disposition: ToolAuthorizationDisposition,
    pub source: &'static str,
    pub matched_rule_ids: Vec<Id>,
}

pub fn evaluate_tool_authorization<'a, Id>(
    request: ToolAuthorizationRequestRef<'a>,
    confirmation_mode: ToolConfirmationMode,
    approval_mode: WorkspaceApprovalMode,
    rules: impl IntoIterator<Item = ToolAuthorizationRuleRef<'a, Id>>,
) -> ToolAuthorizationEvaluation<Id>
where
    Id: Clone + Ord + 'a,
{
    let mut matched = rules
        .into_iter()
        .filter(|rule| rule.execution_side == request.execution_side)
        .filter(|rule| rule.matcher_version == TOOL_PERMISSION_MATCHER_VERSION)
        .filter(|rule| {
            rule.scope
                .map(|scope| scope_matches(scope, request))
                .unwrap_or(true)
        })
        .filter(|rule| matcher_matches(rule.matcher, request.matcher))
        .collect::<Vec<_>>();
    matched.sort_by(|left, right| left.id.cmp(right.id));

    for (effect, disposition, source) in [
        (
            ToolRuleEffect::Deny,
            ToolAuthorizationDisposition::Deny,
            "deny_rule",
        ),
        (
            ToolRuleEffect::Ask,
            ToolAuthorizationDisposition::RequireApproval,
            "ask_rule",
        ),
        (
            ToolRuleEffect::Allow,
            ToolAuthorizationDisposition::Allow,
            "allow_rule",
        ),
    ] {
        if effect == ToolRuleEffect::Allow && confirmation_mode == ToolConfirmationMode::Required {
            break;
        }
        let matched_rule_ids = matched
            .iter()
            .filter(|rule| rule.effect == effect)
            .map(|rule| rule.id.clone())
            .collect::<Vec<_>>();
        if !matched_rule_ids.is_empty() {
            return ToolAuthorizationEvaluation {
                disposition,
                source,
                matched_rule_ids,
            };
        }
    }

    match confirmation_mode {
        ToolConfirmationMode::None => {
            let requires_review = approval_mode == WorkspaceApprovalMode::ReviewChanges
                && request.operation_effect.requires_review();
            ToolAuthorizationEvaluation {
                disposition: if requires_review {
                    ToolAuthorizationDisposition::RequireApproval
                } else {
                    ToolAuthorizationDisposition::Allow
                },
                source: if requires_review {
                    "approval_mode_review_changes"
                } else {
                    "confirmation_none"
                },
                matched_rule_ids: Vec::new(),
            }
        }
        ToolConfirmationMode::Required => ToolAuthorizationEvaluation {
            disposition: ToolAuthorizationDisposition::RequireApproval,
            source: "confirmation_required",
            matched_rule_ids: Vec::new(),
        },
        ToolConfirmationMode::Policy => ToolAuthorizationEvaluation {
            disposition: if approval_mode == WorkspaceApprovalMode::AutoApprove {
                ToolAuthorizationDisposition::Allow
            } else {
                ToolAuthorizationDisposition::RequireApproval
            },
            source: match approval_mode {
                WorkspaceApprovalMode::FollowAgent => "approval_mode_follow_agent",
                WorkspaceApprovalMode::ReviewChanges => "approval_mode_review_changes",
                WorkspaceApprovalMode::AutoApprove => "approval_mode_auto_approve",
            },
            matched_rule_ids: Vec::new(),
        },
    }
}

fn scope_matches(scope: &ToolRuleScope, request: ToolAuthorizationRequestRef<'_>) -> bool {
    match scope {
        ToolRuleScope::Global => true,
        ToolRuleScope::App { app_id } => app_id == request.app_id,
        ToolRuleScope::Agent { app_id, agent_id } => {
            app_id == request.app_id && agent_id == request.agent_id
        }
        ToolRuleScope::Workflow {
            workflow_instance_id,
        } => *workflow_instance_id == request.workflow_instance_id,
        ToolRuleScope::Workspace { workspace_id } => Some(*workspace_id) == request.workspace_id,
    }
}

pub fn matcher_matches(rule: &ToolRuleMatcher, request: &ToolRuleMatcher) -> bool {
    match (rule, request) {
        (
            ToolRuleMatcher::BackendTool { tool_name: rule },
            ToolRuleMatcher::BackendTool { tool_name: request },
        ) => rule == request,
        (
            ToolRuleMatcher::Workspace {
                tool_name: rule_tool,
                mount: rule_mount,
                path_pattern: rule_path,
            },
            ToolRuleMatcher::Workspace {
                tool_name: request_tool,
                mount: request_mount,
                path_pattern: request_path,
            },
        ) => {
            rule_tool == request_tool
                && rule_mount == request_mount
                && workspace_path_matches(rule_path, request_path)
        }
        (
            ToolRuleMatcher::ShellArgvPrefix {
                argv_prefix,
                cwd_mount,
                sandbox_profile,
                network,
                mount_modes,
            },
            ToolRuleMatcher::ShellArgvPrefix {
                argv_prefix: request_argv,
                cwd_mount: request_mount,
                sandbox_profile: request_profile,
                network: request_network,
                mount_modes: request_mount_modes,
            },
        ) => {
            !argv_prefix.is_empty()
                && request_argv.starts_with(argv_prefix)
                && cwd_mount == request_mount
                && sandbox_profile == request_profile
                && network == request_network
                && mount_modes == request_mount_modes
        }
        _ => false,
    }
}

fn workspace_path_matches(rule: &str, request: &str) -> bool {
    if rule == "**" || rule == request {
        return true;
    }
    let Some(prefix) = rule.strip_suffix("/**") else {
        return false;
    };
    request == prefix
        || request
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationChoice {
    AllowOnce,
    AllowRemembered,
    Reject,
}

impl AuthorizationChoice {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AllowOnce => "allow_once",
            Self::AllowRemembered => "allow_remembered",
            Self::Reject => "reject",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthorizationChallengeDetails {
    Generic {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
    },
    Patch {
        summary: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        diff: Option<String>,
    },
    Shell {
        argv: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
    },
    Sandbox {
        provider: String,
        #[serde(default)]
        filesystem: Value,
    },
    Compatibility {
        value: Value,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AuthorizationChallenge {
    pub request_id: String,
    pub action: String,
    pub choices: Vec<AuthorizationChoice>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<AuthorizationChallengeDetails>,
}

impl AuthorizationChallenge {
    pub fn accepts(&self, choice: AuthorizationChoice) -> bool {
        self.choices.contains(&choice)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ToolExecutionOutcome {
    NotExecuted,
    Succeeded,
    Failed { message: String },
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request<'a>(matcher: &'a ToolRuleMatcher) -> ToolAuthorizationRequestRef<'a> {
        ToolAuthorizationRequestRef {
            execution_side: ToolExecutionSide::Client,
            app_id: "app-1",
            agent_id: "agent-1",
            workflow_instance_id: 11,
            workspace_id: Some(12),
            matcher,
            operation_effect: ToolOperationEffect::Mutation,
        }
    }

    fn rule<'a>(
        id: &'a u64,
        effect: ToolRuleEffect,
        matcher: &'a ToolRuleMatcher,
    ) -> ToolAuthorizationRuleRef<'a, u64> {
        ToolAuthorizationRuleRef {
            id,
            effect,
            execution_side: ToolExecutionSide::Client,
            scope: None,
            matcher_version: TOOL_PERMISSION_MATCHER_VERSION,
            matcher,
        }
    }

    #[test]
    fn deny_and_ask_precede_allow_independent_of_input_order() {
        let matcher = ToolRuleMatcher::Workspace {
            tool_name: "Patch".into(),
            mount: "persistent".into(),
            path_pattern: "src/lib.rs".into(),
        };
        let evaluation = evaluate_tool_authorization(
            request(&matcher),
            ToolConfirmationMode::Policy,
            WorkspaceApprovalMode::FollowAgent,
            [
                rule(&3, ToolRuleEffect::Allow, &matcher),
                rule(&2, ToolRuleEffect::Deny, &matcher),
                rule(&1, ToolRuleEffect::Ask, &matcher),
            ],
        );
        assert_eq!(evaluation.disposition, ToolAuthorizationDisposition::Deny);
        assert_eq!(evaluation.matched_rule_ids, vec![2]);
    }

    #[test]
    fn required_confirmation_cannot_be_bypassed_by_allow() {
        let matcher = ToolRuleMatcher::BackendTool {
            tool_name: "external".into(),
        };
        let evaluation = evaluate_tool_authorization(
            request(&matcher),
            ToolConfirmationMode::Required,
            WorkspaceApprovalMode::AutoApprove,
            [rule(&1, ToolRuleEffect::Allow, &matcher)],
        );
        assert_eq!(
            evaluation.disposition,
            ToolAuthorizationDisposition::RequireApproval
        );
    }

    #[test]
    fn workspace_directory_pattern_stays_within_the_directory() {
        let rule = ToolRuleMatcher::Workspace {
            tool_name: "Read".into(),
            mount: "persistent".into(),
            path_pattern: "src/**".into(),
        };
        let nested = ToolRuleMatcher::Workspace {
            tool_name: "Read".into(),
            mount: "persistent".into(),
            path_pattern: "src/domain/mod.rs".into(),
        };
        let sibling_prefix = ToolRuleMatcher::Workspace {
            tool_name: "Read".into(),
            mount: "persistent".into(),
            path_pattern: "src-old/lib.rs".into(),
        };
        assert!(matcher_matches(&rule, &nested));
        assert!(!matcher_matches(&rule, &sibling_prefix));
    }

    #[test]
    fn shell_cwd_mount_is_exact_and_argv_is_prefix() {
        let rule = ToolRuleMatcher::ShellArgvPrefix {
            argv_prefix: vec!["cargo".into(), "test".into()],
            cwd_mount: Some("persistent".into()),
            sandbox_profile: "seatbelt".into(),
            network: "disabled".into(),
            mount_modes: vec!["persistent:read_write".into()],
        };
        let request = ToolRuleMatcher::ShellArgvPrefix {
            argv_prefix: vec!["cargo".into(), "test".into(), "-p".into(), "core".into()],
            cwd_mount: Some("persistent".into()),
            sandbox_profile: "seatbelt".into(),
            network: "disabled".into(),
            mount_modes: vec!["persistent:read_write".into()],
        };
        let other_mount = ToolRuleMatcher::ShellArgvPrefix {
            argv_prefix: vec!["cargo".into(), "test".into(), "-p".into(), "core".into()],
            cwd_mount: Some("temporary".into()),
            sandbox_profile: "seatbelt".into(),
            network: "disabled".into(),
            mount_modes: vec!["persistent:read_write".into()],
        };
        assert!(matcher_matches(&rule, &request));
        assert!(!matcher_matches(&rule, &other_mount));
    }

    #[test]
    fn challenge_rejects_unoffered_remembered_choice() {
        let challenge = AuthorizationChallenge {
            request_id: "call-1".into(),
            action: "Patch".into(),
            choices: vec![AuthorizationChoice::AllowOnce, AuthorizationChoice::Reject],
            details: None,
        };
        assert!(!challenge.accepts(AuthorizationChoice::AllowRemembered));
    }
}
