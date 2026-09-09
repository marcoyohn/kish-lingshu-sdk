use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ClientInstanceId;

/// A transport-neutral client-owned tool definition attached to one root
/// Workflow invocation.
///
/// Only the discovered schema and logical provider identity cross a remote
/// runtime boundary. Connection commands, URLs, headers, environment
/// variables, and other provider credentials remain in the client host.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientToolBinding {
    /// Stable model-facing tool id. MCP adapters use
    /// `mcp__{server_name}__{tool_name}`.
    pub id: String,
    /// Provider-native tool name used for display.
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub input_schema: Value,
    pub provider: ClientToolProvider,
}

/// Logical identity of a client-owned tool provider.
///
/// This deliberately contains no transport configuration. It is sufficient
/// for Core to apply authored Agent policy and for the client host to route a
/// suspended call back to its Session-scoped provider.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClientToolProvider {
    Mcp {
        server_name: String,
        tool_name: String,
    },
}

impl ClientToolProvider {
    pub fn mcp_identity(&self) -> (&str, &str) {
        match self {
            Self::Mcp {
                server_name,
                tool_name,
            } => (server_name, tool_name),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientWorkspaceMount {
    Temporary,
    Persistent,
}

impl ClientWorkspaceMount {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Temporary => "temporary",
            Self::Persistent => "persistent",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ClientWorkspaceMode {
    ReadOnly,
    ReadWrite,
}

/// A client-local root already constrained by the trusted Workspace host.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientRuntimeFileRoot {
    pub name: ClientWorkspaceMount,
    pub path: String,
    pub mode: ClientWorkspaceMode,
    #[serde(default)]
    pub capabilities: BTreeSet<String>,
}

/// The filesystem view issued for one suspended local execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientRuntimeFilesystem {
    pub cwd: String,
    pub roots: Vec<ClientRuntimeFileRoot>,
}

/// Client-visible execution constraints paired with an opaque suspension.
///
/// Internal execution ids, authorization fingerprints, and resume phases stay
/// sealed in `SuspensionHandle`; the local executor only receives the bounded
/// information it needs to validate and execute the call.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClientToolExecutionContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_instance_id: Option<ClientInstanceId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_filesystem: Option<ClientRuntimeFilesystem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_root: Option<ClientRuntimeFileRoot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmation_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_provider: Option<String>,
}
