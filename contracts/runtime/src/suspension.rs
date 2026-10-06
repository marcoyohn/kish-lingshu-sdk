use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{RuntimeError, RuntimeResult, ToolCallId};

pub const SUSPENSION_HANDLE_VERSION: u16 = 1;

/// A versioned value whose payload is only interpreted by a runtime binding.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SuspensionHandle(String);

impl SuspensionHandle {
    pub fn issue(opaque_payload: impl AsRef<str>) -> RuntimeResult<Self> {
        let opaque_payload = opaque_payload.as_ref();
        if opaque_payload.is_empty() || opaque_payload.contains('.') {
            return Err(RuntimeError::invalid_request(
                "suspension payload must be non-empty and must not contain '.'",
            ));
        }
        Ok(Self(format!(
            "v{SUSPENSION_HANDLE_VERSION}.{opaque_payload}"
        )))
    }

    pub fn parse(encoded: impl Into<String>) -> RuntimeResult<Self> {
        let encoded = encoded.into();
        let handle = Self(encoded);
        handle.version()?;
        Ok(handle)
    }

    pub fn version(&self) -> RuntimeResult<u16> {
        let (version, payload) = self
            .0
            .split_once('.')
            .ok_or_else(|| RuntimeError::invalid_request("invalid suspension handle"))?;
        if payload.is_empty() {
            return Err(RuntimeError::invalid_request("invalid suspension handle"));
        }
        let version = version
            .strip_prefix('v')
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or_else(|| RuntimeError::invalid_request("invalid suspension handle version"))?;
        if version != SUSPENSION_HANDLE_VERSION {
            return Err(RuntimeError::new(
                crate::RuntimeErrorCode::Unsupported,
                format!("unsupported suspension handle version: {version}"),
            ));
        }
        Ok(version)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum SuspensionResponsePayload {
    ClientToolResult {
        tool_call_id: ToolCallId,
        status: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    PermissionDecision {
        request_id: String,
        decision: PermissionDecision,
    },
    UserAnswer {
        value: Value,
    },
    ChildCompletion {
        output: Value,
    },
    CompactionDecision {
        decision: CompactionDecision,
    },
    /// Compatibility payload owned by a trusted transport adapter. Runtime
    /// bindings must reject extension kinds they do not explicitly support.
    AdapterExtension {
        kind: String,
        payload: Value,
    },
}

/// Compatibility alias for transport adapters that still use the old name.
pub type SuspensionResponse = SuspensionResponsePayload;

/// Compatibility alias for the pre-signal runtime API.
pub type ResumePayload = SuspensionResponsePayload;

/// Compatibility name retained for existing HTTP and ACP resume payloads.
pub type PermissionDecision = crate::AuthorizationChoice;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionDecision {
    Compact,
    ContinueWithoutCompaction,
    Cancel,
}
