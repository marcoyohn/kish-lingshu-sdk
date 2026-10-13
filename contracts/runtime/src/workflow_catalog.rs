//! Application-scoped immutable published definition discovery.
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PublishedWorkflowDefinition {
    pub application_id: String,
    pub workflow_id: u64,
    /// Immutable publish revision identity, never a branch counter or SDK version.
    pub definition_id: u64,
    pub name: String,
    pub define_schema: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PublishedWorkflowPage {
    pub items: Vec<PublishedWorkflowDefinition>,
    pub next_workflow_id: Option<u64>,
}
