use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    RequestContext, RuntimeResult, SessionId, SessionSnapshot, WorkflowContext, WorkflowId,
};

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default = "default_page_limit")]
    pub limit: u32,
}

fn default_page_limit() -> u32 {
    50
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionUpdate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default = "default_page_limit")]
    pub limit: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowSummary {
    pub workflow_id: WorkflowId,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_id: Option<String>,
    #[serde(default)]
    pub extensions: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AssetUpload {
    pub file_name: String,
    pub media_type: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AssetRef {
    pub asset_id: String,
    pub url: String,
    pub media_type: String,
    #[serde(default)]
    pub extensions: Map<String, Value>,
}

#[async_trait]
pub trait SessionDirectory: Send + Sync {
    async fn list(
        &self,
        context: RequestContext,
        query: SessionQuery,
    ) -> RuntimeResult<Vec<SessionSnapshot>>;
    async fn get_or_create(
        &self,
        context: WorkflowContext,
        name: String,
        session_type: String,
    ) -> RuntimeResult<crate::SessionHandle>;
    async fn update(
        &self,
        context: RequestContext,
        session_id: SessionId,
        update: SessionUpdate,
    ) -> RuntimeResult<SessionSnapshot>;
    async fn delete(&self, context: RequestContext, session_id: SessionId) -> RuntimeResult<()>;
}

#[async_trait]
pub trait WorkflowDirectory: Send + Sync {
    async fn list(
        &self,
        context: RequestContext,
        query: WorkflowQuery,
    ) -> RuntimeResult<Vec<WorkflowSummary>>;
    async fn get(
        &self,
        context: RequestContext,
        workflow_id: WorkflowId,
    ) -> RuntimeResult<Option<WorkflowSummary>>;
}

#[async_trait]
pub trait AssetStore: Send + Sync {
    async fn upload(&self, context: RequestContext, upload: AssetUpload)
        -> RuntimeResult<AssetRef>;
    async fn download(&self, context: RequestContext, asset: AssetRef) -> RuntimeResult<Vec<u8>>;
}
