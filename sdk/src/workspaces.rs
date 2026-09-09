//! Remote Workspace identity and approval configuration.
use crate::{client::ClientInner, Error, Principal, RequestOptions};
pub use kish_lingshu_runtime_contract::{
    BackendPersistentWorkspaceBinding, ClientPersistentWorkspaceBinding, ClientRuntimeFileRoot,
    ClientRuntimeFilesystem, ClientWorkspaceMode, ClientWorkspaceMount, RuntimeBindings,
    WorkspaceApprovalMode, WorkspaceBindingMode, WorkspaceBindings,
};
use std::{marker::PhantomData, sync::Arc};

pub struct Workspaces<P: Principal> {
    inner: Arc<ClientInner>,
    principal: PhantomData<P>,
}
impl<P: Principal> Workspaces<P> {
    pub(crate) fn new(inner: Arc<ClientInner>) -> Self {
        Self {
            inner,
            principal: PhantomData,
        }
    }
    pub async fn allocate_id(&self, options: RequestOptions) -> Result<u64, Error> {
        self.inner.binding.allocate_workspace_id(options).await
    }
    pub async fn update_approval_mode(
        &self,
        application_id: &str,
        workspace_id: u64,
        approval_mode: WorkspaceApprovalMode,
        options: RequestOptions,
    ) -> Result<WorkspaceApprovalMode, Error> {
        self.inner
            .binding
            .update_workspace_approval_mode(application_id, workspace_id, approval_mode, options)
            .await
    }
}
