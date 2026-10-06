use std::{collections::BTreeSet, sync::Arc};

use tokio::task_local;

/// Authenticated caller identity established by a trusted product adapter.
///
/// This context is transport-neutral because workflow execution and connector
/// adapters must preserve the same identity across spawned async work.
#[derive(Clone, Debug)]
pub struct UserInfo {
    pub user_type: String,
    pub user_id: String,
    pub user_name: String,
    pub nick_name: String,
    pub real_name: String,
    pub app_id: String,
    pub token: Option<String>,
    pub auth_app: Option<String>,
    pub auth_brand: Option<String>,
    pub is_admin_mode: bool,
    pub is_admin: bool,
    /// Present only for server-validated, non-interactive Agent Studio identities.
    pub automation_scopes: Option<BTreeSet<String>>,
}

task_local! {
    pub static CURRENT_USER: Arc<UserInfo>;
}
