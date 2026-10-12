//! Run only after the business transaction has durably saved this intent.
//! Retry the same file/key after a lost response; never generate a replacement key.
use kish_lingshu_sdk::{
    workflow::{WorkflowId, WorkflowStart},
    ClientBuilder, ClientConfig, MutationOptions, UserCredential,
};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct SavedDraftIntent {
    workflow_id: u64,
    published_define_id: u64,
    business_ref: String,
    business_revision: String,
    start_idempotency_key: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("Pass the committed draft intent JSON file")?;
    let saved: SavedDraftIntent = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    let client = ClientBuilder::new(
        ClientConfig::new(std::env::var("LINGSHU_URL")?)
            .with_selected_application(std::env::var("LINGSHU_APP_ID")?),
    )
    .user_credential(UserCredential::new(std::env::var("LINGSHU_USER_TOKEN")?)?)
    .connect()?;
    let start = WorkflowStart::structured(json!({
        "business_ref": saved.business_ref,
        "business_revision": saved.business_revision,
    }))
    .published_definition(saved.published_define_id)?;
    let run = client
        .workflows()
        .start(
            WorkflowId(saved.workflow_id),
            start,
            MutationOptions::new(saved.start_idempotency_key)?,
        )
        .await?;
    // Persist this association in business storage. An acknowledgement is not approval completion.
    println!("{}", serde_json::to_string(run.handle())?);
    Ok(())
}
